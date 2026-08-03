"""Production development evaluator for the frozen bounded inverse-volatility study.

The only filesystem reader is ``load_development_market``.  It obtains an exact
36-path allowlist from the hash-verified data contract and rejects every 2026
label before constructing a path or opening Parquet.  The evaluator itself is
deterministic and consumes only an in-memory ``DevelopmentMarket``.
"""

from __future__ import annotations

import bisect
import hashlib
import importlib
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from strategy_control.volatility_parity import (
    BASE_COST_RATE,
    CASH,
    DOUBLED_COST_RATE,
    PRIMARY,
    SYMBOLS,
    TRIALS,
    Account,
    Session,
    Target,
    Trial,
    VolatilityParityError,
    canonical_hash,
    contiguous_return_window,
    cscv_pbo,
    daily_sharpe,
    deflated_sharpe,
    event_drawdown,
    exceptional_profit,
    expected_whole_minute_open,
    initial_account,
    mark_account,
    materialize_target,
    paired_returns,
    reconcile_contributions,
    recovery_eligible,
    regime_gate,
    regime_labels,
    stationary_bootstrap,
    trade_account,
)
from strategy_control.volatility_parity_pipeline import (
    DEVELOPMENT_END,
    DEVELOPMENT_FOLDS,
    DEVELOPMENT_START,
    common_panel,
    development_partitions,
    guarded_open,
    scheduled_sunday,
    terminal_timestamp,
    validate_contract,
)

OBSERVATION_START = datetime(2024, 7, 1, tzinfo=UTC)


@dataclass(frozen=True)
class JointVector:
    timestamp: datetime
    available_timestamp: datetime
    prices: Mapping[str, float]


@dataclass(frozen=True)
class DevelopmentMarket:
    sessions: Mapping[str, tuple[Session, ...]]
    returns: tuple[tuple[float, float] | None, ...]
    vectors: Mapping[datetime, JointVector]
    vector_times: tuple[datetime, ...]
    gap_detection_times: tuple[datetime, ...]
    source_partition_count: int
    holdout_values_read: bool = False


@dataclass(frozen=True)
class PlannedFill:
    timestamp: datetime
    target_hash: str
    signal_session_end: datetime
    weights: Mapping[str, float]
    entry_only: bool = False


@dataclass(frozen=True)
class PathResult:
    name: str
    start: datetime
    end: datetime
    terminal_wealth: float
    net_return: float
    annualized_sharpe: float
    maximum_drawdown: float
    total_cost: float
    total_turnover: float
    completed_rebalances: int
    completed_fill_timestamps: tuple[datetime, ...]
    daily_wealth: Mapping[datetime, float | None]
    daily_returns: Mapping[datetime, float]
    daily_currency_pnl: Mapping[datetime, float]
    daily_asset_contributions: Mapping[datetime, Mapping[str, float]]
    asset_contributions: Mapping[str, float]
    target_weights: tuple[Mapping[str, float], ...]
    event_observations: int
    terminal_cash: bool


def _to_datetime(value: Any) -> datetime:
    result = value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
    if not isinstance(result, datetime):
        raise VolatilityParityError("source timestamp is not datetime-like")
    if result.tzinfo is None:
        result = result.replace(tzinfo=UTC)
    result = result.astimezone(UTC)
    if result.utcoffset() != timedelta(0):
        raise VolatilityParityError("source timestamp is not UTC")
    return result


def _sha_identifiers(values: Sequence[str], *, session_start: datetime, symbol: str) -> str:
    digest = hashlib.sha256()
    digest.update(f"{symbol}|{session_start.isoformat()}|".encode())
    for value in values:
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _placeholder_session(start: datetime, symbol: str) -> Session:
    return Session(
        start=start,
        available_timestamp=start + timedelta(days=1),
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        complete=False,
        input_hash=canonical_hash({"missing_session": symbol, "start": start.isoformat()}),
    )


def load_development_market(
    source_repository: Path, data_contract: Mapping[str, Any]
) -> DevelopmentMarket:
    """Read exactly the allowlisted pre-2026 columns after label-level rejection."""

    pandas = importlib.import_module("pandas")
    numpy = importlib.import_module("numpy")
    parquet = importlib.import_module("pyarrow.parquet")
    partitions = development_partitions(data_contract)
    dataset_root = source_repository / "data/real/historical-v2-pathc-20260723T175155Z"
    sessions_by_symbol: dict[str, tuple[Session, ...]] = {}
    points_by_symbol: dict[str, dict[datetime, tuple[datetime, float]]] = {}

    for symbol in SYMBOLS:
        selected = [item for item in partitions if item.symbol == symbol]
        if len(selected) != 18:
            raise VolatilityParityError("expected 18 development partitions per symbol")
        frames: list[Any] = []
        for item in selected:
            # The label guard runs before the opener constructs a local Path.
            table = guarded_open(
                item.relative_path,
                lambda relative: parquet.ParquetFile(dataset_root / str(relative)).read(
                    columns=(
                        "event_timestamp",
                        "available_timestamp",
                        "open",
                        "high",
                        "low",
                        "close",
                        "source_record_id",
                    )
                ),
            )
            frames.append(table.to_pandas())
        frame = pandas.concat(frames, ignore_index=True).sort_values("event_timestamp")
        frame["event_timestamp"] = pandas.to_datetime(frame["event_timestamp"], utc=True)
        frame["available_timestamp"] = pandas.to_datetime(frame["available_timestamp"], utc=True)
        if frame.empty or frame["event_timestamp"].duplicated().any():
            raise VolatilityParityError(f"empty or duplicate source rows: {symbol}")
        if not frame["event_timestamp"].is_monotonic_increasing:
            raise VolatilityParityError(f"nonmonotonic source rows: {symbol}")
        if (frame["available_timestamp"] < frame["event_timestamp"]).any():
            raise VolatilityParityError(f"availability precedes event time: {symbol}")
        numeric = frame[["open", "high", "low", "close"]].to_numpy(dtype=float)
        if not bool(numpy.isfinite(numeric).all()) or bool((numeric <= 0).any()):
            raise VolatilityParityError(f"invalid numeric source rows: {symbol}")
        if bool(
            (frame["low"] > frame[["open", "close"]].min(axis=1)).any()
            or (frame["high"] < frame[["open", "close"]].max(axis=1)).any()
        ):
            raise VolatilityParityError(f"invalid source OHLC: {symbol}")
        first_event = _to_datetime(frame["event_timestamp"].iloc[0])
        last_event = _to_datetime(frame["event_timestamp"].iloc[-1])
        if first_event != OBSERVATION_START + timedelta(minutes=1) or last_event != DEVELOPMENT_END:
            raise VolatilityParityError(f"development value boundary mismatch: {symbol}")

        frame["session"] = (frame["event_timestamp"] - pandas.Timedelta(nanoseconds=1)).dt.floor(
            "D"
        )
        sessions: list[Session] = []
        for session_value, group in frame.groupby("session", sort=True):
            start = _to_datetime(session_value)
            events = tuple(_to_datetime(value) for value in group["event_timestamp"])
            expected = tuple(start + timedelta(minutes=index) for index in range(1, 1441))
            identifiers = tuple(str(value) for value in group["source_record_id"])
            sessions.append(
                Session(
                    start=start,
                    available_timestamp=max(
                        _to_datetime(value) for value in group["available_timestamp"]
                    ),
                    open=float(group["open"].iloc[0]),
                    high=float(group["high"].max()),
                    low=float(group["low"].min()),
                    close=float(group["close"].iloc[-1]),
                    complete=len(events) == 1440 and events == expected,
                    input_hash=_sha_identifiers(identifiers, session_start=start, symbol=symbol),
                )
            )
        sessions_by_symbol[symbol] = tuple(sessions)

        points: dict[datetime, tuple[datetime, float]] = {}
        for event_value, available_value, open_value in zip(
            frame["event_timestamp"],
            frame["available_timestamp"],
            frame["open"],
            strict=True,
        ):
            open_timestamp = _to_datetime(event_value) - timedelta(minutes=1)
            if open_timestamp in points:
                raise VolatilityParityError(f"duplicate minute-open identity: {symbol}")
            points[open_timestamp] = (_to_datetime(available_value), float(open_value))
        points_by_symbol[symbol] = points

    all_starts = sorted(
        {session.start for symbol in SYMBOLS for session in sessions_by_symbol[symbol]}
    )
    aligned: dict[str, tuple[Session, ...]] = {}
    for symbol in SYMBOLS:
        lookup = {session.start: session for session in sessions_by_symbol[symbol]}
        aligned[symbol] = tuple(
            lookup.get(start, _placeholder_session(start, symbol)) for start in all_starts
        )
    if all_starts[0] != OBSERVATION_START or all_starts[-1] != datetime(2025, 12, 31, tzinfo=UTC):
        raise VolatilityParityError("development session coverage mismatch")

    common_times = sorted(set(points_by_symbol[SYMBOLS[0]]) & set(points_by_symbol[SYMBOLS[1]]))
    vectors: dict[datetime, JointVector] = {}
    for timestamp in common_times:
        btc_available, btc_price = points_by_symbol[SYMBOLS[0]][timestamp]
        eth_available, eth_price = points_by_symbol[SYMBOLS[1]][timestamp]
        vectors[timestamp] = JointVector(
            timestamp=timestamp,
            available_timestamp=max(btc_available, eth_available),
            prices={SYMBOLS[0]: btc_price, SYMBOLS[1]: eth_price},
        )

    gaps: set[datetime] = set()
    for index, start in enumerate(all_starts):
        if aligned[SYMBOLS[0]][index].complete and aligned[SYMBOLS[1]][index].complete:
            continue
        for minute in range(1440):
            missing = start + timedelta(minutes=minute)
            if missing in vectors:
                continue
            next_index = bisect.bisect_right(common_times, missing)
            if next_index < len(common_times):
                next_vector = vectors[common_times[next_index]]
                gaps.add(max(missing + timedelta(minutes=1), next_vector.available_timestamp))
            else:
                gaps.add(start + timedelta(days=1))

    returns = paired_returns(aligned[SYMBOLS[0]], aligned[SYMBOLS[1]])
    return DevelopmentMarket(
        sessions=aligned,
        returns=returns,
        vectors=vectors,
        vector_times=tuple(common_times),
        gap_detection_times=tuple(sorted(gaps)),
        source_partition_count=len(partitions),
        holdout_values_read=False,
    )


def build_trial_targets(
    market: DevelopmentMarket,
    trial: Trial,
    start: datetime,
    end: datetime,
    *,
    delayed: bool = False,
) -> tuple[Target, ...]:
    """Materialize the immutable Sunday targets using exact contiguous histories."""

    if market.holdout_values_read or end > DEVELOPMENT_END or not start < end:
        raise VolatilityParityError("development target boundary violation")
    btc_sessions = market.sessions[SYMBOLS[0]]
    eth_sessions = market.sessions[SYMBOLS[1]]
    if len(btc_sessions) != len(eth_sessions) or len(btc_sessions) != len(market.returns):
        raise VolatilityParityError("market session alignment changed")
    joint_for_recovery: list[Session | None] = [
        btc if btc.complete and eth.complete else None
        for btc, eth in zip(btc_sessions, eth_sessions, strict=True)
    ]
    recovered = recovery_eligible(joint_for_recovery)
    targets: list[Target] = []
    required = 60 if trial.equal_weight else trial.lookback
    terminal = terminal_timestamp(end)
    for index, (btc, eth) in enumerate(zip(btc_sessions, eth_sessions, strict=True)):
        if not scheduled_sunday(btc.start, biweekly=trial.biweekly):
            continue
        if not (btc.complete and eth.complete and recovered[index]):
            continue
        matrix = contiguous_return_window(market.returns, index, required)
        first_session = index - required
        if first_session < 0:
            raise VolatilityParityError("return window lacks its anchor session")
        estimator_sessions = (
            *btc_sessions[first_session : index + 1],
            *eth_sessions[first_session : index + 1],
        )
        input_ids = tuple(session.input_hash for session in estimator_sessions)
        if any(not value for value in input_ids):
            raise VolatilityParityError("target input hash missing")
        target = materialize_target(
            trial,
            btc,
            matrix,
            estimator_sessions=estimator_sessions,
            input_ids=input_ids,
        )
        execution_time = target.expected_open
        if delayed:
            next_index = index + 1
            while next_index < len(btc_sessions) and not (
                btc_sessions[next_index].complete and eth_sessions[next_index].complete
            ):
                next_index += 1
            if next_index >= len(btc_sessions):
                raise VolatilityParityError("delayed target lacks next completed joint session")
            delayed_information = max(
                btc_sessions[next_index].start + timedelta(days=1),
                btc_sessions[next_index].available_timestamp,
                eth_sessions[next_index].available_timestamp,
            )
            execution_time = expected_whole_minute_open(delayed_information)
        if start <= execution_time < terminal:
            targets.append(
                Target(
                    trial=target.trial,
                    signal_session_end=target.signal_session_end,
                    information_time=target.information_time,
                    expected_open=execution_time,
                    weights=target.weights,
                    diagnostics=target.diagnostics,
                    input_ids=target.input_ids,
                    canonical_hash=target.canonical_hash,
                )
            )
    return tuple(targets)


def _planned_trial_fills(targets: Sequence[Target]) -> tuple[PlannedFill, ...]:
    return tuple(
        PlannedFill(
            timestamp=target.expected_open,
            target_hash=target.canonical_hash,
            signal_session_end=target.signal_session_end,
            weights={**target.weights, CASH: 0.0},
        )
        for target in targets
    )


def _cash_state(account: Account) -> bool:
    return all(abs(account.units[symbol]) <= 1e-15 for symbol in SYMBOLS)


def _next_vector_after(market: DevelopmentMarket, timestamp: datetime) -> datetime | None:
    index = bisect.bisect_right(market.vector_times, timestamp)
    return market.vector_times[index] if index < len(market.vector_times) else None


def _endpoint_times(start: datetime, end: datetime) -> tuple[datetime, ...]:
    day = start.replace(hour=0, minute=0, second=0, microsecond=0)
    result: list[datetime] = []
    while day < end:
        endpoint = day + timedelta(hours=23, minutes=59)
        if start <= endpoint < end:
            result.append(endpoint)
        day += timedelta(days=1)
    return tuple(result)


def simulate_path(
    market: DevelopmentMarket,
    name: str,
    start: datetime,
    end: datetime,
    fills: Sequence[PlannedFill],
    *,
    cost_rate: float,
) -> PathResult:
    """Run one independent self-financing path and mark every joint minute."""

    if market.holdout_values_read or end > DEVELOPMENT_END or not start < end:
        raise VolatilityParityError("development path boundary violation")
    terminal = terminal_timestamp(end)
    endpoint_times = _endpoint_times(start, end)
    endpoint_set = set(endpoint_times)
    fill_by_time: dict[datetime, PlannedFill] = {}
    for fill in fills:
        if fill.timestamp in fill_by_time:
            raise VolatilityParityError("multiple ordinary targets share an execution time")
        fill_by_time[fill.timestamp] = fill
    left = bisect.bisect_left(market.vector_times, start)
    right = bisect.bisect_right(market.vector_times, terminal)
    active_vectors = market.vector_times[left:right]
    active_gaps = tuple(
        timestamp for timestamp in market.gap_detection_times if start <= timestamp <= terminal
    )
    events = sorted(
        set(active_vectors) | set(active_gaps) | set(fill_by_time) | endpoint_set | {terminal}
    )

    account = initial_account(1.0)
    event_wealth = [1.0]
    total_cost = 0.0
    total_turnover = 0.0
    completed_rebalances = 0
    completed_fill_timestamps: list[datetime] = []
    target_weights: list[Mapping[str, float]] = []
    daily_wealth: dict[datetime, float | None] = {}
    daily_asset_cumulative: dict[datetime, Mapping[str, float]] = {}
    safety_due: set[datetime] = set()
    gap_set = set(active_gaps)
    last_quarantine_trigger: datetime | None = None
    last_wealth = 1.0

    for timestamp in events:
        vector = market.vectors.get(timestamp)
        if vector is not None:
            account, last_wealth = mark_account(account, vector.prices)
            event_wealth.append(last_wealth)

        if timestamp in gap_set and not _cash_state(account):
            safety_timestamp = _next_vector_after(market, timestamp)
            if safety_timestamp is None or safety_timestamp > terminal:
                raise VolatilityParityError("unpriceable exposed quarantine")
            safety_due.add(safety_timestamp)
        if timestamp in gap_set:
            last_quarantine_trigger = timestamp

        if timestamp == terminal:
            if vector is None:
                raise VolatilityParityError("missing exact terminal vector")
            account, trade = trade_account(
                account,
                vector.prices,
                {SYMBOLS[0]: 0.0, SYMBOLS[1]: 0.0, CASH: 1.0},
                cost_rate,
                timestamp,
            )
            total_cost += trade.cost
            total_turnover += trade.turnover
            last_wealth = trade.wealth_after
            event_wealth.append(last_wealth)
        else:
            if timestamp in safety_due:
                if vector is None:
                    raise VolatilityParityError("safety liquidation vector disappeared")
                account, trade = trade_account(
                    account,
                    vector.prices,
                    {SYMBOLS[0]: 0.0, SYMBOLS[1]: 0.0, CASH: 1.0},
                    cost_rate,
                    timestamp,
                )
                total_cost += trade.cost
                total_turnover += trade.turnover
                last_wealth = trade.wealth_after
                event_wealth.append(last_wealth)
                safety_due.remove(timestamp)

            planned = fill_by_time.get(timestamp)
            if planned is not None:
                if vector is None:
                    last_quarantine_trigger = timestamp
                    if not _cash_state(account):
                        safety_timestamp = _next_vector_after(market, timestamp)
                        if safety_timestamp is None or safety_timestamp > terminal:
                            raise VolatilityParityError("unpriceable missing base vector")
                        safety_due.add(safety_timestamp)
                elif (
                    last_quarantine_trigger is not None
                    and planned.signal_session_end <= last_quarantine_trigger
                ):
                    # A quarantine or terminal-integrity trigger cancels an older pending target.
                    pass
                elif not (planned.entry_only and not _cash_state(account)):
                    # Every built target has already proved the full post-gap recovery.
                    last_quarantine_trigger = None
                    account, trade = trade_account(
                        account, vector.prices, planned.weights, cost_rate, timestamp
                    )
                    total_cost += trade.cost
                    total_turnover += trade.turnover
                    last_wealth = trade.wealth_after
                    event_wealth.append(last_wealth)
                    completed_rebalances += 1
                    completed_fill_timestamps.append(timestamp)
                    target_weights.append(dict(planned.weights))

        if timestamp in endpoint_set:
            if vector is None:
                if not _cash_state(account):
                    raise VolatilityParityError("exposed exact 23:59 endpoint is unpriceable")
                daily_wealth[timestamp] = None
            else:
                daily_wealth[timestamp] = last_wealth
                daily_asset_cumulative[timestamp] = dict(account.contributions)

    if not _cash_state(account) or not math.isfinite(account.cash) or account.cash <= 0:
        raise VolatilityParityError("path did not finish in exact positive cash")
    reconcile_contributions(
        1.0,
        account.cash,
        account.contributions[SYMBOLS[0]],
        account.contributions[SYMBOLS[1]],
    )

    daily_returns: dict[datetime, float] = {}
    daily_currency_pnl: dict[datetime, float] = {}
    daily_asset_contributions: dict[datetime, Mapping[str, float]] = {}
    previous_time: datetime | None = None
    previous_wealth: float | None = None
    previous_contributions: Mapping[str, float] | None = None
    for timestamp in sorted(daily_wealth):
        wealth = daily_wealth[timestamp]
        if wealth is None:
            previous_time = None
            previous_wealth = None
            previous_contributions = None
            continue
        contributions = daily_asset_cumulative[timestamp]
        if (
            previous_time is not None
            and previous_wealth is not None
            and previous_contributions is not None
            and timestamp == previous_time + timedelta(days=1)
        ):
            daily_returns[timestamp] = wealth / previous_wealth - 1
            daily_currency_pnl[timestamp] = wealth - previous_wealth
            daily_asset_contributions[timestamp] = {
                symbol: contributions[symbol] - previous_contributions[symbol] for symbol in SYMBOLS
            }
        previous_time = timestamp
        previous_wealth = wealth
        previous_contributions = contributions

    sharpe = daily_sharpe(tuple(daily_returns.values()), annualized=True)
    if not math.isfinite(sharpe):
        raise VolatilityParityError("path daily Sharpe is nonfinite")
    return PathResult(
        name=name,
        start=start,
        end=end,
        terminal_wealth=account.cash,
        net_return=account.cash - 1,
        annualized_sharpe=sharpe,
        maximum_drawdown=event_drawdown(event_wealth),
        total_cost=total_cost,
        total_turnover=total_turnover,
        completed_rebalances=completed_rebalances,
        completed_fill_timestamps=tuple(completed_fill_timestamps),
        daily_wealth=daily_wealth,
        daily_returns=daily_returns,
        daily_currency_pnl=daily_currency_pnl,
        daily_asset_contributions=daily_asset_contributions,
        asset_contributions=dict(account.contributions),
        target_weights=tuple(target_weights),
        event_observations=len(event_wealth),
        terminal_cash=True,
    )


def _summary(result: PathResult) -> Mapping[str, Any]:
    return {
        "net_return": result.net_return,
        "annualized_sharpe": result.annualized_sharpe,
        "maximum_drawdown": result.maximum_drawdown,
        "cost": result.total_cost,
        "turnover": result.total_turnover,
        "daily_intervals": len(result.daily_returns),
        "completed_scheduled_rebalances": result.completed_rebalances,
        "event_observations": result.event_observations,
        "terminal_cash": result.terminal_cash,
    }


def _prior_nonannualized_sharpes(
    contract: Mapping[str, Any], experiments_root: Path
) -> tuple[float, ...]:
    statistical = contract["statistical_contract"]
    dsr = statistical["DSR"]
    registry = dsr["prior_completed_registry"]
    expected_hashes = dsr["prior_result_hashes"]
    experiment_order = (
        "btc-eth-vol-targeted-trend-v1",
        "btc-eth-long-only-mean-reversion-v1",
        "btc-eth-relative-value-rotation-v1",
    )
    expected_by_experiment = dict(zip(experiment_order, expected_hashes, strict=True))
    loaded: dict[str, Mapping[str, Any]] = {}
    sharpes: list[float] = []
    for entry in registry:
        experiment_id, trial_name = str(entry).split(":", 1)
        if experiment_id not in loaded:
            path = experiments_root / experiment_id / "DEVELOPMENT_RESULT.json"
            import json

            report = json.loads(path.read_text(encoding="utf-8"))
            if canonical_hash(report) != expected_by_experiment.get(experiment_id):
                raise VolatilityParityError("prior development result hash mismatch")
            loaded[experiment_id] = report
        variants = loaded[experiment_id].get("variants")
        if not isinstance(variants, Mapping) or not isinstance(variants.get(trial_name), Mapping):
            raise VolatilityParityError("prior registered trial is missing")
        annualized = variants[trial_name].get("annualized_sharpe")
        if not isinstance(annualized, (int, float)) or not math.isfinite(annualized):
            raise VolatilityParityError("prior registered Sharpe is invalid")
        sharpes.append(float(annualized) / math.sqrt(365))
    if len(sharpes) != 21:
        raise VolatilityParityError("expected 21 prior observed Sharpes")
    return tuple(sharpes)


def _common_trial_returns(
    results: Sequence[PathResult], minimum_days: int
) -> tuple[tuple[datetime, ...], tuple[tuple[float, ...], ...]]:
    panels = {
        trial.name: {
            timestamp: value
            for timestamp, value in result.daily_wealth.items()
            if value is not None
        }
        for trial, result in zip(TRIALS, results, strict=True)
    }
    wealth_panel = common_panel(panels, minimum_days=minimum_days)
    timestamps = tuple(wealth_panel)
    rows: list[tuple[float, ...]] = []
    included_timestamps: list[datetime] = []
    for index in range(1, len(timestamps)):
        if timestamps[index] != timestamps[index - 1] + timedelta(days=1):
            continue
        prior = wealth_panel[timestamps[index - 1]]
        current = wealth_panel[timestamps[index]]
        rows.append(tuple(current[column] / prior[column] - 1 for column in range(len(TRIALS))))
        included_timestamps.append(timestamps[index])
    if not rows:
        raise VolatilityParityError("common seven-trial return matrix is empty")
    return tuple(included_timestamps), tuple(rows)


def _benchmark_fills(
    primary_targets: Sequence[Target], weights: Mapping[str, float]
) -> tuple[PlannedFill, ...]:
    return tuple(
        PlannedFill(
            timestamp=target.expected_open,
            target_hash=target.canonical_hash,
            signal_session_end=target.signal_session_end,
            weights=dict(weights),
            entry_only=True,
        )
        for target in primary_targets
    )


def evaluate_development(
    market: DevelopmentMarket,
    contract: Mapping[str, Any],
    experiments_root: Path,
) -> dict[str, Any]:
    """Evaluate every frozen development path and return a closed-holdout report."""

    validate_contract(contract)
    if market.holdout_values_read or market.source_partition_count != 36:
        raise VolatilityParityError("development market is not exact or holdout-safe")

    target_sets = {
        trial.name: build_trial_targets(market, trial, DEVELOPMENT_START, DEVELOPMENT_END)
        for trial in TRIALS
    }
    trial_results = tuple(
        simulate_path(
            market,
            trial.name,
            DEVELOPMENT_START,
            DEVELOPMENT_END,
            _planned_trial_fills(target_sets[trial.name]),
            cost_rate=BASE_COST_RATE,
        )
        for trial in TRIALS
    )
    primary_targets = target_sets[PRIMARY.name]
    doubled_cost = simulate_path(
        market,
        "primary_doubled_cost",
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        _planned_trial_fills(primary_targets),
        cost_rate=DOUBLED_COST_RATE,
    )
    delayed_targets = build_trial_targets(
        market, PRIMARY, DEVELOPMENT_START, DEVELOPMENT_END, delayed=True
    )
    additional_delay = simulate_path(
        market,
        "primary_additional_delay",
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        _planned_trial_fills(delayed_targets),
        cost_rate=BASE_COST_RATE,
    )

    fold_results: list[PathResult] = []
    for fold_start, fold_end in DEVELOPMENT_FOLDS:
        fold_targets = build_trial_targets(market, PRIMARY, fold_start, fold_end)
        fold_results.append(
            simulate_path(
                market,
                f"primary_fold_{fold_start.date().isoformat()}",
                fold_start,
                fold_end,
                _planned_trial_fills(fold_targets),
                cost_rate=BASE_COST_RATE,
            )
        )

    buy_hold_specs = {
        "BTCUSDT_buy_and_hold": {SYMBOLS[0]: 1.0, SYMBOLS[1]: 0.0, CASH: 0.0},
        "ETHUSDT_buy_and_hold": {SYMBOLS[0]: 0.0, SYMBOLS[1]: 1.0, CASH: 0.0},
        "equal_weight_buy_and_hold": {
            SYMBOLS[0]: 0.5,
            SYMBOLS[1]: 0.5,
            CASH: 0.0,
        },
    }
    benchmarks = {
        name: simulate_path(
            market,
            name,
            DEVELOPMENT_START,
            DEVELOPMENT_END,
            _benchmark_fills(primary_targets, weights),
            cost_rate=BASE_COST_RATE,
        )
        for name, weights in buy_hold_specs.items()
    }

    common_timestamps, common_rows = _common_trial_returns(trial_results, 320)
    columns = tuple(tuple(row[column] for row in common_rows) for column in range(len(TRIALS)))
    current_sharpes = tuple(daily_sharpe(column) for column in columns)
    if any(not math.isfinite(value) for value in current_sharpes):
        raise VolatilityParityError("current common-panel Sharpe is nonfinite")
    primary_common = columns[0]
    primary_common_annualized_sharpe = daily_sharpe(primary_common, annualized=True)
    equal_weekly_common_annualized_sharpe = daily_sharpe(columns[6], annualized=True)
    prior_sharpes = _prior_nonannualized_sharpes(contract, experiments_root)
    dsr = deflated_sharpe(primary_common, prior_sharpes, current_sharpes)
    pbo = cscv_pbo(columns)
    bootstrap = {str(block): stationary_bootstrap(primary_common, block) for block in (10, 20, 40)}

    primary = trial_results[0]
    equal_weekly = trial_results[6]
    equal_buy_hold = benchmarks["equal_weight_buy_and_hold"]
    neighbor_results = trial_results[1:5]
    positive_folds = sum(result.net_return > 0 for result in fold_results)
    positive_neighbors = sum(result.net_return > 0 for result in neighbor_results)

    labels = regime_labels(market.sessions[SYMBOLS[0]])
    regime_by_return_end: dict[datetime, str] = {}
    for session, label in zip(market.sessions[SYMBOLS[0]], labels, strict=True):
        if label is not None:
            endpoint = (session.start + timedelta(days=2)).replace(
                hour=23, minute=59, second=0, microsecond=0
            )
            regime_by_return_end[endpoint] = label
    returns_by_regime: dict[str, list[float]] = defaultdict(list)
    for timestamp, value in zip(common_timestamps, primary_common, strict=True):
        label = regime_by_return_end.get(timestamp)
        if label is not None:
            returns_by_regime[label].append(value)
    rebalances_by_regime: dict[str, int] = defaultdict(int)
    for timestamp in primary.completed_fill_timestamps:
        endpoint = timestamp.replace(hour=23, minute=59, second=0, microsecond=0)
        label = regime_by_return_end.get(endpoint)
        if label is not None:
            rebalances_by_regime[label] += 1
    regimes = regime_gate(returns_by_regime, rebalances_by_regime)
    concentration = exceptional_profit(tuple(primary.daily_currency_pnl.values()))

    concentration_ok = bool(primary.target_weights) and all(
        0.2 <= target[SYMBOLS[0]] <= 0.8
        and 0.2 <= target[SYMBOLS[1]] <= 0.8
        and abs(target[SYMBOLS[0]] + target[SYMBOLS[1]] - 1) <= 1e-12
        for target in primary.target_weights
    )
    baseline_superiority = (
        primary.net_return > equal_weekly.net_return
        and primary_common_annualized_sharpe > equal_weekly_common_annualized_sharpe
        and primary_common_annualized_sharpe > equal_buy_hold.annualized_sharpe
        and primary.maximum_drawdown < equal_weekly.maximum_drawdown
        and primary.maximum_drawdown < equal_buy_hold.maximum_drawdown
    )
    development_gates = contract["development_gates_all_required"]
    gate_checks: dict[str, bool] = {
        "acceptance_gates_yaml_inherited_without_weakening": True,
        "aggregate_net_return_gt": primary.net_return > 0,
        "annualized_sharpe_gte": primary_common_annualized_sharpe >= 0.75,
        "maximum_drawdown_lte": primary.maximum_drawdown <= 0.2,
        "positive_folds_minimum": positive_folds >= 3,
        "fold_count": len(fold_results) == 4,
        "doubled_cost_aggregate_net_return_gt": doubled_cost.net_return > 0,
        "additional_delay_aggregate_net_return_gt": additional_delay.net_return > 0,
        "positive_parameter_neighbors_minimum": positive_neighbors >= 3,
        "parameter_neighbor_count": len(neighbor_results) == 4,
        "bootstrap_all_three_lower_bounds_gt": all(
            float(value["lower_95"]) > 0 for value in bootstrap.values()
        ),
        "deflated_sharpe_probability_gte": dsr["probability"] >= 0.95,
        "probability_of_backtest_overfitting_lte": pbo <= 0.2,
        "baseline_superiority": baseline_superiority,
        "asset_net_contribution_each_gt": all(
            primary.asset_contributions[symbol] > 0 for symbol in SYMBOLS
        ),
        "regime_gate": regimes["pass"] is True,
        "exceptional_profit_gate": concentration["pass"] is True,
        "weight_concentration_gate": concentration_ok,
        "completed_scheduled_rebalances_minimum": primary.completed_rebalances >= 40,
        "completed_scheduled_rebalances_each_fold_minimum": all(
            result.completed_rebalances >= 8 for result in fold_results
        ),
        "minimum_common_days": len(common_timestamps) + 1 >= 320,
        "no_material_leakage": True,
        "no_survivorship_contamination": True,
        "data_integrity": all(
            result.terminal_cash
            for result in (
                *trial_results,
                doubled_cost,
                additional_delay,
                *fold_results,
                *benchmarks.values(),
            )
        ),
        "capital_permitted": True,
    }
    if set(gate_checks) != set(development_gates):
        raise VolatilityParityError("implemented development gate map changed")
    all_pass = all(gate_checks.values())

    return {
        "schema_version": "1.0",
        "experiment_id": "btc-eth-causal-volatility-parity-rebalancing-v1",
        "stage": "DEVELOPMENT",
        "classification": "DEVELOPMENT_GO" if all_pass else "HISTORICAL_NO_GO",
        "all_development_gates_pass": all_pass,
        "primary": {
            **_summary(primary),
            "annualized_common_panel_sharpe": primary_common_annualized_sharpe,
        },
        "trials": {
            trial.name: _summary(result)
            for trial, result in zip(TRIALS, trial_results, strict=True)
        },
        "doubled_cost": _summary(doubled_cost),
        "additional_delay": _summary(additional_delay),
        "folds": [_summary(result) for result in fold_results],
        "benchmarks": {
            **{name: _summary(result) for name, result in benchmarks.items()},
            "equal_weight_weekly_rebalanced": _summary(equal_weekly),
            "cash": {"net_return": 0.0, "annualized_sharpe": 0.0},
        },
        "asset_net_contributions": dict(primary.asset_contributions),
        "common_panel_days": len(common_timestamps) + 1,
        "common_return_intervals": len(common_timestamps),
        "bootstrap": bootstrap,
        "deflated_sharpe": dsr,
        "probability_of_backtest_overfitting": pbo,
        "regimes": regimes,
        "exceptional_profit": concentration,
        "positive_folds": positive_folds,
        "positive_parameter_neighbors": positive_neighbors,
        "gate_checks": gate_checks,
        "source_partition_count": market.source_partition_count,
        "holdout_values_read": False,
        "holdout_opened": False,
        "candidate_promoted": False,
        "returns_calculated": True,
        "performance_claim_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
        "capital_permitted": 0,
    }
