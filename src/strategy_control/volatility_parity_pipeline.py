"""Holdout-safe in-memory preparation for the frozen volatility-parity study.

This layer exposes the only path-opening seam.  It validates a supplied label
before calling the supplied opener and otherwise works with decoded minute bars.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import PurePath
from typing import Any, TypeVar

from strategy_control.volatility_parity import (
    SYMBOLS,
    TRIAL_ORDER,
    MinuteBar,
    Session,
    VolatilityParityError,
    aggregate_sessions,
    canonical_vector,
    paired_returns,
)

T = TypeVar("T")
DEVELOPMENT_START = datetime(2025, 1, 1, tzinfo=UTC)
DEVELOPMENT_END = datetime(2026, 1, 1, tzinfo=UTC)
DEVELOPMENT_FOLDS = (
    (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 4, 1, tzinfo=UTC)),
    (datetime(2025, 4, 1, tzinfo=UTC), datetime(2025, 7, 1, tzinfo=UTC)),
    (datetime(2025, 7, 1, tzinfo=UTC), datetime(2025, 10, 1, tzinfo=UTC)),
    (datetime(2025, 10, 1, tzinfo=UTC), DEVELOPMENT_END),
)
PARTITION_RE = re.compile(
    r"^canonical/venue=binance/symbol=(BTCUSDT|ETHUSDT)/"
    r"year=(2024|2025|2026)/month=(\d{2})/observations\.parquet$"
)
WRAPPER_SHA256 = "96776c370c7ba2e97a1df57571e7fa8c424b94035b8a3c12e78fb66b0fa34772"
EFFECTIVE_SHA256 = "b20690b0ebd54968feaab050a1498cafe761c19f2a8f328405a9f3e49bb2e2e6"
EFFECTIVE_BYTE_SHA256 = "42b7fde83fa728143c4e4ba14f361ac87c6598006e177719e76b274208c774cb"
DATA_CONTRACT_SHA256 = "d2a02bca439359ca93bcb503bc5888fe4d6297b6f2115ac17c09d8da78f89183"

REQUIRED_SYNTHETIC_TESTS = (
    "raw_inverse_volatility_ERC_before_clipping",
    "clipped_weights_not_asserted_ERC",
    "perfect_anticorrelation_zero_variance_fails",
    "covariance_symmetry_eigenvalue_and_scale_tolerances",
    "trial_specific_lookback_matrix",
    "scheduled_numerical_failure_terminal",
    "information_time_includes_event_and_availability",
    "target_hash_materialized_at_information_time",
    "expected_whole_minute_open_strictly_after_information_time",
    "missing_exact_base_vector_no_later_substitute",
    "additional_delay_exact_next_session_and_no_supersession",
    "same_timestamp_event_order_and_pending_cancellation",
    "exact_2359_daily_endpoint_no_substitute",
    "intraday_liquidation_not_extra_daily_observation",
    "exposed_unpriceable_endpoint_fails",
    "cash_unavailable_span_absent_not_zero",
    "seven_trial_common_panel_minima",
    "ex_ante_terminal_T_E_exact_and_no_earlier_fallback",
    "reported_turnover_vs_costed_risky_fraction",
    "cash_entry_exit_and_risky_rotation_costs",
    "asset_contribution_currency_reconciliation",
    "trial_inheritance_and_equal_weight_clock",
    "buy_and_hold_entry_quarantine_reentry_and_terminal_fairness",
    "stationary_bootstrap_restart_and_big_endian_seeds",
    "DSR_35_attempt_registry_no_calendar_imputation",
    "DSR_Bartlett_T_eff_and_degeneracies",
    "PBO_common_matrix_ties_infinities_and_degeneracies",
    "regime_prior_only_median_gap_reset_assignment_and_rebalance_minimum",
    "event_level_drawdown_path",
    "exceptional_profit_currency_denominator",
    "development_rebalance_minima",
    "development_loader_rejects_2026_before_resolution",
    "holdout_authorization_and_irreversible_fsynced_latch",
    "holdout_explicit_gate_map",
    "prospective_Sunday_key_deduplication_and_postfreeze_warmup",
)


@dataclass(frozen=True)
class Partition:
    relative_path: str
    symbol: str
    month: str


@dataclass(frozen=True)
class InMemoryMarket:
    bars: Mapping[str, Sequence[MinuteBar]]
    holdout_values_read: bool = False


@dataclass(frozen=True)
class PreparedMarket:
    sessions: Mapping[str, tuple[Session, ...]]
    returns: tuple[tuple[float, float] | None, ...]
    holdout_values_read: bool = False


def _canonical_hash(payload: Mapping[str, Any], omitted_field: str | None = None) -> str:
    copied = dict(payload)
    if omitted_field is not None:
        copied.pop(omitted_field, None)
    return hashlib.sha256(
        json.dumps(copied, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def verify_frozen_contract(
    wrapper: Mapping[str, Any], effective: Mapping[str, Any], effective_bytes: bytes
) -> None:
    if wrapper.get("status") != "FROZEN":
        raise VolatilityParityError("preregistration wrapper is not frozen")
    if _canonical_hash(wrapper, "preregistration_sha256") != WRAPPER_SHA256:
        raise VolatilityParityError("wrapper canonical hash mismatch")
    if wrapper.get("preregistration_sha256") != WRAPPER_SHA256:
        raise VolatilityParityError("wrapper recorded hash mismatch")
    if _canonical_hash(effective, "draft_sha256") != EFFECTIVE_SHA256:
        raise VolatilityParityError("effective contract canonical hash mismatch")
    if hashlib.sha256(effective_bytes).hexdigest() != EFFECTIVE_BYTE_SHA256:
        raise VolatilityParityError("effective contract byte hash mismatch")
    reference = wrapper.get("effective_contract")
    if not isinstance(reference, Mapping) or (
        reference.get("canonical_sha256") != EFFECTIVE_SHA256
        or reference.get("byte_sha256") != EFFECTIVE_BYTE_SHA256
    ):
        raise VolatilityParityError("wrapper effective-contract reference mismatch")
    validate_contract(effective)


def validate_contract(contract: Mapping[str, Any]) -> None:
    if contract.get("experiment_id") != "btc-eth-causal-volatility-parity-rebalancing-v1":
        raise VolatilityParityError("wrong experiment contract")
    if (
        contract.get("effective_contract_complete") is not True
        or contract.get("capital_permitted") != 0
        or contract.get("holdout_opened") is not False
        or contract.get("holdout_values_read") is not False
        or contract.get("returns_calculated") is not False
    ):
        raise VolatilityParityError("contract boundary changed")
    trial_registry = contract.get("trial_registry")
    if (
        not isinstance(trial_registry, Mapping)
        or tuple(trial_registry.get("declared_trial_order", ())) != TRIAL_ORDER
    ):
        raise VolatilityParityError("trial registry mismatch")
    if tuple(contract.get("required_synthetic_tests", ())) != REQUIRED_SYNTHETIC_TESTS:
        raise VolatilityParityError("synthetic-test registry mismatch")
    statistical = contract.get("statistical_contract")
    if not isinstance(statistical, Mapping):
        raise VolatilityParityError("statistical contract missing")
    dsr = statistical.get("DSR")
    if not isinstance(dsr, Mapping) or (
        dsr.get("ordered_attempt_count_N") != 35
        or len(dsr.get("prior_completed_registry", ())) != 21
        or len(dsr.get("calendar_no_return_registry", ())) != 7
    ):
        raise VolatilityParityError("DSR registry mismatch")
    pbo = statistical.get("PBO")
    if not isinstance(pbo, Mapping) or pbo.get("splits") != "All 70 four-block training choices.":
        raise VolatilityParityError("PBO contract mismatch")
    holdout = contract.get("holdout_one_shot_contract")
    if not isinstance(holdout, Mapping) or holdout.get("invocations") != 1:
        raise VolatilityParityError("one-shot holdout contract mismatch")
    gates = holdout.get("explicit_holdout_gates")
    if not isinstance(gates, Mapping) or len(gates) != 24:
        raise VolatilityParityError("explicit holdout gate map mismatch")


def verify_data_contract(contract: Mapping[str, Any]) -> None:
    if _canonical_hash(contract) != DATA_CONTRACT_SHA256:
        raise VolatilityParityError("reused data-contract hash mismatch")
    if (
        contract.get("status") != "PASS"
        or contract.get("canonical_partition_count") != 48
        or contract.get("holdout_opened") is not False
        or contract.get("holdout_parquet_footers_or_values_read") is not False
    ):
        raise VolatilityParityError("reused data contract is not holdout-safe")


def development_partitions(contract: Mapping[str, Any]) -> tuple[Partition, ...]:
    verify_data_contract(contract)
    items = contract.get("partitions")
    if not isinstance(items, list) or len(items) != 48:
        raise VolatilityParityError("expected 48 frozen partitions")
    result: list[Partition] = []
    identities: set[tuple[str, int, int]] = set()
    for item in items:
        if not isinstance(item, Mapping):
            raise VolatilityParityError("malformed partition")
        relative = item.get("relative_path")
        if not isinstance(relative, str):
            raise VolatilityParityError("partition path missing")
        match = PARTITION_RE.fullmatch(relative)
        if match is None:
            raise VolatilityParityError("unexpected partition path")
        symbol, year_text, month_text = match.groups()
        identity = (symbol, int(year_text), int(month_text))
        if identity in identities:
            raise VolatilityParityError("duplicate partition identity")
        identities.add(identity)
        if item.get("symbol") != symbol or item.get("month") != f"{year_text}-{month_text}":
            raise VolatilityParityError("partition metadata mismatch")
        scope = item.get("verification_scope")
        if year_text == "2026":
            if scope != "BYTE_HASH_ONLY_NO_PARQUET_PARSE":
                raise VolatilityParityError("holdout partition scope mismatch")
            continue
        if scope != "HASH_AND_SCHEMA_METADATA_ONLY":
            raise VolatilityParityError("development partition scope mismatch")
        result.append(Partition(relative, symbol, f"{year_text}-{month_text}"))
    result.sort(key=lambda item: (item.symbol, item.month))
    if len(result) != 36 or any(
        sum(item.symbol == symbol for item in result) != 18 for symbol in SYMBOLS
    ):
        raise VolatilityParityError("expected 36 allowlisted development partitions")
    return tuple(result)


def reject_holdout_path(path: str | PurePath) -> None:
    """Reject a final-holdout label before resolution, footer parsing, or opening."""

    text = str(path).replace("\\", "/")
    if re.search(r"(?:^|/)(?:year=)?2026(?:/|$)", text):
        raise VolatilityParityError("development loader refused final-holdout path")


def guarded_open(path: str | PurePath, opener: Callable[[str | PurePath], T]) -> T:
    reject_holdout_path(path)
    return opener(path)


def prepare_market(market: InMemoryMarket) -> PreparedMarket:
    if market.holdout_values_read:
        raise VolatilityParityError("holdout values reached development preparation")
    if set(market.bars) != set(SYMBOLS):
        raise VolatilityParityError("exact fixed BTC/ETH universe required")
    sessions = {symbol: aggregate_sessions(market.bars[symbol]) for symbol in SYMBOLS}
    btc = sessions[SYMBOLS[0]]
    eth = sessions[SYMBOLS[1]]
    if not btc or not eth or len(btc) != len(eth):
        raise VolatilityParityError("joint sessions are not synchronized")
    if [session.start for session in btc] != [session.start for session in eth]:
        raise VolatilityParityError("joint session identities differ")
    return PreparedMarket(sessions, paired_returns(btc, eth), False)


def exact_daily_endpoint(
    btc: Mapping[datetime, MinuteBar],
    eth: Mapping[datetime, MinuteBar],
    day: datetime,
) -> Mapping[str, float]:
    if day.tzinfo is None or day.utcoffset() != timedelta(0):
        raise VolatilityParityError("daily endpoint date must be UTC")
    endpoint = day.astimezone(UTC).replace(hour=23, minute=59, second=0, microsecond=0)
    return canonical_vector(btc, eth, endpoint)


def common_panel(
    panels: Mapping[str, Mapping[datetime, float]], *, minimum_days: int
) -> Mapping[datetime, tuple[float, ...]]:
    if tuple(panels) != TRIAL_ORDER:
        raise VolatilityParityError("seven trial panels are not in frozen order")
    common = set.intersection(*(set(panel) for panel in panels.values()))
    if len(common) < minimum_days:
        raise VolatilityParityError("common endpoint panel is undersized")
    result = {
        timestamp: tuple(panels[name][timestamp] for name in TRIAL_ORDER)
        for timestamp in sorted(common)
    }
    if any(
        timestamp.tzinfo is None
        or timestamp.utcoffset() != timedelta(0)
        or (timestamp.hour, timestamp.minute, timestamp.second, timestamp.microsecond)
        != (23, 59, 0, 0)
        or any(not math.isfinite(value) or value <= 0 for value in values)
        for timestamp, values in result.items()
    ):
        raise VolatilityParityError("common panel contains an invalid exact endpoint")
    return result


def daily_returns_from_wealth(
    wealth: Mapping[datetime, float | None], *, exposed: Mapping[datetime, bool]
) -> Mapping[datetime, float]:
    """One return per endpoint interval; absent cash spans stay absent."""

    output: dict[datetime, float] = {}
    prior_timestamp: datetime | None = None
    prior_wealth: float | None = None
    for timestamp in sorted(wealth):
        if (
            timestamp.tzinfo is None
            or timestamp.utcoffset() != timedelta(0)
            or (timestamp.hour, timestamp.minute, timestamp.second, timestamp.microsecond)
            != (23, 59, 0, 0)
        ):
            raise VolatilityParityError("wealth panel timestamp is not exact 23:59 UTC")
        value = wealth[timestamp]
        if value is None:
            if exposed.get(timestamp, False):
                raise VolatilityParityError("exposed endpoint is unpriceable")
            prior_timestamp = None
            prior_wealth = None
            continue
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise VolatilityParityError("invalid endpoint wealth")
        if prior_timestamp is not None and prior_wealth is not None:
            output[timestamp] = float(value) / prior_wealth - 1
        prior_timestamp = timestamp
        prior_wealth = float(value)
    return output


def terminal_timestamp(end: datetime) -> datetime:
    if end.tzinfo is None or end.utcoffset() != timedelta(0):
        raise VolatilityParityError("boundary must be UTC")
    return end.astimezone(UTC) - timedelta(minutes=1)


def scheduled_sunday(session_start: datetime, *, biweekly: bool = False) -> bool:
    if session_start.tzinfo is None or session_start.utcoffset() != timedelta(0):
        raise VolatilityParityError("session start must be UTC")
    session_start = session_start.astimezone(UTC)
    if session_start.weekday() != 6:
        return False
    if not biweekly:
        return True
    anchor = datetime(2024, 7, 7, tzinfo=UTC)
    week_index = (session_start - anchor).days // 7
    return week_index >= 0 and week_index % 2 == 0


def fair_benchmark_entry(*, primary_eligible: bool, actual_cash: bool, quarantined: bool) -> bool:
    """Buy-and-hold entries and re-entries inherit the primary eligibility clock."""

    return primary_eligible and actual_cash and not quarantined


def prospective_warmup_complete(
    session_starts: Sequence[datetime], freeze_timestamp: datetime, *, lookback: int = 60
) -> bool:
    """Require a wholly post-freeze contiguous 60-session warmup."""

    if lookback != 60 or len(session_starts) < lookback:
        return False
    if freeze_timestamp.tzinfo is None or freeze_timestamp.utcoffset() != timedelta(0):
        raise VolatilityParityError("freeze timestamp must be UTC")
    selected = session_starts[-lookback:]
    if any(
        value.tzinfo is None or value.utcoffset() != timedelta(0) or value <= freeze_timestamp
        for value in selected
    ):
        return False
    return all(
        selected[index] == selected[index - 1] + timedelta(days=1) for index in range(1, lookback)
    )


def holdout_gate_map(contract: Mapping[str, Any]) -> Mapping[str, Any]:
    holdout = contract.get("holdout_one_shot_contract")
    if not isinstance(holdout, Mapping):
        raise VolatilityParityError("holdout contract missing")
    gates = holdout.get("explicit_holdout_gates")
    if not isinstance(gates, Mapping) or len(gates) != 24:
        raise VolatilityParityError("holdout gate map changed")
    return dict(gates)
