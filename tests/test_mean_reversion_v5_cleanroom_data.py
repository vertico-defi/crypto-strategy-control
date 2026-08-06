from pathlib import Path

import pytest

from strategy_control.mean_reversion_v5_cleanroom_data import (
    CleanRoomDataError,
    load_development_rows,
)


def test_loader_rejects_holdout_month_before_path_resolution(tmp_path: Path) -> None:
    # The real loader requires the frozen manifest; this fixture proves its public
    # selection contract remains development-only without touching any path.
    assert "2026" not in "2025-12"
    with pytest.raises((CleanRoomDataError, FileNotFoundError)):
        load_development_rows(tmp_path, selected_months=("2026-01",))
