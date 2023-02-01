import banyan as bn

from ..communication.location_spec import LocationSpec
from . import df


def read_csv(p):
    # res = LocationSpec(p) where p: Blocked | Consolidated | Grouped
    return df.DataFrame(
        bn.record_task(
            "res",
            LocationSpec,
            [p, "csv"],
            ["Blocked", "Consolidated", "Grouped"],
        )
    )
