import logging

from .data import get_history
from .dbm import Manager


__all__ = ("download_data", "get_data")


logger = logging.getLogger(__name__)


def download_data(update_only: bool = True):
    """Download available data and insert it into the configured database.

    ...

    Parameters
    ----------
    update_only : bool
    """
    pass


def get_data(
    funds: list = Optional[None],
    start_dt: Optional[Union[str, datetime]] = None,
    end_dt: Optional[Union[str, datetime]] = None,
):
    """Easily query the configured database.

    ...

    Parameters
    ----------

    """
    pass


if __name__ == "__main__":
    from . import settings

    dbm = Manager(**settings.MONGODB)
