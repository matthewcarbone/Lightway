import importlib
import json
from functools import lru_cache


@lru_cache(maxsize=1)
def get_element_and_edges_list():
    """Returns a dictionary containing allowed values for elements and XAS
    edges. Caches its result so it only has to read from disk once.

    Returns
    -------
    dict
    """

    fname = (
        importlib.resources.files("lightway")
        / "_data"
        / "elements_and_edges.json"
    )
    with open(fname, "r") as f:
        data = json.load(f)
    return data


def check_versions():

    import tiled

    v = tiled.__version__
    should_be = "0.1.0a81"
    if v != should_be:
        print(
            f"tiled should be of versions {should_be}, was found to be {v}, "
            "some things may not work"
        )
    del tiled

    import databroker

    v = databroker.__version__
    should_be = "2.0.0b12"
    if v != should_be:
        print(
            f"databroker should be of versions {should_be}, was found to be, "
            f"{v}, some things may not work"
        )
    del databroker
