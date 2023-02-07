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
