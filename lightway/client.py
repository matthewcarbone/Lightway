from warnings import warn

from tiled.client.dataframe import DataFrameClient

from lightway.postprocessing.operators import XASDataQuality


class OperatorHelperMixin:
    """A mixin class that defines how operators can act on the classes."""


class XASDatasetClient(DataFrameClient):
    """A custom DataFrame client for DataFrames that have XDI metadata."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Optional sanity check to ensure this cannot be accidentally
        # registered for use with data that is not a dataframe.
        assert self.item["attributes"]["structure_family"] == "dataframe"

    def __repr__(self):
        md = self.metadata
        element = md["sample_metadata"]["element"]
        edge = md["sample_metadata"]["edge"]
        return f'<{self.item["id"]} {element} {edge}>'

    def check_quality_(self, **kwargs):
        """Performs a quality assurance/quality control check on the data."""

        op = XASDataQuality(**kwargs)
        _, new_metadata = op(self)
        self.update_metadata(new_metadata)


def from_uri(*args, **kwargs):
    """Manually registers all clients. Lightweight wrapper for
    `tiled.client:from_uri`. Note that this is not a substitute for registering
    the clients via Python entrypoints, as that adds additional functionality
    that this function cannot. Hence, this is mostly useful for developemnt or
    for when you know exactly what you're doing."""

    from tiled.client.node import DEFAULT_STRUCTURE_CLIENT_DISPATCH
    from tiled.client import from_uri as _from_uri

    custom = dict(DEFAULT_STRUCTURE_CLIENT_DISPATCH["numpy"])
    custom["ExperimentalXAS"] = XASDatasetClient

    if "structure_clients" in kwargs.keys():
        warn(
            "structure_clients key found in kwargs. This will attempt to be "
            "merged with the existing clients"
        )
        custom = {**kwargs["structure_clients"], **custom}

    return _from_uri(*args, **kwargs, structure_clients=custom)
