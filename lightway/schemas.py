from enum import Enum
import pydantic

from lightway.utils import get_element_and_edges_list

# Currently, only NSLSII is allowed, but we can add to this later
FACILITIES = {"NSLSII"}

# Currently, only ISS is allowed, but we can add to this later
BEAMLINES = {"ISS"}


class MeasurementEnum(str, Enum):
    xas = "xas"


class SampleMetadata(pydantic.BaseModel):
    element: str
    edge: str

    @pydantic.validator("element")
    def check_element(cls, s):
        elements = get_element_and_edges_list()["elements"]
        if s not in elements:
            raise ValueError(f"{s} not a valid element element")
        return s

    @pydantic.validator("edge")
    def check_edge(cls, e):
        edges = get_element_and_edges_list()["edges"]
        if e not in edges:
            raise ValueError(f"{e} not a valid edge")
        return e


class ExperimentMetadata(pydantic.BaseModel, extra=pydantic.Extra.allow):
    facility: str
    beamline: str
    sample_id: str

    @pydantic.validator("facility")
    def check_facility(cls, facility):
        if facility not in FACILITIES:
            raise ValueError(f"{facility} not a valid facility ({FACILITIES})")

    @pydantic.validator("beamline")
    def check_beamline(cls, beamline):
        if beamline not in BEAMLINES:
            raise ValueError(f"{beamline} not a valid beamline ({BEAMLINES})")


class ExperimentalXASMetadata(pydantic.BaseModel, extra=pydantic.Extra.allow):
    sample_metadata: SampleMetadata
    experiment_metadata: ExperimentMetadata
    measurement_type: MeasurementEnum = pydantic.Field("xas", const=True)
    dataset: str
