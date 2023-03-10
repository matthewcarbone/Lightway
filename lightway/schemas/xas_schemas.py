import pydantic

from lightway.schemas._xas_schemas_helpers import (
    SampleMetadata,
    ExperimentMetadata,
    MeasurementEnum,
)


class ExperimentalXASMetadata(pydantic.BaseModel, extra=pydantic.Extra.allow):
    sample_metadata: SampleMetadata
    experiment_metadata: ExperimentMetadata
    measurement_type: MeasurementEnum = pydantic.Field("xas", const=True)
    dataset: str
