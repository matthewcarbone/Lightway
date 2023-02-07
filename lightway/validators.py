import pydantic
from tiled.validation_registration import ValidationError

from lightway.schemas import ExperimentalXASMetadata


MINIMUM_XAS_COLUMNS = {"energy", "mu"}


def _validate_structure_family_(structure_family, errors):
    if structure_family != "dataframe":
        raise ValidationError(
            f"structure_family {structure_family} != dataframe"
        )
        errors.append(f"structure_family {structure_family} != dataframe")


def _validate_ExperimentalXASMetadata_(metadata, errors):
    try:
        ExperimentalXASMetadata.parse_obj(metadata)
    except pydantic.ValidationError as e:
        errors.append(str(e))


def _validate_minimum_XAS_column_names_subset_(columns, errors):
    if not MINIMUM_XAS_COLUMNS.issubset(columns):
        errors.append(f"columns {columns} must contain i0 and tey")


def validate_ExperimentalXASMetadata(
    metadata, structure_family, structure, spec, references
):
    errors = []
    _validate_structure_family_(structure_family, errors)
    _validate_ExperimentalXASMetadata_(metadata, errors)
    columns = set(structure.macro.columns)
    _validate_minimum_XAS_column_names_subset_(columns, errors)
    if len(errors) > 0:
        raise ValidationError(errors)
