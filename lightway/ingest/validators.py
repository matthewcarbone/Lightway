import numpy as np


def validate_iss(df, metadata):
    """Validates an ISS dataframe result.

    Parameters
    ----------
    df : pd.DataFrame
    metadata: dict
    """

    SUBSET = {"energy", "mu"}
    REQUIRED_KEYS = ["Scan-uid", "channel"]

    # Assert correct columns
    if not SUBSET.issubset(df.columns):
        raise ValueError(
            f"{SUBSET} is not a subset of {df.columns}, all of which are "
            "required"
        )

    # Assert that the energy is monotonically increasing
    diffs = np.diff(df["energy"])
    if not np.all(diffs > 0):
        raise ValueError("energy column must be monotonically increasing")

    # Assert that the correct keys exist in the metadata
    for key in REQUIRED_KEYS:
        if key not in metadata.keys():
            raise KeyError(f"{key} must be in the metadata")
