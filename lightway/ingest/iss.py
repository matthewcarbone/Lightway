import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from uuid import uuid4
import warnings


from lightway.ingest.validators import validate_iss


# Horrendous submodule hack - need to deploy Eli's ISS tools in PyPI.
from lightway.ingest import xas  # noqa
from xas.process import get_df_and_metadata_from_db


def read_metadata_and_header(path):
    """Read through commented lines of dat file to get metadata and DataFrame
    header

    Parameters
    ----------
    path: str, or path object
        Path to .dat file from ISS beamline

    Returns
    -------
    tuple[dict[str, str], str]
        First element is dictionary of metadata. Second element is header as
        single string, with column names separated by whitespace.
    """

    metadata: dict
    header: str

    with open(path, "r") as dat_file:
        lines = dat_file.readlines()

    # remove starting hash and whitespace
    comment_lines = [line[2:] for line in lines if line.startswith("#")]

    metadata = {
        # split line at colon for key-value pairs and strip whitespace from
        # values
        line.split(":", maxsplit=1)[0]
        .replace(".", "-"): line.split(":", maxsplit=1)[1]
        .strip()
        for line in comment_lines
        if ":" in line
    }

    # only comment line without colon should be header
    header = [line for line in comment_lines if ":" not in line][0]

    return metadata, header


def _process_df_and_metadata(df, metadata):

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df["mu_trans"] = np.nan_to_num(-np.log(df["it"] / df["i0"]))
        df["mu_fluor"] = np.nan_to_num(df["iff"] / df["i0"])
        df["mu_ref"] = np.nan_to_num(-np.log(df["ir"] / df["i0"]))

    df_trans = df[["energy", "mu_trans"]]
    metadata["channel"] = "transmission"
    md_trans = metadata.copy()
    df_trans = df_trans.rename(columns={"mu_trans": "mu"})

    df_fluor = df[["energy", "mu_fluor"]]
    metadata["channel"] = "fluorescence"
    md_fluor = metadata.copy()
    df_fluor = df_fluor.rename(columns={"mu_fluor": "mu"})

    df_ref = df[["energy", "mu_ref"]]
    metadata["channel"] = "reference"
    md_ref = metadata.copy()
    df_ref = df_ref.rename(columns={"mu_ref": "mu"})

    # Assign a uid for now, and mark it as assigned
    # Note we cannot have "." in any of the keys when they go into tiled
    if "Scan.uid" not in md_trans.keys():
        md_trans["Scan-uid"] = f"assigned-{str(uuid4())}"
    else:
        md_trans["Scan-uid"] = md_trans.pop("Scan.id")
    if "Scan.uid" not in md_fluor.keys():
        md_fluor["Scan-uid"] = f"assigned-{str(uuid4())}"
    else:
        md_fluor["Scan-uid"] = md_fluor.pop("Scan.id")
    if "Scan.uid" not in md_ref.keys():
        md_ref["Scan-uid"] = f"assigned-{str(uuid4())}"
    else:
        md_ref["Scan-uid"] = md_ref.pop("Scan.id")

    validate_iss(df_trans, md_trans)
    validate_iss(df_fluor, md_fluor)
    validate_iss(df_ref, md_ref)

    return df_trans, md_trans, df_fluor, md_fluor, df_ref, md_ref


def load_from_disk(path):
    """Prepare scan data from Eli (ISS beamline) for entry into AIMMDB

    Data is stored in .dat file which contains commented lines (denoted by #)
    with metadata, and columnated data below the metadata. Data columns should
    correspond to energy, i0, it, ir, iff, and aux channels. DataFrame for
    aimmdb contains only energy and absorption coefficient (mu). There are
    several ways to calculate mu based on the mode of the measurement:
    - mu_trans = -ln(it/i0)
    - mu_fluor = iff/i0
    - mu_ref = -ln(ir/i0)

    These are each calculated and three database entries are created. Each
    entry contains a DataFrame with energy and mu as columns, as well as a
    metadata dictionary containing the commented metadata from the file plus an
    additional key indicating the measurement channel (trans, fluor, or ref).

    Parameters
    ----------
    path: str, or path object
        path to .dat file from ISS beamline

    Returns
    -------
    list
        Database entries corresponding to the transmission, fluorescence, and
        reference channels.
    """

    metadata, hdr = read_metadata_and_header(path)

    df = pd.read_csv(
        path, delim_whitespace=True, comment="#", names=hdr.split()
    )

    (
        df_trans,
        md_trans,
        df_fluor,
        md_fluor,
        df_ref,
        md_ref,
    ) = _process_df_and_metadata(df, metadata)

    return [
        dict(data=df_trans, metadata=md_trans),
        dict(data=df_fluor, metadata=md_fluor),
        dict(data=df_ref, metadata=md_ref),
    ]


def _write_from_res(res, client):
    for r in res:
        sample_metadata = {
            "edge": r["metadata"]["Element-edge"],
            "element": r["metadata"]["Element-symbol"],
        }
        channel = r["metadata"].pop("channel")
        metadata = {
            "original_sample_metadata": r["metadata"],
            "sample_metadata": sample_metadata,
            "experiment_metadata": {
                "facility": "NSLSII",
                "beamline": "ISS",
                "sample_id": r["metadata"]["Scan-uid"],
                "channel": channel,
            },
            "dataset": "raw",
        }
        client.write_dataframe(
            r["data"], metadata=metadata, specs=["ExperimentalXAS"]
        )


def ingest_all_from_disk(client, root, extension=".dat", pbar=True):
    """Loads in all files matching the provided extension.

    Parameters
    ----------
    root : os.PathLike
    """

    t = not pbar
    for path in tqdm(list(Path(root).rglob(f"*{extension}")), disable=t):
        res = load_from_disk(path)
        _write_from_res(res, client)


def ingest_from_DataBroker(client, db, pbar=True):
    """Loads in all files matching the provided extension.

    NOTE: this will require some check at some point to ensure unique entries
    from databroker are not rewritten every time this function is called

    Parameters
    ----------
    db
    """

    # !!!PSEUDOCODE!!!!!!!!
    for uid in tqdm(db.uids, disable=not pbar):
        df, metadata = get_df_and_metadata_from_db(db, uid, ...)
        # !!!!!!!!!!!!!!!!!!!!!

        res = _process_df_and_metadata(df, metadata)
        _write_from_res(res, client)
