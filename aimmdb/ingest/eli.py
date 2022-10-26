import numpy as np
import pandas as pd
from pathlib import Path


def read_metadata_and_header(path):
    """Read through commented lines of dat file to get metadata and DataFrame header

    Parameters
    ----------
    path: str, or path object
        path to .dat file from ISS beamline

    Returns
    -------
    tuple[dict[str, str], str]
        First element is dictionary of metadata.
        Second element is header as single string, with column names separated by whitespace.
    """
    metadata: dict
    header: str
    with open(path, "r") as dat_file:
        lines = dat_file.readlines()
    # remove starting hash and whitespace
    comment_lines = [line[2:] for line in lines if line.startswith("#")]

    metadata = {
        # split line at colon for key-value pairs and strip whitespace from values
        line.split(":", maxsplit=1)[0]: line.split(":", maxsplit=1)[1].strip()
        for line in comment_lines
        if ":" in line
    }

    # only comment line without colon should be header
    header = [line for line in comment_lines if ":" not in line]
    header = header[0]

    return metadata, header


def ingest(path, return_uid=False):
    """Prepare scan data from Eli (ISS beamline) for entry into AIMMDB

    .dat file contains commented lines (denoted by #) with metadata, and columnated data.
    Data columns should correspond to energy, i0, it, ir, iff, and aux channels.
    DataFrame for aimmdb contains only energy and absorption coefficient (mu).
    There are several ways to calculate mu based on the mode of the measurement:
    - mu_trans = -ln(it/i0)
    - mu_fluor = iff/i0
    - mu_ref = -ln(ir/i0)

    These are each calculated and used as columns in final DataFrame.

    Parameters
    ----------
    path: str, or path object
        path to .dat file from ISS beamline

    Returns
    -------
    tuple[DataFrame, dict[str, str]]
        First element is DataFrame of scan data (i.e., spectra).
        Second element is the metadata in a dictionary of str key-value pairs.

    Other Parameters
    ----------------
    return_uid: bool
        Optional keyword to return uid as third element in tuple.
        The uid should be present in the metadata, but it is an especially important
        piece of information that may be useful to store separately.
    """
    md, hdr = read_metadata_and_header(path)

    temp_data = pd.read_csv(path, delim_whitespace=True, comment="#", names=hdr.split())
    temp_data["mu_trans"] = -np.log(temp_data["it"] / temp_data["i0"])
    temp_data["mu_fluor"] = temp_data["iff"] / temp_data["i0"]
    temp_data["mu_ref"] = -np.log(temp_data["ir"] / temp_data["i0"])

    data = temp_data[["energy", "mu_trans", "mu_fluor", "mu_ref"]]

    if return_uid:
        uid = md["Scan.uid"]
        return data, md, uid
    else:
        return data, md
