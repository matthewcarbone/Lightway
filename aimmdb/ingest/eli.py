import numpy as np
import pandas as pd
from pathlib import Path


def read_metadata_and_header(path: Path):
    metadata = {}
    header = ""
    dat_file = path.open()
    for line in dat_file:
        if line.startswith("#"):
            line = line[2:]  # remove hash and whitespace
            if ":" in line:
                key, value = line.split(":", maxsplit=1)
                metadata[key] = value.strip()
            else:
                header = line
    return metadata, header


def ingest(path: Path, return_uid=False):
    md, hdr = read_metadata_and_header(path)
    # print(header)
    temp_data = pd.read_csv(path, sep="\s+", comment="#", names=hdr.split())
    temp_data["mu_trans"] = -np.log(temp_data["it"] / temp_data["i0"])
    temp_data["mu_fluor"] = -(temp_data["iff"] / temp_data["i0"])
    temp_data["mu_ref"] = -np.log(temp_data["ir"] / temp_data["i0"])

    data = temp_data[["energy", "mu_trans", "mu_fluor", "mu_ref"]]

    if return_uid:
        uid = md["Scan.uid"]
        return data, md, uid
    else:
        return data, md


if __name__ == "__main__":

    def test():
        test_path = Path("/home/charles/Desktop/test_data/aimmdb_ingestion/some_scans")
        for p in test_path.iterdir():
            if ".dat" in p.name:
                data, md = ingest(p)
                print(p)
                print(data)
                print(md)

    test()
