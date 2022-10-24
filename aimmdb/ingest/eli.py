import numpy as np
import pandas as pd
from pathlib import Path


def get_metadata_and_header(path: Path):
    md = {}
    header = ""
    dat_file = path.open()
    for line in dat_file:
        if line.startswith("#"):
            line = line[2:]  # remove hash and whitespace
            if ":" in line:
                key, value = line.split(":", maxsplit=1)
                md[key] = value.strip()
            else:
                header = line
    return md, header


def ingest(path: Path):
    md, header = get_metadata_and_header(path)
    # print(header)
    data = pd.read_csv(path, sep="\s+", comment="#", names=header.split())
    print(path)
    print(md.values())


if __name__ == "__main__":

    def test():
        test_path = Path("/home/charles/Desktop/test_data/aimmdb_ingestion/some_scans")
        for p in test_path.iterdir():
            ingest(p)

    test()
