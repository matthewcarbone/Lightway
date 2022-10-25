from aimmdb.ingest.eli import ingest
from pathlib import Path


def test_ingest():
    test_path = Path("./eli_test_file.dat")
    data, md = ingest(test_path)

    print(data)
    print(md)
