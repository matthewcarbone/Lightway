from aimmdb.ingest.eli import ingest, read_metadata_and_header
from pathlib import Path


def test_read_md_and_header():
    test_path = Path(
        "/home/charles/Desktop/AIMMDB_ingestion/aimmdb/aimmdb/_tests/eli_test_file.dat"
    )
    md, hdr = read_metadata_and_header(test_path)
    assert set(map(type, md.values())) == {str}  # assert all md values are of type str
    assert hdr.split() == [
        "energy",
        "i0",
        "it",
        "ir",
        "iff",
        "aux1",
        "aux2",
        "aux3",
        "aux4",
    ]


def test_ingest():
    test_path = Path(
        "/home/charles/Desktop/AIMMDB_ingestion/aimmdb/aimmdb/_tests/eli_test_file.dat"
    )
    data, md, uid = ingest(test_path, return_uid=True)
    assert uid == "d66dda13-d69c-4ca6-8fb7-76290ad71073"
    assert sorted(data.columns) == sorted(["energy", "mu_trans", "mu_fluor", "mu_ref"])
