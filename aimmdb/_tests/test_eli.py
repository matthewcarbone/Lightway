from aimmdb.ingest.eli import ingest, read_metadata_and_header
from pathlib import Path


def test_read_md_and_header():
    test_path = Path("aimmdb/_tests/eli_test_file.dat")
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
    test_path = Path("aimmdb/_tests/eli_test_file.dat")
    data = ingest(test_path, return_uid=True)
    for channel_data in data:
        uid = channel_data[2]
        assert uid == "d66dda13-d69c-4ca6-8fb7-76290ad71073"
        md = channel_data[1]
        channel = md["channel"]
        assert channel in ["transmission", "fluorescence", "reference"]
        df = channel_data[0]
        with open(test_path) as test_file:
            data_lines = [
                line for line in test_file.readlines() if not line.startswith("#")
            ]
            assert df.shape[0] == len(data_lines)
        mu_channels = ["mu_trans", "mu_fluor", "mu_ref"]
        assert any([(mu in df.columns) for mu in mu_channels])
