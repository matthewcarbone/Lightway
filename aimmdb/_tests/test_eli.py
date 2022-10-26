from aimmdb.ingest.eli import ingest, read


def test_ingest():
    test_path = "eli_test_file.dat"
    data, md, uid = ingest(test_path, return_uid=True)

    print(uid)
    print(data)
    print(md)
