from databroker.experimental.server_ext import MongoAdapter


# key_to_query = {
#     "element": "metadata.sample_metadata.element",
#     "edge": "metadata.sample_metadata.edge",
#     "dataset": "metadata.dataset",
#     "sample": "metadata.sample_id",
# }


class LightwayMongoInMemory(MongoAdapter):
    @classmethod
    def from_uri(cls, uri, directory, *, metadata=None):
        raise NotImplementedError(
            "This is an in-memory adapter for testing only: from_uri is "
            "disabled"
        )
