import os
import json

from avro_validator.avro_types import RecordType


class Schema:
    """Represents a avro schema."""

    def __init__(self, schema: str):
        """Inits the Schema based on a json file or json string.

        Args:
            schema: the json string containing the schema, or the path to a json file containing the schema
        """
        if os.path.isfile(schema):
            self._schema = open(schema, 'r').read()
        else:
            self._schema = schema

    def parse(self) -> RecordType:
        """Parses the schema and returns a RecordType containing the schema.

        Returns:
            The RecordType representing the parsed schema.
        """
        schema = json.loads(self._schema)
        return RecordType.build(schema)
