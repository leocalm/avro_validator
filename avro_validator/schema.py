import json

from avro_validator.avro_types import RecordType


class Schema:
    """Represents a avro schema."""

    def __init__(self, schema: str):
        """Inits the Schema based on a json file or json string.

        Args:
            schema: the json string containing the schema, or the path to a json file containing the schema
        """
        try:
            with open(schema, 'r') as schema_file:
                self._schema = schema_file.read()
        except Exception:
            self._schema = schema

    def parse(self, skip_extra_keys=False) -> RecordType:
        """Parses the schema and returns a RecordType containing the schema.

        Returns:
            The RecordType representing the parsed schema.
        """
        schema = json.loads(self._schema)
        return RecordType.build(schema, skip_extra_keys=skip_extra_keys)
