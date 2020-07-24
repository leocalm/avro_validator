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

    def parse(self) -> RecordType:
        """Parses the schema and returns a RecordType containing the schema.

        Returns:
            The RecordType representing the parsed schema.
        """
        schema = json.loads(self._schema)
        return RecordType.build(schema)

    def validate(self) -> bool:
        """Parses the schema returns a boolean indicating if the schema is valid.

        Returns:
            A boolean indicating if the schema is valid.
        """
        try:
            self.parse()
            return True
        except ValueError:
            return False
