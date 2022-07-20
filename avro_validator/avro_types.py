import abc
import re
import struct
import sys
from typing import Any, Sequence, Mapping, Optional, Set, Union, Dict, List

FIELD_MAPPING = {
    'string': 'StringType',
    'int': 'IntType',
    'long': 'LongType',
    'float': 'FloatType',
    'double': 'DoubleType',
    'boolean': 'BooleanType',
    'null': 'NullType',
    'bytes': 'BytesType',
    'map': 'MapType',
    'fixed': 'FixedType',
    'array': 'ArrayType',
    'union': 'UnionType',
    'enum': 'EnumType',
    'record': 'RecordType'
}

LOGICAL_TYPES = {
    'decimal': {
        'types': ['FixedType', 'BytesType'],
        'extra_fields': {'precision': int, 'scale': int},
        'fixed_size': None
    },
    'duration': {
        'types': ['FixedType'],
        'extra_fields': {},
        'fixed_size': 12
    },
    'uuid': {
        'types': ['StringType'],
        'extra_fields': {},
        'fixed_size': None
    },
    'date': {
        'types': ['IntType'],
        'extra_fields': {},
        'fixed_size': None
    },
    'time-millis': {
        'types': ['IntType'],
        'extra_fields': {},
        'fixed_size': None
    },
    'time-micros': {
        'types': ['LongType'],
        'extra_fields': {},
        'fixed_size': None
    },
    'timestamp-millis': {
        'types': ['LongType'],
        'extra_fields': {},
        'fixed_size': None
    },
    'timestamp-micros': {
        'types': ['LongType'],
        'extra_fields': {},
        'fixed_size': None
    },
    'local-timestamp-millis': {
        'types': ['LongType'],
        'extra_fields': {},
        'fixed_size': None
    },
    'local-timestamp-micros': {
        'types': ['LongType'],
        'extra_fields': {},
        'fixed_size': None
    },
}

LOGICAL_TYPE_FIELDS: Set[str] = {'precision', 'scale'}


class Type:
    """Base abstract class to represent avro types"""

    @property
    def python_types(self):
        """Gets the python type associated with the avro type"""
        raise NotImplementedError

    @abc.abstractmethod
    def validate(self, value: Any) -> bool:
        """Validates the value"""

    def check_type(self, value: Any) -> bool:
        """Checks the type of a value against the type defined in python_type.

        Args:
          value: The value from which to check the type.

        Returns:
          True if the value has the correct type and False otherwise.
        """
        return any(isinstance(value, t) for t in self.python_types)

    @staticmethod
    def _validate_logical_type_fields(
        json_repr: Mapping[str, Any],
        logical_type: str,
        logical_type_definition: Mapping[str, Any]
    ) -> None:
        for logical_type_field in LOGICAL_TYPE_FIELDS:
            if json_repr.get(logical_type_field):
                if logical_type_field not in logical_type_definition['extra_fields'].keys():
                    raise ValueError(
                        f'The logicalType {logical_type} does not accept the field {logical_type_field}'
                    )

                expected_type = logical_type_definition['extra_fields'][logical_type_field]
                if type(json_repr.get(logical_type_field)) != expected_type:
                    raise TypeError(
                        f'The field {logical_type_field} '
                        f'must have type {logical_type_definition["extra_fields"][logical_type_field]}. '
                        f'Got {type(json_repr.get(logical_type_field))}'
                    )

    @staticmethod
    def _validate_logical_type_size(
        json_repr: Mapping[str, Any],
        logical_type: str,
        logical_type_definition: Mapping[str, Any]
    ) -> None:
        fixed_size = logical_type_definition['fixed_size']
        if fixed_size and json_repr['size'] != fixed_size:
            raise ValueError(
                f'The allowed size for the logicalType {logical_type} is {fixed_size}. '
                f'Current value: {json_repr["size"]}'
            )

    @classmethod
    def _validate_logical_type(cls, json_repr: Mapping[str, Any]) -> bool:
        if not json_repr:
            return True

        logical_type = json_repr.get('logicalType')
        if not logical_type:
            return True

        if type(logical_type) != str:
            raise TypeError(f'LogicalType must always be a string. Got {type(logical_type)}')

        logical_type_lower = logical_type.lower()
        if logical_type_lower not in LOGICAL_TYPES:
            raise ValueError(
                f'LogicalType can only have one of these values: {list(LOGICAL_TYPES.keys())}. Got {logical_type}'
            )

        logical_type_definition = LOGICAL_TYPES[logical_type_lower]
        if cls.__name__ not in logical_type_definition['types']:
            raise ValueError(
                f'The logicalType {logical_type} can only be used with the types {logical_type_definition["types"]}. '
                f'Got {cls}.'
            )

        cls._validate_logical_type_fields(
            json_repr=json_repr, logical_type=logical_type, logical_type_definition=logical_type_definition
        )
        cls._validate_logical_type_size(
            json_repr=json_repr, logical_type=logical_type, logical_type_definition=logical_type_definition
        )

    @classmethod
    def build(
            cls,
            json_repr: Union[Mapping[str, Any], Sequence[Any]],
            custom_fields: Optional[Mapping[str, 'Type']]
    ) -> 'Type':
        cls._validate_logical_type(json_repr)
        return cls()


class ComplexType(Type):
    required_attributes: Set[str] = set()
    optional_attributes: Set[str] = set()

    @classmethod
    def _validate_json_repr(
        cls,
        json_repr: Mapping[str, Any],
        skip_extra_keys=False
    ) -> bool:

        if cls.required_attributes.intersection(json_repr.keys()) != cls.required_attributes:
            raise ValueError(f'The {cls.__name__} must have {cls.required_attributes} defined.')

        all_fields = (
            cls.optional_attributes.union(cls.required_attributes).union(LOGICAL_TYPE_FIELDS).union({'logicalType'})
        )

        if not skip_extra_keys and not all_fields.issuperset(json_repr.keys()):
            raise ValueError(
                f'The {cls.__name__} can only contains '
                f'{cls.required_attributes.union(cls.optional_attributes)} keys, '
                f'but does contain also '
                f'{set(json_repr.keys()).difference(cls.optional_attributes, cls.required_attributes)}'
            )

        return True

    @staticmethod
    def _get_field_from_json(field_type: Any, custom_fields: Mapping[str, Type]) -> Type:
        if isinstance(field_type, dict):
            return getattr(sys.modules[__name__], FIELD_MAPPING[field_type['type']]).build(field_type, custom_fields)

        if isinstance(field_type, list):
            return UnionType.build(field_type, custom_fields)

        if custom_fields.get(field_type, None) is not None:
            return field_type

        if not FIELD_MAPPING.get(field_type):
            raise ValueError(
                f'The type [{field_type}] is not recognized by Avro')

        return getattr(sys.modules[__name__], FIELD_MAPPING[field_type]).build(None, custom_fields)


class IntType(Type):
    """Represents the int primitive type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:
      - int: 32-bit signed integer

    Attributes:
      python_type: The python type associated with the int avro type.
    """
    python_types: List[type] = [int]

    def check_type(self, value: Any) -> bool:
        if isinstance(value, bool):
            return False
        return super().check_type(value)

    def validate(self, value: Any) -> bool:
        """Checks if the value can be used as an integer.

        Ensures that the value is smaller than 2.147.483.647.

        Args:
          value: Value to be validated

        Returns:
          True is the value is a valid integer

        Raises:
          ValueError: If the value is greater than 2.147.483.647.
        """
        if value > 2_147_483_647:
            raise ValueError(f'The value [{value}] is too large for int.')

        return True

    def __repr__(self):  # pragma: no cover
        return 'IntType'


class LongType(Type):
    """Represents the long primitive type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:
      - long: 64-bit signed integer

    Attributes:
      python_type: The python type associated with the long avro type.
    """
    python_types: List[type] = [int]

    def check_type(self, value: Any) -> bool:
        if isinstance(value, bool):
            return False
        return super().check_type(value)

    def validate(self, value: Any) -> bool:
        """Checks if the value can be used as an long.

        Ensures that the value is smaller than 9.223.372.036.854.775.807.

        Args:
          value: Value to be validated

        Returns:
          True is the value is a valid long

        Raises:
          ValueError: If the value is greater than 9.223.372.036.854.775.807.
        """
        if value > 9_223_372_036_854_775_807:
            raise ValueError(f'The value [{value}] is too large for long.')

        return True

    def __repr__(self):  # pragma: no cover
        return 'LongType'


class NullType(Type):
    """Represents the null primitive type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:
      - null: no value

    Attributes:
      python_type: The python type associated with the null avro type.
    """
    python_types: List[type] = [type(None)]

    def validate(self, value: Any) -> bool:
        """For null values, no validation is needed, the check_type is enough.

        Args:
          value: The value to be validated.

        Returns:
          True, since there is no validation needed
        """
        return True

    def __repr__(self):  # pragma: no cover
        return 'NullType'


class BooleanType(Type):
    """Represents the boolean primitive type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:
      - boolean: a binary value

    Attributes:
      python_type: The python type associated with the boolean avro type.
    """
    python_types: List[type] = [bool]

    def validate(self, value: Any) -> bool:
        """For boolean values, no validation is needed, the check_type is enough.

        Args:
          value: The value to be validated.

        Returns:
          True, since there is no validation needed
        """
        return True

    def __repr__(self):  # pragma: no cover
        return 'BooleanType'


class StringType(Type):
    """Represents the string primitive type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:
      - string: unicode character sequence

    Attributes:
      python_type: The python type associated with the string avro type.
    """
    python_types: List[type] = [str]

    def validate(self, value: Any) -> bool:
        """For string values, no validation is needed, the check_type is enough.

        Args:
          value: The value to be validated.

        Returns:
          True, since there is no validation needed
        """
        return True

    def __repr__(self):  # pragma: no cover
        return 'StringType'


class FloatType(Type):
    """Represents the float primitive type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:
      - float: single precision (32-bit) IEEE 754 floating-point number

    Attributes:
      python_type: The python type associated with the float avro type.
    """
    python_types: List[type] = [float, int]

    def check_type(self, value: Any) -> bool:
        if isinstance(value, bool):
            return False
        return super().check_type(value)

    def validate(self, value: Any) -> bool:
        """Checks if the value can be used as an float.

        Ensures that the value is not too large for float by using struct.pack.

        Args:
          value: Value to be validated

        Returns:
          True is the value is a valid float

        Raises:
          ValueError: If the value is not too large for float.
        """
        if value == float('inf'):
            raise ValueError(f'The value [{value}] is too large for float.')

        try:
            struct.pack('<f', value)
            return True
        except OverflowError:
            raise ValueError(f'The value [{value}] is too large for float.')

    def __repr__(self):  # pragma: no cover
        return 'FloatType'


class DoubleType(Type):
    """Represents the double primitive type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:
      - double: double precision (64-bit) IEEE 754 floating-point number

    Attributes:
      python_type: The python type associated with the double avro type.
    """
    python_types: List[type] = [float, int]

    def check_type(self, value: Any) -> bool:
        if isinstance(value, bool):
            return False
        return super().check_type(value)

    def validate(self, value: Any) -> bool:
        """Checks if the value can be used as an double.

        Ensures that the value is not too large for float by using struct.pack.

        Args:
          value: Value to be validated

        Returns:
          True is the value is a valid float

        Raises:
          ValueError: If the value is not too large for double.
        """
        if value == float('inf'):
            raise ValueError(f'The value [{value}] is too large for double.')

        return True

    def __repr__(self):  # pragma: no cover
        return 'DoubleType'


class BytesType(Type):
    """Represents the bytes primitive type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:
      - bytes: sequence of 8-bit unsigned bytes

    Attributes:
      python_type: The python type associated with the bytes avro type.
    """
    python_types: List[type] = [bytes]

    def validate(self, value: Any) -> bool:
        """For bytes values, no validation is needed, the check_type is enough.

        Args:
          value: The value to be validated.

        Returns:
          True, since there is no validation needed
        """
        return True

    def __repr__(self):  # pragma: no cover
        return 'BytesType'


class RecordTypeField(ComplexType):
    """Represents one field of the record complex type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:

    Records use the type name "record" and support three attributes:
      - name: a JSON string providing the name of the record (required).
      - namespace: a JSON string that qualifies the name (optional)
      - doc: a JSON string providing documentation to the user of this schema (optional).
      - aliases: a JSON array of strings, providing alternate names for this record (optional).
      - fields: a JSON array, listing fields (required). Each field is a JSON object with the following attributes:
        * name: a JSON string providing the name of the field (required).
        * doc: a JSON string describing this field for users (optional).
        * type: A JSON object defining a schema, or a JSON string naming a record definition (required).
        * default: A default value for this field, used when reading instances that lack this field (optional).
        * order: specifies how this field impacts sort ordering of this record (optional).
                 Valid values are "ascending" (the default), "descending", or "ignore".
        * aliases: a JSON array of strings, providing alternate names for this field (optional).

    Attributes:
      python_type: The python type associated with the record avro type.
    """

    required_attributes: Set[str] = {'name', 'type'}
    optional_attributes: Set[str] = {'doc', 'default', 'order', 'aliases'}

    def __init__(self, name: str = None, field_type: Type = None) -> None:
        """Inits RecordTypeField with the fields.

        Args:
          name: the name of the field.
          field_type: the type of the field.
        """
        self.__name: str = name
        self.__type: Type = field_type
        self.__doc: Optional[str] = None
        self.__default: Optional[Any] = None
        self.__order: Optional[str] = None
        self.__aliases: Optional[Sequence[str]] = None

    @property
    def type(self) -> Type:
        """Getter for the type of the field.

        Returns:
            The type of the field.
        """
        return self.__type

    @classmethod
    def __build_field_type(
            cls,
            json_repr: Union[Mapping[str, Any], Sequence[Any]],
            custom_fields: Mapping[str, Type]
    ) -> Type:
        try:
            return cls._get_field_from_json(json_repr['type'], custom_fields)
        except ValueError as error:
            error_msg = error.args[0]
            match = re.match(r'^Error parsing the field \[(.*)\]: (.*)$', error_msg)
            if match:
                actual_error = match.group(2)
                previous_fields = list(reversed(match.group(1).split(',')))
                fields = ','.join(reversed(previous_fields + [json_repr['name']]))
            else:
                actual_error = error_msg
                fields = json_repr['name']

            raise ValueError(f'Error parsing the field [{fields}]: {actual_error}')

    @classmethod
    def build(
            cls,
            json_repr: Union[Mapping[str, Any], Sequence[Any]],
            custom_fields: Optional[Mapping[str, Type]] = None,
            skip_extra_keys=False,
    ) -> 'RecordTypeField':
        """Build an instance of the RecordTypeField, based on a json representation of it.

        Args:
            json_repr: The json representation of a RecordTypeField, according to avro specification
            custom_fields: Map of custom_fields used to build the records
            skip_extra_keys: Skip the avro validation for all unknown fields. Default to False

        Returns:
            An newly created instance of RecordTypeField, based on the json representation
        """
        if custom_fields is None:
            custom_fields = {}

        cls._validate_json_repr(json_repr, skip_extra_keys=skip_extra_keys)

        field = cls()

        field.__name = json_repr['name']

        field.__type = cls.__build_field_type(json_repr, custom_fields)

        field.__doc = json_repr.get('doc')
        field.__default = json_repr.get('default')
        field.__order = json_repr.get('order')
        field.__aliases = json_repr.get('aliases')

        return field

    def __repr__(self) -> str:
        return (f'RecordTypeField <name: {self.__name}, type: {self.__type}, doc: {self.__doc}, '
                f'default: {self.__default}, order: {self.__order}, aliases: {self.__aliases}>')


class RecordType(ComplexType):
    """Represents the record complex type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:

    Records use the type name "record" and support three attributes:
      - name: a JSON string providing the name of the record (required).
      - namespace: a JSON string that qualifies the name (optional)
      - doc: a JSON string providing documentation to the user of this schema (optional).
      - aliases: a JSON array of strings, providing alternate names for this record (optional).
      - fields: a JSON array, listing fields (required). Each field is a JSON object with the following attributes:
        * name: a JSON string providing the name of the field (required).
        * doc: a JSON string describing this field for users (optional).
        * type: A JSON object defining a schema, or a JSON string naming a record definition (required).
        * default: A default value for this field, used when reading instances that lack this field (optional).
        * order: specifies how this field impacts sort ordering of this record (optional).
                 Valid values are "ascending" (the default), "descending", or "ignore".
        * aliases: a JSON array of strings, providing alternate names for this field (optional).

    Attributes:
      python_type: The python type associated with the record avro type.
    """
    python_types: List[type] = [dict]
    optional_attributes: Set[str] = {'type', 'namespace', 'aliases', 'doc'}
    required_attributes: Set[str] = {'fields', 'name'}

    def __init__(self, fields: Mapping[str, RecordTypeField] = None) -> None:
        """Inits RecordType with the fields.

        Args:
          fields: the list of dictionaries representing the fields of the record type.
        """
        self.__fields: Mapping[str, RecordTypeField] = fields
        self.__name: str = ''
        self.__namespace: Optional[str] = None
        self.__aliases: Optional[str] = None
        self.__doc: Optional[str] = None
        self.__custom_fields = {}

    @property
    def fields(self) -> Mapping[str, RecordTypeField]:
        """Getter for the fields of the RecordType.

        Returns:
            The list of fields for the RecordType.
        """
        return self.__fields

    def _validate_field(self, field_key: str, field_value: Any) -> bool:
        """Validates a field from the list of fields.

        This will check if the field is one of the record fields and validate it.
        Assuming the following record::

        {
          ...
          "fields": [
             {
               "name": "age",
               "type": "int",
               ...
             },
             ...
          ],
          ...
        }

        When validating the following value::

        {
          'age': 30,
          ...
        }

        The field_key is 'age' and the field value is '30'. This will be used to scan all the fields and check
        if there is a field with name age, then the value 30 will be validated against the 'type' of the field,
        which in this case is int.

        If the validation of the value against type validations fail, or if the key is not found in the record
        fields or if the value has the wrong type, an exception will be raised.

        Args:
          field_key: the dictionary key that contains the field which will be validated
          field_value: the dictionary value of the field that is being validated

        Returns:
          True, if the field is valid

        Raises:
          ValueError if the field is not valid, according to the definition above.
        """
        field_type = self.__fields[field_key].type
        if isinstance(field_type, str):
            field_type = self.__custom_fields[field_type]

        if not field_type.check_type(field_value):
            msg = f'The value [{field_value}] for field [{field_key}] should be [{self.__fields[field_key].type}].'
            raise ValueError(msg)

        try:
            return field_type.validate(field_value)
        except ValueError as error:
            error_msg = error.args[0]
            match = re.match(r'^Error validating value for field \[(.*)\]: (.*)$', error_msg)

            if match:
                actual_error = match.group(2)
                previous_fields = list(reversed(match.group(1).split(',')))
                fields = ','.join(reversed(previous_fields + [field_key]))
            else:
                actual_error = error_msg
                fields = field_key

            raise ValueError(f'Error validating value for field [{fields}]: {actual_error}')

    def validate(self, value: Any, skip_extra_keys=False) -> bool:
        """Validates the value against the record type.

        First, the type of the value is checked, and if it is not a dict, an exception is raised.
        Then, all the keys in the value are checked to make sure that they are present in the record fields.
        Finally, all fields are validated against their definitions.

        Args:
          value: the value to be validated
          skip_extra_keys: Skip the avro validation for all unknown fields. Default to False

        Returns:
          True if the value is valid.

        Raises:
          ValueError if the value is not valid according to the record type definition.
        """
        if not self.check_type(value):
            raise ValueError(f'The value [{value}] should have type dict but it has [{type(value)}].')

        value_keys = set(value.keys())
        required_fields = set(
            [field for (field, field_record) in self.__fields.items()
             if not (isinstance(field_record.type, NullType)
                     or (isinstance(field_record.type, UnionType)
                         and NullType in [type(t) for t in field_record.type.types]))])

        if not required_fields.issubset(value_keys):
            missing_fields = required_fields - value_keys
            raise ValueError(f'The fields from value [{value_keys}] differs from the fields '
                             f'of the record type [{required_fields}]. The following fields are '
                             f'required, but not present: [{missing_fields}].')

        if not skip_extra_keys and not value_keys.issubset(set(self.__fields.keys())):
            extra_fields = value_keys - set(self.__fields.keys())
            raise ValueError(f'The fields from value [{value_keys}] differs from the fields '
                             f'of the record type [{required_fields}]. The following fields are '
                             f'not in the schema, but are present: [{extra_fields}].')

        extra_fields = value_keys - set(self.__fields.keys())

        for key, field_value in value.items():
            if key not in extra_fields:
                self._validate_field(key, field_value)

        return True

    def validate_without_raising(self, value: Any) -> bool:
        """Validates the value against the record type.

        First, the type of the value is checked, and if it is not a dict, an exception is raised.
        Then, all the keys in the value are checked to make sure that they are present in the record fields.
        Finally, all fields are validated against their definitions.

        Args:
          value: the value to be validated

        Returns:
          True if the value is valid, False otherwise.

        """
        try:
            return self.validate(value)
        except (ValueError, KeyError):
            return False

    @classmethod
    def build(
            cls,
            json_repr: Union[Mapping[str, Any], Sequence[Any]],
            custom_fields: Optional[Mapping[str, Type]] = None,
            skip_extra_keys=False
    ) -> 'RecordType':
        """Build an instance of the RecordType, based on a json representation of it.

        Args:
            json_repr: The json representation of a RecordType, according to avro specification
            custom_fields: Map of custom_fields used to build the records
            skip_extra_keys: Skip the avro validation for all unknown fields. Default to False

        Returns:
            An newly created instance of RecordType, based on the json representation
        """
        if custom_fields is None:
            custom_fields: Dict[str, RecordType] = {}

        cls._validate_json_repr(json_repr, skip_extra_keys=skip_extra_keys)

        name = json_repr['name']

        custom_fields[name] = {}

        record_type = cls()
        record_type.__custom_fields = custom_fields
        record_type.__name = name
        record_type.__namespace = json_repr.get('namespace')
        record_type.__aliases = json_repr.get('aliases')
        record_type.__doc = json_repr.get('doc')
        record_type.__fields = {
            field['name']: RecordTypeField.build(
                field,
                record_type.__custom_fields,
                skip_extra_keys) for field in json_repr['fields']
        }

        record_type.__custom_fields[name] = record_type

        return record_type

    def __repr__(self) -> str:  # pragma: no cover
        return f'RecordType <{self.__fields}>'


class EnumType(ComplexType):
    """Represents the enum complex type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:

    Enums use the type name "enum" and support the following attributes:
      - name: a JSON string providing the name of the enum (required).
      - namespace: a JSON string that qualifies the name (optional).
      - aliases: a JSON array of strings, providing alternate names for this enum (optional).
      - doc: a JSON string providing documentation to the user of this schema (optional).
      - symbols: a JSON array, listing symbols, as JSON strings (required).
                 All symbols in an enum must be unique; duplicates are prohibited.
                 Every symbol must match the regular expression [A-Za-z_][A-Za-z0-9_]*
                 (the same requirement as for names).

    Attributes:
      python_type: The python type associated with the enum avro type.
    """
    python_types: List[type] = [str]
    optional_attributes: Set[str] = {'type', 'namespace', 'aliases', 'doc'}
    required_attributes: Set[str] = {'symbols', 'name'}

    def __init__(self, symbols: Sequence[str] = None) -> None:
        """Inits EnumType with the possible symbols.

        Args:
          symbols: the list of strings that defines the possible symbols for the enum.
        """
        self.__symbols = symbols
        self.__name: str = ''
        self.__namespace: Optional[str] = None
        self.__aliases: Optional[Sequence[str]] = None
        self.__doc: Optional[str] = None

    @property
    def symbols(self) -> Sequence[str]:
        """Getter for the symbols allowed in the enum.

        Returns:
            The symbols allowed in the EnumType
        """
        return self.__symbols

    def validate(self, value: Any) -> bool:
        """Validates if the value is one of the has the correct type and if it is contained in the symbols list.

        Args:
          value: The value to be validated

        Returns:
          True if the value is valid

        Raises:
          ValueError: if the value is not str or is not contained in the symbols list.
        """

        if not self.check_type(value):
            raise ValueError(f'The value [{value}] should have type str but it has [{type(value)}].')

        if value not in self.__symbols:
            raise ValueError(f'The value [{value}] is not a valid symbol for the symbols [{self.__symbols}]')

        return True

    @classmethod
    def build(
            cls,
            json_repr: Union[Mapping[str, Any], Sequence[Any]],
            _: Optional[Mapping[str, Type]] = None
    ) -> 'EnumType':
        """Build an instance of the EnumType, based on a json representation of it.

        Args:
            json_repr: The json representation of a EnumType, according to avro specification
            _: custom_fields used to build the schema

        Returns:
            An newly created instance of EnumType, based on the json representation
        """
        cls._validate_json_repr(json_repr)

        symbols = json_repr['symbols']

        if len(symbols) != len(set(symbols)):
            raise ValueError('Symbols must be unique for EnumType.')

        for symbol in symbols:
            if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', symbol):
                raise ValueError(f'Every symbol must match the regular expression [A-Za-z_][A-Za-z0-9_]*. '
                                 f'Wrong symbol: {symbol}')

        enum_type = cls()
        enum_type.__symbols = symbols
        enum_type.__name = json_repr['name']
        enum_type.__namespace = json_repr.get('namespace')
        enum_type.__aliases = json_repr.get('aliases')
        enum_type.__doc = json_repr.get('doc')

        return enum_type

    def __repr__(self) -> str:  # pragma: no cover
        return (f'EnumType <symbols: {self.__symbols}, name: {self.__name}, namespace: {self.__namespace}, '
                f'aliases: {self.__aliases}, doc: {self.__doc}>')


class ArrayType(ComplexType):
    """Represents the array complex type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:

    Arrays use the type name "array" and support a single attribute:
      - items: the schema of the array's items.

    Attributes:
      python_type: The python type associated with the enum avro type.
    """
    python_types: List[type] = [list]
    optional_attributes: Set[str] = {'type'}
    required_attributes: Set[str] = {'items'}

    def __init__(self, items_type: Type = None) -> None:
        """Inits the array type with the items type

        Args:
          items_type: the type of the items in the array.
        """
        self.__items = items_type
        self.__custom_fields = {}

    @property
    def items(self) -> Type:
        """Getter for the type of the items of the ArrayType

        Returns:
            The type of the items.
        """
        return self.__items

    def validate(self, value: Any) -> bool:
        """Validates the value against the array type definition.

        First, the type of the value is checked, to make sure that it is a list.
        Then the type of all items in the array is checked and finally, the values in the array are validated
        using the validate method  for the items type.

        Args:
          value: the value to be validated.

        Returns:
          True if the value  is valid.

        Raises:
          ValueError: if the value is not valid.
        """
        if not self.check_type(value):
            raise ValueError(f'The value [{value}] should be list but it is not.')

        for index, item in enumerate(value):
            if isinstance(self.__items, str):
                self.__custom_fields[self.__items].validate(item)
            else:
                if not self.__items.check_type(item):
                    raise ValueError(f'The item at index [{index}]: [{item}] is not from the type [{self.__items}]')
                self.__items.validate(item)

        return True

    @classmethod
    def build(
            cls,
            json_repr: Union[Mapping[str, Any], Sequence[Any]],
            custom_fields: Optional[Mapping[str, Type]] = None
    ) -> 'ArrayType':
        """Build an instance of the ArrayType, based on a json representation of it.

        Args:
            json_repr: The json representation of a ArrayType, according to avro specification
            custom_fields: Map of custom_fields used to build the records

        Returns:
            An newly created instance of ArrayType, based on the json representation
        """
        if custom_fields is None:
            custom_fields = {}

        cls._validate_json_repr(json_repr)

        array_type = cls()
        array_type.__custom_fields = custom_fields
        array_type.__items = ArrayType._get_field_from_json(json_repr['items'], custom_fields)

        return array_type

    def __repr__(self) -> str:  # pragma: no cover
        return f'ArrayType <items: {self.__items}>'


class UnionType(ComplexType):
    """Represents the union complex type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:

    Unions, as mentioned above, are represented using JSON arrays.
    For example, ["null", "string"] declares a schema which may be either a null or string.
    Unions may not immediately contain other unions.

    Attributes:
      python_type: The python type associated with the union avro type.
    """
    python_types: List[type] = [Any]

    def __init__(self, types: Sequence[Type] = None) -> None:
        """Inits the enum type with the types allowed.

        Args:
          types: the list of Types that are allowed in the union type.
        """
        self.__types = types
        self.__custom_fields = {}

    @property
    def types(self) -> Sequence[Type]:
        """Getter for the types allowed in the union type.

        Returns:
            The types allowed.
        """
        return self.__types

    def check_type(self, value: Any) -> bool:
        """For the union type is not necessary to check the typeof the value, because it can be any.

        Args:
          value: the value to check the type.

        Returns:
          True, because the check is not needed
        """
        return True

    def validate(self, value: Any) -> bool:
        """Validates the value against the Union type definition.

        The type of the field os checked against the allowed types for the union, and if the value match one of the
        types, the value is validated against this type.

        If the value doesn't match any of the allowed types, an exception is raised.

        Args:
          value: the value to be validated

        Returns:
          True is the value is valid

        Raises:
          ValueError: if the value is not valid according to the union type definition
        """
        for data_type in self.__types:
            if isinstance(data_type, str):
                return self.__custom_fields[data_type].validate(value)

            if data_type.check_type(value):
                return data_type.validate(value)

        raise ValueError(f'The value [{value}] is not from one of the following types: [{self.__types}]')

    @classmethod
    def build(
            cls,
            json_repr: Union[Mapping[str, Any], Sequence[Any]],
            custom_fields: Optional[Mapping[str, Type]] = None
    ) -> 'UnionType':
        """Build an instance of the UnionType, based on a json representation of it.

        Args:
            json_repr: The json representation of a UnionType, according to avro specification
            custom_fields: Map of custom_fields used to build the records

        Returns:
            A newly created instance of UnionType, based on the json representation
        """
        if custom_fields is None:
            custom_fields = {}

        for f in json_repr:
            if isinstance(f, list):
                raise ValueError('Unions may not immediately contain other unions.')

        union_type = cls()
        union_type.__custom_fields = custom_fields
        union_type.__types = [ComplexType._get_field_from_json(t, custom_fields) for t in json_repr]

        return union_type

    def __repr__(self) -> str:  # pragma: no cover
        return f'UnionType <{self.__types}>'


class MapType(ComplexType):
    """Represents the map complex type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:

    Maps use the type name "map" and support one attribute:
      - values: the schema of the map's values

    Map keys are assumed to be strings.

    Attributes:
      python_type: The python type associated with the map avro type.
    """
    python_types: List[type] = [dict]
    optional_attributes: Set[str] = {'type'}
    required_attributes: Set[str] = {'values'}

    def __init__(self, values: Type = None) -> None:
        """Inits the map type with the type of the values.

        Args:
          values: the Type instance that represent the type of the values in the map
        """
        self.__values = values
        self.__custom_fields = {}

    @property
    def values(self) -> Type:
        """Getter for the type of values for the MapType

        Returns:
            THe type of values.
        """
        return self.__values

    def validate(self, value: Any) -> bool:
        """Validated the value against the map definition.

        First, the type of the value is checked against the dict type.
        Then, the keys are checked to make sure all of them are strings and values are checked to make sure all
        of them have the defined Type.
        Finally, every value is validated against their Type.

        Args:
          value: the value to be validated

        Returns:
          True is the value is valid

        Raises:
          ValueError: if the value is not valid according to the map type definition
        """
        if not self.check_type(value):
            raise ValueError(f'The value [{value}] should be dict but it is not.')

        for map_key, map_value in value.items():
            if not StringType().check_type(map_key):
                raise ValueError(f'The key [{map_key}], value [{map_value}] is not from the type StringType')

            if isinstance(self.__values, str):
                self.__custom_fields[self.__values].validate(map_value)
            else:
                if not self.__values.check_type(map_value):
                    raise ValueError(f'The key [{map_key}], value [{map_value}] is not from the type [{self.__values}]')
                self.__values.validate(map_value)

        return True

    @classmethod
    def build(
            cls,
            json_repr: Union[Mapping[str, Any], Sequence[Any]],
            custom_fields: Optional[Mapping[str, Type]] = None
    ) -> 'MapType':
        """Build an instance of the MapType, based on a json representation of it.

        Args:
            json_repr: The json representation of a MapType, according to avro specification
            custom_fields: Map of custom_fields used to build the records

        Returns:
            A newly created instance of MapType, based on the json representation
        """
        if custom_fields is None:
            custom_fields = {}

        cls._validate_json_repr(json_repr)

        map_type = cls()
        map_type.__custom_fields = custom_fields
        map_type.__values = MapType._get_field_from_json(json_repr['values'], custom_fields)

        return map_type

    def __repr__(self) -> str:  # pragma: no cover
        return f'MapType <values: {self.__values}>'


class FixedType(ComplexType):
    """Represents the fixed complex type in avro.

    Avro specification (https://avro.apache.org/docs/1.8.2/spec.html) has the following definition:

    Fixed uses the type name "fixed" and supports two attributes:
      - name: a string naming this fixed (required).
      - namespace: a string that qualifies the name (optional).
      - aliases: a JSON array of strings, providing alternate names for this enum (optional).
      - size: an integer, specifying the number of bytes per value (required).

    Attributes:
      python_type: The python type associated with the map avro type.
    """
    python_types: List[type] = [bytes]
    optional_attributes: Set[str] = {'type', 'namespace', 'aliases'}
    required_attributes: Set[str] = {'name', 'size'}

    def __init__(self, size: int = None) -> None:
        """Inits the map type with the size of the value.

        Args:
          size: the allowed size of the bytes value.
        """
        self.__size: int = size
        self.__name: str = ''
        self.__namespace: Optional[str] = None
        self.__aliases: Optional[Sequence[str]] = None

    @property
    def fixed_size(self) -> int:
        """Getter for the fixed type size

        Returns:
            The fixed type size.
        """
        return self.__size

    def validate(self, value: Any) -> bool:
        """Validated the value against the fixed type definition.

        First, the type of the value is checked against the bytes type.
        Then, the length of the value is compared to the pre-defined size.

        Args:
          value: the value to be validated

        Returns:
          True is the value is valid

        Raises:
          ValueError: if the value is not valid according to the fixed type definition
        """
        if not self.check_type(value):
            raise ValueError(f'The value [{value}] must be bytes.')

        if len(value) != self.__size:
            raise ValueError(f'The value [{value}] has size [{len(value)}], it should be [{self.__size}]')

        return True

    @classmethod
    def build(
            cls,
            json_repr: Union[Mapping[str, Any], Sequence[Any]],
            custom_fields: Optional[Mapping[str, Type]] = None
    ) -> 'FixedType':
        """Build an instance of the FixedType, based on a json representation of it.

        Args:
            json_repr: The json representation of a FixedType, according to avro specification
            custom_fields: Map of custom_fields used to build the records

        Returns:
            A newly created instance of FixedType, based on the json representation
        """
        super(cls, cls).build(json_repr, custom_fields)

        if custom_fields is None:
            custom_fields = {}

        cls._validate_json_repr(json_repr)

        fixed_type = cls()
        fixed_type.__size = json_repr['size']
        fixed_type.__name = json_repr['name']
        fixed_type.__namespace = json_repr.get('namespace')
        fixed_type.__aliases = json_repr.get('aliases')

        return fixed_type

    def __repr__(self) -> str:  # pragma: no cover
        return (f'FixedType <size: {self.__size}, name: {self.__name}, namespace: {self.__namespace}, '
                f'aliases: {self.__aliases}>')
