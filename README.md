[![CI](https://github.com/leocalm/avro_validator/actions/workflows/ci.yaml/badge.svg)](https://github.com/leocalm/avro_validator/actions/workflows/ci.yaml)
[![Documentation Status](https://readthedocs.org/projects/avro-validator/badge/?version=latest)](https://avro-validator.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/avro-validator.svg)](https://badge.fury.io/py/avro-validator)
[![Downloads](https://pepy.tech/badge/avro-validator)](https://pepy.tech/project/avro-validator)
[![Coverage Status](https://coveralls.io/repos/github/leocalm/avro_validator/badge.svg?branch=main)](https://coveralls.io/github/leocalm/avro_validator?branch=main)

# Avro Validator
A pure python avro schema validator.

The default avro library for Python provide validation of data against the schema, the problem is that the output of 
this validation doesn't provide information about the error. 
All you get is the `the datum is not an example of the schema` error message.

When working with bigger avro schemas, sometimes is not easy to visually find the field that has an issue.

This library provide clearer exceptions when validating data against the avro schema, in order to be easier to 
identify the field that is not compliant with the schema and the problem with that field.

## Installing
Install using pip:
```bash
$ pip install -U avro_validator
```

## Validating data against Avro schema
The validator can be used as a console application. It receives a schema file, and a data file, validating the data
and returning the error message in case of failure.

The avro_validator can also be used as a library in python code.

### Console usage
In order to validate the `data_to_validate.json` file against the `schema.avsc` using the `avro_validator` callable, just type:
```bash
$ avro_validator schema.avsc data_to_valdate.json
OK
```
Since the data is valid according to the schema, the return message is `OK`.

#### Error validating the data
If the data is not valid, the program returns an error message:
```bash
$ avro_validator schema.avsc data_to_valdate.json
Error validating value for field [data,my_boolean_value]: The value [123] is not from one of the following types: [[NullType, BooleanType]]
```
This message indicates that the field `my_boolean_value` inside the `data` dictionary has value `123`, which is not 
compatible with the `bool` type.

#### Command usage
It is possible to get information about usage of the `avro_validator` using the help:
```bash
$ avro_validator -h
```

### Library usage
#### Using schema file
When using the avr_validator as a library, it is possible to pass the schema as a file:
```python
from avro_validator.schema import Schema

schema_file = 'schema.avsc'

schema = Schema(schema_file)
parsed_schema = schema.parse()

data_to_validate = {
    'name': 'My Name'
}

parsed_schema.validate(data_to_validate)
```
In this example, if the `data_to_validate` is valid according to the schema, then the
 `parsed_schema.validate(data_to_validate)` call will return `True`.

#### Using a dict as schema
It is also possible to provide the schema as a json string:
```python
import json
from avro_validator.schema import Schema

schema = json.dumps({
    'name': 'test schema',
    'type': 'record',
    'doc': 'schema for testing avro_validator',
    'fields': [
        {
            'name': 'name',
            'type': 'string'
        }
    ]
})

schema = Schema(schema)
parsed_schema = schema.parse()

data_to_validate = {
    'name': 'My Name'
}

parsed_schema.validate(data_to_validate)
```
In this example, the `parsed_schema.validate(data_to_validate)` call will return `True`, since the data is valid according to the schema.

#### Invalid data
If the data is not valid, the `parsed_schema.validate` will raise a `ValueError`, with the message containing the error description.
```python
import json
from avro_validator.schema import Schema

schema = json.dumps({
    'name': 'test schema',
    'type': 'record',
    'doc': 'schema for testing avro_validator',
    'fields': [
        {
            'name': 'name',
            'type': 'string',
            'doc': 'Field that stores the name'
        }
    ]
})

schema = Schema(schema)
parsed_schema = schema.parse()

data_to_validate = {
    'my_name': 'My Name'
}

parsed_schema.validate(data_to_validate)
```
The schema defined expects only one field, named `name`, but the data contains only the field `name_2`, 
making it invalid according to the schema. In this case, the `validate` method will return the following error:
```
Traceback (most recent call last):
  File "/Users/leonardo.almeida/.pyenv/versions/avro_validator_venv/lib/python3.7/site-packages/IPython/core/interactiveshell.py", line 3326, in run_code
    exec(code_obj, self.user_global_ns, self.user_ns)
  File "<ipython-input-3-a5e8ce95d21c>", line 23, in <module>
    parsed_schema.validate(data_to_validate)
  File "/opt/dwh/avro_validator/avro_validator/avro_types.py", line 563, in validate
    raise ValueError(f'The fields from value [{value}] differs from the fields '
ValueError: The fields from value [{'my_name': 'My Name'}] differs from the fields of the record type [{'name': RecordTypeField <name: name, type: StringType, doc: Field that stores the name, default: None, order: None, aliases: None>}]
```
The message detailed enough to enable the developer to pinpoint the error in the data.

#### Invalid schema
If the schema is not valid according to avro specifications, the `parse` method will also return a `ValueError`.
```python
import json
from avro_validator.schema import Schema

schema = json.dumps({
    'name': 'test schema',
    'type': 'record',
    'doc': 'schema for testing avro_validator',
    'fields': [
        {
            'name': 'name',
            'type': 'invalid_type',
            'doc': 'Field that stores the name'
        }
    ]
})

schema = Schema(schema)
parsed_schema = schema.parse()
```
Since the schema tries to define the `name` field as `invalid_type`, the schema declaration is invalid, 
thus the following exception will be raised:
```
Traceback (most recent call last):
  File "/Users/leonardo.almeida/.pyenv/versions/avro_validator_venv/lib/python3.7/site-packages/IPython/core/interactiveshell.py", line 3326, in run_code
    exec(code_obj, self.user_global_ns, self.user_ns)
  File "<ipython-input-2-7f3f77000f08>", line 18, in <module>
    parsed_schema = schema.parse()
  File "/opt/dwh/avro_validator/avro_validator/schema.py", line 28, in parse
    return RecordType.build(schema)
  File "/opt/dwh/avro_validator/avro_validator/avro_types.py", line 588, in build
    record_type.__fields = {field['name']: RecordTypeField.build(field) for field in json_repr['fields']}
  File "/opt/dwh/avro_validator/avro_validator/avro_types.py", line 588, in <dictcomp>
    record_type.__fields = {field['name']: RecordTypeField.build(field) for field in json_repr['fields']}
  File "/opt/dwh/avro_validator/avro_validator/avro_types.py", line 419, in build
    field.__type = cls.__build_field_type(json_repr)
  File "/opt/dwh/avro_validator/avro_validator/avro_types.py", line 401, in __build_field_type
    raise ValueError(f'Error parsing the field [{fields}]: {actual_error}')
ValueError: Error parsing the field [name]: The type [invalid_type] is not recognized by Avro
```
The message is clearly indicating that the the `invalid_type` is not recognized by avro.
