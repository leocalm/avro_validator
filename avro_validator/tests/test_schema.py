import json

from avro_validator.avro_types import RecordType
from avro_validator.schema import Schema

TEST_SCHEMA = {
    'name': 'my_schema',
    'type': 'record',
    'fields': [
        {'name': 'name', 'type': 'string'},
        {'name': 'age', 'type': 'int'},
        {'name': 'data', 'type': {
            'type': 'record',
            'name': 'data',
            'fields': [
                {'name': 'count', 'type': 'int'}
            ]
        }}
    ],
}

WRONG_TEST_SCHEMA = {
    'name': 'my_schema',
    'type': 'record',
    'fields': [
        {'name': 'name'},
        {'name': 'age', 'type': 'int'},
        {'name': 'data', 'type': {
            'type': 'record',
            'name': 'data',
            'fields': [
                {'name': 'count', 'type': 'int'}
            ]
        }}
    ],
}


def test_create_schema_from_string():
    schema_json = json.dumps(TEST_SCHEMA)

    schema = Schema(schema_json)
    parsed = schema.parse()
    assert isinstance(parsed, RecordType)


def test_create_schema_from_json_file(tmpdir):
    json_file = tmpdir.mkdir("files").join("my_schema.avsc")
    json_file.write(json.dumps(TEST_SCHEMA))

    schema = Schema(json_file.realpath())
    parsed = schema.parse()
    assert isinstance(parsed, RecordType)


def test_create_schema_from_multi_json_file(tmpdir):
    schema = Schema("avro_validator/tests/schemas/todo.List.avsc")
    parsed = schema.parse()
    assert isinstance(parsed, RecordType)

    good_instance = {
        'title': 'bla',
        'items': [
            {
                'title': 'foo',
                'description': 'desc'
            }
        ],
        'priority': {
            'order': 1,
            'requester': {
                'name': 'XYZ'
            }
        }
    }

    bad_instance = {
        'items': [
            {
                'title': 'foo',
                'description': 'desc'
            }
        ],
        'priority': {
            'order': 1,
            'requester': {
                'name': 'XYZ'
            }
        }
    }

    parsed.validate(good_instance)

    try:
        parsed.validate(bad_instance)
    except ValueError as exc:
        assert 'title' in str(exc)
        assert 'but not present' in str(exc)
