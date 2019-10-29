import pytest
from hypothesis import given
from hypothesis.strategies import (integers,
                                   text,
                                   booleans,
                                   lists,
                                   dictionaries,
                                   floats,
                                   one_of,
                                   none,
                                   sampled_from,
                                   fixed_dictionaries,
                                   binary)

from avro_validator.avro_types import (Type,
                                       LongType,
                                       IntType,
                                       StringType,
                                       NullType,
                                       BooleanType,
                                       FloatType,
                                       DoubleType,
                                       BytesType,
                                       UnionType,
                                       ArrayType,
                                       MapType,
                                       EnumType,
                                       RecordType,
                                       FixedType,
                                       RecordTypeField)


def test_not_implementing_python_type():
    class MyType(Type):
        def validate(self, value):
            """Just for testing..."""

    with pytest.raises(NotImplementedError):
        t = MyType()
        print(t.python_type)


@given(value=integers(max_value=9_223_372_036_854_775_808))
def test_long_type(value):
    assert LongType().check_type(value) is True
    assert LongType().validate(value) is True


@given(value=integers(min_value=9_223_372_036_854_775_808))
def test_bigger_than_float(value):
    with pytest.raises(ValueError, match=r'The value \[\d+\] is too large for long.'):
        assert LongType().validate(value)


@given(value=one_of(text(),
                    booleans(),
                    lists(elements=integers()),
                    dictionaries(keys=text(), values=integers()),
                    floats(),
                    none(),
                    binary()))
def test_invalid_long(value):
    assert LongType().check_type(value) is False


@given(value=integers(max_value=2_147_483_648))
def test_int_type(value):
    assert IntType().check_type(value) is True
    assert IntType().validate(value) is True


@given(value=booleans())
def test_int_type_dont_allow_bool(value):
    assert IntType().check_type(value) is False


@given(value=integers(min_value=2_147_483_648))
def test_bigger_than_int(value):
    with pytest.raises(ValueError, match=r'The value \[\d+\] is too large for int.'):
        assert IntType().validate(value)


@given(value=one_of(text(),
                    booleans(),
                    lists(elements=integers()),
                    dictionaries(keys=text(), values=integers()),
                    floats(),
                    none(),
                    binary()))
def test_invalid_int(value):
    assert IntType().check_type(value) is False


@given(value=text())
def test_string_type(value):
    assert StringType().validate(value) is True


@given(value=one_of(integers(),
                    booleans(),
                    lists(elements=integers()),
                    dictionaries(keys=text(), values=integers()),
                    floats(),
                    none(),
                    binary()))
def test_invalid_string(value):
    assert StringType().check_type(value) is False


@given(value=none())
def test_null_type(value):
    assert NullType().check_type(value) is True


@given(value=one_of(integers(),
                    booleans(),
                    lists(elements=integers()),
                    dictionaries(keys=text(), values=integers()),
                    floats(),
                    text(),
                    binary()))
def test_not_null_for_null_type(value):
    assert NullType().check_type(value) is False


@given(value=booleans())
def test_boolean_type(value):
    assert BooleanType().check_type(value) is True
    assert BooleanType().validate(value) is True


@given(value=one_of(integers(),
                    none(),
                    lists(elements=integers()),
                    dictionaries(keys=text(), values=integers()),
                    floats(),
                    text(),
                    binary()))
def test_invalid_boolean(value):
    assert BooleanType().check_type(value) is False


@given(value=floats(max_value=1e+38, min_value=-1e+38))
def test_float_type(value):
    assert FloatType().check_type(value) is True
    assert FloatType().validate(value) is True


@given(value=one_of(integers(),
                    none(),
                    lists(elements=integers()),
                    dictionaries(keys=text(), values=integers()),
                    booleans(),
                    text(),
                    binary()))
def test_invalid_float(value):
    assert FloatType().check_type(value) is False


@given(value=floats(min_value=4e+38))
def test_float_too_large(value):
    with pytest.raises(ValueError, match=r'The value \[.*\] is too large for float.'):
        FloatType().validate(value)


@given(value=floats(max_value=1e+308))
def test_double_type(value):
    assert DoubleType().check_type(value) is True
    assert DoubleType().validate(value) is True


@given(value=one_of(integers(),
                    none(),
                    lists(elements=integers()),
                    dictionaries(keys=text(), values=integers()),
                    booleans(),
                    text(),
                    binary()))
def test_invalid_double(value):
    assert DoubleType().check_type(value) is False


@given(value=floats(min_value=float('inf')))
def test_double_too_large(value):
    with pytest.raises(ValueError, match=r'The value \[.*\] is too large for double.'):
        DoubleType().validate(value)


@given(value=binary())
def test_bytes_type(value):
    assert BytesType().check_type(value) is True
    assert BytesType().validate(value) is True


@given(value=one_of(integers(),
                    none(),
                    lists(elements=integers()),
                    dictionaries(keys=text(), values=integers()),
                    booleans(),
                    text(),
                    floats()))
def test_invalid_bytes_type(value):
    assert BytesType().check_type(value) is False


@given(value=one_of(text(), none()))
def test_union_type(value):
    t = UnionType([NullType(), StringType()])

    assert t.validate(value) is True


@given(value=one_of(integers(),
                    lists(elements=integers()),
                    dictionaries(keys=text(), values=integers()),
                    booleans(),
                    floats()))
def test_invalid_union(value):
    t = UnionType([NullType(), StringType()])

    with pytest.raises(ValueError):
        t.validate(value)


@given(value=lists(elements=text()))
def test_array_type(value):
    assert ArrayType(StringType()).validate(value) is True


@given(value=one_of(integers(),
                    none(),
                    floats(),
                    dictionaries(keys=text(), values=integers()),
                    booleans(),
                    text(),
                    binary()))
def test_invalid_array(value):
    with pytest.raises(ValueError, match=r'The value \[(.|\s)*\] should be list but it is not.'):
        assert ArrayType(StringType()).validate(value)


@given(value=lists(elements=one_of(text(), none())))
def test_array_union_type(value):
    assert ArrayType(UnionType([NullType(), StringType()])).validate(value) is True


@given(value=lists(elements=one_of(integers(),
                                   lists(elements=integers()),
                                   dictionaries(keys=text(), values=integers()),
                                   booleans(),
                                   floats(),
                                   none()),
                   min_size=1))
def test_invalid_array_item_type(value):
    with pytest.raises(ValueError, match=r'The item at index \[\d+\]: \[.+\] is not from the type \[StringType\]'):
        ArrayType(StringType()).validate(value)


@given(value=sampled_from(['A', 'B', 'C']))
def test_enum_type(value):
    assert EnumType(['A', 'B', 'C']).validate(value) is True


@given(value=one_of(integers(),
                    booleans(),
                    lists(elements=integers()),
                    dictionaries(keys=text(), values=integers()),
                    floats(),
                    none(),
                    binary()))
def test_invalid_enum(value):
    with pytest.raises(ValueError, match=r'The value \[.*\] should have type str but it has \[.*\].'):
        assert EnumType(['A', 'B', 'C']).validate(value)


@given(value=sampled_from(['D', 'E', 'F']))
def test_invalid_enum_value(value):
    regex = r'The value \[(D|E|F)] is not a valid symbol for the symbols \[\[\'A\', \'B\', \'C\'\]\]'
    with pytest.raises(ValueError, match=regex):
        EnumType(['A', 'B', 'C']).validate(value)


@given(value=fixed_dictionaries({'name': text()}))
def test_record_type(value):
    assert RecordType({'name': RecordTypeField('name', StringType())}).validate(value) is True


@given(value=fixed_dictionaries({'name': text()}))
def test_record_type_repr(value):
    msg = "RecordType <{'name': RecordTypeField <name: name, type: StringType, " \
          "doc: None, default: None, order: None, aliases: None>}>"
    assert str(RecordType({'name': RecordTypeField('name', StringType())})) == msg


@given(value=one_of(integers(),
                    none(),
                    lists(elements=integers()),
                    booleans(),
                    text(),
                    floats(),
                    binary()))
def test_invalid_record_type(value):
    with pytest.raises(ValueError, match=r'The value \[(.|\s)*\] should have type dict but it has \[.*\].'):
        RecordType({'name': RecordTypeField('name', StringType())}).validate(value)


@given(value=fixed_dictionaries({'name': integers()}))
def test_record_type_wrong_value_type(value):
    with pytest.raises(ValueError, match=r'The value \[.*\] for field \[name\] should be \[StringType\]\.'):
        RecordType({'name': RecordTypeField('name', StringType())}).validate(value)


@given(value=fixed_dictionaries({'name': integers(min_value=2_147_483_648)}))
def test_record_type_invalid_value_type(value):
    with pytest.raises(ValueError, match=r'Error validating value for field \[.*\]'):
        RecordType({'name': RecordTypeField('name', IntType())}).validate(value)


@given(value=fixed_dictionaries({'name': text(), 'age': integers()}))
def test_record_type_additional_key_in_value(value):
    regex = r"The fields from value \[.*\] differs from the fields of the record type \[.*\].  " \
            r"The following fields are not in the schema, but are present: \[.*\]."
    with pytest.raises(ValueError, match=regex):
        RecordType({'name': RecordTypeField('name', StringType())}).validate(value)


@given(value=fixed_dictionaries({'name': text(), 'age': integers(max_value=200)}))
def test_record_type_optional_key_in_value(value):
    assert RecordType(
            {'name': RecordTypeField('name', StringType()),
             'age': RecordTypeField('age', UnionType([NullType(), IntType()]))
             }
          ).validate(value) is True


@given(value=fixed_dictionaries({'name': text()}))
def test_record_type_optional_key_not_in_value(value):
    assert RecordType(
            {'name': RecordTypeField('name', StringType()),
             'age': RecordTypeField('age', UnionType([NullType(), IntType()]))
             }
          ).validate(value) is True


@given(value=fixed_dictionaries({'name': text()}))
def test_record_type_missing_key_in_value(value):
    regex = r'The fields from value \[.*\] differs from the fields of the record type \[.*\]'
    with pytest.raises(ValueError, match=regex):
        RecordType({
            'name': RecordTypeField('name', StringType()),
            'age': RecordTypeField('age', IntType())
        }).validate(value)


@given(value=dictionaries(keys=text(), values=integers(max_value=2_147_483_647)))
def test_map_type(value):
    assert MapType(values=IntType()).validate(value) is True


@given(value=dictionaries(keys=text(),
                          values=one_of(integers(),
                                        booleans(),
                                        lists(elements=integers(), max_size=1),
                                        dictionaries(keys=text(), values=integers(), max_size=1),
                                        floats(),
                                        none(),
                                        binary()),
                          min_size=1))
def test_map_invalid_value(value):
    with pytest.raises(ValueError, match=r'The key \[(.|\s)*\], value \[.*\] is not from the type \[.*\]'):
        assert MapType(values=StringType()).validate(value)


@given(value=dictionaries(values=text(),
                          keys=one_of(integers(),
                                      booleans(),
                                      floats(),
                                      none(),
                                      binary()),
                          min_size=1))
def test_map_invalid_key(value):
    with pytest.raises(ValueError, match=r'The key \[.*\], value \[(.|\s)*\] is not from the type StringType'):
        assert MapType(values=StringType()).validate(value)


@given(value=one_of(integers(),
                    none(),
                    lists(elements=integers()),
                    booleans(),
                    text(),
                    floats(),
                    binary()))
def test_invalid_map_type(value):
    with pytest.raises(ValueError, match=r'The value \[(.|\s)*\] should be dict but it is not.'):
        MapType(StringType()).validate(value)


@given(value=binary(min_size=16, max_size=16))
def test_fixed(value):
    assert FixedType(size=16).validate(value) is True


@given(value=one_of(integers(),
                    none(),
                    lists(elements=integers()),
                    booleans(),
                    text(),
                    floats()))
def test_fixed_invalid_type(value):
    with pytest.raises(ValueError, match=r'The value \[(.|\s)*\] must be bytes.'):
        FixedType(size=32).validate(value)


@given(value=binary(min_size=17, max_size=160))
def test_fixed_wrong_size(value):
    with pytest.raises(ValueError, match=r'The value \[.*\] has size \[.*\], it should be \[.*\]'):
        FixedType(size=16).validate(value)


def test_build_fixed_type():
    fixed_type = FixedType.build({'size': 20, 'name': 'my_fixed_value'})
    assert isinstance(fixed_type, FixedType)
    assert fixed_type.fixed_size == 20


def test_build_fixed_type_extra_field():
    with pytest.raises(ValueError, match=r'The FixedType can only contains .* keys'):
        FixedType.build({'size': 20, 'name': 'my_fixed_value', 'invalid_field': 1})


def test_build_fixed_type_missing_required_field():
    with pytest.raises(ValueError, match=r'The FixedType must have .* defined.'):
        FixedType.build({'size': 20})


def test_build_map_type():
    map_type = MapType.build({'values': 'int'})
    assert isinstance(map_type, MapType)
    assert isinstance(map_type.values, IntType)

    map_type_2 = MapType.build({
        'values': {
            'type': 'map',
            'values': {
                'type': 'fixed',
                'name': 'my_fixed_value',
                'size': 10
            }
        }
    })
    assert isinstance(map_type_2, MapType)

    inner_map_type = map_type_2.values
    assert isinstance(inner_map_type, MapType)

    fixed_type = inner_map_type.values
    assert isinstance(fixed_type, FixedType)
    assert fixed_type.fixed_size == 10


def test_build_union_type():
    union_type = UnionType.build(['string', 'int'])
    assert isinstance(union_type, UnionType)
    for t in union_type.types:
        assert isinstance(t, StringType) or isinstance(t, IntType)


def test_build_union_type_inside_union_type():
    with pytest.raises(ValueError, match='Unions may not immediately contain other unions.'):
        UnionType.build(['string', ['int', 'float']])


def test_build_array_type():
    array_type = ArrayType.build({'items': 'int'})
    assert isinstance(array_type, ArrayType)
    assert isinstance(array_type.items, IntType)


def test_build_enum_type():
    enum_type = EnumType.build({'symbols': ['A', 'B', 'C'], 'name': 'my_enum'})
    assert isinstance(enum_type, EnumType)
    assert enum_type.symbols == ['A', 'B', 'C']


def test_build_enum_type_duplicated_symbols():
    with pytest.raises(ValueError, match=r'Symbols must be unique for EnumType.'):
        EnumType.build({'symbols': ['A', 'B', 'C', 'A'], 'name': 'my_enum'})


def test_build_enum_type_invalid_symbol():
    with pytest.raises(ValueError, match=r'Every symbol must match the regular expression.*'):
        EnumType.build({'symbols': ['9A', 'B', 'C'], 'name': 'my_enum'})


def test_build_record_type_field():
    field = RecordTypeField.build({'type': ['int', 'string'], 'name': 'myField'})
    assert isinstance(field, RecordTypeField)
    assert isinstance(field.type, UnionType)


def test_build_record_type():
    record_type = RecordType.build({
        'name': 'a',
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
    })
    assert isinstance(record_type, RecordType)
    assert isinstance(record_type.fields, dict)
    assert record_type.fields.get('name') is not None
    assert record_type.fields.get('age') is not None
    assert record_type.fields.get('data') is not None
    assert isinstance(record_type.fields['name'].type, StringType)
    assert isinstance(record_type.fields['data'].type, RecordType)
    data_record: RecordType = record_type.fields['data'].type
    assert isinstance(data_record.fields['count'].type, IntType)


def test_build_unrecognized_field_type():
    with pytest.raises(ValueError, match=r'The type \[my_type\] is not recognized by Avro'):
        ArrayType.build({'items': 'my_type'})


def test_beautiful_schema_exception():
    msg = (r'Error parsing the field \[data,inner,count\]\: '
           r'The type \[invalid_type\] is not recognized by Avro')
    with pytest.raises(ValueError, match=msg):
        RecordType.build({
            'name': 'root',
            'type': 'record',
            'fields': [
                {'name': 'data', 'type': {
                    'type': 'record',
                    'name': 'data',
                    'fields': [
                        {
                            'name': 'inner',
                            'type': {
                                'name': 'inner',
                                'type': 'record',
                                'fields': [
                                    {'name': 'count', 'type': 'invalid_type'}
                                ]
                            }
                        }
                    ]
                }}
            ],
        })


def test_beautiful_data_exception():
    record_type = RecordType.build({
            'name': 'root',
            'type': 'record',
            'fields': [
                {'name': 'data', 'type': {
                    'type': 'record',
                    'name': 'data',
                    'fields': [
                        {
                            'name': 'inner',
                            'type': {
                                'name': 'inner',
                                'type': 'record',
                                'fields': [
                                    {'name': 'count', 'type': 'int'}
                                ]
                            }
                        }
                    ]
                }}
            ],
        })

    msg = r'Error validating value for field \[data,inner\]\: ' \
          r'The value \[a\] for field \[count\] should be \[IntType\].'
    with pytest.raises(ValueError, match=msg):
        record_type.validate({
            'data': {
                'inner': {
                    'count': 'a'
                }
            }
        })
