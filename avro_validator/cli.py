import os
import sys
import argparse
import json

from avro_validator.schema import Schema


def main() -> None:
    """Main entrypoint for the command line"""

    parser = argparse.ArgumentParser(
        description='Validate json against avro schema.')
    parser.add_argument(
        '-s', '--skip-extra-keys',
        default=False,
        action="store_true",
        help='Skip the avro validation for all unknown fields.',
    )
    parser.add_argument(
        'schema_file',
        help='The path to the file containing the avro schema.',
    )
    parser.add_argument(
        'data_file',
        help='The path to a file containing the data to validate.',
    )
    args = parser.parse_args()

    if not os.path.exists(args.schema_file):
        print('ERROR: The schema file does not exist.')
        sys.exit(1)

    if not os.path.exists(args.data_file):
        print('ERROR: The data file does not exist.')
        sys.exit(1)

    schema = Schema(args.schema_file)

    try:
        parsed_schema = schema.parse(args.skip_extra_keys)
    except ValueError as error:
        print('Error parsing the schema. Problem found:\n', error)
        sys.exit(1)

    parsed_data = json.load(open(args.data_file, 'r'))

    try:
        parsed_schema.validate(parsed_data)
        print('OK')
    except ValueError as error:
        print(error)
        sys.exit(1)


if __name__ == '__main__':
    main()
