

# SAMPLE CODE FOR VALIDATING SCHEMA AND DATA

This is the sample code for testing the schema and json data. 

To test the schema and json data file compatbility execute

`python3 example/test_avro.py example/test.avsc example/test.json`

`test_avro.py`: main driver program
`test.avsc`: Schema file
`test.json`: Json data file


## Changes:
This is the fork of leocalm/avro_validator

1. Updated float and double array type to accept both integer `123` and decimal `123.45` numbers
2. Changed the code for record type to accept `logicalType` as parameter. Additional code needs to added to check type matching the logical type. 