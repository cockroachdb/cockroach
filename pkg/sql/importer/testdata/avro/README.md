###### Prerequisites

You will need two tools installed:

1. Avro tools
`$ brew install avro-tools`
2. jq to manipulate json files: `$ brew install jq`

_simple.schema_ contains JSON schema for our _simple_ table.

###### Notes on file extensions

* .json: json records, one per line, compact format
* .bjson: avro datum encoded binary json
* .pjson: pretty printed json
* .ocf: OCF (object container format) output

###### Test Data Generation

We use `avro-tools` to generate some random data, and the manipulate
the data using `jq`

Generate OCF:

`$ avro-tools random --schema-file simple-schema.json  --count 1000 simple.ocf`

Generated sorted pretty printed json:

`$ avro-tools tojson simple.ocf  | jq -s 'sort_by(.i) | .[]' > simple-sorted.pjson`

Generate sorted compact json:

`$ avro-tools tojson simple.ocf  | jq -s -c 'sort_by(.i) | .[]' > simple-sorted.json`

Generate avro fragments (BIN_RECORDS):
 
`$ avro-tools jsontofrag --schema-file simple-schema.json simple-sorted.json > simple-sorted-records.avro`

Note: avro-tools, of course, could produce non-unique random data.
Spot check/verify that your primary key data does not have duplicates.