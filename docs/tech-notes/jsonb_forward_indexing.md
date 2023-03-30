JSONB Key Encoding 
===========================================

This document intends to build on the JSONB value encoding [RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20171005_jsonb_encoding.md). 
Previously, it was only possible to value-encode JSONB values in CRDB. The following document describes a format 
for key-encoding JSONB values for the purpose of having a semantic ordering for JSON which in turn allows forward indexes on JSON columns. 
In order to implement this, a lexicographical order is required for the encoding of each JSONB value. 

Motivation
------------------------
JSON is stored in the JSONB format in CRDB. The motivation and use-cases for including 
JSONB were included in the scoping [RFC](https://github.com/cockroachdb/cockroach/pull/18739) 
of JSONB. Previously, it was only possible to value-encode JSONB values as
forward-indexes on JSON columns were not permitted. A key-encoding of JSONB now defines a valid 
lexicographical ordering for JSON which shall in turn allow the creation of primary 
indexes as well as the execution of `ORDER BY` statements on a JSON column. Moreover, it is imperative to note that
JSON numbers that are decimals and JSON containers (arrays and objects) that contain such numbers could have 
composite encodings. Thus, these changes also allow JSON columns to now be composite in nature.

Detailed Design
------------------------
A JSON value can be one of seven types:

1. true,
2. false,
3. null,
4. a UTF-8 string,
5. an arbitrary-precision number,
6. an array of zero or more JSON values,
7. or an object consisting of key-value pairs, where the keys are unique strings and the values are JSON values.


The following rules were kept in mind while designing this form of encoding, as defined in the postgres [documentation](https://www.postgresql.org/docs/current/datatype-json.html#:~:text=jsonb%20also%20supports%20btree%20and%20hash%20indexes.%20These%20are%20usually%20useful%20only%20if%20it%27s%20important%20to%20check%20equality%20of%20complete%20JSON%20documents.%20The%20btree%20ordering%20for%20jsonb%20datums%20is%20seldom%20of%20great%20interest%2C%20but%20for%20completeness%20it%20is%3A)

1. Object > Array > Boolean > Number > String > Null

2. Object with n pairs > Object with n-1 pairs

3. Array with n elements > Array with n - 1 elements

4. Moreover, arrays with equal number of elements are compared in the order:
`element1`, `element2`, `element3`, ….

5. Objects with an equal number of key value pairs are compared in the order:
`key1`, `value1`, `key2`, `value2`, ….

**NOTE:** There is one exception to these rules, which is neither documented by
Postgres, nor mentioned in the source code: empty arrays are the minimum JSON
value. As far as we can tell, this is a Postgres bug that has existed for some
time. We've decided to replicate this behavior to remain consistent with
Postgres. We've filed a [Postgres bug report](https://www.postgresql.org/message-id/17873-826fdc8bbcace4f1%40postgresql.org)
to track the issue.

In order to satisfy property 1 at all times, tags are defined in an increasing order of bytes. 
These tags will also have to be defined in a way where the tag representing an object is a large byte representation 
for a hexadecimal value (such as 0xff) and the subsequent objects have a value 1 less than the previous one,
where the ordering is described in point 1 above. There is a special tag for empty JSON arrays
in order to handle the special case of empty arrays being ordered before all other JSON values.

Additionally, tags representing terminators will also be defined. There will be two terminators, one for the ascending designation and the other for the descending one, and will be required to denote the end of a key encoding of the following JSON values: Objects, Arrays, Number and Strings. JSON Boolean and JSON Null are not required to have the terminator since they do not have variable length encoding due to the presence of a single tag (as explained later in this document).

When converting textual JSON input into JSONB, the primitive types described by [RFC 7159](https://www.rfc-editor.org/rfc/pdfrfc/rfc7159.txt.pdf) are mapped into the primitive types described in CRDB:

### NULL
A JSONB null is stored solely as the tag representing the type.

### String
A JSONB String is stored as:

* a JSONB String tag 
* a UTF-8 string 
* JSONB terminator tag

### Number
A JSONB Number is stored as:

* a JSONB Number tag 
* encoding of the number using CRDB’s Decimal encoding. 
* JSONB terminator tag

### Boolean
It is imperative to note that the following query results in being true in postgres:

		select 'true'::JSONB > 'false'::JSONB;

Thus, instead of a single Boolean tag in the enum (which was described above), two separate tags, one for true and one for false, shall be present. Moreover, the tag for true will have a higher value, in bytes, than the tag for false.

Thus, a JSONB Boolean is stored as just the tag depicting its type.

### Array
A JSONB array is stored as:

* a JSONB array tag 
* number of elements present in the array 
* encoding of the JSON values 
* JSONB terminator tag

### Object
An interesting property to note about a JSON object, while conversing about its possible encoding, is the fact that there is no order in which JSON keys are stored inside the object container. This results in the following query to be true (tested inside of psql):

	select '{"a": "b", "c":"d"}'::JSONB = '{"c":"d", "a":"b"}'::JSONB;

Thus, it is important to sort the key-value pairs present within a JSON object. In order to achieve a sorting “effect”, encoding will be done in a fashion where all the key-value pairs will be sorted first according to the encodings of the current keys of these pairs. This will be followed by an encoding of each key and value present in these pairs and concatenating them to build the resulting encoding. The key and value encodings, of each such pair, will be intertwined to keep the property that the resultant encoding shall sort the same way as the original object.

Thus, a JSONB object is stored as:
* a JSONB object tag 
* number of key-value pairs present in the object 
* an encoding of the key-value pairs where the pairs have been sorted first on the encodings of the keys. 
* JSONB terminator tag

### Examples
| Example JSON's                              |                                                                             Encodings                                                                             |
|---------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| enc(`NULL::JSONB`)                          |                                                                           [JSONB_NULL]                                                                            
| enc(`"a"::JSONB`)                           |                                                             [JSONB_String, enc("a"), JSONB_Terminator]                                                             |
| enc(`'1'::JSONB`)                           |                                                             [JSONB_Number, enc(1), JSONB_Terminator]                                                              |
| enc(`'False'::JSONB`)                       |                                                                           [JSONB_False]                                                                           |
| enc(`'True'::JSONB`)                        |                                                                           [JSONB_True]                                                                            |
| enc(`'[1, 2, "a"]'::JSONB`)                 | [JSONB_Array, enc(3), JSONB_Number, enc(1), JSONB_Terminator, JSONB_Number, enc(2), JSONB_Terminator, JSONB_String, enc("a"), JSONB_Terminator, JSONB_Terminator] |
| enc(`'{“a”:”b”, “c”:”d”}'::JSONB`)          |                                                                           [ JSONB_Object, enc(2), JSONB_String, enc(a), JSONB_Terminator, JSONB_String, enc(b), JSONB_Terminator, JSONB_String, enc(c), JSONB_Terminator, JSONB_String, enc(d), JSONB_Terminator, JSONB_Terminator]                                                                           |
| enc(`'{“a”: ‘[1]’, “b”: {“c”: 0}}'::JSONB`) |                                                                         [ JSONB_Object, enc(2), JSONB_String, enc(a), JSONB_Terminator, JSONB_Array, enc(1), JSONB_Number, enc(1), JSONB_Terminator, JSONB_Terminator, JSONB_String, enc(b), JSONB_Terminator, JSONB_Object, enc(1), JSONB_String, enc(c), JSONB_Terminator, JSONB_Number, enc(0), JSONB_Terminator, JSONB_Terminator, JSONB_Terminator ]                                                                  |
