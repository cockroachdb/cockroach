- Feature Name: Built-in Protobuf Type
- Status: Draft
- Start Date: 2024-07-20
- Authors: Xiaochen Cui
- RFC PR: N/A
- Cockroach Issue: [#47534]https://github.com/cockroachdb/cockroach/issues/47534

Table of Contents

- [Summary](#summary)
- [Motivation](#motivation)
- [Guide-level Explanation](#guide-level-explanation)
- [Reference-level Explanation](#reference-level-explanation)
- [Drawbacks](#drawbacks)
- [Rationale and Alternatives](#rationale-and-alternatives)
- [Work Itmes](#work-itmes)
- [Resources](#resources)

# Summary

With the built-in "protobuf" type, the protobuf data which is stored in the database can be rendered as a human-readable format directly. It can also be queried and indexed directly.

# Motivation

Currently, we have some interal columns that store protobuf data as bytes, the accessing of the data is through built-in functions `crdb_internal.pb_to_json`, `crdb_internal.json_to_pb`, etc. The built-in protobuf type can provide a more convenient way to access the data.

# Guide-level Explanation

## Column Definition

The `protobuf` type should be initialized with a file descriptor and a message type name.

```sql
CREATE TABLE t (
    id INT PRIMARY KEY,
    data PROTOBUF(file_descriptor='file_descriptor_content', message_type='message_type_name')
);
```

## Insert Data

The data can be inserted into the `protobuf` type column with the `protobuf` format. An error will be thrown if the data format is not correct.

```sql
INSERT INTO t VALUES (1, 'protobuf_data');
```

## Query Data

General query:

```sql
SELECT * FROM t;
----
+----+----------------+
| id | data           |
+----+----------------+
| 1  | protobuf_data  |
+----+----------------+
```

## Alter Column Type

The `protobuf` type can be altered to `bytes` type without any parameter. We can also alter the `bytes` type to `protobuf` type with the file descriptor and message type name.

```sql
ALTER TABLE t ALTER COLUMN data TYPE BYTES;
ALTER TABLE t ALTER COLUMN data TYPE PROTOBUF(file_descriptor='file_descriptor_content', message_type='message_type_name');
```

## Alter Protobuf Definition

The `protobuf` type can be altered to change the file descriptor and message type name, only when the updated type
definition is compatible with the original one.

```sql
ALTER TABLE t ALTER COLUMN data TYPE PROTOBUF(file_descriptor='new_file_descriptor_content', message_type='new_message_type_name');
```

# Reference-level Explanation

# Drawbacks

# Rationale and Alternatives

# Work Itmes

- [ ] Implement the built-in `protobuf` type.
- [ ] Implement the `PROTOBUF` type definition in the parser.
- [ ] Privode the tool to convert existing `bytes` type to `protobuf` type.

# Resources

- [cpustejovsky/filedescriptorjson: Wrapper Code to Convert a protobuf binary to a JSON message based on the FileDescritpro](https://github.com/cpustejovsky/filedescriptorjson)