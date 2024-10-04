// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package cdcevent facilitates conversion from low level roachpb.KeyValue into a higher
level Row.  This package abstracts away the low level catalog objects
(catalog.TableDescriptor, catalog.ColumnDescriptor, etc) so that the rest of the cdc code
can operate on a higher level Row object representing the row of decoded datums.

KVs arriving from kvfeed are decoded using Decoder interface into Row object.
Eventually, this package will contain logic to perform projections and filtering of KV events.
Decoder hides the complexity of converting low level KVs into encoded datums (EncDatumRow).

Row should be used by the rest of the cdcpipeline to encode and emit data into the sink.
Row provides access to the underlying *decoded* datums via various ForEach* methods -- such as
ForEachColumn, or ForEachKeyColumn, etc.
*/
package cdcevent
