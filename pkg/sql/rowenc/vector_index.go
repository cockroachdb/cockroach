// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowenc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// VectorIndexEncodingHelper is used to provide the information needed to encode
// index entries for vector indexes. Unlike row.VectorIndexUpdateHelper, it is
// used either for an index put *or* an index del, not both.
//
// This information is tracked separately from the table row because these
// values are not stored in the table, and instead are materialized by a vector
// search prior to executing the mutation.
type VectorIndexEncodingHelper struct {
	// PartitionKeys indicates the target vector index partition for each vector
	// index. Null values indicate a no-op.
	PartitionKeys map[descpb.IndexID]tree.Datum
	// QuantizedVecs contains the quantized and encoded vectors for each indexed
	// vector column. It may be unset if the helper is being used for a delete
	// operation.
	QuantizedVecs map[descpb.IndexID]tree.Datum
}

// EmptyVectorIndexEncodingHelper is a helper with no partition keys or encoded
// vectors.
var EmptyVectorIndexEncodingHelper = VectorIndexEncodingHelper{}
