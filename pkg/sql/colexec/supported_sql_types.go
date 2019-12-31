// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import "github.com/cockroachdb/cockroach/pkg/sql/types"

// allSupportedSQLTypes is a slice of all SQL types that the vectorized engine
// currently supports. It should be kept in sync with typeconv.FromColumnType().
var allSupportedSQLTypes = []types.T{
	*types.Bool,
	*types.Bytes,
	*types.Date,
	*types.Decimal,
	*types.Int2,
	*types.Int4,
	*types.Int,
	*types.Oid,
	*types.Float,
	*types.Float4,
	*types.String,
	*types.Uuid,
	*types.Timestamp,
	*types.TimestampTZ,
}
