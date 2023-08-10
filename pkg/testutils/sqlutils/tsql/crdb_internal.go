// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tsql

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

func PBToJSON(message string, e tree.Expr) tree.Expr {
	return Func("crdb_internal.pb_to_json", message, e)
}

func JSONToPB(message string, e tree.Expr, defaults bool) tree.Expr {
	return Func("crdb_internal.json_to_pb", message, e, defaults)
}

func UnsafeUpsertDescriptor(id, desc tree.Expr, force bool) tree.Expr {
	return Func("crdb_internal.unsafe_upsert_descriptor", id, desc, force)
}
