// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package valueside

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

func Encode2(
	appendTo []byte, colID descpb.ColumnID, val tree.Datum, scratch []byte,
) ([]byte, error) {
	if val == tree.DNull {
		return encoding.EncodeNullValue(appendTo, uint32(colID)), nil
	}
	switch val.ResolvedType().Family() {
	case types.IntFamily:
		appendTo = encoding.EncodeUint32Ascending(appendTo, uint32(colID))
		appendTo = encoding.EncodeUint32Ascending(appendTo, uint32(encoding.Int))
		return encoding.EncodeUint64Ascending(appendTo, uint64(*val.(*tree.DInt))), nil
	default:
		return nil, errors.AssertionFailedf("not supported yet %s", val.ResolvedType())
	}
}
