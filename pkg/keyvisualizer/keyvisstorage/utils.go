// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keyvisstorage

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func datumToNative(datum tree.Datum) (interface{}, error) {
	datum = tree.UnwrapDOidWrapper(datum)
	if datum == tree.DNull {
		return nil, nil
	}
	switch d := datum.(type) {
	case *tree.DUuid:
		return d.UUID.String(), nil
	case *tree.DTimestamp:
		return d.Time, nil
	case *tree.DBytes:
		return []byte(*d), nil
	case *tree.DInt:
		return int64(*d), nil
	}
	return nil, errors.Newf("cannot handle type %T", datum)
}
