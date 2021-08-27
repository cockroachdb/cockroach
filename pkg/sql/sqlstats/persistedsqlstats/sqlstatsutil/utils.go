// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlstatsutil

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// DatumToUint64 Convert a bytes datum to uint64.
func DatumToUint64(d tree.Datum) (uint64, error) {
	b := []byte(tree.MustBeDBytes(d))

	_, val, err := encoding.DecodeUint64Ascending(b)
	if err != nil {
		return 0, err
	}

	return val, nil
}
