// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// DecodeDatum decodes the given bytes slice into a datum of the given type. It
// returns an error if the decoding is not valid, or if there are any remaining
// bytes.
func DecodeDatum(datumAlloc *rowenc.DatumAlloc, typ *types.T, data []byte) (tree.Datum, error) {
	datum, rem, err := rowenc.DecodeTableValue(datumAlloc, typ, data)
	if err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"error decoding %d bytes", errors.Safe(len(data)))
	}
	if len(rem) != 0 {
		return nil, errors.AssertionFailedf(
			"%d trailing bytes in encoded value", errors.Safe(len(rem)))
	}
	return datum, nil
}
