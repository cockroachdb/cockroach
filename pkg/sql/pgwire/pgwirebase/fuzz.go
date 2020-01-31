// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build gofuzz

package pgwirebase

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/lib/pq/oid"
)

var (
	timeCtx = tree.NewParseTimeContext(timeutil.Now())
	// Compile a slice of all oids.
	oids = func() []oid.Oid {
		var ret []oid.Oid
		for oid := range types.OidToType {
			ret = append(ret, oid)
		}
		return ret
	}()
)

func FuzzDecodeOidDatum(data []byte) int {
	if len(data) < 2 {
		return 0
	}

	id := oids[int(data[1])%len(oids)]
	code := FormatCode(data[0]) % (FormatBinary + 1)
	b := data[2:]

	_, err := DecodeOidDatum(timeCtx, id, code, b)
	if err != nil {
		return 0
	}
	return 1
}
