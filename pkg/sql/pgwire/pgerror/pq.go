// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !wasm

package pgerror

import (
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

// fullErrorFromPQ detects if the error is a pq.Error and, if so, formats it
// according to the scheme used for FullError.
func fullErrorFromPQ(err error) (string, bool) {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		return "", false
	}
	return formatMsgHintDetail("pq", pqErr.Message, pqErr.Hint, pqErr.Detail), true
}
