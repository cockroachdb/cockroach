// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lsnutil

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/lsn"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// HLCToLSN converts a HLC to a LSN.
// It is in a separate package to prevent the `lsn` package importing `log`.
func HLCToLSN(h hlc.Timestamp) lsn.LSN {
	// TODO(#105130): correctly populate this field.
	return lsn.LSN(h.WallTime/int64(time.Millisecond)) << 32
}
