// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigptsreader

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// TestingEmptyProtectedTSReader returns a ProtectedTSReader which contains no records
// and is always up-to date. This is intended for testing.
func TestingEmptyProtectedTSReader(c *hlc.Clock) spanconfig.ProtectedTSReader {
	return (*emptyProtectedTSReader)(c)
}

type emptyProtectedTSReader hlc.Clock

// GetProtectionTimestamps is part of the spanconfig.ProtectedTSReader
// interface.
func (r *emptyProtectedTSReader) GetProtectionTimestamps(
	context.Context, roachpb.Span,
) ([]hlc.Timestamp, hlc.Timestamp, error) {
	return nil, (*hlc.Clock)(r).Now(), nil
}
