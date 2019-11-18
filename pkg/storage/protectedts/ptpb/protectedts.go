// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptpb

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// NewRecord creates a new record with a random UUID.
func NewRecord(
	ts hlc.Timestamp, mode ProtectionMode, metaType string, meta []byte, spans ...roachpb.Span,
) Record {
	return Record{
		ID:        uuid.MakeV4(),
		Timestamp: ts,
		Mode:      mode,
		MetaType:  metaType,
		Meta:      meta,
		Spans:     spans,
	}
}
