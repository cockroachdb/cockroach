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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

var (
	errZeroTimestamp = errors.New("invalid zero value timestamp")
	errZeroID        = errors.New("invalid zero value ID")
	errEmptySpans    = errors.Errorf("invalid empty set of spans")
	errInvalidMeta   = errors.Errorf("invalid Meta with empty MetaType")
)

// Validate returns nil if the Record is valid. A valid record has a
// non-zero ID and Timestamp as well as non-empty set of spans. A valid record
// may not contain Meta unless it has a non-zero MetaType.
func (r *Record) Validate() error {
	if r.Timestamp == (hlc.Timestamp{}) {
		return errZeroTimestamp
	}
	if r.ID == uuid.Nil {
		return errZeroID
	}
	if len(r.Spans) == 0 {
		return errEmptySpans
	}
	if len(r.Meta) > 0 && len(r.MetaType) == 0 {
		return errInvalidMeta
	}
	return nil
}
