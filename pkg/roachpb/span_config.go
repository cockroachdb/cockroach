// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

// Equal compares two span config entries.
func (e SpanConfigEntry) Equal(other SpanConfigEntry) bool {
	return e.Span.Equal(other.Span) && e.Config.Equal(other.Config)
}
