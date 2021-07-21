// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// Entry encapsulates a Span <-> Config pair.
type Entry struct {
	roachpb.Span

	Config Config
}

// MakeEntry constructs an entry using the given Span and Config pair.
func MakeEntry(span roachpb.Span, zone Config) Entry {
	return Entry{
		Span:   span,
		Config: zone,
	}
}
