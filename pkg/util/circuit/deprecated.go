// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package circuit

import "github.com/cockroachdb/errors"

// Ready ...
// TODO(tbg): remove, use Err() == nil instead.
func (b *BreakerV2) Ready() bool {
	return b.Err() == nil
}

// Fail ...
// TODO(tbg): remove, use Report instead.
func (b *BreakerV2) Fail(err error) {
	b.Report(err)
}

// Success ...
// TODO(tbg): remove this and the callers.
func (b *BreakerV2) Success() {
	b.Reset()
}

// Trip ...
// TODO(tbg): remove this.
func (b *BreakerV2) Trip() {
	// Trip until the breaker trips, but give up at some point
	// because it turns out that in some tests we configure
	// breakers to never ever trip:
	//
	// https://github.com/cockroachdb/cockroach/blob/885075b9c16ae04f537ffe4a0cfe7113c28c4811/pkg/server/testserver.go#L1304-L1310
	//
	// These tests in turn are probably not getting what they think
	// they are getting when they call Trip(), so they should be
	// adjusted.
	for i := 0; i < 10 && b.Err() == nil; i++ {
		b.Report(errors.New("breaker tripped on purpose"))
	}
}

// Tripped ...
// TODO(tbg): remove this, use Err() != nil instead.
func (b *BreakerV2) Tripped() bool {
	return b.Err() != nil
}
