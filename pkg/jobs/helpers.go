// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import "github.com/cockroachdb/cockroach/pkg/jobs/jobspb"

// ResetConstructors resets the registered Resumer constructors.
func ResetConstructors() func() {
	globalMu.Lock()
	defer globalMu.Unlock()
	old := make(map[jobspb.Type]Constructor)
	for k, v := range globalMu.constructors {
		old[k] = v
	}
	return func() {
		globalMu.Lock()
		defer globalMu.Unlock()
		globalMu.constructors = old
	}
}

// TestingWrapResumerConstructor injects a wrapper around resumer creation for
// the specified job type.
func (r *Registry) TestingWrapResumerConstructor(typ jobspb.Type, wrap func(Resumer) Resumer) {
	r.creationKnobs.Store(typ, wrap)
}
