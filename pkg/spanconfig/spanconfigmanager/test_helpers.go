// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigmanager

import "context"

// TestingCreateAndStartJobIfNoneExists is a wrapper around
// createAndStartJobIfNoneExists for testing it.
func (m *Manager) TestingCreateAndStartJobIfNoneExists(ctx context.Context) (bool, error) {
	return m.createAndStartJobIfNoneExists(ctx)
}
