// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slinstance

import "context"

// ClearSessionForTest is used in test to
// immediately delete the current session.
func (l *Instance) ClearSessionForTest(ctx context.Context) {
	l.clearSession(ctx)
}
