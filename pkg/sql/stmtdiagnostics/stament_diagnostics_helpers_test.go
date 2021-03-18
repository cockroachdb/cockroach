// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stmtdiagnostics

import "context"

// InsertRequestInternal exposes the form of insert which returns the request ID
// as an int64 to tests in this package.
func (r *Registry) InsertRequestInternal(ctx context.Context, fprint string) (int64, error) {
	id, err := r.insertRequestInternal(ctx, fprint)
	return int64(id), err
}

// PollingInterval is exposed to override in tests.
var PollingInterval = pollingInterval
