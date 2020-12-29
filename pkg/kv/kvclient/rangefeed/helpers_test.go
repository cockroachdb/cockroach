// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

// NewDBAdapter allows tests to construct a dbAdapter.
var NewDBAdapter = newDBAdapter

// NewFactoryWithDB allows tests to construct a factory with an injected db.
var NewFactoryWithDB = newFactory

// KVDB forwards the definition of kvDB to tests.
type KVDB = kvDB

// SetTargetScanBytes is exposed for testing.
func (dbc *dbAdapter) SetTargetScanBytes(limit int64) {
	dbc.targetScanBytes = limit
}
