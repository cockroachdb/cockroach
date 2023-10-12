// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package constants

const (
	// ObservabilityDatabaseName is the name of the obs database.
	//
	// We use the special character "$" and capitals because we want to
	// minimize the risk that this database name will overlap with any
	// database a customer may have created already.
	ObservabilityDatabaseName = "$OBS"
)
