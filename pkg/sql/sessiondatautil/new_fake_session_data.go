// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessiondatautil

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/clustermode"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
)

// NewFakeSessionData returns "fake" session data for use in internal queries
// that are not run on behalf of a user session, such as those run during the
// steps of background jobs and schema changes.
func NewFakeSessionData(sv *settings.Values) *sessiondata.SessionData {
	sd := &sessiondata.SessionData{
		SessionData: sessiondatapb.SessionData{
			// The database is not supposed to be needed in schema changes, as there
			// shouldn't be unqualified identifiers in backfills, and the pure functions
			// that need it should have already been evaluated.
			//
			// TODO(andrei): find a way to assert that this field is indeed not used.
			// And in fact it is used by `current_schemas()`, which, although is a pure
			// function, takes arguments which might be impure (so it can't always be
			// pre-evaluated).
			Database:      "",
			UserProto:     username.NodeUserName().EncodeProto(),
			VectorizeMode: sessiondatapb.VectorizeExecMode(clustermode.VectorizeClusterMode.Get(sv)),
			Internal:      true,
		},
		LocalOnlySessionData: sessiondatapb.LocalOnlySessionData{
			DistSQLMode: sessiondatapb.DistSQLExecMode(clustermode.DistSQLClusterExecMode.Get(sv)),
		},
		SearchPath:    sessiondata.DefaultSearchPathForUser(username.NodeUserName()),
		SequenceState: sessiondata.NewSequenceState(),
		Location:      time.UTC,
	}

	return sd
}
