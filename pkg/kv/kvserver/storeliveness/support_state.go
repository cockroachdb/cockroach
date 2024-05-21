// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storeliveness

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
)

type supportState struct {
	// A local or remote store to heartbeat.
	storeID              storelivenesspb.StoreIdent
	heartbeatTimerHandle callbackSchedulerHandle
	// Add support for and from data structures here.
}

func (ss *supportState) supportFor() (Epoch, bool) {
	// Respond based on the state in hbs.
	return 0, false
}

func (ss *supportState) supportFrom() (Epoch, Expiration, bool) {
	// Respond based on the state in hbs.
	return 0, Expiration{WallTime: 0, Logical: 0}, false
}

func (ss *supportState) handleHeartbeat(epoch Epoch, exp Expiration) (bool, error) {
	// Respond based on the state in hbs.
	return true, nil
}

func (ss *supportState) handleHeartbeatResponse(ack bool) error {
	// Respond based on the state in hbs.
	return nil
}
