// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storagepb

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// IsLive returns whether the node is considered live at the given time with the
// given clock offset.
func (l *Liveness) IsLive(now hlc.Timestamp) bool {
	expiration := hlc.Timestamp(l.Expiration)
	return now.Less(expiration)
}

// IsDead returns true if the liveness expired more than threshold ago.
func (l *Liveness) IsDead(now hlc.Timestamp, threshold time.Duration) bool {
	deadAsOf := hlc.Timestamp(l.Expiration).GoTime().Add(threshold)
	return !now.GoTime().Before(deadAsOf)
}
