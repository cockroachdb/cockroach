// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"google.golang.org/grpc"
)

// Dialer is used for dialing based on node IDs. It extracts out the single
// method that outboxes need from nodedialer.Dialer so that we can mock it
// in tests outside of this package.
type Dialer interface {
	DialNoBreaker(context.Context, roachpb.NodeID, rpc.ConnectionClass) (*grpc.ClientConn, error)
}

// GetConnForOutbox is a shared function between the rowexec and colexec
// outboxes. It attempts to dial the destination ignoring the breaker, up to the
// given timeout and returns the connection or an error.
// This connection attempt is retried since failure results in a query error. In
// the past, we have seen cases where a gateway node, n1, would send a flow
// request to n2, but n2 would be unable to connect back to n1 due to this
// connection attempt failing.
// Retrying here alleviates these flakes and causes no impact to the end
// user, since the receiver at the other end will hang for
// SettingFlowStreamTimeout waiting for a successful connection attempt.
func GetConnForOutbox(
	ctx context.Context, dialer Dialer, nodeID roachpb.NodeID, timeout time.Duration,
) (conn *grpc.ClientConn, err error) {
	firstConnectionAttempt := timeutil.Now()
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		conn, err = dialer.DialNoBreaker(ctx, nodeID, rpc.DefaultClass)
		if err == nil || timeutil.Since(firstConnectionAttempt) > timeout {
			break
		}
	}
	return
}
