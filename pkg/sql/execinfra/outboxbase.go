// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfra

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// GetDistSQLClientForOutbox is a shared function between the rowexec and
// colexec outboxes. It attempts to dial the destination ignoring the breaker,
// up to the given timeout and returns the connection or an error.
// This connection attempt is retried since failure results in a query error.
// In the past, we have seen cases where a gateway node, n1, would send a flow
// request to n2, but n2 would be unable to connect back to n1 due to this
// connection attempt failing.
// Retrying here alleviates these flakes and causes no impact to the end
// user, since the receiver at the other end will hang for
// SettingFlowStreamTimeout waiting for a successful connection attempt.
func GetDistSQLClientForOutbox(
	ctx context.Context,
	dialer rpcbase.NodeDialerNoBreaker,
	sqlInstanceID base.SQLInstanceID,
	timeout time.Duration,
) (client execinfrapb.RPCDistSQLClient, err error) {
	firstConnectionAttempt := timeutil.Now()
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		client, err = execinfrapb.DialDistSQLClientNoBreaker(dialer, ctx, roachpb.NodeID(sqlInstanceID), rpcbase.DefaultClass)
		if err == nil || timeutil.Since(firstConnectionAttempt) > timeout {
			break
		}
	}
	return
}
