// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachtestutil

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// The following functions are augmented basic cluster functions but there tends
// to be common networking issues that cause test failures and require putting
// a retry block around them.

var canaryRetryOptions = retry.Options{
	InitialBackoff: 10 * time.Second,
	Multiplier:     2,
	MaxBackoff:     5 * time.Minute,
	MaxRetries:     10,
}

// RepeatRunE is the same function as c.RunE but with an automatic retry loop.
func RepeatRunE(
	ctx context.Context,
	t Fataler,
	c cluster.Cluster,
	node option.NodeListOption,
	operation string,
	args ...string,
) error {
	var lastError error
	for attempt, r := 0, retry.StartWithCtx(ctx, canaryRetryOptions); r.Next(); {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if t.Failed() {
			return fmt.Errorf("test has failed")
		}
		attempt++
		t.L().Printf("attempt %d - %s", attempt, operation)
		lastError = c.RunE(ctx, option.WithNodes(node), args...)
		if lastError != nil {
			t.L().Printf("error - retrying: %s", lastError)
			continue
		}
		return nil
	}
	return errors.Wrapf(lastError, "all attempts failed for %s", operation)
}
