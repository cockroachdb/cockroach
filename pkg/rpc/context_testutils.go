// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// NewInsecureTestingContext creates an insecure rpc Context suitable for tests.
func NewInsecureTestingContext(clock *hlc.Clock, stopper *stop.Stopper) *Context {
	clusterID := uuid.MakeV4()
	return NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)
}

// NewInsecureTestingContextWithClusterID creates an insecure rpc Context
// suitable for tests. The context is given the provided cluster ID.
func NewInsecureTestingContextWithClusterID(
	clock *hlc.Clock, stopper *stop.Stopper, clusterID uuid.UUID,
) *Context {
	ctx := NewContext(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		&base.Config{Insecure: true},
		clock,
		stopper,
		&cluster.MakeTestingClusterSettings().Version,
	)
	// Ensure that tests using this test context and restart/shut down
	// their servers do not inadvertently start talking to servers from
	// unrelated concurrent tests.
	ctx.ClusterID.Set(context.TODO(), clusterID)
	return ctx
}
