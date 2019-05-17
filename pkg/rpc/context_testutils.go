// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
