// Copyright 2018 The Cockroach Authors.
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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

type planClearable interface {
	// Clear is meant to release as many resources taken by a plan as
	// possible but leave the plan "alive", not losing information so
	// that future re-execution remains possible.
	// This includes clearing memory monitor accounts, clearing
	// row containers, etc.
	Clear(ctx context.Context)
}
type planClosable interface {
	// Close is meant to release all the resources not released by
	// Clear(). This includes e.g. releasing a plan node to a sync.Pool.
	// After Close, a plan is fully unusable.
	Close(ctx context.Context)
}

func clearPlan(ctx context.Context, plan planNode) { return cleanupPlan(ctx, plan, true, false) }
func closePlan(ctx context.Context, plan planNode) { return cleanupPlan(ctx, plan, true, true) }

func cleanupPlan(ctx context.Context, plan planNode, doClear, doClose bool) {
	o := planObserver{
		leaveNode: func(_ string, n planNode) error {
			if doClear {
				if c, ok := n.(planClearable); ok {
					c.Clear(params.ctx)
				}
			}
			if doClose {
				if c, ok := n.(planClosable); ok {
					c.Close(params.ctx)
				}
			}
			return nil
		},
	}
	if err := walkPlan(params.ctx, plan, o); err != nil {
		// walkPlan should never error out. If it does, don't drop the
		// error on the floor.
		log.Errorf(params.ctx, errors.Wrapf("error while closing plan: %v", err))
	}
}
