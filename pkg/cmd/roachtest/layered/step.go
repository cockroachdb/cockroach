// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package layered

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
)

// Step is a step in a sequence of operations against a cluster. Steps have to
// be cooperative, i.e. they need to consume only a reasonable amount of
// resources (i.e. no stress testing, intentional overload, or cluster
// operations such as stopping nodes).
//
// Steps that are not Idempotent() will only ever be invoked once, and against a
// cluster that is (or should be) fully functional. Note that a cluster in which
// nodes are cleanly terminated or are in the process of being added qualify as
// fully functional, and in particular not all nodes may be reachable.
// TODO(tbg): getting the above right will require some care to both semantics
// and UX. For example, a workload that doesn't tolerate errors (which is how
// we want to run most of the time) is prone to spurious "bogus" errors when
// nodes restart (for example circuit breaker errors). Of course these bogus
// errors should not bubble up! But currently they occasionally do and we don't
// want systemic issues to affect many steps randomly all of the time unless
// we deliberately embark on a push to fix them.
//
// Steps that are Resilient() may be invoked against a cluster that is in an
// arbitrarily bad shape. They are not expected to run successfully in that
// case. However, they are expected to run successfully if conditions are such
// that Resilient() Steps would currently be permissible, i.e. if the cluster
// is (supposed to be) healthy.
type Step interface {
	Supporter
	String() string
	Owner() registry.Owner
	Idempotent() bool
	// Run invokes the Step. If the Step returns without invoking the Fataler, it
	// is considered successful.
	//
	// TODO(tbg): should pass a to-be-created metricschecker in here to make it
	// easy for the step to check metrics.
	// TODO(tbg): should thin out the Env interface here as a Step is not
	// allowed to start and stop nodes, etc. Also should be more opinionated
	// about which node to run workloads on, etc. We can rely on convention for
	// all of the above at first.
	Run(_ context.Context, r *rand.Rand, _ Fataler, _ SeqEnv)
}

// An OrchestrationStep is a special Step.
type OrchestrationStep interface {
	Step
	Orchestrator() bool
}
