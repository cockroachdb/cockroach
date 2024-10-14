// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspectz

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/inspectz/inspectzpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
)

// Unsupported is an inspectzpb.InspectzServer that only returns "unsupported"
// errors.
type Unsupported struct{}

var _ inspectzpb.InspectzServer = Unsupported{}

// KVFlowController is part of the inspectzpb.InspectzServer interface.
func (u Unsupported) KVFlowController(
	ctx context.Context, request *kvflowinspectpb.ControllerRequest,
) (*kvflowinspectpb.ControllerResponse, error) {
	return nil, errorutil.UnsupportedUnderClusterVirtualization(errorutil.FeatureNotAvailableToNonSystemTenantsIssue)
}

// KVFlowHandles is part of the inspectzpb.InspectzServer interface.
func (u Unsupported) KVFlowHandles(
	ctx context.Context, request *kvflowinspectpb.HandlesRequest,
) (*kvflowinspectpb.HandlesResponse, error) {
	return nil, errorutil.UnsupportedUnderClusterVirtualization(errorutil.FeatureNotAvailableToNonSystemTenantsIssue)
}

// KVFlowControllerV2 is part of the inspectzpb.InspectzServer interface.
func (u Unsupported) KVFlowControllerV2(
	ctx context.Context, request *kvflowinspectpb.ControllerRequest,
) (*kvflowinspectpb.ControllerResponse, error) {
	return nil, errorutil.UnsupportedUnderClusterVirtualization(errorutil.FeatureNotAvailableToNonSystemTenantsIssue)
}

// KVFlowHandlesV2 is part of the inspectzpb.InspectzServer interface.
func (u Unsupported) KVFlowHandlesV2(
	ctx context.Context, request *kvflowinspectpb.HandlesRequest,
) (*kvflowinspectpb.HandlesResponse, error) {
	return nil, errorutil.UnsupportedUnderClusterVirtualization(errorutil.FeatureNotAvailableToNonSystemTenantsIssue)
}
