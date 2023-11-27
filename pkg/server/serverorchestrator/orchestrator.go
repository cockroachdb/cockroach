// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package serverorchestrator

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// A ServerOrchestrator allows the SQL-layer to check on the status of
// on-demand servers.
type ServerOrchestrator interface {
	// CheckRunningServer returns an error if there isn't a server
	// ready for the given tenant.
	CheckRunningServer(context.Context, roachpb.TenantName) (<-chan struct{}, error)
}

var (
	// ErrTenantServerNotReady is returned from GetServer when the
	// ServerOrchestrator knows about a tenant server but it is not yet
	// fully started.
	ErrTenantServerNotReady error = errors.New("tenant server not ready")
	// ErrNoTenantServerRunning is returned from GetServer when the
	// ServerOrchestrator has no record of the given tenant.
	ErrNoTenantServerRunning error = errors.New("no server for tenant")
)
