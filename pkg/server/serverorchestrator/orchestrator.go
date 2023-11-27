package serverorchestrator

import (
	"context"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
