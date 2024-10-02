// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantdirsvr

import (
	"container/list"
	"context"
	"net"
	"os"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/logtags"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Make sure that TestDirectoryServer implements the DirectoryServer interface.
var _ tenant.DirectoryServer = (*TestDirectoryServer)(nil)

// NewSubStopper creates a new stopper that will be stopped when either the
// parent is stopped or its own Stop is called.
func NewSubStopper(parentStopper *stop.Stopper) *stop.Stopper {
	subStopper := stop.NewStopper(stop.WithTracer(parentStopper.Tracer()))
	parentStopper.AddCloser(stop.CloserFn(func() {
		subStopper.Stop(context.Background())
	}))
	return subStopper
}

// Process stores information about a running tenant process.
type Process struct {
	Stopper *stop.Stopper
	SQLAddr string
}

// TestDirectoryServer is a directory server implementation that is used for
// testing.
type TestDirectoryServer struct {
	stopper             *stop.Stopper
	grpcServer          *grpc.Server
	cockroachExecutable string
	// tenantStarterFunc will be used to launch a new tenant process.
	tenantStarterFunc TenantStarterFunc

	mu struct {
		syncutil.Mutex
		// A map of tenantIDs to sql server addresses
		processByAddrByTenantID map[uint64][]Process
		eventListeners          *list.List
	}
}

type TenantStarterFunc func(ctx context.Context, stopper *stop.Stopper, tenantID uint64) (string, error)

// New will create a new server.
func New(stopper *stop.Stopper, createTenant TenantStarterFunc) (*TestDirectoryServer, error) {
	// Determine the path to cockroach executable.
	cockroachExecutable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	dir := &TestDirectoryServer{
		grpcServer:          grpc.NewServer(),
		stopper:             stopper,
		cockroachExecutable: cockroachExecutable,
	}
	dir.tenantStarterFunc = createTenant
	dir.mu.processByAddrByTenantID = map[uint64][]Process{}
	dir.mu.eventListeners = list.New()
	stopper.AddCloser(stop.CloserFn(dir.grpcServer.GracefulStop))
	tenant.RegisterDirectoryServer(dir.grpcServer, dir)
	return dir, nil
}

// Stopper returns the stopper that can be used to shut down the test directory
// server.
func (s *TestDirectoryServer) Stopper() *stop.Stopper {
	return s.stopper
}

// Get a tenant's list of pods and the process information for each pod.
func (s *TestDirectoryServer) Get(id roachpb.TenantID) (result []Process) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result = append(result, s.mu.processByAddrByTenantID[id.ToUint64()]...)
	return
}

// serverShutdownErr is returned when TestDirectoryServer is no longer accepting
// new requests.
var serverShutdownErr = status.Error(codes.Unavailable, "server shutting down")

// StartTenant will forcefully start a new tenant pod
// instance. This may be useful to test the behavior when more
// than one tenant is running.
func (s *TestDirectoryServer) StartTenant(ctx context.Context, id roachpb.TenantID) error {
	select {
	case <-s.stopper.ShouldQuiesce():
		return context.Canceled
	default:
	}

	ctx = logtags.AddTag(ctx, "tenant", id)

	tenantStopper := NewSubStopper(s.stopper)

	sqlAddr, err := s.tenantStarterFunc(ctx, tenantStopper, id.ToUint64())
	if err != nil {
		return err
	}

	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.registerInstanceLocked(id.ToUint64(), Process{
			Stopper: tenantStopper,
			SQLAddr: sqlAddr,
		})
	}()

	tenantStopper.AddCloser(stop.CloserFn(func() {
		s.deregisterInstance(id.ToUint64(), sqlAddr)
	}))

	return nil
}

// GetTenant returns tenant metadata for a given ID. Hard coded to return every
// tenant's cluster name as "tenant-cluster"
func (s *TestDirectoryServer) GetTenant(
	_ context.Context, req *tenant.GetTenantRequest,
) (*tenant.GetTenantResponse, error) {
	return &tenant.GetTenantResponse{
		Tenant: &tenant.Tenant{
			TenantID:          req.TenantID,
			ClusterName:       "tenant-cluster",
			AllowedCIDRRanges: []string{"0.0.0.0/0"},
		},
	}, nil
}

// ListPods returns a list of tenant process pods as well as status of
// the processes.
func (s *TestDirectoryServer) ListPods(
	ctx context.Context, req *tenant.ListPodsRequest,
) (*tenant.ListPodsResponse, error) {
	ctx = logtags.AddTag(ctx, "tenant", req.TenantID)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listLocked(ctx, req)
}

// WatchPods returns a new stream, that can be used to monitor server
// activity.
func (s *TestDirectoryServer) WatchPods(
	_ *tenant.WatchPodsRequest, server tenant.Directory_WatchPodsServer,
) error {
	select {
	case <-s.stopper.ShouldQuiesce():
		return serverShutdownErr
	default:
	}
	// Make the channel with a small buffer to allow for a burst of notifications
	// and a slow receiver.
	c := make(chan *tenant.WatchPodsResponse, 10)

	elem := func() *list.Element {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.mu.eventListeners.PushBack(c)
	}()
	err := s.stopper.RunTask(context.Background(), "watch-pods-server",
		func(ctx context.Context) {
		out:
			for {
				select {
				case e, ok := <-c:
					if !ok {
						break out
					}
					if err := server.Send(e); err != nil {
						s.removeEventListenerAndCloseChan(elem, c)
						break out
					}
				case <-s.stopper.ShouldQuiesce():
					s.removeEventListenerAndCloseChan(elem, c)
					break out
				}
			}
		})
	return err
}

// WatchTenants returns a new stream, that can be used to monitor server
// activity. This is a no-op since this directory's implementation has been
// deprecated.
func (s *TestDirectoryServer) WatchTenants(
	_ *tenant.WatchTenantsRequest, server tenant.Directory_WatchTenantsServer,
) error {
	// Insted of returning right away, we block until context is done.
	// This prevents the proxy server from constantly trying to establish
	// a watch in test environments, causing spammy logs.
	<-server.Context().Done()
	return nil
}

// Drain sends out DRAINING pod notifications for each process managed by the
// test directory. This causes the proxy to start enforcing short idle
// connection timeouts in order to drain the connections to the pod.
func (s *TestDirectoryServer) Drain() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for tenantID, processByAddr := range s.mu.processByAddrByTenantID {
		for _, process := range processByAddr {
			s.notifyEventListenersLocked(&tenant.WatchPodsResponse{
				Pod: &tenant.Pod{
					TenantID:       tenantID,
					Addr:           process.SQLAddr,
					State:          tenant.DRAINING,
					StateTimestamp: timeutil.Now(),
				},
			})
		}
	}
}

func (s *TestDirectoryServer) notifyEventListenersLocked(req *tenant.WatchPodsResponse) {
	for e := s.mu.eventListeners.Front(); e != nil; {
		select {
		case e.Value.(chan *tenant.WatchPodsResponse) <- req:
			e = e.Next()
		default:
			// The receiver is unable to consume fast enough. Close the channel and
			// remove it from the list.
			eToClose := e
			e = e.Next()
			close(eToClose.Value.(chan *tenant.WatchPodsResponse))
			s.mu.eventListeners.Remove(eToClose)
		}
	}
}

// EnsurePod will ensure that there is either an already active tenant
// process or it will start a new one. It will return an error if starting a new
// tenant process is impossible.
func (s *TestDirectoryServer) EnsurePod(
	ctx context.Context, req *tenant.EnsurePodRequest,
) (*tenant.EnsurePodResponse, error) {
	select {
	case <-s.stopper.ShouldQuiesce():
		return nil, context.Canceled
	default:
	}

	ctx = logtags.AddTag(ctx, "tenant", req.TenantID)

	tenantStopper, sqlAddr, err := func() (*stop.Stopper, string, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		lst, err := s.listLocked(ctx, &tenant.ListPodsRequest{TenantID: req.TenantID})
		if err != nil {
			return nil, "", err
		}
		if len(lst.Pods) != 0 {
			return nil, "", nil
		}
		tenantStopper := NewSubStopper(s.stopper)
		sqlAddr, err := s.tenantStarterFunc(ctx, tenantStopper, req.TenantID)
		if err != nil {
			return nil, "", err
		}
		s.registerInstanceLocked(req.TenantID, Process{
			Stopper: tenantStopper,
			SQLAddr: sqlAddr,
		})
		return tenantStopper, sqlAddr, nil
	}()
	if err != nil {
		return nil, err
	}

	if tenantStopper != nil {
		// Registering a stopper closer that removes the instance must occur
		// outside of the mutex.
		tenantStopper.AddCloser(stop.CloserFn(func() {
			s.deregisterInstance(req.TenantID, sqlAddr)
		}))
	}

	return &tenant.EnsurePodResponse{}, nil
}

// Serve requests on the given listener.
func (s *TestDirectoryServer) Serve(listener net.Listener) error {
	return s.grpcServer.Serve(listener)
}

func (s *TestDirectoryServer) listLocked(
	_ context.Context, req *tenant.ListPodsRequest,
) (*tenant.ListPodsResponse, error) {
	processByAddr, ok := s.mu.processByAddrByTenantID[req.TenantID]
	if !ok {
		return &tenant.ListPodsResponse{}, nil
	}
	resp := tenant.ListPodsResponse{}
	for _, process := range processByAddr {
		resp.Pods = append(resp.Pods, &tenant.Pod{
			TenantID:       req.TenantID,
			Addr:           process.SQLAddr,
			State:          tenant.RUNNING,
			StateTimestamp: timeutil.Now(),
		})
	}
	return &resp, nil
}

func (s *TestDirectoryServer) registerInstanceLocked(tenantID uint64, process Process) {
	s.mu.processByAddrByTenantID[tenantID] = append(s.mu.processByAddrByTenantID[tenantID], process)
	s.notifyEventListenersLocked(&tenant.WatchPodsResponse{
		Pod: &tenant.Pod{
			TenantID:       tenantID,
			Addr:           process.SQLAddr,
			State:          tenant.RUNNING,
			StateTimestamp: timeutil.Now(),
		},
	})
}

func (s *TestDirectoryServer) deregisterInstance(tenantID uint64, sqlAddr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	processByAddr := s.mu.processByAddrByTenantID[tenantID]

	var remaining []Process
	for _, process := range processByAddr {
		if sqlAddr == process.SQLAddr {
			s.notifyEventListenersLocked(&tenant.WatchPodsResponse{
				Pod: &tenant.Pod{
					TenantID:       tenantID,
					Addr:           sqlAddr,
					State:          tenant.DELETING,
					StateTimestamp: timeutil.Now(),
				},
			})
		} else {
			remaining = append(remaining, process)
		}
	}

	if len(remaining) == 0 {
		delete(s.mu.processByAddrByTenantID, tenantID)
	} else {
		s.mu.processByAddrByTenantID[tenantID] = remaining
	}
}

func (s *TestDirectoryServer) removeEventListenerAndCloseChan(
	elem *list.Element, c chan *tenant.WatchPodsResponse,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.eventListeners.Remove(elem)
	close(c)
}
