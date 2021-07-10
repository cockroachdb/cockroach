// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantdirsvr

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"google.golang.org/grpc"
)

// Make sure that TestDirectoryServer implements the DirectoryServer interface.
var _ tenant.DirectoryServer = (*TestDirectoryServer)(nil)

// Process stores information about a running tenant process.
type Process struct {
	Stopper *stop.Stopper
	Cmd     *exec.Cmd
	SQL     net.Addr
}

// NewSubStopper creates a new stopper that will be stopped when either the
// parent is stopped or its own Stop is called. The code is slightly more
// complicated that simply calling NewStopper followed by AddCloser since there
// is a possibility that between the two calls, the parent stopper completes a
// stop and then the leak detection may find a leaked stopper.
func NewSubStopper(parentStopper *stop.Stopper) *stop.Stopper {
	mu := &syncutil.Mutex{}
	var subStopper *stop.Stopper
	parentStopper.AddCloser(stop.CloserFn(func() {
		mu.Lock()
		defer mu.Unlock()
		if subStopper == nil {
			subStopper = stop.NewStopper()
		}
		subStopper.Stop(context.Background())
	}))
	mu.Lock()
	defer mu.Unlock()
	if subStopper == nil {
		subStopper = stop.NewStopper()
	}
	return subStopper
}

// TestDirectoryServer is a directory server implementation that is used for
// testing.
type TestDirectoryServer struct {
	stopper             *stop.Stopper
	grpcServer          *grpc.Server
	cockroachExecutable string
	// TenantStarterFunc will be used to launch a new tenant process.
	TenantStarterFunc func(ctx context.Context, tenantID uint64) (*Process, error)

	// When both mutexes need to be held, the locking should always be proc
	// first and listen second.
	proc struct {
		syncutil.RWMutex
		processByAddrByTenantID map[uint64]map[net.Addr]*Process
	}
	listen struct {
		syncutil.RWMutex
		eventListeners *list.List
	}
}

// New will create a new server.
func New(stopper *stop.Stopper) (*TestDirectoryServer, error) {
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
	dir.TenantStarterFunc = dir.startTenantLocked
	dir.proc.processByAddrByTenantID = map[uint64]map[net.Addr]*Process{}
	dir.listen.eventListeners = list.New()
	stopper.AddCloser(stop.CloserFn(func() { dir.grpcServer.GracefulStop() }))
	tenant.RegisterDirectoryServer(dir.grpcServer, dir)
	return dir, nil
}

// Stopper returns the stopper that can be used to shut down the test directory
// server.
func (s *TestDirectoryServer) Stopper() *stop.Stopper {
	return s.stopper
}

// Get a tenant's list of pods and the process information for each pod.
func (s *TestDirectoryServer) Get(id roachpb.TenantID) (result map[net.Addr]*Process) {
	result = make(map[net.Addr]*Process)
	s.proc.RLock()
	defer s.proc.RUnlock()
	processes, ok := s.proc.processByAddrByTenantID[id.ToUint64()]
	if ok {
		for k, v := range processes {
			result[k] = v
		}
	}
	return
}

// GetTenant returns tenant metadata for a given ID. Hard coded to return every
// tenant's cluster name as "tenant-cluster"
func (s *TestDirectoryServer) GetTenant(
	_ context.Context, _ *tenant.GetTenantRequest,
) (*tenant.GetTenantResponse, error) {
	return &tenant.GetTenantResponse{
		ClusterName: "tenant-cluster",
	}, nil
}

// ListPods returns a list of tenant process pods as well as status of
// the processes.
func (s *TestDirectoryServer) ListPods(
	ctx context.Context, req *tenant.ListPodsRequest,
) (*tenant.ListPodsResponse, error) {
	ctx = logtags.AddTag(ctx, "tenant", req.TenantID)
	s.proc.RLock()
	defer s.proc.RUnlock()
	return s.listLocked(ctx, req)
}

// WatchPods returns a new stream, that can be used to monitor server
// activity.
func (s *TestDirectoryServer) WatchPods(
	_ *tenant.WatchPodsRequest, server tenant.Directory_WatchPodsServer,
) error {
	select {
	case <-s.stopper.ShouldQuiesce():
		return context.Canceled
	default:
	}
	// Make the channel with a small buffer to allow for a burst of notifications
	// and a slow receiver.
	c := make(chan *tenant.WatchPodsResponse, 10)
	s.listen.Lock()
	elem := s.listen.eventListeners.PushBack(c)
	s.listen.Unlock()
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
						s.listen.Lock()
						s.listen.eventListeners.Remove(elem)
						close(c)
						s.listen.Unlock()
						break out
					}
				case <-s.stopper.ShouldQuiesce():
					s.listen.Lock()
					s.listen.eventListeners.Remove(elem)
					close(c)
					s.listen.Unlock()
					break out
				}
			}
		})
	return err
}

// Drain sends out DRAINING pod notifications for each process managed by the
// test directory. This causes the proxy to start enforcing short idle
// connection timeouts in order to drain the connections to the pod.
func (s *TestDirectoryServer) Drain() {
	s.proc.RLock()
	defer s.proc.RUnlock()

	for tenantID, processByAddr := range s.proc.processByAddrByTenantID {
		for addr := range processByAddr {
			s.listen.RLock()
			defer s.listen.RUnlock()
			s.notifyEventListenersLocked(&tenant.WatchPodsResponse{
				Pod: &tenant.Pod{
					TenantID: tenantID,
					Addr:     addr.String(),
					State:    tenant.DRAINING,
				},
			})
		}
	}
}

func (s *TestDirectoryServer) notifyEventListenersLocked(req *tenant.WatchPodsResponse) {
	for e := s.listen.eventListeners.Front(); e != nil; {
		select {
		case e.Value.(chan *tenant.WatchPodsResponse) <- req:
			e = e.Next()
		default:
			// The receiver is unable to consume fast enough. Close the channel and
			// remove it from the list.
			eToClose := e
			e = e.Next()
			close(eToClose.Value.(chan *tenant.WatchPodsResponse))
			s.listen.eventListeners.Remove(eToClose)
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

	s.proc.Lock()
	defer s.proc.Unlock()

	lst, err := s.listLocked(ctx, &tenant.ListPodsRequest{TenantID: req.TenantID})
	if err != nil {
		return nil, err
	}
	if len(lst.Pods) == 0 {
		process, err := s.TenantStarterFunc(ctx, req.TenantID)
		if err != nil {
			return nil, err
		}
		s.registerInstanceLocked(req.TenantID, process)
		process.Stopper.AddCloser(stop.CloserFn(func() {
			s.deregisterInstance(req.TenantID, process.SQL)
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
	processByAddr, ok := s.proc.processByAddrByTenantID[req.TenantID]
	if !ok {
		return &tenant.ListPodsResponse{}, nil
	}
	resp := tenant.ListPodsResponse{}
	for addr := range processByAddr {
		resp.Pods = append(resp.Pods, &tenant.Pod{Addr: addr.String()})
	}
	return &resp, nil
}

func (s *TestDirectoryServer) registerInstanceLocked(tenantID uint64, process *Process) {
	processByAddr, ok := s.proc.processByAddrByTenantID[tenantID]
	if !ok {
		processByAddr = map[net.Addr]*Process{}
		s.proc.processByAddrByTenantID[tenantID] = processByAddr
	}
	processByAddr[process.SQL] = process

	s.listen.RLock()
	defer s.listen.RUnlock()
	s.notifyEventListenersLocked(&tenant.WatchPodsResponse{
		Pod: &tenant.Pod{
			TenantID: tenantID,
			Addr:     process.SQL.String(),
			State:    tenant.RUNNING,
		},
	})
}

func (s *TestDirectoryServer) deregisterInstance(tenantID uint64, sql net.Addr) {
	s.proc.Lock()
	defer s.proc.Unlock()
	processByAddr, ok := s.proc.processByAddrByTenantID[tenantID]
	if !ok {
		return
	}

	if _, ok = processByAddr[sql]; ok {
		delete(processByAddr, sql)

		s.listen.RLock()
		defer s.listen.RUnlock()
		s.notifyEventListenersLocked(&tenant.WatchPodsResponse{
			Pod: &tenant.Pod{
				TenantID: tenantID,
				Addr:     sql.String(),
				State:    tenant.DELETING,
			},
		})
	}
}

// startTenantLocked is the default tenant process startup logic that runs the
// cockroach db executable out of process.
func (s *TestDirectoryServer) startTenantLocked(
	ctx context.Context, tenantID uint64,
) (*Process, error) {
	// A hackish way to have the sql tenant process listen on known ports.
	sql, err := net.Listen("tcp", "")
	if err != nil {
		return nil, err
	}
	http, err := net.Listen("tcp", "")
	if err != nil {
		return nil, err
	}
	process := &Process{SQL: sql.Addr()}
	args := []string{
		"mt", "start-sql", "--kv-addrs=127.0.0.1:26257", "--idle-exit-after=30s",
		fmt.Sprintf("--sql-addr=%s", sql.Addr().String()),
		fmt.Sprintf("--http-addr=%s", http.Addr().String()),
		fmt.Sprintf("--tenant-id=%d", tenantID),
		"--insecure",
	}
	if err = sql.Close(); err != nil {
		return nil, err
	}
	if err = http.Close(); err != nil {
		return nil, err
	}

	c := exec.Command(s.cockroachExecutable, args...)
	process.Cmd = c
	c.Env = append(os.Environ(), "COCKROACH_TRUST_CLIENT_PROVIDED_SQL_REMOTE_ADDR=true")

	if c.Stdout != nil {
		return nil, errors.New("exec: Stdout already set")
	}
	if c.Stderr != nil {
		return nil, errors.New("exec: Stderr already set")
	}
	var b bytes.Buffer
	c.Stdout = &b
	c.Stderr = &b
	err = c.Start()
	if err != nil {
		return nil, err
	}
	process.Stopper = NewSubStopper(s.stopper)
	process.Stopper.AddCloser(stop.CloserFn(func() {
		_ = c.Process.Kill()
		s.deregisterInstance(tenantID, process.SQL)
	}))
	err = s.stopper.RunAsyncTask(ctx, "cmd-wait", func(ctx context.Context) {
		if err := c.Wait(); err != nil {
			log.Infof(ctx, "finished %s with err %s", process.Cmd.Args, err)
			log.Infof(ctx, "output %s", b.Bytes())
			return
		}
		log.Infof(ctx, "finished %s with success", process.Cmd.Args)
		process.Stopper.Stop(ctx)
	})
	if err != nil {
		return nil, err
	}

	// Need to wait here for the spawned cockroach tenant process to get ready.
	// Ideally - we want to check that it is up and connected to the KV host
	// before we return.
	time.Sleep(500 * time.Millisecond)
	return process, nil
}
