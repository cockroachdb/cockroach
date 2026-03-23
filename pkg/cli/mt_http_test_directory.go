// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenantdirsvr"
	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var mtHTTPTestDirectorySvr = &cobra.Command{
	Use:   "http-test-directory",
	Short: "start a test directory server that exposes an HTTP API to manage pod lifecycles",
	Long: `
http-test-directory is like test-directory, but instead of managing tenant SQL instances as
processes on the local machine, it expects instances to be created externally and provides
an HTTP API to update pod state.

HTTP API endpoints:
  POST /api/create-tenant  - Create a tenant
  POST /api/add-pod        - Add a pod to a tenant (RUNNING state)
  POST /api/remove-pod     - Remove a pod from a tenant
  POST /api/drain-pod      - Drain a pod (DRAINING state for connection migration)
`,
	RunE: clierrorplus.MaybeDecorateError(runStaticTestDirectory),
	Args: cobra.NoArgs,
}

func runStaticTestDirectory(cmd *cobra.Command, args []string) (returnErr error) {
	ctx := context.Background()
	stopper, err := ClearStoresAndSetupLoggingForMTCommands(cmd, ctx)
	if err != nil {
		return err
	}
	defer stopper.Stop(ctx)

	log.Ops.Infof(ctx, "Starting static test directory server (GRPC:%d, HTTP:%d)",
		httpTestDirectorySvrContext.grpcPort, httpTestDirectorySvrContext.httpPort)

	dir := tenantdirsvr.NewTestStaticDirectoryServer(stopper, nil)
	if err := dir.Start(ctx); err != nil {
		return err
	}

	srv := &staticDirectoryServer{dir: dir}

	// Start GRPC server for proxy connections
	grpcServer := grpc.NewServer()
	tenant.RegisterDirectoryServer(grpcServer, dir)

	grpcListener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", httpTestDirectorySvrContext.grpcPort))
	if err != nil {
		return err
	}
	stopper.AddCloser(stop.CloserFn(func() { grpcServer.GracefulStop() }))

	if err := stopper.RunAsyncTask(ctx, "serve-grpc", func(ctx context.Context) {
		log.Ops.Infof(ctx, "GRPC server listening at %s", grpcListener.Addr())
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Ops.Infof(ctx, "GRPC server stopped: %v", err)
		}
	}); err != nil {
		return err
	}

	// Start HTTP control API for test management
	http.HandleFunc("/api/create-tenant", srv.handleCreateTenant)
	http.HandleFunc("/api/add-pod", srv.handleAddPod)
	http.HandleFunc("/api/remove-pod", srv.handleRemovePod)
	http.HandleFunc("/api/drain-pod", srv.handleDrainPod)

	httpListener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", httpTestDirectorySvrContext.httpPort))
	if err != nil {
		return err
	}
	stopper.AddCloser(stop.CloserFn(func() { _ = httpListener.Close() }))

	if err := stopper.RunAsyncTask(ctx, "serve-http", func(ctx context.Context) {
		log.Ops.Infof(ctx, "HTTP API listening at %s", httpListener.Addr())
		if err := http.Serve(httpListener, nil); err != nil {
			log.Ops.Infof(ctx, "HTTP server stopped: %v", err)
		}
	}); err != nil {
		return err
	}

	log.Ops.Infof(ctx, "Directory server ready - use Ctrl+C to stop")

	// Wait for shutdown signal
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, DrainSignals...)

	select {
	case <-stopper.ShouldQuiesce():
		<-stopper.IsStopped()
	case sig := <-signalCh:
		log.Ops.Infof(ctx, "received signal '%s', shutting down", sig)
		// Close listeners to unblock Serve() calls
		_ = grpcListener.Close()
		_ = httpListener.Close()
		stopper.Stop(ctx)
		<-stopper.IsStopped()
		if sig == os.Interrupt {
			returnErr = errors.New("interrupted")
		}
	case <-log.FatalChan():
		stopper.Stop(ctx)
		select {}
	}

	return returnErr
}

type staticDirectoryServer struct {
	dir *tenantdirsvr.TestStaticDirectoryServer
}

func (s *staticDirectoryServer) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TenantID          uint64   `json:"tenant_id"`
		Name              string   `json:"name"`
		AllowedCIDRRanges []string `json:"allowed_cidr_ranges"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	tenantID, err := roachpb.MakeTenantID(req.TenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.dir.CreateTenant(tenantID, &tenant.Tenant{
		TenantID:          req.TenantID,
		ClusterName:       req.Name,
		AllowedCIDRRanges: req.AllowedCIDRRanges,
	})

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *staticDirectoryServer) handleAddPod(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TenantID uint64 `json:"tenant_id"`
		Addr     string `json:"addr"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	tenantID, err := roachpb.MakeTenantID(req.TenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	success := s.dir.AddPod(tenantID, &tenant.Pod{
		TenantID:       req.TenantID,
		Addr:           req.Addr,
		State:          tenant.RUNNING,
		StateTimestamp: timeutil.Now(),
	})

	if !success {
		http.Error(w, "Failed to add pod", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *staticDirectoryServer) handleRemovePod(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TenantID uint64 `json:"tenant_id"`
		Addr     string `json:"addr"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	tenantID, err := roachpb.MakeTenantID(req.TenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	success := s.dir.RemovePod(tenantID, req.Addr)

	if !success {
		http.Error(w, "Failed to remove pod", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *staticDirectoryServer) handleDrainPod(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TenantID uint64 `json:"tenant_id"`
		Addr     string `json:"addr"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	tenantID, err := roachpb.MakeTenantID(req.TenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	success := s.dir.DrainPod(tenantID, req.Addr)

	if !success {
		http.Error(w, "Failed to drain pod", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
