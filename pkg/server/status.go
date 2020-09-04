// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"go.etcd.io/etcd/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	// Default Maximum number of log entries returned.
	defaultMaxLogEntries = 1000

	// stackTraceApproxSize is the approximate size of a goroutine stack trace.
	stackTraceApproxSize = 1024

	// statusPrefix is the root of the cluster statistics and metrics API.
	statusPrefix = "/_status/"

	// statusVars exposes prometheus metrics for monitoring consumption.
	statusVars = statusPrefix + "vars"

	// raftStateDormant is used when there is no known raft state.
	raftStateDormant = "StateDormant"

	// maxConcurrentRequests is the maximum number of RPC fan-out requests
	// that will be made at any point of time.
	maxConcurrentRequests = 100

	// omittedKeyStr is the string returned in place of a key when keys aren't
	// permitted in responses.
	omittedKeyStr = "omitted (due to the 'server.remote_debugging.mode' setting)"
)

var (
	// Pattern for local used when determining the node ID.
	localRE = regexp.MustCompile(`(?i)local`)

	// Error used to convey that remote debugging is needs to be enabled for an
	// endpoint to be usable.
	remoteDebuggingErr = grpcstatus.Error(
		codes.PermissionDenied, "not allowed (due to the 'server.remote_debugging.mode' setting)")

	// Counter to count accesses to the prometheus vars endpoint /_status/vars .
	telemetryPrometheusVars = telemetry.GetCounterOnce("monitoring.prometheus.vars")

	// Counter to count accesses to the health check endpoint /health .
	telemetryHealthCheck = telemetry.GetCounterOnce("monitoring.health.details")
)

type metricMarshaler interface {
	json.Marshaler
	PrintAsText(io.Writer) error
}

func propagateGatewayMetadata(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		return metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

// A statusServer provides a RESTful status API.
type statusServer struct {
	log.AmbientContext

	st                       *cluster.Settings
	cfg                      *base.Config
	admin                    *adminServer
	db                       *kv.DB
	gossip                   *gossip.Gossip
	metricSource             metricMarshaler
	nodeLiveness             *kvserver.NodeLiveness
	storePool                *kvserver.StorePool
	rpcCtx                   *rpc.Context
	stores                   *kvserver.Stores
	stopper                  *stop.Stopper
	sessionRegistry          *sql.SessionRegistry
	si                       systemInfoOnce
	stmtDiagnosticsRequester StmtDiagnosticsRequester
	internalExecutor         *sql.InternalExecutor
}

// StmtDiagnosticsRequester is the interface into *stmtdiagnostics.Registry
// used by AdminUI endpoints.
type StmtDiagnosticsRequester interface {

	// InsertRequest adds an entry to system.statement_diagnostics_requests for
	// tracing a query with the given fingerprint. Once this returns, calling
	// shouldCollectDiagnostics() on the current node will return true for the given
	// fingerprint.
	InsertRequest(ctx context.Context, fprint string) error
}

// newStatusServer allocates and returns a statusServer.
func newStatusServer(
	ambient log.AmbientContext,
	st *cluster.Settings,
	cfg *base.Config,
	adminServer *adminServer,
	db *kv.DB,
	gossip *gossip.Gossip,
	metricSource metricMarshaler,
	nodeLiveness *kvserver.NodeLiveness,
	storePool *kvserver.StorePool,
	rpcCtx *rpc.Context,
	stores *kvserver.Stores,
	stopper *stop.Stopper,
	sessionRegistry *sql.SessionRegistry,
	internalExecutor *sql.InternalExecutor,
) *statusServer {
	ambient.AddLogTag("status", nil)
	server := &statusServer{
		AmbientContext:   ambient,
		st:               st,
		cfg:              cfg,
		admin:            adminServer,
		db:               db,
		gossip:           gossip,
		metricSource:     metricSource,
		nodeLiveness:     nodeLiveness,
		storePool:        storePool,
		rpcCtx:           rpcCtx,
		stores:           stores,
		stopper:          stopper,
		sessionRegistry:  sessionRegistry,
		internalExecutor: internalExecutor,
	}

	return server
}

// setStmtDiagnosticsRequester is used to provide a StmtDiagnosticsRequester to
// the status server. This cannot be done at construction time because the
// implementation of StmtDiagnosticsRequester depends on an executor which in
// turn depends on the statusServer.
func (s *statusServer) setStmtDiagnosticsRequester(sr StmtDiagnosticsRequester) {
	s.stmtDiagnosticsRequester = sr
}

// RegisterService registers the GRPC service.
func (s *statusServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterStatusServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse
// proxy) that proxies HTTP requests to the appropriate gRPC endpoints.
func (s *statusServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	ctx = s.AnnotateCtx(ctx)
	return serverpb.RegisterStatusHandler(ctx, mux, conn)
}

func (s *statusServer) parseNodeID(nodeIDParam string) (roachpb.NodeID, bool, error) {
	// No parameter provided or set to local.
	if len(nodeIDParam) == 0 || localRE.MatchString(nodeIDParam) {
		return s.gossip.NodeID.Get(), true, nil
	}

	id, err := strconv.ParseInt(nodeIDParam, 0, 32)
	if err != nil {
		return 0, false, errors.Wrap(err, "node ID could not be parsed")
	}
	nodeID := roachpb.NodeID(id)
	return nodeID, nodeID == s.gossip.NodeID.Get(), nil
}

func (s *statusServer) dialNode(
	ctx context.Context, nodeID roachpb.NodeID,
) (serverpb.StatusClient, error) {
	addr, err := s.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return nil, err
	}
	conn, err := s.rpcCtx.GRPCDialNode(addr.String(), nodeID,
		rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return nil, err
	}
	return serverpb.NewStatusClient(conn), nil
}

// Gossip returns gossip network status.
func (s *statusServer) Gossip(
	ctx context.Context, req *serverpb.GossipRequest,
) (*gossip.InfoStatus, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	if !debug.GatewayRemoteAllowed(ctx, s.st) {
		return nil, remoteDebuggingErr
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	if local {
		infoStatus := s.gossip.GetInfoStatus()
		return &infoStatus, nil
	}
	status, err := s.dialNode(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	return status.Gossip(ctx, req)
}

func (s *statusServer) EngineStats(
	ctx context.Context, req *serverpb.EngineStatsRequest,
) (*serverpb.EngineStatsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.EngineStats(ctx, req)
	}

	resp := new(serverpb.EngineStatsResponse)
	err = s.stores.VisitStores(func(store *kvserver.Store) error {
		engineStatsInfo := serverpb.EngineStatsInfo{
			StoreID:              store.Ident.StoreID,
			TickersAndHistograms: nil,
			EngineType:           store.Engine().Type(),
		}

		switch e := store.Engine().(type) {
		case *storage.RocksDB:
			tickersAndHistograms, err := e.GetTickersAndHistograms()
			if err != nil {
				return grpcstatus.Errorf(codes.Internal, err.Error())
			}
			engineStatsInfo.TickersAndHistograms = tickersAndHistograms
		}

		resp.Stats = append(resp.Stats, engineStatsInfo)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Allocator returns simulated allocator info for the ranges on the given node.
func (s *statusServer) Allocator(
	ctx context.Context, req *serverpb.AllocatorRequest,
) (*serverpb.AllocatorResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	// TODO(a-robinson): It'd be nice to allow this endpoint and just avoid
	// logging range start/end keys in the simulated allocator runs.
	if !debug.GatewayRemoteAllowed(ctx, s.st) {
		return nil, remoteDebuggingErr
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.Allocator(ctx, req)
	}

	output := new(serverpb.AllocatorResponse)
	err = s.stores.VisitStores(func(store *kvserver.Store) error {
		// All ranges requested:
		if len(req.RangeIDs) == 0 {
			// Use IterateRangeDescriptors to read from the engine only
			// because it's already exported.
			err := kvserver.IterateRangeDescriptors(ctx, store.Engine(),
				func(desc roachpb.RangeDescriptor) (bool, error) {
					rep, err := store.GetReplica(desc.RangeID)
					if err != nil {
						if errors.HasType(err, (*roachpb.RangeNotFoundError)(nil)) {
							return true, nil // continue
						}
						return true, err
					}
					if !rep.OwnsValidLease(ctx, store.Clock().Now()) {
						return false, nil
					}
					allocatorSpans, err := store.AllocatorDryRun(ctx, rep)
					if err != nil {
						return true, err
					}
					output.DryRuns = append(output.DryRuns, &serverpb.AllocatorDryRun{
						RangeID: desc.RangeID,
						Events:  recordedSpansToTraceEvents(allocatorSpans),
					})
					return false, nil
				})
			return err
		}

		// Specific ranges requested:
		for _, rid := range req.RangeIDs {
			rep, err := store.GetReplica(rid)
			if err != nil {
				// Not found: continue.
				continue
			}
			if !rep.OwnsValidLease(ctx, store.Clock().Now()) {
				continue
			}
			allocatorSpans, err := store.AllocatorDryRun(ctx, rep)
			if err != nil {
				return err
			}
			output.DryRuns = append(output.DryRuns, &serverpb.AllocatorDryRun{
				RangeID: rep.RangeID,
				Events:  recordedSpansToTraceEvents(allocatorSpans),
			})
		}
		return nil
	})
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, err.Error())
	}
	return output, nil
}

func recordedSpansToTraceEvents(spans []tracing.RecordedSpan) []*serverpb.TraceEvent {
	var output []*serverpb.TraceEvent
	var buf bytes.Buffer
	for _, sp := range spans {
		for _, entry := range sp.Logs {
			event := &serverpb.TraceEvent{
				Time: entry.Time,
			}
			if len(entry.Fields) == 1 {
				event.Message = entry.Fields[0].Value
			} else {
				buf.Reset()
				for i, f := range entry.Fields {
					if i != 0 {
						buf.WriteByte(' ')
					}
					fmt.Fprintf(&buf, "%s:%v", f.Key, f.Value)
				}
				event.Message = buf.String()
			}
			output = append(output, event)
		}
	}
	return output
}

// AllocatorRange returns simulated allocator info for the requested range.
func (s *statusServer) AllocatorRange(
	ctx context.Context, req *serverpb.AllocatorRangeRequest,
) (*serverpb.AllocatorRangeResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	// TODO(a-robinson): It'd be nice to allow this endpoint and just avoid
	// logging range start/end keys in the simulated allocator runs.
	if !debug.GatewayRemoteAllowed(ctx, s.st) {
		return nil, remoteDebuggingErr
	}

	isLiveMap := s.nodeLiveness.GetIsLiveMap()
	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *serverpb.AllocatorResponse
		err    error
	}
	responses := make(chan nodeResponse)
	// TODO(bram): consider abstracting out this repeated pattern.
	for nodeID := range isLiveMap {
		nodeID := nodeID
		if err := s.stopper.RunAsyncTask(
			ctx,
			"server.statusServer: requesting remote Allocator simulation",
			func(ctx context.Context) {
				_ = contextutil.RunWithTimeout(ctx, "allocator range", base.NetworkTimeout, func(ctx context.Context) error {
					status, err := s.dialNode(ctx, nodeID)
					var allocatorResponse *serverpb.AllocatorResponse
					if err == nil {
						allocatorRequest := &serverpb.AllocatorRequest{
							RangeIDs: []roachpb.RangeID{roachpb.RangeID(req.RangeId)},
						}
						allocatorResponse, err = status.Allocator(ctx, allocatorRequest)
					}
					response := nodeResponse{
						nodeID: nodeID,
						resp:   allocatorResponse,
						err:    err,
					}

					select {
					case responses <- response:
						// Response processed.
					case <-ctx.Done():
						// Context completed, response no longer needed.
					}
					return nil
				})
			}); err != nil {
			return nil, grpcstatus.Errorf(codes.Internal, err.Error())
		}
	}

	errs := make(map[roachpb.NodeID]error)
	for remainingResponses := len(isLiveMap); remainingResponses > 0; remainingResponses-- {
		select {
		case resp := <-responses:
			if resp.err != nil {
				errs[resp.nodeID] = resp.err
				continue
			}
			if len(resp.resp.DryRuns) > 0 {
				return &serverpb.AllocatorRangeResponse{
					NodeID: resp.nodeID,
					DryRun: resp.resp.DryRuns[0],
				}, nil
			}
		case <-ctx.Done():
			return nil, grpcstatus.Errorf(codes.DeadlineExceeded, "request timed out")
		}
	}

	// We didn't get a valid simulated Allocator run. Just return whatever errors
	// we got instead. If we didn't even get any errors, then there is no active
	// leaseholder for the range.
	if len(errs) > 0 {
		var buf bytes.Buffer
		for nodeID, err := range errs {
			if buf.Len() > 0 {
				buf.WriteByte('\n')
			}
			fmt.Fprintf(&buf, "n%d: %s", nodeID, err)
		}
		return nil, grpcstatus.Errorf(codes.Internal, buf.String())
	}
	return &serverpb.AllocatorRangeResponse{}, nil
}

// Certificates returns the x509 certificates.
func (s *statusServer) Certificates(
	ctx context.Context, req *serverpb.CertificatesRequest,
) (*serverpb.CertificatesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	if s.cfg.Insecure {
		return nil, errors.New("server is in insecure mode, cannot examine certificates")
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.Certificates(ctx, req)
	}

	cm, err := s.rpcCtx.GetCertificateManager()
	if err != nil {
		return nil, err
	}

	// The certificate manager gives us a list of CertInfo objects to avoid
	// making security depend on serverpb.
	certs, err := cm.ListCertificates()
	if err != nil {
		return nil, err
	}

	cr := &serverpb.CertificatesResponse{}
	for _, cert := range certs {
		details := serverpb.CertificateDetails{}
		switch cert.FileUsage {
		case security.CAPem:
			details.Type = serverpb.CertificateDetails_CA
		case security.ClientCAPem:
			details.Type = serverpb.CertificateDetails_CLIENT_CA
		case security.UICAPem:
			details.Type = serverpb.CertificateDetails_UI_CA
		case security.NodePem:
			details.Type = serverpb.CertificateDetails_NODE
		case security.UIPem:
			details.Type = serverpb.CertificateDetails_UI
		case security.ClientPem:
			details.Type = serverpb.CertificateDetails_CLIENT
		default:
			return nil, errors.Errorf("unknown certificate type %v for file %s", cert.FileUsage, cert.Filename)
		}

		if cert.Error == nil {
			details.Data = cert.FileContents
			if err := extractCertFields(details.Data, &details); err != nil {
				details.ErrorMessage = err.Error()
			}
		} else {
			details.ErrorMessage = cert.Error.Error()
		}
		cr.Certificates = append(cr.Certificates, details)
	}

	return cr, nil
}

func formatCertNames(p pkix.Name) string {
	return fmt.Sprintf("CommonName=%s, Organization=%s", p.CommonName, strings.Join(p.Organization, ","))
}

func extractCertFields(contents []byte, details *serverpb.CertificateDetails) error {
	certs, err := security.PEMContentsToX509(contents)
	if err != nil {
		return err
	}

	for _, c := range certs {
		addresses := c.DNSNames
		for _, ip := range c.IPAddresses {
			addresses = append(addresses, ip.String())
		}

		extKeyUsage := make([]string, len(c.ExtKeyUsage))
		for i, eku := range c.ExtKeyUsage {
			extKeyUsage[i] = security.ExtKeyUsageToString(eku)
		}

		var pubKeyInfo string
		if rsaPub, ok := c.PublicKey.(*rsa.PublicKey); ok {
			pubKeyInfo = fmt.Sprintf("%d bit RSA", rsaPub.N.BitLen())
		} else if ecdsaPub, ok := c.PublicKey.(*ecdsa.PublicKey); ok {
			pubKeyInfo = fmt.Sprintf("%d bit ECDSA", ecdsaPub.Params().BitSize)
		} else {
			// go's x509 library does not support other types (so far).
			pubKeyInfo = fmt.Sprintf("unknown key type %T", c.PublicKey)
		}

		details.Fields = append(details.Fields, serverpb.CertificateDetails_Fields{
			Issuer:             formatCertNames(c.Issuer),
			Subject:            formatCertNames(c.Subject),
			ValidFrom:          c.NotBefore.UnixNano(),
			ValidUntil:         c.NotAfter.UnixNano(),
			Addresses:          addresses,
			SignatureAlgorithm: c.SignatureAlgorithm.String(),
			PublicKey:          pubKeyInfo,
			KeyUsage:           security.KeyUsageToString(c.KeyUsage),
			ExtendedKeyUsage:   extKeyUsage,
		})
	}
	return nil
}

// Details returns node details.
func (s *statusServer) Details(
	ctx context.Context, req *serverpb.DetailsRequest,
) (*serverpb.DetailsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.Details(ctx, req)
	}

	remoteNodeID := s.gossip.NodeID.Get()
	resp := &serverpb.DetailsResponse{
		NodeID:     remoteNodeID,
		BuildInfo:  build.GetInfo(),
		SystemInfo: s.si.systemInfo(ctx),
	}
	if addr, err := s.gossip.GetNodeIDAddress(remoteNodeID); err == nil {
		resp.Address = *addr
	}
	if addr, err := s.gossip.GetNodeIDSQLAddress(remoteNodeID); err == nil {
		resp.SQLAddress = *addr
	}

	return resp, nil
}

// GetFiles returns a list of files of type defined in the request.
func (s *statusServer) GetFiles(
	ctx context.Context, req *serverpb.GetFilesRequest,
) (*serverpb.GetFilesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	if !debug.GatewayRemoteAllowed(ctx, s.st) {
		return nil, remoteDebuggingErr
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.GetFiles(ctx, req)
	}

	var dir string
	switch req.Type {
	//TODO(ridwanmsharif): Serve logfiles so debug-zip can fetch them
	// intead of reading indididual entries.
	case serverpb.FileType_HEAP: // Requesting for saved Heap Profiles.
		dir = s.admin.server.cfg.HeapProfileDirName
	case serverpb.FileType_GOROUTINES: // Requesting for saved Goroutine dumps.
		dir = s.admin.server.cfg.GoroutineDumpDirName
	default:
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "unknown file type: %s", req.Type)
	}
	if dir == "" {
		return nil, grpcstatus.Errorf(codes.Unimplemented, "dump directory not configured: %s", req.Type)
	}
	var resp serverpb.GetFilesResponse
	for _, pattern := range req.Patterns {
		if err := checkFilePattern(pattern); err != nil {
			return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
		}
		filepaths, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return nil, grpcstatus.Errorf(codes.InvalidArgument, "bad pattern: %s", pattern)
		}

		for _, path := range filepaths {
			fileinfo, _ := os.Stat(path)
			var contents []byte
			if !req.ListOnly {
				contents, err = ioutil.ReadFile(path)
				if err != nil {
					return nil, grpcstatus.Errorf(codes.Internal, err.Error())
				}
			}
			resp.Files = append(resp.Files,
				&serverpb.File{Name: fileinfo.Name(), FileSize: fileinfo.Size(), Contents: contents})
		}
	}
	return &resp, err
}

// checkFilePattern checks if a pattern is acceptable for the GetFiles call.
// Only patterns to match filenames are acceptable, not more general paths.
func checkFilePattern(pattern string) error {
	if strings.Contains(pattern, string(os.PathSeparator)) {
		return errors.New("invalid pattern: cannot have path seperators")
	}
	return nil
}

// LogFilesList returns a list of available log files.
func (s *statusServer) LogFilesList(
	ctx context.Context, req *serverpb.LogFilesListRequest,
) (*serverpb.LogFilesListResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.LogFilesList(ctx, req)
	}
	log.Flush()
	logFiles, err := log.ListLogFiles()
	if err != nil {
		return nil, err
	}
	return &serverpb.LogFilesListResponse{Files: logFiles}, err
}

// LogFile returns a single log file.
func (s *statusServer) LogFile(
	ctx context.Context, req *serverpb.LogFileRequest,
) (*serverpb.LogEntriesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	if !debug.GatewayRemoteAllowed(ctx, s.st) {
		return nil, remoteDebuggingErr
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.LogFile(ctx, req)
	}

	// Determine how to redact.
	inputEditMode := log.SelectEditMode(req.Redact, req.KeepRedactable)

	// Ensure that the latest log entries are available in files.
	log.Flush()

	// Read the logs.
	reader, err := log.GetLogReader(req.File, true /* restricted */)
	if reader == nil || err != nil {
		return nil, fmt.Errorf("log file %s could not be opened: %s", req.File, err)
	}
	defer reader.Close()

	var entry log.Entry
	var resp serverpb.LogEntriesResponse
	decoder := log.NewEntryDecoder(reader, inputEditMode)
	for {
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		resp.Entries = append(resp.Entries, entry)
	}

	// Erase the redactable bit if requested by client.
	if !req.KeepRedactable {
		for i := range resp.Entries {
			resp.Entries[i].Redactable = false
		}
	}

	return &resp, nil
}

// parseInt64WithDefault attempts to parse the passed in string. If an empty
// string is supplied or parsing results in an error the default value is
// returned.  If an error does occur during parsing, the error is returned as
// well.
func parseInt64WithDefault(s string, defaultValue int64) (int64, error) {
	if len(s) == 0 {
		return defaultValue, nil
	}
	result, err := strconv.ParseInt(s, 10, 0)
	if err != nil {
		return defaultValue, err
	}
	return result, nil
}

// Logs returns the log entries parsed from the log files stored on
// the server. Log entries are returned in reverse chronological order. The
// following options are available:
// * "starttime" query parameter filters the log entries to only ones that
//   occurred on or after the "starttime". Defaults to a day ago.
// * "endtime" query parameter filters the log entries to only ones that
//   occurred before on on the "endtime". Defaults to the current time.
// * "pattern" query parameter filters the log entries by the provided regexp
//   pattern if it exists. Defaults to nil.
// * "max" query parameter is the hard limit of the number of returned log
//   entries. Defaults to defaultMaxLogEntries.
// To filter the log messages to only retrieve messages from a given level,
// use a pattern that excludes all messages at the undesired levels.
// (e.g. "^[^IW]" to only get errors, fatals and panics). An exclusive
// pattern is better because panics and some other errors do not use
// a prefix character.
func (s *statusServer) Logs(
	ctx context.Context, req *serverpb.LogsRequest,
) (*serverpb.LogEntriesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	if !debug.GatewayRemoteAllowed(ctx, s.st) {
		return nil, remoteDebuggingErr
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.Logs(ctx, req)
	}

	// Determine how to redact.
	inputEditMode := log.SelectEditMode(req.Redact, req.KeepRedactable)

	// Select the time interval.
	startTimestamp, err := parseInt64WithDefault(
		req.StartTime,
		timeutil.Now().AddDate(0, 0, -1).UnixNano())
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "StartTime could not be parsed: %s", err)
	}

	endTimestamp, err := parseInt64WithDefault(req.EndTime, timeutil.Now().UnixNano())
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "EndTime could not be parsed: %s", err)
	}

	if startTimestamp > endTimestamp {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "StartTime: %d should not be greater than endtime: %d", startTimestamp, endTimestamp)
	}

	maxEntries, err := parseInt64WithDefault(req.Max, defaultMaxLogEntries)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "Max could not be parsed: %s", err)
	}
	if maxEntries < 1 {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "Max: %d should be set to a value greater than 0", maxEntries)
	}

	var regex *regexp.Regexp
	if len(req.Pattern) > 0 {
		if regex, err = regexp.Compile(req.Pattern); err != nil {
			return nil, grpcstatus.Errorf(codes.InvalidArgument, "regex pattern could not be compiled: %s", err)
		}
	}

	// Ensure that the latest log entries are available in files.
	log.Flush()

	// Read the logs.
	entries, err := log.FetchEntriesFromFiles(
		startTimestamp, endTimestamp, int(maxEntries), regex, inputEditMode)
	if err != nil {
		return nil, err
	}

	// Erase the redactable bit if requested by client.
	if !req.KeepRedactable {
		for i := range entries {
			entries[i].Redactable = false
		}
	}

	return &serverpb.LogEntriesResponse{Entries: entries}, nil
}

// TODO(tschottdorf): significant overlap with /debug/pprof/goroutine, except
// that this one allows querying by NodeID.
//
// Stacks returns goroutine or thread stack traces.
func (s *statusServer) Stacks(
	ctx context.Context, req *serverpb.StacksRequest,
) (*serverpb.JSONResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.Stacks(ctx, req)
	}

	switch req.Type {
	case serverpb.StacksType_GOROUTINE_STACKS:
		bufSize := runtime.NumGoroutine() * stackTraceApproxSize
		for {
			buf := make([]byte, bufSize)
			length := runtime.Stack(buf, true)
			// If this wasn't large enough to accommodate the full set of
			// stack traces, increase by 2 and try again.
			if length == bufSize {
				bufSize = bufSize * 2
				continue
			}
			return &serverpb.JSONResponse{Data: buf[:length]}, nil
		}
	case serverpb.StacksType_THREAD_STACKS:
		return &serverpb.JSONResponse{Data: []byte(storage.ThreadStacks())}, nil
	default:
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "unknown stacks type: %s", req.Type)
	}
}

// TODO(tschottdorf): significant overlap with /debug/pprof/heap, except that
// this one allows querying by NodeID.
//
// Profile returns a heap profile.
func (s *statusServer) Profile(
	ctx context.Context, req *serverpb.ProfileRequest,
) (*serverpb.JSONResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.Profile(ctx, req)
	}

	switch req.Type {
	case serverpb.ProfileRequest_HEAP:
		p := pprof.Lookup("heap")
		if p == nil {
			return nil, grpcstatus.Errorf(codes.InvalidArgument, "unable to find profile: heap")
		}
		var buf bytes.Buffer
		if err := p.WriteTo(&buf, 0); err != nil {
			return nil, grpcstatus.Errorf(codes.Internal, err.Error())
		}
		return &serverpb.JSONResponse{Data: buf.Bytes()}, nil
	case serverpb.ProfileRequest_CPU:
		var buf bytes.Buffer
		if err := debug.CPUProfileDo(s.st, cluster.CPUProfileWithLabels, func() error {
			duration := 30 * time.Second
			if req.Seconds != 0 {
				duration = time.Duration(req.Seconds) * time.Second
			}
			if err := pprof.StartCPUProfile(&buf); err != nil {
				return err
			}
			defer pprof.StopCPUProfile()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(duration):
				return nil
			}
		}); err != nil {
			return nil, err
		}
		return &serverpb.JSONResponse{Data: buf.Bytes()}, nil
	default:
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "unknown profile: %s", req.Type)
	}
}

// Nodes returns all node statuses.
//
// The LivenessByNodeID in the response returns the known liveness
// information according to gossip. Nodes for which there is no gossip
// information will not have an entry. Clients can exploit the fact
// that status "UNKNOWN" has value 0 (the default) when accessing the
// map.
func (s *statusServer) Nodes(
	ctx context.Context, req *serverpb.NodesRequest,
) (*serverpb.NodesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)
	startKey := keys.StatusNodePrefix
	endKey := startKey.PrefixEnd()

	b := &kv.Batch{}
	b.Scan(startKey, endKey)
	if err := s.db.Run(ctx, b); err != nil {
		log.Errorf(ctx, "%v", err)
		return nil, grpcstatus.Errorf(codes.Internal, err.Error())
	}
	rows := b.Results[0].Rows

	resp := serverpb.NodesResponse{
		Nodes: make([]statuspb.NodeStatus, len(rows)),
	}
	for i, row := range rows {
		if err := row.ValueProto(&resp.Nodes[i]); err != nil {
			log.Errorf(ctx, "%v", err)
			return nil, grpcstatus.Errorf(codes.Internal, err.Error())
		}
	}

	clock := s.admin.server.clock
	resp.LivenessByNodeID = getLivenessStatusMap(s.nodeLiveness, clock.Now().GoTime(), s.st)

	return &resp, nil
}

// nodesStatusWithLiveness is like Nodes but for internal
// use within this package.
func (s *statusServer) nodesStatusWithLiveness(
	ctx context.Context,
) (map[roachpb.NodeID]nodeStatusWithLiveness, error) {
	nodes, err := s.Nodes(ctx, nil)
	if err != nil {
		return nil, err
	}
	clock := s.admin.server.clock
	statusMap := getLivenessStatusMap(s.nodeLiveness, clock.Now().GoTime(), s.st)
	ret := make(map[roachpb.NodeID]nodeStatusWithLiveness)
	for _, node := range nodes.Nodes {
		nodeID := node.Desc.NodeID
		livenessStatus := statusMap[nodeID]
		if livenessStatus == kvserverpb.NodeLivenessStatus_DECOMMISSIONED {
			// Skip over removed nodes.
			continue
		}
		ret[nodeID] = nodeStatusWithLiveness{
			NodeStatus:     node,
			livenessStatus: livenessStatus,
		}
	}
	return ret, nil
}

// nodeStatusWithLiveness combines a NodeStatus with a NodeLivenessStatus.
type nodeStatusWithLiveness struct {
	statuspb.NodeStatus
	livenessStatus kvserverpb.NodeLivenessStatus
}

// handleNodeStatus handles GET requests for a single node's status.
func (s *statusServer) Node(
	ctx context.Context, req *serverpb.NodeRequest,
) (*statuspb.NodeStatus, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)
	nodeID, _, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	key := keys.NodeStatusKey(nodeID)
	b := &kv.Batch{}
	b.Get(key)
	if err := s.db.Run(ctx, b); err != nil {
		log.Errorf(ctx, "%v", err)
		return nil, grpcstatus.Errorf(codes.Internal, err.Error())
	}

	var nodeStatus statuspb.NodeStatus
	if err := b.Results[0].Rows[0].ValueProto(&nodeStatus); err != nil {
		err = errors.Errorf("could not unmarshal NodeStatus from %s: %s", key, err)
		log.Errorf(ctx, "%v", err)
		return nil, grpcstatus.Errorf(codes.Internal, err.Error())
	}
	return &nodeStatus, nil
}

// Metrics return metrics information for the server specified.
func (s *statusServer) Metrics(
	ctx context.Context, req *serverpb.MetricsRequest,
) (*serverpb.JSONResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)
	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.Metrics(ctx, req)
	}
	return marshalJSONResponse(s.metricSource)
}

// RaftDebug returns raft debug information for all known nodes.
func (s *statusServer) RaftDebug(
	ctx context.Context, req *serverpb.RaftDebugRequest,
) (*serverpb.RaftDebugResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	nodes, err := s.Nodes(ctx, nil)
	if err != nil {
		return nil, err
	}

	mu := struct {
		syncutil.Mutex
		resp serverpb.RaftDebugResponse
	}{
		resp: serverpb.RaftDebugResponse{
			Ranges: make(map[roachpb.RangeID]serverpb.RaftRangeStatus),
		},
	}

	// Parallelize fetching of ranges to minimize total time.
	var wg sync.WaitGroup
	for _, node := range nodes.Nodes {
		wg.Add(1)
		nodeID := node.Desc.NodeID
		go func() {
			defer wg.Done()
			ranges, err := s.Ranges(ctx, &serverpb.RangesRequest{NodeId: nodeID.String(), RangeIDs: req.RangeIDs})

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				err := errors.Wrapf(err, "failed to get ranges from %d", nodeID)
				mu.resp.Errors = append(mu.resp.Errors, serverpb.RaftRangeError{Message: err.Error()})
				return
			}

			for _, rng := range ranges.Ranges {
				rangeID := rng.State.Desc.RangeID
				status, ok := mu.resp.Ranges[rangeID]
				if !ok {
					status = serverpb.RaftRangeStatus{
						RangeID: rangeID,
					}
				}
				status.Nodes = append(status.Nodes, serverpb.RaftRangeNode{
					NodeID: nodeID,
					Range:  rng,
				})
				mu.resp.Ranges[rangeID] = status
			}
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()

	// Check for errors.
	for i, rng := range mu.resp.Ranges {
		for j, node := range rng.Nodes {
			desc := node.Range.State.Desc
			// Check for whether replica should be GCed.
			containsNode := false
			for _, replica := range desc.Replicas().All() {
				if replica.NodeID == node.NodeID {
					containsNode = true
				}
			}
			if !containsNode {
				rng.Errors = append(rng.Errors, serverpb.RaftRangeError{
					Message: fmt.Sprintf("node %d not in range descriptor and should be GCed", node.NodeID),
				})
			}

			// Check for replica descs not matching.
			if j > 0 {
				prevDesc := rng.Nodes[j-1].Range.State.Desc
				if !desc.Equal(prevDesc) {
					prevNodeID := rng.Nodes[j-1].NodeID
					rng.Errors = append(rng.Errors, serverpb.RaftRangeError{
						Message: fmt.Sprintf("node %d range descriptor does not match node %d", node.NodeID, prevNodeID),
					})
				}
			}
			mu.resp.Ranges[i] = rng
		}
	}
	return &mu.resp, nil
}

type varsHandler struct {
	metricSource metricMarshaler
}

func (h varsHandler) handleVars(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(httputil.ContentTypeHeader, httputil.PlaintextContentType)
	err := h.metricSource.PrintAsText(w)
	if err != nil {
		log.Errorf(r.Context(), "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	telemetry.Inc(telemetryPrometheusVars)
}

func (s *statusServer) handleVars(w http.ResponseWriter, r *http.Request) {
	varsHandler{s.metricSource}.handleVars(w, r)
}

// Ranges returns range info for the specified node.
func (s *statusServer) Ranges(
	ctx context.Context, req *serverpb.RangesRequest,
) (*serverpb.RangesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.Ranges(ctx, req)
	}

	output := serverpb.RangesResponse{
		Ranges: make([]serverpb.RangeInfo, 0, s.stores.GetStoreCount()),
	}

	convertRaftStatus := func(raftStatus *raft.Status) serverpb.RaftState {
		if raftStatus == nil {
			return serverpb.RaftState{
				State: raftStateDormant,
			}
		}

		state := serverpb.RaftState{
			ReplicaID:      raftStatus.ID,
			HardState:      raftStatus.HardState,
			Applied:        raftStatus.Applied,
			Lead:           raftStatus.Lead,
			State:          raftStatus.RaftState.String(),
			Progress:       make(map[uint64]serverpb.RaftState_Progress),
			LeadTransferee: raftStatus.LeadTransferee,
		}

		for id, progress := range raftStatus.Progress {
			state.Progress[id] = serverpb.RaftState_Progress{
				Match:           progress.Match,
				Next:            progress.Next,
				Paused:          progress.IsPaused(),
				PendingSnapshot: progress.PendingSnapshot,
				State:           progress.State.String(),
			}
		}

		return state
	}

	includeRawKeys := debug.GatewayRemoteAllowed(ctx, s.st)

	constructRangeInfo := func(
		desc roachpb.RangeDescriptor, rep *kvserver.Replica, storeID roachpb.StoreID, metrics kvserver.ReplicaMetrics,
	) serverpb.RangeInfo {
		raftStatus := rep.RaftStatus()
		raftState := convertRaftStatus(raftStatus)
		leaseHistory := rep.GetLeaseHistory()
		var span serverpb.PrettySpan
		if includeRawKeys {
			span.StartKey = desc.StartKey.String()
			span.EndKey = desc.EndKey.String()
		} else {
			span.StartKey = omittedKeyStr
			span.EndKey = omittedKeyStr
		}
		state := rep.State()
		if !includeRawKeys {
			state.ReplicaState.Desc.StartKey = nil
			state.ReplicaState.Desc.EndKey = nil
		}
		return serverpb.RangeInfo{
			Span:          span,
			RaftState:     raftState,
			State:         state,
			SourceNodeID:  nodeID,
			SourceStoreID: storeID,
			LeaseHistory:  leaseHistory,
			Stats: serverpb.RangeStatistics{
				QueriesPerSecond: rep.QueriesPerSecond(),
				WritesPerSecond:  rep.WritesPerSecond(),
			},
			Problems: serverpb.RangeProblems{
				Unavailable:            metrics.Unavailable,
				LeaderNotLeaseHolder:   metrics.Leader && metrics.LeaseValid && !metrics.Leaseholder,
				NoRaftLeader:           !kvserver.HasRaftLeader(raftStatus) && !metrics.Quiescent,
				Underreplicated:        metrics.Underreplicated,
				Overreplicated:         metrics.Overreplicated,
				NoLease:                metrics.Leader && !metrics.LeaseValid && !metrics.Quiescent,
				QuiescentEqualsTicking: raftStatus != nil && metrics.Quiescent == metrics.Ticking,
				RaftLogTooLarge:        metrics.RaftLogTooLarge,
			},
			LatchesLocal:  metrics.LatchInfoLocal,
			LatchesGlobal: metrics.LatchInfoGlobal,
			LeaseStatus:   metrics.LeaseStatus,
			Quiescent:     metrics.Quiescent,
			Ticking:       metrics.Ticking,
		}
	}

	isLiveMap := s.nodeLiveness.GetIsLiveMap()
	clusterNodes := s.storePool.ClusterNodeCount()

	err = s.stores.VisitStores(func(store *kvserver.Store) error {
		timestamp := store.Clock().Now()
		if len(req.RangeIDs) == 0 {
			// All ranges requested.

			// Use IterateRangeDescriptors to read from the engine only
			// because it's already exported.
			err := kvserver.IterateRangeDescriptors(ctx, store.Engine(),
				func(desc roachpb.RangeDescriptor) (done bool, _ error) {
					rep, err := store.GetReplica(desc.RangeID)
					if errors.HasType(err, (*roachpb.RangeNotFoundError)(nil)) {
						return false, nil // continue
					}
					if err != nil {
						return true, err
					}
					output.Ranges = append(output.Ranges,
						constructRangeInfo(
							desc,
							rep,
							store.Ident.StoreID,
							rep.Metrics(ctx, timestamp, isLiveMap, clusterNodes),
						))
					return false, nil
				})
			return err
		}

		// Specific ranges requested:
		for _, rid := range req.RangeIDs {
			rep, err := store.GetReplica(rid)
			if err != nil {
				// Not found: continue.
				continue
			}
			desc := rep.Desc()
			output.Ranges = append(output.Ranges,
				constructRangeInfo(
					*desc,
					rep,
					store.Ident.StoreID,
					rep.Metrics(ctx, timestamp, isLiveMap, clusterNodes),
				))
		}
		return nil
	})
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, err.Error())
	}
	return &output, nil
}

// HotRanges returns the hottest ranges on each store on the requested node(s).
func (s *statusServer) HotRanges(
	ctx context.Context, req *serverpb.HotRangesRequest,
) (*serverpb.HotRangesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	response := &serverpb.HotRangesResponse{
		NodeID:            s.gossip.NodeID.Get(),
		HotRangesByNodeID: make(map[roachpb.NodeID]serverpb.HotRangesResponse_NodeResponse),
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}

		// Only hot ranges from the local node.
		if local {
			response.HotRangesByNodeID[requestedNodeID] = s.localHotRanges(ctx)
			return response, nil
		}

		// Only hot ranges from one non-local node.
		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return status.HotRanges(ctx, req)
	}

	// Hot ranges from all nodes.
	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	remoteRequest := serverpb.HotRangesRequest{NodeID: "local"}
	nodeFn := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		return status.HotRanges(ctx, &remoteRequest)
	}
	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		hotRangesResp := resp.(*serverpb.HotRangesResponse)
		response.HotRangesByNodeID[nodeID] = hotRangesResp.HotRangesByNodeID[nodeID]
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.HotRangesByNodeID[nodeID] = serverpb.HotRangesResponse_NodeResponse{
			ErrorMessage: err.Error(),
		}
	}

	if err := s.iterateNodes(ctx, "hot ranges", dialFn, nodeFn, responseFn, errorFn); err != nil {
		return nil, err
	}

	return response, nil
}

func (s *statusServer) localHotRanges(ctx context.Context) serverpb.HotRangesResponse_NodeResponse {
	var resp serverpb.HotRangesResponse_NodeResponse
	includeRawKeys := debug.GatewayRemoteAllowed(ctx, s.st)
	err := s.stores.VisitStores(func(store *kvserver.Store) error {
		ranges := store.HottestReplicas()
		storeResp := &serverpb.HotRangesResponse_StoreResponse{
			StoreID:   store.StoreID(),
			HotRanges: make([]serverpb.HotRangesResponse_HotRange, len(ranges)),
		}
		for i, r := range ranges {
			storeResp.HotRanges[i].Desc = *r.Desc
			if !includeRawKeys {
				storeResp.HotRanges[i].Desc.StartKey = nil
				storeResp.HotRanges[i].Desc.EndKey = nil
			}
			storeResp.HotRanges[i].QueriesPerSecond = r.QPS
		}
		resp.Stores = append(resp.Stores, storeResp)
		return nil
	})
	if err != nil {
		return serverpb.HotRangesResponse_NodeResponse{ErrorMessage: err.Error()}
	}
	return resp
}

// Range returns rangeInfos for all nodes in the cluster about a specific
// range. It also returns the range history for that range as well.
func (s *statusServer) Range(
	ctx context.Context, req *serverpb.RangeRequest,
) (*serverpb.RangeResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	response := &serverpb.RangeResponse{
		RangeID:           roachpb.RangeID(req.RangeId),
		NodeID:            s.gossip.NodeID.Get(),
		ResponsesByNodeID: make(map[roachpb.NodeID]serverpb.RangeResponse_NodeResponse),
	}

	rangesRequest := &serverpb.RangesRequest{
		RangeIDs: []roachpb.RangeID{roachpb.RangeID(req.RangeId)},
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	nodeFn := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		return status.Ranges(ctx, rangesRequest)
	}
	nowNanos := timeutil.Now().UnixNano()
	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		rangesResp := resp.(*serverpb.RangesResponse)
		// Age the MVCCStats to a consistent current timestamp. An age that is
		// not up to date is less useful.
		for i := range rangesResp.Ranges {
			rangesResp.Ranges[i].State.Stats.AgeTo(nowNanos)
		}
		response.ResponsesByNodeID[nodeID] = serverpb.RangeResponse_NodeResponse{
			Response: true,
			Infos:    rangesResp.Ranges,
		}
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.ResponsesByNodeID[nodeID] = serverpb.RangeResponse_NodeResponse{
			ErrorMessage: err.Error(),
		}
	}

	if err := s.iterateNodes(
		ctx, fmt.Sprintf("details about range %d", req.RangeId), dialFn, nodeFn, responseFn, errorFn,
	); err != nil {
		return nil, err
	}
	return response, nil
}

// ListLocalSessions returns a list of SQL sessions on this node.
func (s *statusServer) ListLocalSessions(
	ctx context.Context, req *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	sessionUser, isAdmin, err := s.admin.getUserAndRole(ctx)
	if err != nil {
		return nil, err
	}
	hasViewActivity, err := s.admin.hasRoleOption(ctx, sessionUser, roleoption.VIEWACTIVITY)
	if err != nil {
		return nil, err
	}

	if !isAdmin && !hasViewActivity {
		// For non-superusers, requests with an empty username is
		// implicitly a request for the client's own sessions.
		if req.Username == "" {
			req.Username = sessionUser
		}

		// Non-superusers are not allowed to query sessions others than their own.
		if sessionUser != req.Username {
			return nil, grpcstatus.Errorf(
				codes.PermissionDenied,
				"client user %q does not have permission to view sessions from user %q",
				sessionUser, req.Username)
		}
	}

	// The empty username means "all sessions".
	showAll := req.Username == ""

	registry := s.sessionRegistry
	sessions := registry.SerializeAll()
	userSessions := make([]serverpb.Session, 0, len(sessions))

	for _, session := range sessions {
		if req.Username != session.Username && !showAll {
			continue
		}

		session.NodeID = s.gossip.NodeID.Get()
		userSessions = append(userSessions, session)
	}

	return &serverpb.ListSessionsResponse{Sessions: userSessions}, nil
}

// iterateNodes iterates nodeFn over all non-removed nodes concurrently.
// It then calls nodeResponse for every valid result of nodeFn, and
// nodeError on every error result.
func (s *statusServer) iterateNodes(
	ctx context.Context,
	errorCtx string,
	dialFn func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error),
	nodeFn func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error),
	responseFn func(nodeID roachpb.NodeID, resp interface{}),
	errorFn func(nodeID roachpb.NodeID, nodeFnError error),
) error {
	nodeStatuses, err := s.nodesStatusWithLiveness(ctx)
	if err != nil {
		return err
	}

	// channels for responses and errors.
	type nodeResponse struct {
		nodeID   roachpb.NodeID
		response interface{}
		err      error
	}

	numNodes := len(nodeStatuses)
	responseChan := make(chan nodeResponse, numNodes)

	nodeQuery := func(ctx context.Context, nodeID roachpb.NodeID) {
		var client interface{}
		err := contextutil.RunWithTimeout(ctx, "dial node", base.NetworkTimeout, func(ctx context.Context) error {
			var err error
			client, err = dialFn(ctx, nodeID)
			return err
		})
		if err != nil {
			err = errors.Wrapf(err, "failed to dial into node %d (%s)",
				nodeID, nodeStatuses[nodeID].livenessStatus)
			responseChan <- nodeResponse{nodeID: nodeID, err: err}
			return
		}

		res, err := nodeFn(ctx, client, nodeID)
		if err != nil {
			err = errors.Wrapf(err, "error requesting %s from node %d (%s)",
				errorCtx, nodeID, nodeStatuses[nodeID].livenessStatus)
		}
		responseChan <- nodeResponse{nodeID: nodeID, response: res, err: err}
	}

	// Issue the requests concurrently.
	sem := quotapool.NewIntPool("node status", maxConcurrentRequests)
	ctx, cancel := s.stopper.WithCancelOnStop(ctx)
	defer cancel()
	for nodeID := range nodeStatuses {
		nodeID := nodeID // needed to ensure the closure below captures a copy.
		if err := s.stopper.RunLimitedAsyncTask(
			ctx, fmt.Sprintf("server.statusServer: requesting %s", errorCtx),
			sem, true, /* wait */
			func(ctx context.Context) { nodeQuery(ctx, nodeID) },
		); err != nil {
			return err
		}
	}

	var resultErr error
	for numNodes > 0 {
		select {
		case res := <-responseChan:
			if res.err != nil {
				errorFn(res.nodeID, res.err)
			} else {
				responseFn(res.nodeID, res.response)
			}
		case <-ctx.Done():
			resultErr = errors.Errorf("request of %s canceled before completion", errorCtx)
		}
		numNodes--
	}
	return resultErr
}

// ListSessions returns a list of SQL sessions on all nodes in the cluster.
func (s *statusServer) ListSessions(
	ctx context.Context, req *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, _, err := s.admin.getUserAndRole(ctx); err != nil {
		return nil, err
	}

	response := &serverpb.ListSessionsResponse{
		Sessions: make([]serverpb.Session, 0),
		Errors:   make([]serverpb.ListSessionsError, 0),
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	nodeFn := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		return status.ListLocalSessions(ctx, req)
	}
	responseFn := func(_ roachpb.NodeID, nodeResp interface{}) {
		sessions := nodeResp.(*serverpb.ListSessionsResponse)
		response.Sessions = append(response.Sessions, sessions.Sessions...)
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		errResponse := serverpb.ListSessionsError{NodeID: nodeID, Message: err.Error()}
		response.Errors = append(response.Errors, errResponse)
	}

	if err := s.iterateNodes(ctx, "session list", dialFn, nodeFn, responseFn, errorFn); err != nil {
		err := serverpb.ListSessionsError{Message: err.Error()}
		response.Errors = append(response.Errors, err)
	}
	return response, nil
}

// CancelSession responds to a session cancellation request by canceling the
// target session's associated context.
func (s *statusServer) CancelSession(
	ctx context.Context, req *serverpb.CancelSessionRequest,
) (*serverpb.CancelSessionResponse, error) {
	ctx = s.AnnotateCtx(ctx)

	// reqUser is the user who made the cancellation request.
	var reqUser string
	{
		sessionUser, isAdmin, err := s.admin.getUserAndRole(ctx)
		if err != nil {
			return nil, err
		}
		if req.Username == "" || req.Username == sessionUser {
			reqUser = sessionUser
		} else {
			// When CANCEL QUERY is run as a SQL statement, sessionUser is always root
			// and the user who ran the statement is passed as req.Username.
			if !isAdmin {
				return nil, errRequiresAdmin
			}
			reqUser = req.Username
		}
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)

	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.CancelSession(ctx, req)
	}

	hasAdmin, err := s.admin.hasAdminRole(ctx, reqUser)
	if err != nil {
		return nil, err
	}
	if !hasAdmin {
		// Check if the user has permission to see the session.
		var session serverpb.Session
		for _, s := range s.sessionRegistry.SerializeAll() {
			if bytes.Equal(req.SessionID, s.ID) {
				session = s
				break
			}
		}
		if len(session.ID) == 0 {
			return nil, fmt.Errorf("session ID %s not found", sql.BytesToClusterWideID(req.SessionID))
		}

		if session.Username != reqUser {
			// Must have CANCELQUERY privilege to cancel other users' sessions.
			ok, err := s.admin.hasRoleOption(ctx, reqUser, roleoption.CANCELQUERY)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, errRequiresRoleOption(roleoption.CANCELQUERY)
			}
			// Non-admins cannot cancel admins' sessions.
			isAdminSession, err := s.admin.hasAdminRole(ctx, session.Username)
			if err != nil {
				return nil, err
			}
			if isAdminSession {
				return nil, status.Error(
					codes.PermissionDenied, "permission denied to cancel admin session")
			}
		}
	}

	return s.sessionRegistry.CancelSession(req.SessionID)
}

// CancelQuery responds to a query cancellation request, and cancels
// the target query's associated context and sets a cancellation flag.
func (s *statusServer) CancelQuery(
	ctx context.Context, req *serverpb.CancelQueryRequest,
) (*serverpb.CancelQueryResponse, error) {
	// reqUser is the user who made the cancellation request.
	var reqUser string
	{
		sessionUser, isAdmin, err := s.admin.getUserAndRole(ctx)
		if err != nil {
			return nil, err
		}
		if req.Username == "" || req.Username == sessionUser {
			reqUser = sessionUser
		} else {
			// When CANCEL QUERY is run as a SQL statement, sessionUser is always root
			// and the user who ran the statement is passed as req.Username.
			if !isAdmin {
				return nil, errRequiresAdmin
			}
			reqUser = req.Username
		}
	}

	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)
	nodeID, local, err := s.parseNodeID(req.NodeId)

	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.CancelQuery(ctx, req)
	}

	hasAdmin, err := s.admin.hasAdminRole(ctx, reqUser)
	if err != nil {
		return nil, err
	}
	if !hasAdmin {
		// Check if the user has permission to see the query's session.
		var session serverpb.Session
		for _, s := range s.sessionRegistry.SerializeAll() {
			for _, q := range s.ActiveQueries {
				if req.QueryID == q.ID {
					session = s
					break
				}
			}
		}
		if len(session.ID) == 0 {
			return nil, fmt.Errorf("query ID %s not found", req.QueryID)
		}

		if session.Username != reqUser {
			// Must have CANCELQUERY privilege to cancel other users' queries.
			ok, err := s.admin.hasRoleOption(ctx, reqUser, roleoption.CANCELQUERY)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, errRequiresRoleOption(roleoption.CANCELQUERY)
			}
			// Non-admins cannot cancel admins' queries.
			isAdminQuery, err := s.admin.hasAdminRole(ctx, session.Username)
			if err != nil {
				return nil, err
			}
			if isAdminQuery {
				return nil, status.Error(
					codes.PermissionDenied, "permission denied to cancel admin query")
			}
		}
	}

	output := &serverpb.CancelQueryResponse{}
	canceled, err := s.sessionRegistry.CancelQuery(req.QueryID)

	if err != nil {
		output.Error = err.Error()
	}

	output.Canceled = canceled
	return output, nil
}

// SpanStats requests the total statistics stored on a node for a given key
// span, which may include multiple ranges.
func (s *statusServer) SpanStats(
	ctx context.Context, req *serverpb.SpanStatsRequest,
) (*serverpb.SpanStatsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeID)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.SpanStats(ctx, req)
	}

	output := &serverpb.SpanStatsResponse{}
	err = s.stores.VisitStores(func(store *kvserver.Store) error {
		result, err := store.ComputeStatsForKeySpan(req.StartKey.Next(), req.EndKey)
		if err != nil {
			return err
		}
		output.TotalStats.Add(result.MVCC)
		output.RangeCount += int32(result.ReplicaCount)
		output.ApproximateDiskBytes += result.ApproximateDiskBytes
		return nil
	})
	if err != nil {
		return nil, err
	}

	return output, nil
}

// Diagnostics returns an anonymized diagnostics report.
func (s *statusServer) Diagnostics(
	ctx context.Context, req *serverpb.DiagnosticsRequest,
) (*diagnosticspb.DiagnosticReport, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)
	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.Diagnostics(ctx, req)
	}

	return s.admin.server.getReportingInfo(ctx, telemetry.ReadOnly), nil
}

// Stores returns details for each store.
func (s *statusServer) Stores(
	ctx context.Context, req *serverpb.StoresRequest,
) (*serverpb.StoresResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.Stores(ctx, req)
	}

	resp := &serverpb.StoresResponse{}
	err = s.stores.VisitStores(func(store *kvserver.Store) error {
		storeDetails := serverpb.StoreDetails{
			StoreID: store.Ident.StoreID,
		}

		envStats, err := store.Engine().GetEnvStats()
		if err != nil {
			return err
		}

		if len(envStats.EncryptionStatus) > 0 {
			storeDetails.EncryptionStatus = envStats.EncryptionStatus
		}
		storeDetails.TotalFiles = envStats.TotalFiles
		storeDetails.TotalBytes = envStats.TotalBytes
		storeDetails.ActiveKeyFiles = envStats.ActiveKeyFiles
		storeDetails.ActiveKeyBytes = envStats.ActiveKeyBytes

		resp.Stores = append(resp.Stores, storeDetails)

		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// jsonWrapper provides a wrapper on any slice data type being
// marshaled to JSON. This prevents a security vulnerability
// where a phishing attack can trick a user's browser into
// requesting a document from Cockroach as an executable script,
// allowing the contents of the fetched document to be treated
// as executable javascript. More details here:
// http://haacked.com/archive/2009/06/25/json-hijacking.aspx/
type jsonWrapper struct {
	Data interface{} `json:"d"`
}

// marshalToJSON marshals the given value into nicely indented JSON. If the
// value is an array or slice it is wrapped in jsonWrapper and then marshaled.
func marshalToJSON(value interface{}) ([]byte, error) {
	switch reflect.ValueOf(value).Kind() {
	case reflect.Array, reflect.Slice:
		value = jsonWrapper{Data: value}
	}
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, errors.Errorf("unable to marshal %+v to json: %s", value, err)
	}
	return body, nil
}

// marshalJSONResponse converts an arbitrary value into a JSONResponse protobuf
// that can be sent via grpc.
func marshalJSONResponse(value interface{}) (*serverpb.JSONResponse, error) {
	data, err := marshalToJSON(value)
	if err != nil {
		return nil, err
	}
	return &serverpb.JSONResponse{Data: data}, nil
}

func userFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		// If the incoming context has metadata but no attached web session user,
		// it's a gRPC / internal SQL connection which has root on the cluster.
		return security.RootUser, nil
	}
	usernames, ok := md[webSessionUserKeyStr]
	if !ok {
		// If the incoming context has metadata but no attached web session user,
		// it's a gRPC / internal SQL connection which has root on the cluster.
		return security.RootUser, nil
	}
	if len(usernames) != 1 {
		log.Warningf(ctx, "context's incoming metadata contains unexpected number of usernames: %+v ", md)
		return "", fmt.Errorf(
			"context's incoming metadata contains unexpected number of usernames: %+v ", md)
	}
	return usernames[0], nil
}

type systemInfoOnce struct {
	once sync.Once
	info serverpb.SystemInfo
}

func (si *systemInfoOnce) systemInfo(ctx context.Context) serverpb.SystemInfo {
	// We only want to attempt to populate the si.info once. If an error occurs
	// it is logged but the corresponding field in the returned struct is just
	// left empty as there isn't anything to do with an error from this function.
	si.once.Do(func() {
		// Don't use CommandContext because uname ought to not run for too long and
		// if the status request were canceled, we don't want future requests to
		// get blank information because we bailed out of this Once.
		cmd := exec.Command("uname", "-a")
		var errBuf bytes.Buffer
		cmd.Stderr = &errBuf
		output, err := cmd.Output()
		if err != nil {
			log.Warningf(ctx, "failed to get system information: %v\nstderr: %v",
				err, errBuf.String())
			return
		}
		si.info.SystemInfo = string(bytes.TrimSpace(output))
		cmd = exec.Command("uname", "-r")
		errBuf.Reset()
		cmd.Stderr = &errBuf
		output, err = cmd.Output()
		if err != nil {
			log.Warningf(ctx, "failed to get kernel information: %v\nstderr: %v",
				err, errBuf.String())
			return
		}
		si.info.KernelInfo = string(bytes.TrimSpace(output))
	})
	return si.info
}

// JobRegistryStatus returns details about the jobs running on the registry at a
// particular node.
func (s *statusServer) JobRegistryStatus(
	ctx context.Context, req *serverpb.JobRegistryStatusRequest,
) (*serverpb.JobRegistryStatusResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return status.JobRegistryStatus(ctx, req)
	}

	remoteNodeID := s.gossip.NodeID.Get()
	resp := &serverpb.JobRegistryStatusResponse{
		NodeID: remoteNodeID,
	}
	for _, jID := range s.admin.server.sqlServer.jobRegistry.CurrentlyRunningJobs() {
		job := serverpb.JobRegistryStatusResponse_Job{
			Id: jID,
		}
		resp.RunningJobs = append(resp.RunningJobs, &job)
	}
	return resp, nil
}

// JobStatus returns details about the jobs running on the registry at a
// particular node.
func (s *statusServer) JobStatus(
	ctx context.Context, req *serverpb.JobStatusRequest,
) (*serverpb.JobStatusResponse, error) {
	ctx = s.AnnotateCtx(propagateGatewayMetadata(ctx))

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	j, err := s.admin.server.sqlServer.jobRegistry.LoadJob(ctx, req.JobId)
	if err != nil {
		return nil, err
	}
	res := &jobspb.Job{
		Payload:  &jobspb.Payload{},
		Progress: &jobspb.Progress{},
	}
	res.Id = *j.ID()
	// j is not escaping this function and hence is immutable so a shallow copy
	// is fine. Also we can't really clone the field Payload as it may contain
	// types that are not supported by Clone().
	// See https://github.com/cockroachdb/cockroach/issues/46049.
	*res.Payload = j.Payload()
	*res.Progress = j.Progress()

	return &serverpb.JobStatusResponse{Job: res}, nil
}
