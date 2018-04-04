// Copyright 2014 The Cockroach Authors.
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
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"

	"github.com/coreos/etcd/raft"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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

	st              *cluster.Settings
	cfg             *base.Config
	admin           *adminServer
	db              *client.DB
	gossip          *gossip.Gossip
	metricSource    metricMarshaler
	nodeLiveness    *storage.NodeLiveness
	rpcCtx          *rpc.Context
	stores          *storage.Stores
	stopper         *stop.Stopper
	sessionRegistry *sql.SessionRegistry
}

// newStatusServer allocates and returns a statusServer.
func newStatusServer(
	ambient log.AmbientContext,
	st *cluster.Settings,
	cfg *base.Config,
	adminServer *adminServer,
	db *client.DB,
	gossip *gossip.Gossip,
	metricSource metricMarshaler,
	nodeLiveness *storage.NodeLiveness,
	rpcCtx *rpc.Context,
	stores *storage.Stores,
	stopper *stop.Stopper,
	sessionRegistry *sql.SessionRegistry,
) *statusServer {
	ambient.AddLogTag("status", nil)
	server := &statusServer{
		AmbientContext:  ambient,
		st:              st,
		cfg:             cfg,
		admin:           adminServer,
		db:              db,
		gossip:          gossip,
		metricSource:    metricSource,
		nodeLiveness:    nodeLiveness,
		rpcCtx:          rpcCtx,
		stores:          stores,
		stopper:         stopper,
		sessionRegistry: sessionRegistry,
	}

	return server
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
		return 0, false, errors.Wrap(err, "node id could not be parsed")
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
	conn, err := s.rpcCtx.GRPCDial(addr.String()).Connect(ctx)
	if err != nil {
		return nil, err
	}
	return serverpb.NewStatusClient(conn), nil
}

// Gossip returns gossip network status.
func (s *statusServer) Gossip(
	ctx context.Context, req *serverpb.GossipRequest,
) (*gossip.InfoStatus, error) {
	if !debug.GatewayRemoteAllowed(ctx, s.st) {
		return nil, remoteDebuggingErr
	}

	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)
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

// Allocator returns simulated allocator info for the ranges on the given node.
func (s *statusServer) Allocator(
	ctx context.Context, req *serverpb.AllocatorRequest,
) (*serverpb.AllocatorResponse, error) {
	// TODO(a-robinson): It'd be nice to allow this endpoint and just avoid
	// logging range start/end keys in the simulated allocator runs.
	if !debug.GatewayRemoteAllowed(ctx, s.st) {
		return nil, remoteDebuggingErr
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
		return status.Allocator(ctx, req)
	}

	output := new(serverpb.AllocatorResponse)
	err = s.stores.VisitStores(func(store *storage.Store) error {
		// All ranges requested:
		if len(req.RangeIDs) == 0 {
			// Use IterateRangeDescriptors to read from the engine only
			// because it's already exported.
			err := storage.IterateRangeDescriptors(ctx, store.Engine(),
				func(desc roachpb.RangeDescriptor) (bool, error) {
					rep, err := store.GetReplica(desc.RangeID)
					if err != nil {
						return true, err
					}
					if !rep.OwnsValidLease(store.Clock().Now()) {
						return false, nil
					}
					allocatorSpans, err := store.AllocatorDryRun(ctx, rep)
					if err != nil {
						return true, err
					}
					output.DryRuns = append(output.DryRuns, &serverpb.AllocatorDryRun{
						RangeID: desc.RangeID,
						Events:  recordedSpansToAllocatorEvents(allocatorSpans),
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
			if !rep.OwnsValidLease(store.Clock().Now()) {
				continue
			}
			allocatorSpans, err := store.AllocatorDryRun(ctx, rep)
			if err != nil {
				return err
			}
			output.DryRuns = append(output.DryRuns, &serverpb.AllocatorDryRun{
				RangeID: rep.RangeID,
				Events:  recordedSpansToAllocatorEvents(allocatorSpans),
			})
		}
		return nil
	})
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, err.Error())
	}
	return output, nil
}

func recordedSpansToAllocatorEvents(
	spans []tracing.RecordedSpan,
) []*serverpb.AllocatorDryRun_Event {
	var output []*serverpb.AllocatorDryRun_Event
	var buf bytes.Buffer
	for _, sp := range spans {
		for _, entry := range sp.Logs {
			event := &serverpb.AllocatorDryRun_Event{
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
	// TODO(a-robinson): It'd be nice to allow this endpoint and just avoid
	// logging range start/end keys in the simulated allocator runs.
	if !debug.GatewayRemoteAllowed(ctx, s.st) {
		return nil, remoteDebuggingErr
	}

	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)
	nodeCtx, cancel := context.WithTimeout(ctx, base.NetworkTimeout)
	defer cancel()

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
			nodeCtx,
			"server.statusServer: requesting remote Allocator simulation",
			func(ctx context.Context) {
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

	cm, err := s.cfg.GetCertificateManager()
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
		case security.NodePem:
			details.Type = serverpb.CertificateDetails_NODE
		case security.ClientPem:
			// Ignore client certificates for now.
			continue
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

	resp := &serverpb.DetailsResponse{
		NodeID:    s.gossip.NodeID.Get(),
		BuildInfo: build.GetInfo(),
	}
	if addr, err := s.gossip.GetNodeIDAddress(s.gossip.NodeID.Get()); err == nil {
		resp.Address = *addr
	}

	// If Ready is not set, the client doesn't want to know whether this node is
	// ready to receive client traffic.
	if !req.Ready {
		return resp, nil
	}

	serveMode := s.admin.server.serveMode.get()
	if serveMode != modeOperational {
		return nil, grpcstatus.Error(codes.Unavailable, "node is not ready")
	}

	isHealthy, err := s.nodeLiveness.IsHealthy(nodeID)
	if err != nil {
		return nil, grpcstatus.Error(codes.Internal, err.Error())
	}
	if !isHealthy {
		return nil, grpcstatus.Error(codes.Unavailable, "node is not ready")
	}

	return resp, nil
}

// LogFilesList returns a list of available log files.
func (s *statusServer) LogFilesList(
	ctx context.Context, req *serverpb.LogFilesListRequest,
) (*serverpb.LogFilesListResponse, error) {
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
	if !debug.GatewayRemoteAllowed(ctx, s.st) {
		return nil, remoteDebuggingErr
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
		return status.LogFile(ctx, req)
	}

	log.Flush()
	reader, err := log.GetLogReader(req.File, true /* restricted */)
	if reader == nil || err != nil {
		return nil, fmt.Errorf("log file %s could not be opened: %s", req.File, err)
	}
	defer reader.Close()

	var entry log.Entry
	var resp serverpb.LogEntriesResponse
	decoder := log.NewEntryDecoder(reader)
	for {
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		resp.Entries = append(resp.Entries, entry)
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
	if !debug.GatewayRemoteAllowed(ctx, s.st) {
		return nil, remoteDebuggingErr
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
		return status.Logs(ctx, req)
	}

	log.Flush()

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

	entries, err := log.FetchEntriesFromFiles(startTimestamp, endTimestamp, int(maxEntries), regex)
	if err != nil {
		return nil, err
	}

	return &serverpb.LogEntriesResponse{Entries: entries}, nil
}

// TODO(tschottdorf): significant overlap with /debug/pprof/goroutine, except
// that this one allows querying by NodeID.
//
// Stacks returns goroutine stack traces.
func (s *statusServer) Stacks(
	ctx context.Context, req *serverpb.StacksRequest,
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
		return status.Stacks(ctx, req)
	}

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
}

// TODO(tschottdorf): significant overlap with /debug/pprof/heap, except that
// this one allows querying by NodeID.
//
// Profile returns a heap profile.
func (s *statusServer) Profile(
	ctx context.Context, req *serverpb.ProfileRequest,
) (*serverpb.JSONResponse, error) {
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

	default:
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "unknown profile: %s", req.Type)
	}
}

// Nodes returns all node statuses.
func (s *statusServer) Nodes(
	ctx context.Context, req *serverpb.NodesRequest,
) (*serverpb.NodesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)
	startKey := keys.StatusNodePrefix
	endKey := startKey.PrefixEnd()

	b := &client.Batch{}
	b.Scan(startKey, endKey)
	if err := s.db.Run(ctx, b); err != nil {
		log.Error(ctx, err)
		return nil, grpcstatus.Errorf(codes.Internal, err.Error())
	}
	rows := b.Results[0].Rows

	resp := serverpb.NodesResponse{
		Nodes: make([]status.NodeStatus, len(rows)),
	}
	for i, row := range rows {
		if err := row.ValueProto(&resp.Nodes[i]); err != nil {
			log.Error(ctx, err)
			return nil, grpcstatus.Errorf(codes.Internal, err.Error())
		}
	}
	return &resp, nil
}

// handleNodeStatus handles GET requests for a single node's status.
func (s *statusServer) Node(
	ctx context.Context, req *serverpb.NodeRequest,
) (*status.NodeStatus, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)
	nodeID, _, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, err.Error())
	}

	key := keys.NodeStatusKey(nodeID)
	b := &client.Batch{}
	b.Get(key)
	if err := s.db.Run(ctx, b); err != nil {
		log.Error(ctx, err)
		return nil, grpcstatus.Errorf(codes.Internal, err.Error())
	}

	var nodeStatus status.NodeStatus
	if err := b.Results[0].Rows[0].ValueProto(&nodeStatus); err != nil {
		err = errors.Errorf("could not unmarshal NodeStatus from %s: %s", key, err)
		log.Error(ctx, err)
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

	// Subtract base.NetworkTimeout from the deadline so we have time to process
	// the results and return them.
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline.Add(-base.NetworkTimeout))
		defer cancel()
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
			for _, replica := range desc.Replicas {
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

func (s *statusServer) handleVars(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(httputil.ContentTypeHeader, httputil.PlaintextContentType)
	err := s.metricSource.PrintAsText(w)
	if err != nil {
		log.Error(r.Context(), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Ranges returns range info for the specified node.
func (s *statusServer) Ranges(
	ctx context.Context, req *serverpb.RangesRequest,
) (*serverpb.RangesResponse, error) {
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
				Paused:          progress.Paused,
				PendingSnapshot: progress.PendingSnapshot,
				State:           progress.State.String(),
			}
		}

		return state
	}

	includeRawKeys := debug.GatewayRemoteAllowed(ctx, s.st)

	constructRangeInfo := func(
		desc roachpb.RangeDescriptor, rep *storage.Replica, storeID roachpb.StoreID, metrics storage.ReplicaMetrics,
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
				Unavailable:          metrics.Unavailable,
				LeaderNotLeaseHolder: metrics.Leader && metrics.LeaseValid && !metrics.Leaseholder,
				NoRaftLeader:         !storage.HasRaftLeader(raftStatus) && !metrics.Quiescent,
				Underreplicated:      metrics.Underreplicated,
				NoLease:              metrics.Leader && !metrics.LeaseValid && !metrics.Quiescent,
			},
			CmdQLocal:   serverpb.CommandQueueMetrics(metrics.CmdQMetricsLocal),
			CmdQGlobal:  serverpb.CommandQueueMetrics(metrics.CmdQMetricsGlobal),
			LeaseStatus: metrics.LeaseStatus,
			Quiescent:   metrics.Quiescent,
		}
	}

	cfg, ok := s.gossip.GetSystemConfig()
	if !ok {
		// Very little on the status pages requires the system config -- as of June
		// 2017, only the underreplicated range metric does. Refusing to return a
		// status page (that may help debug why the config isn't available) due to
		// such a small piece of missing information is overly harsh.
		log.Error(ctx, "system config not yet available, serving status page without it")
		cfg = config.SystemConfig{}
	}
	isLiveMap := s.nodeLiveness.GetIsLiveMap()

	err = s.stores.VisitStores(func(store *storage.Store) error {
		timestamp := store.Clock().Now()
		if len(req.RangeIDs) == 0 {
			// All ranges requested.

			// Use IterateRangeDescriptors to read from the engine only
			// because it's already exported.
			err := storage.IterateRangeDescriptors(ctx, store.Engine(),
				func(desc roachpb.RangeDescriptor) (bool, error) {
					rep, err := store.GetReplica(desc.RangeID)
					if err != nil {
						return true, err
					}
					output.Ranges = append(output.Ranges,
						constructRangeInfo(
							desc,
							rep,
							store.Ident.StoreID,
							rep.Metrics(ctx, timestamp, cfg, isLiveMap),
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
					rep.Metrics(ctx, timestamp, cfg, isLiveMap),
				))
		}
		return nil
	})
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, err.Error())
	}
	return &output, nil
}

// Range returns rangeInfos for all nodes in the cluster about a specific
// range. It also returns the range history for that range as well.
func (s *statusServer) Range(
	ctx context.Context, req *serverpb.RangeRequest,
) (*serverpb.RangeResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)
	response := &serverpb.RangeResponse{
		RangeID:           roachpb.RangeID(req.RangeId),
		NodeID:            s.gossip.NodeID.Get(),
		ResponsesByNodeID: make(map[roachpb.NodeID]serverpb.RangeResponse_NodeResponse),
	}

	nodeCtx, cancel := context.WithTimeout(ctx, base.NetworkTimeout)
	defer cancel()

	isLiveMap := s.nodeLiveness.GetIsLiveMap()
	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *serverpb.RangesResponse
		err    error
	}

	responses := make(chan nodeResponse)
	// TODO(bram): consider abstracting out this repeated pattern.
	for nodeID := range isLiveMap {
		nodeID := nodeID
		if err := s.stopper.RunAsyncTask(
			nodeCtx,
			"server.statusServer: requesting remote ranges",
			func(ctx context.Context) {
				status, err := s.dialNode(ctx, nodeID)
				var rangesResponse *serverpb.RangesResponse
				if err == nil {
					rangesRequest := &serverpb.RangesRequest{
						RangeIDs: []roachpb.RangeID{roachpb.RangeID(req.RangeId)},
					}
					rangesResponse, err = status.Ranges(ctx, rangesRequest)
				}
				response := nodeResponse{
					nodeID: nodeID,
					resp:   rangesResponse,
					err:    err,
				}

				select {
				case responses <- response:
					// Response processed.
				case <-ctx.Done():
					// Context completed, response no longer needed.
				}
			}); err != nil {
			return nil, grpcstatus.Errorf(codes.Internal, err.Error())
		}
	}
	for remainingResponses := len(isLiveMap); remainingResponses > 0; remainingResponses-- {
		select {
		case resp := <-responses:
			if resp.err != nil {
				response.ResponsesByNodeID[resp.nodeID] = serverpb.RangeResponse_NodeResponse{
					ErrorMessage: resp.err.Error(),
				}
				continue
			}
			response.ResponsesByNodeID[resp.nodeID] = serverpb.RangeResponse_NodeResponse{
				Response: true,
				Infos:    resp.resp.Ranges,
			}
		case <-ctx.Done():
			return nil, grpcstatus.Errorf(codes.DeadlineExceeded, "request timed out")
		}
	}

	return response, nil
}

// CommandQueue returns a snapshot of the command queue state for the
// specified range.
func (s *statusServer) CommandQueue(
	ctx context.Context, req *serverpb.CommandQueueRequest,
) (*serverpb.CommandQueueResponse, error) {
	rangeID := roachpb.RangeID(req.RangeId)
	replica, err := s.stores.GetReplicaForRangeID(rangeID)
	if err != nil {
		return nil, err
	}

	if replica == nil {
		return nil, roachpb.NewRangeNotFoundError(rangeID)
	}

	return &serverpb.CommandQueueResponse{
		Snapshot: replica.GetCommandQueueSnapshot(),
	}, nil
}

// ListLocalSessions returns a list of SQL sessions on this node.
func (s *statusServer) ListLocalSessions(
	ctx context.Context, req *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	if !debug.GatewayRemoteAllowed(ctx, s.st) {
		return nil, remoteDebuggingErr
	}

	registry := s.sessionRegistry

	sessions := registry.SerializeAll()
	userSessions := make([]serverpb.Session, 0, len(sessions))

	for _, session := range sessions {
		if !(req.Username == security.RootUser || req.Username == session.Username) {
			continue
		}

		session.NodeID = s.gossip.NodeID.Get()
		userSessions = append(userSessions, session)
	}

	return &serverpb.ListSessionsResponse{Sessions: userSessions}, nil
}

// ListSessions returns a list of SQL sessions on all nodes in the cluster.
func (s *statusServer) ListSessions(
	ctx context.Context, req *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	if !debug.GatewayRemoteAllowed(ctx, s.st) {
		return nil, remoteDebuggingErr
	}

	ctx = s.AnnotateCtx(ctx)
	nodes, err := s.Nodes(ctx, nil)
	if err != nil {
		return nil, err
	}

	resp := serverpb.ListSessionsResponse{
		Sessions: make([]serverpb.Session, 0),
		Errors:   make([]serverpb.ListSessionsError, 0),
	}

	// Issue LocalSessions requests in parallel.
	// Semaphore that guarantees not more than maxConcurrentRequests requests at once.
	sem := make(chan struct{}, maxConcurrentRequests)
	numNodes := len(nodes.Nodes)

	// Channel for session responses and errors.
	sessionsChan := make(chan *serverpb.ListSessionsResponse, numNodes)
	errorsChan := make(chan serverpb.ListSessionsError, numNodes)

	getNodeSessions := func(ctx context.Context, nodeID roachpb.NodeID) {
		rpcCtx, cancel := context.WithTimeout(ctx, base.NetworkTimeout)
		defer cancel()

		status, err := s.dialNode(ctx, nodeID)

		if err != nil {
			err = errors.Wrapf(err, "failed to dial into node %d", nodeID)
			errorsChan <- serverpb.ListSessionsError{
				NodeID:  nodeID,
				Message: err.Error(),
			}
			return
		}

		sessions, err := status.ListLocalSessions(rpcCtx, req)

		if err != nil {
			err = errors.Wrapf(err, "failed to get sessions from node %d", nodeID)
			errorsChan <- serverpb.ListSessionsError{
				NodeID:  nodeID,
				Message: err.Error(),
			}
			return
		}

		sessionsChan <- sessions
	}

	for _, node := range nodes.Nodes {
		nodeID := node.Desc.NodeID
		getNodeSessionsTask := func(ctx context.Context) {
			getNodeSessions(ctx, nodeID)
		}
		if err := s.stopper.RunLimitedAsyncTask(
			ctx, "server.statusServe: requesting remote sessions", sem, true /* wait */, getNodeSessionsTask,
		); err != nil {
			return nil, err
		}
	}

	for numNodes > 0 {
		select {
		case sessions := <-sessionsChan:
			resp.Sessions = append(resp.Sessions, sessions.Sessions...)
		case err := <-errorsChan:
			resp.Errors = append(resp.Errors, err)
		case <-ctx.Done():
			err := serverpb.ListSessionsError{Message: "ListSessions canceled before completion"}
			resp.Errors = append(resp.Errors, err)
		}
		numNodes--
	}
	return &resp, nil
}

// CancelSession responds to a session cancellation request by canceling the
// target session's associated context.
func (s *statusServer) CancelSession(
	ctx context.Context, req *serverpb.CancelSessionRequest,
) (*serverpb.CancelSessionResponse, error) {
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
		return status.CancelSession(ctx, req)
	}

	output := &serverpb.CancelSessionResponse{}
	canceled, err := s.sessionRegistry.CancelSession(req.SessionID, req.Username)

	if err != nil {
		output.Error = err.Error()
	}

	output.Canceled = canceled
	return output, nil
}

// CancelQuery responds to a query cancellation request, and cancels
// the target query's associated context and sets a cancellation flag.
func (s *statusServer) CancelQuery(
	ctx context.Context, req *serverpb.CancelQueryRequest,
) (*serverpb.CancelQueryResponse, error) {
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

	output := &serverpb.CancelQueryResponse{}
	canceled, err := s.sessionRegistry.CancelQuery(req.QueryID, req.Username)

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
	err = s.stores.VisitStores(func(store *storage.Store) error {
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
