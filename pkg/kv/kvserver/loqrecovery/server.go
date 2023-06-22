// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

const rangeMetadataScanChunkSize = 100

const retrieveNodeStatusTimeout = 30 * time.Second
const retrieveKeyspaceHealthTimeout = time.Minute

var fanOutConnectionRetryOptions = retry.Options{
	MaxRetries:     3,
	InitialBackoff: time.Second,
	Multiplier:     1,
}

var errMarkRetry = errors.New("retryable")

func IsRetryableError(err error) bool {
	return errors.Is(err, errMarkRetry)
}

type visitNodeAdminFn func(ctx context.Context, retryOpts retry.Options,
	nodeFilter func(nodeID roachpb.NodeID) bool,
	visitor func(nodeID roachpb.NodeID, client serverpb.AdminClient) error,
) error

type visitNodeStatusFn func(ctx context.Context, nodeID roachpb.NodeID, retryOpts retry.Options,
	visitor func(client serverpb.StatusClient) error,
) error

type Server struct {
	nodeIDContainer    *base.NodeIDContainer
	clusterIDContainer *base.ClusterIDContainer
	settings           *cluster.Settings
	stores             *kvserver.Stores
	visitAdminNodes    visitNodeAdminFn
	visitStatusNode    visitNodeStatusFn
	planStore          PlanStore
	decommissionFn     func(context.Context, roachpb.NodeID) error

	metadataQueryTimeout time.Duration
	forwardReplicaFilter func(*serverpb.RecoveryCollectLocalReplicaInfoResponse) error
}

func NewServer(
	nodeIDContainer *base.NodeIDContainer,
	settings *cluster.Settings,
	stores *kvserver.Stores,
	planStore PlanStore,
	g *gossip.Gossip,
	loc roachpb.Locality,
	rpcCtx *rpc.Context,
	knobs base.ModuleTestingKnobs,
	decommission func(context.Context, roachpb.NodeID) error,
) *Server {
	// Server side timeouts are necessary in recovery collector since we do best
	// effort operations where cluster info collection as an operation succeeds
	// even if some parts of it time out.
	metadataQueryTimeout := 1 * time.Minute
	var forwardReplicaFilter func(*serverpb.RecoveryCollectLocalReplicaInfoResponse) error
	if rk, ok := knobs.(*TestingKnobs); ok {
		if rk.MetadataScanTimeout > 0 {
			metadataQueryTimeout = rk.MetadataScanTimeout
		}
		forwardReplicaFilter = rk.ForwardReplicaFilter
	}
	return &Server{
		nodeIDContainer:      nodeIDContainer,
		clusterIDContainer:   rpcCtx.StorageClusterID,
		settings:             settings,
		stores:               stores,
		visitAdminNodes:      makeVisitAvailableNodes(g, loc, rpcCtx),
		visitStatusNode:      makeVisitNode(g, loc, rpcCtx),
		planStore:            planStore,
		decommissionFn:       decommission,
		metadataQueryTimeout: metadataQueryTimeout,
		forwardReplicaFilter: forwardReplicaFilter,
	}
}

func (s Server) ServeLocalReplicas(
	ctx context.Context,
	_ *serverpb.RecoveryCollectLocalReplicaInfoRequest,
	stream serverpb.Admin_RecoveryCollectLocalReplicaInfoServer,
) error {
	v := s.settings.Version.ActiveVersion(ctx)
	return s.stores.VisitStores(func(s *kvserver.Store) error {
		reader := s.TODOEngine().NewSnapshot()
		defer reader.Close()
		return visitStoreReplicas(ctx, reader, s.StoreID(), s.NodeID(), v,
			func(info loqrecoverypb.ReplicaInfo) error {
				return stream.Send(&serverpb.RecoveryCollectLocalReplicaInfoResponse{ReplicaInfo: &info})
			})
	})
}

func (s Server) ServeClusterReplicas(
	ctx context.Context,
	_ *serverpb.RecoveryCollectReplicaInfoRequest,
	outStream serverpb.Admin_RecoveryCollectReplicaInfoServer,
	kvDB *kv.DB,
) (err error) {
	// Block requests that require fan-out to other nodes until upgrade is finalized.
	// We can't assume that caller is up-to-date with cluster version and process
	// regardless of version known by current node as recommended for RPC
	// requests because caller is a CLI which that only knows its binary version.
	if !s.settings.Version.IsActive(ctx, clusterversion.V23_1) {
		return errors.Newf("loss of quorum recovery service requires cluster upgraded to 23.1")
	}

	var (
		descriptors, nodes, replicas int
	)
	defer func() {
		if err == nil {
			log.Infof(ctx, "streamed info: range descriptors %d, nodes %d, replica infos %d", descriptors,
				nodes, replicas)
		}
	}()

	v := s.settings.Version.ActiveVersion(ctx)
	if err = outStream.Send(&serverpb.RecoveryCollectReplicaInfoResponse{
		Info: &serverpb.RecoveryCollectReplicaInfoResponse_Metadata{
			Metadata: &loqrecoverypb.ClusterMetadata{
				ClusterID: s.clusterIDContainer.String(),
				Version:   v.Version,
			},
		},
	}); err != nil {
		return err
	}

	err = timeutil.RunWithTimeout(ctx, "scan range descriptors", s.metadataQueryTimeout,
		func(txnCtx context.Context) error {
			txn := kvDB.NewTxn(txnCtx, "scan-range-descriptors")
			if err := txn.SetFixedTimestamp(txnCtx, kvDB.Clock().Now()); err != nil {
				return err
			}
			defer func() { _ = txn.Rollback(txnCtx) }()
			log.Infof(txnCtx, "serving recovery range descriptors for all ranges")
			return txn.Iterate(txnCtx, keys.Meta2Prefix, keys.MetaMax, rangeMetadataScanChunkSize,
				func(kvs []kv.KeyValue) error {
					for _, rangeDescKV := range kvs {
						var rangeDesc roachpb.RangeDescriptor
						if err := rangeDescKV.ValueProto(&rangeDesc); err != nil {
							return err
						}
						if err := outStream.Send(&serverpb.RecoveryCollectReplicaInfoResponse{
							Info: &serverpb.RecoveryCollectReplicaInfoResponse_RangeDescriptor{
								RangeDescriptor: &rangeDesc,
							},
						}); err != nil {
							return err
						}
						descriptors++
					}
					return nil
				})
		})
	if err != nil {
		// Error means either kv transaction error or stream send error.
		// We don't care about transaction errors because cluster is might be in a
		// crippled state, but we don't want to keep continue if client stream is
		// closed.
		if outStream.Context().Err() != nil {
			return err
		}
		log.Infof(ctx, "failed to iterate all descriptors: %s", err)
	}

	// Stream local replica info from all nodes wrapping them in response stream.
	return s.visitAdminNodes(ctx,
		fanOutConnectionRetryOptions,
		allNodes,
		func(nodeID roachpb.NodeID, client serverpb.AdminClient) error {
			log.Infof(ctx, "trying to get info from node n%d", nodeID)
			nodeReplicas := 0
			inStream, err := client.RecoveryCollectLocalReplicaInfo(ctx,
				&serverpb.RecoveryCollectLocalReplicaInfoRequest{})
			if err != nil {
				return errors.Mark(errors.Wrapf(err,
					"failed retrieving replicas from node n%d during fan-out", nodeID), errMarkRetry)
			}
			for {
				r, err := inStream.Recv()
				if err == io.EOF {
					break
				}
				if s.forwardReplicaFilter != nil {
					err = s.forwardReplicaFilter(r)
				}
				if err != nil {
					// Some replicas were already sent back, need to notify client of stream
					// restart.
					if err := outStream.Send(&serverpb.RecoveryCollectReplicaInfoResponse{
						Info: &serverpb.RecoveryCollectReplicaInfoResponse_NodeStreamRestarted{
							NodeStreamRestarted: &serverpb.RecoveryCollectReplicaRestartNodeStream{
								NodeID: nodeID,
							},
						},
					}); err != nil {
						return err
					}
					return errors.Mark(errors.Wrapf(err,
						"failed retrieving replicas from node n%d during fan-out",
						nodeID), errMarkRetry)
				}
				if err := outStream.Send(&serverpb.RecoveryCollectReplicaInfoResponse{
					Info: &serverpb.RecoveryCollectReplicaInfoResponse_ReplicaInfo{
						ReplicaInfo: r.ReplicaInfo,
					},
				}); err != nil {
					return err
				}
				nodeReplicas++
			}

			replicas += nodeReplicas
			nodes++
			return nil
		})
}

func (s Server) StagePlan(
	ctx context.Context, req *serverpb.RecoveryStagePlanRequest,
) (*serverpb.RecoveryStagePlanResponse, error) {
	// Block requests that require fan-out to other nodes until upgrade is finalized.
	// We can't assume that caller is up-to-date with cluster version and process
	// regardless of version known by current node as recommended for RPC
	// requests because caller is a CLI which that only knows its binary version.
	if !s.settings.Version.IsActive(ctx, clusterversion.V23_1) {
		return nil, errors.Newf("loss of quorum recovery service requires cluster upgraded to 23.1")
	}

	if !req.ForcePlan && req.Plan == nil {
		return nil, errors.New("stage plan request can't be used with empty plan without force flag")
	}
	if p := req.Plan; p != nil {
		clusterID := s.clusterIDContainer.Get().String()
		if p.ClusterID != clusterID {
			return nil, errors.Newf("attempting to stage plan from cluster %s on cluster %s",
				p.ClusterID, clusterID)
		}
		version := s.settings.Version.ActiveVersion(ctx)
		if err := checkPlanVersionMatches(p.Version, version.Version, req.ForceLocalInternalVersion); err != nil {
			return nil, errors.Wrap(err, "incompatible plan")
		}
		// It is safe to always update internal to reflect active version since it
		// is allowed by the check above or is not needed.
		p.Version.Internal = version.Internal
	}

	localNodeID := s.nodeIDContainer.Get()
	// Create a plan copy with all empty fields to shortcut all plan nil checks
	// below to avoid unnecessary nil checks.
	var plan loqrecoverypb.ReplicaUpdatePlan
	if req.Plan != nil {
		plan = *req.Plan
	}
	if req.AllNodes {
		// Scan cluster for conflicting recovery plans and for stray nodes that are
		// planned for forced decommission, but rejoined cluster.
		foundNodes := make(map[roachpb.NodeID]bool)
		err := s.visitAdminNodes(
			ctx,
			fanOutConnectionRetryOptions,
			allNodes,
			func(nodeID roachpb.NodeID, client serverpb.AdminClient) error {
				res, err := client.RecoveryNodeStatus(ctx, &serverpb.RecoveryNodeStatusRequest{})
				if err != nil {
					return errors.Mark(err, errMarkRetry)
				}
				// If operation fails here, we don't want to find all remaining
				// violating nodes because cli must ensure that cluster is safe for
				// staging.
				if !req.ForcePlan && res.Status.PendingPlanID != nil && !res.Status.PendingPlanID.Equal(plan.PlanID) {
					return errors.Newf("plan %s is already staged on node n%d", res.Status.PendingPlanID, nodeID)
				}
				foundNodes[nodeID] = true
				return nil
			})
		if err != nil {
			return nil, err
		}

		// Check that no nodes that must be decommissioned are present.
		for _, dID := range plan.DecommissionedNodeIDs {
			if foundNodes[dID] {
				return nil, errors.Newf("node n%d was planned for decommission, but is present in cluster", dID)
			}
		}

		// Check out that all nodes that should save plan are present.
		for _, u := range plan.Updates {
			if !foundNodes[u.NodeID()] {
				return nil, errors.Newf("node n%d has planned changed but is unreachable in the cluster", u.NodeID())
			}
		}
		for _, n := range plan.StaleLeaseholderNodeIDs {
			if !foundNodes[n] {
				return nil, errors.Newf("node n%d has planned restart but is unreachable in the cluster", n)
			}
		}

		// Distribute plan - this should not use fan out to available, but use
		// list from previous step.
		var nodeErrors []string
		err = s.visitAdminNodes(
			ctx,
			fanOutConnectionRetryOptions,
			onlyListed(foundNodes),
			func(nodeID roachpb.NodeID, client serverpb.AdminClient) error {
				delete(foundNodes, nodeID)
				res, err := client.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
					Plan:                      req.Plan,
					AllNodes:                  false,
					ForcePlan:                 req.ForcePlan,
					ForceLocalInternalVersion: req.ForceLocalInternalVersion,
				})
				if err != nil {
					nodeErrors = append(nodeErrors,
						errors.Wrapf(err, "failed staging the plan on node n%d", nodeID).Error())
					return nil
				}
				nodeErrors = append(nodeErrors, res.Errors...)
				return nil
			})
		if err != nil {
			nodeErrors = append(nodeErrors,
				errors.Wrapf(err, "failed to perform fan-out to cluster nodes from n%d",
					localNodeID).Error())
		}
		if len(foundNodes) > 0 {
			// We didn't talk to some of originally found nodes. Need to report
			// disappeared nodes as we don't know what is happening with the cluster.
			for n := range foundNodes {
				nodeErrors = append(nodeErrors, fmt.Sprintf("node n%d disappeared while performing plan staging operation", n))
			}
		}
		return &serverpb.RecoveryStagePlanResponse{Errors: nodeErrors}, nil
	}

	log.Infof(ctx, "attempting to stage loss of quorum recovery plan")

	responseFromError := func(err error) (*serverpb.RecoveryStagePlanResponse, error) {
		return &serverpb.RecoveryStagePlanResponse{
			Errors: []string{
				errors.Wrapf(err, "failed to stage plan on node n%d", localNodeID).Error(),
			},
		}, nil
	}

	existingPlan, exists, err := s.planStore.LoadPlan()
	if err != nil {
		return responseFromError(err)
	}
	if exists && !existingPlan.PlanID.Equal(plan.PlanID) && !req.ForcePlan {
		return responseFromError(errors.Newf("conflicting plan %s is already staged", existingPlan.PlanID))
	}

	for _, node := range plan.DecommissionedNodeIDs {
		if err := s.decommissionFn(ctx, node); err != nil {
			return responseFromError(err)
		}
	}

	if req.ForcePlan {
		if err := s.planStore.RemovePlan(); err != nil {
			return responseFromError(err)
		}
	}

	needsUpdate := false
	for _, r := range plan.Updates {
		if r.NodeID() == localNodeID {
			needsUpdate = true
			break
		}
	}
	if !needsUpdate {
		for _, n := range plan.StaleLeaseholderNodeIDs {
			if n == localNodeID {
				needsUpdate = true
				break
			}
		}
	}

	if needsUpdate {
		if err := s.planStore.SavePlan(plan); err != nil {
			return responseFromError(err)
		}
	}

	return &serverpb.RecoveryStagePlanResponse{}, nil
}

func (s Server) NodeStatus(
	ctx context.Context, _ *serverpb.RecoveryNodeStatusRequest,
) (*serverpb.RecoveryNodeStatusResponse, error) {
	status := loqrecoverypb.NodeRecoveryStatus{
		NodeID: s.nodeIDContainer.Get(),
	}
	plan, exists, err := s.planStore.LoadPlan()
	if err != nil {
		return nil, err
	}
	if exists {
		status.PendingPlanID = &plan.PlanID
	}
	err = s.stores.VisitStores(func(s *kvserver.Store) error {
		r, ok, err := readNodeRecoveryStatusInfo(ctx, s.TODOEngine())
		if err != nil {
			return err
		}
		if ok {
			status.AppliedPlanID = &r.AppliedPlanID
			status.ApplyTimestamp = &r.ApplyTimestamp
			status.Error = r.Error
			return iterutil.StopIteration()
		}
		return nil
	})
	if err = iterutil.Map(err); err != nil {
		log.Errorf(ctx, "failed to read loss of quorum recovery application status %s", err)
		return nil, err
	}

	return &serverpb.RecoveryNodeStatusResponse{
		Status: status,
	}, nil
}

func (s Server) Verify(
	ctx context.Context, req *serverpb.RecoveryVerifyRequest, nl *liveness.NodeLiveness, db *kv.DB,
) (*serverpb.RecoveryVerifyResponse, error) {
	// Block requests that require fan-out to other nodes until upgrade is finalized.
	if !s.settings.Version.IsActive(ctx, clusterversion.V23_1) {
		return nil, errors.Newf("loss of quorum recovery service requires cluster upgraded to 23.1")
	}

	var nss []loqrecoverypb.NodeRecoveryStatus
	err := s.visitAdminNodes(ctx, fanOutConnectionRetryOptions,
		notListed(req.DecommissionedNodeIDs),
		func(nodeID roachpb.NodeID, client serverpb.AdminClient) error {
			return timeutil.RunWithTimeout(ctx, fmt.Sprintf("retrieve status of n%d", nodeID),
				retrieveNodeStatusTimeout,
				func(ctx context.Context) error {
					res, err := client.RecoveryNodeStatus(ctx, &serverpb.RecoveryNodeStatusRequest{})
					if err != nil {
						return errors.Mark(errors.Wrapf(err, "failed to retrieve status of n%d", nodeID),
							errMarkRetry)
					}
					nss = append(nss, res.Status)
					return nil
				})
		})
	if err != nil {
		return nil, err
	}
	decomNodes := make(map[roachpb.NodeID]bool)
	decomStatus := make(map[roachpb.NodeID]livenesspb.MembershipStatus, len(req.DecommissionedNodeIDs))
	for _, plannedID := range req.DecommissionedNodeIDs {
		decomNodes[plannedID] = true
		decomStatus[plannedID] = nl.GetNodeVitalityFromCache(plannedID).MembershipStatus()
	}

	isNodeLive := func(rd roachpb.ReplicaDescriptor) bool {
		// Preemptively remove dead nodes as they would return Forbidden error if
		// liveness is not stale enough.
		if decomNodes[rd.NodeID] {
			return false
		}
		return nl.GetNodeVitalityFromCache(rd.NodeID).IsLive(livenesspb.LossOfQuorum)
	}

	getRangeInfo := func(
		ctx context.Context, rID roachpb.RangeID, nID roachpb.NodeID,
	) (serverpb.RangeInfo, error) {
		var info serverpb.RangeInfo
		err := s.visitStatusNode(ctx, nID, fanOutConnectionRetryOptions,
			func(c serverpb.StatusClient) error {
				resp, err := c.Range(ctx, &serverpb.RangeRequest{RangeId: int64(rID)})
				if err != nil {
					return err
				}
				res := resp.ResponsesByNodeID[nID]
				if len(res.Infos) > 0 {
					info = res.Infos[0]
					return nil
				}
				return errors.Newf("range r%d not found on node n%d", rID, nID)
			})
		if err != nil {
			return serverpb.RangeInfo{}, err
		}
		return info, nil
	}

	// Note that rangeCheckErr is a partial error, so we may have subset of ranges
	// and an error, both of them will go to response.
	unavailable, rangeCheckErr := func() ([]loqrecoverypb.RangeRecoveryStatus, error) {
		var unavailable []loqrecoverypb.RangeRecoveryStatus
		if req.MaxReportedRanges == 0 {
			return nil, nil
		}
		err := timeutil.RunWithTimeout(ctx, "retrieve ranges health", retrieveKeyspaceHealthTimeout,
			func(ctx context.Context) error {
				start := keys.Meta2Prefix
				for {
					kvs, err := db.Scan(ctx, start, keys.MetaMax, rangeMetadataScanChunkSize)
					if err != nil {
						return err
					}
					if len(kvs) == 0 {
						break
					}
					var endKey roachpb.Key
					for _, rangeDescKV := range kvs {
						endKey = rangeDescKV.Key
						var d roachpb.RangeDescriptor
						if err := rangeDescKV.ValueProto(&d); err != nil {
							continue
						}
						h := checkRangeHealth(ctx, d, isNodeLive, getRangeInfo)
						if h != loqrecoverypb.RangeHealth_HEALTHY {
							if len(unavailable) >= int(req.MaxReportedRanges) {
								return errors.Newf("found more failed ranges than limit %d",
									req.MaxReportedRanges)
							}
							unavailable = append(unavailable, loqrecoverypb.RangeRecoveryStatus{
								RangeID: d.RangeID,
								Span: roachpb.Span{
									Key:    d.StartKey.AsRawKey(),
									EndKey: d.EndKey.AsRawKey(),
								},
								Health: h,
							})
						}
					}
					start = endKey.Next()
				}
				return nil
			})
		// Note: we are returning partial results and an error in case we time out
		// or hit unavailability or scan limit
		return unavailable, err
	}()
	rangeHealth := serverpb.RecoveryVerifyResponse_UnavailableRanges{
		Ranges: unavailable,
	}
	if rangeCheckErr != nil {
		rangeHealth.Error = rangeCheckErr.Error()
	}

	return &serverpb.RecoveryVerifyResponse{
		Statuses:                   nss,
		DecommissionedNodeStatuses: decomStatus,
		UnavailableRanges:          rangeHealth,
	}, nil
}

func checkRangeHealth(
	ctx context.Context,
	d roachpb.RangeDescriptor,
	liveFunc func(rd roachpb.ReplicaDescriptor) bool,
	rangeInfo func(ctx context.Context, id roachpb.RangeID, nID roachpb.NodeID) (serverpb.RangeInfo, error),
) loqrecoverypb.RangeHealth {
	if d.Replicas().CanMakeProgress(liveFunc) {
		return loqrecoverypb.RangeHealth_HEALTHY
	}
	stuckReplica := false
	healthyReplica := false
	for _, r := range d.Replicas().Descriptors() {
		// Check if node is in deleted nodes first.
		if liveFunc(r) {
			info, err := rangeInfo(ctx, d.RangeID, r.NodeID)
			if err != nil {
				// We can't reach node which is reported as live, skip this replica
				// for now and check if remaining nodes could serve the range.
				continue
			}
			canMakeProgress := info.State.Desc.Replicas().CanMakeProgress(liveFunc)

			healthyReplica = healthyReplica || canMakeProgress
			stuckReplica = stuckReplica || !canMakeProgress
		}
	}
	// If we have a leaseholder that can't make progress it could block all
	// operations on the range. Upreplication of healthy replica will update
	// meta and resolve the issue.
	if stuckReplica && healthyReplica {
		return loqrecoverypb.RangeHealth_WAITING_FOR_META
	}
	// If we have healthy replica and no stuck replicas then this replica
	// will respond.
	if healthyReplica && !stuckReplica {
		return loqrecoverypb.RangeHealth_HEALTHY
	}
	return loqrecoverypb.RangeHealth_LOSS_OF_QUORUM
}

func makeVisitAvailableNodes(
	g *gossip.Gossip, loc roachpb.Locality, rpcCtx *rpc.Context,
) visitNodeAdminFn {
	return func(ctx context.Context, retryOpts retry.Options,
		nodeFilter func(nodeID roachpb.NodeID) bool,
		visitor func(nodeID roachpb.NodeID, client serverpb.AdminClient) error,
	) error {
		visitWithRetry := func(node roachpb.NodeDescriptor) error {
			var err error
			for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
				log.Infof(ctx, "visiting node n%d, attempt %d", node.NodeID, r.CurrentAttempt())
				addr := node.AddressForLocality(loc)
				var conn *grpc.ClientConn
				conn, err = rpcCtx.GRPCDialNode(addr.String(), node.NodeID, rpc.DefaultClass).Connect(ctx)
				// Nodes would contain dead nodes that we don't need to visit. We can skip
				// them and let caller handle incomplete info.
				if err != nil {
					if grpcutil.IsConnectionUnavailable(err) {
						return nil
					}
					// This was an initial heartbeat type error, we must retry as node seems
					// live.
					continue
				}
				client := serverpb.NewAdminClient(conn)
				err = visitor(node.NodeID, client)
				if err == nil {
					return nil
				}
				log.Infof(ctx, "failed calling a visitor for node n%d: %s", node.NodeID, err)
				if !IsRetryableError(err) {
					return err
				}
			}
			return err
		}

		var nodes []roachpb.NodeDescriptor
		if err := g.IterateInfos(gossip.KeyNodeDescPrefix, func(key string, i gossip.Info) error {
			b, err := i.Value.GetBytes()
			if err != nil {
				return errors.Wrapf(err, "failed to get node gossip info for key %s", key)
			}

			var d roachpb.NodeDescriptor
			if err := protoutil.Unmarshal(b, &d); err != nil {
				return errors.Wrapf(err, "failed to unmarshal node gossip info for key %s", key)
			}

			// Don't use node descriptors with NodeID 0, because that's meant to
			// indicate that the node has been removed from the cluster.
			if d.NodeID != 0 && nodeFilter(d.NodeID) {
				nodes = append(nodes, d)
			}

			return nil
		}); err != nil {
			return err
		}

		for _, node := range nodes {
			if err := visitWithRetry(node); err != nil {
				return err
			}
		}
		return nil
	}
}

func makeVisitNode(g *gossip.Gossip, loc roachpb.Locality, rpcCtx *rpc.Context) visitNodeStatusFn {
	return func(ctx context.Context, nodeID roachpb.NodeID, retryOpts retry.Options,
		visitor func(client serverpb.StatusClient) error,
	) error {
		node, err := g.GetNodeDescriptor(nodeID)
		if err != nil {
			return err
		}
		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			log.Infof(ctx, "visiting node n%d, attempt %d", node.NodeID, r.CurrentAttempt())
			addr := node.AddressForLocality(loc)
			var conn *grpc.ClientConn
			conn, err = rpcCtx.GRPCDialNode(addr.String(), node.NodeID, rpc.DefaultClass).Connect(ctx)
			if err != nil {
				if grpcutil.IsClosedConnection(err) {
					return err
				}
				// Retry any other transient connection flakes.
				continue
			}
			client := serverpb.NewStatusClient(conn)
			err = visitor(client)
			if err == nil {
				return nil
			}
			log.Infof(ctx, "failed calling a visitor for node n%d: %s", node.NodeID, err)
			if !IsRetryableError(err) {
				return err
			}
		}
		return err
	}
}

func allNodes(roachpb.NodeID) bool {
	return true
}

func onlyListed(nodes map[roachpb.NodeID]bool) func(id roachpb.NodeID) bool {
	return func(id roachpb.NodeID) bool {
		return nodes[id]
	}
}

func notListed(ids []roachpb.NodeID) func(id roachpb.NodeID) bool {
	ignored := make(map[roachpb.NodeID]bool)
	for _, id := range ids {
		ignored[id] = true
	}
	return func(id roachpb.NodeID) bool {
		return !ignored[id]
	}
}
