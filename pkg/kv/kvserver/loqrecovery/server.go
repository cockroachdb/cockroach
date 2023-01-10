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
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const rangeMetadataScanChunkSize = 100

var replicaInfoStreamRetryOptions = retry.Options{
	MaxRetries:     3,
	InitialBackoff: time.Second,
	Multiplier:     1,
}

var errMarkRetry = errors.New("retryable")

func IsRetryableError(err error) bool {
	return errors.Is(err, errMarkRetry)
}

type visitNodesFn func(ctx context.Context, retryOpts retry.Options,
	visitor func(nodeID roachpb.NodeID, client serverpb.AdminClient) error,
) error

type Server struct {
	nodeIDContainer *base.NodeIDContainer
	stores          *kvserver.Stores
	visitNodes      visitNodesFn
	planStore       PlanStore
	decommissionFn  func(context.Context, roachpb.NodeID) error

	metadataQueryTimeout time.Duration
	forwardReplicaFilter func(*serverpb.RecoveryCollectLocalReplicaInfoResponse) error
}

func NewServer(
	nodeIDContainer *base.NodeIDContainer,
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
		stores:               stores,
		visitNodes:           makeVisitAvailableNodes(g, loc, rpcCtx),
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
	return s.stores.VisitStores(func(s *kvserver.Store) error {
		reader := s.Engine().NewSnapshot()
		defer reader.Close()
		return visitStoreReplicas(ctx, reader, s.StoreID(), s.NodeID(),
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
	var (
		descriptors, nodes, replicas int
	)
	defer func() {
		if err == nil {
			log.Infof(ctx, "streamed info: range descriptors %d, nodes %d, replica infos %d", descriptors,
				nodes, replicas)
		}
	}()

	err = contextutil.RunWithTimeout(ctx, "scan-range-descriptors", s.metadataQueryTimeout,
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
	return s.visitNodes(ctx,
		replicaInfoStreamRetryOptions,
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
	if !req.ForcePlan && req.Plan == nil {
		return nil, errors.New("stage plan request can't be used with empty plan without force flag")
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
		foundNodes := make(map[roachpb.NodeID]struct{})
		err := s.visitNodes(
			ctx,
			replicaInfoStreamRetryOptions,
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
				foundNodes[nodeID] = struct{}{}
				return nil
			})
		if err != nil {
			return nil, err
		}

		// Check that no nodes that must be decommissioned are present.
		for _, dID := range plan.DecommissionedNodeIDs {
			if _, ok := foundNodes[dID]; ok {
				return nil, errors.Newf("node n%d was planned for decommission, but is present in cluster", dID)
			}
		}

		// Check out that all nodes that should save plan are present.
		for _, u := range plan.Updates {
			if _, ok := foundNodes[u.NodeID()]; !ok {
				return nil, errors.Newf("node n%d has planned changed but is unreachable in the cluster", u.NodeID())
			}
		}

		// Distribute plan - this should not use fan out to available, but use
		// list from previous step.
		var nodeErrors []string
		err = s.visitNodes(
			ctx,
			replicaInfoStreamRetryOptions,
			func(nodeID roachpb.NodeID, client serverpb.AdminClient) error {
				delete(foundNodes, nodeID)
				res, err := client.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
					Plan:      req.Plan,
					AllNodes:  false,
					ForcePlan: req.ForcePlan,
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

	existingPlan, err := s.planStore.HasPlan()
	if err != nil {
		return responseFromError(err)
	}
	if !existingPlan.Empty() && !existingPlan.SamePlan(plan.PlanID) && !req.ForcePlan {
		return responseFromError(errors.Newf("conflicting plan %s is already staged", existingPlan.PlanID))
	}

	for _, node := range plan.DecommissionedNodeIDs {
		if err := s.decommissionFn(ctx, node); err != nil {
			return responseFromError(err)
		}
	}

	if req.ForcePlan {
		if err := s.planStore.RemovePlans(); err != nil {
			return responseFromError(err)
		}
	}

	for _, r := range plan.Updates {
		if r.NodeID() == localNodeID {
			if err := s.planStore.SavePlan(plan); err != nil {
				return responseFromError(err)
			}
			break
		}
	}

	return &serverpb.RecoveryStagePlanResponse{}, nil
}

func (s Server) NodeStatus(
	_ context.Context, _ *serverpb.RecoveryNodeStatusRequest,
) (*serverpb.RecoveryNodeStatusResponse, error) {
	// TODO: report full status.
	plan, err := s.planStore.HasPlan()
	if err != nil {
		return nil, err
	}
	var planID *uuid.UUID
	if !plan.Empty() {
		planID = &plan.PlanID
	}
	return &serverpb.RecoveryNodeStatusResponse{
		Status: &loqrecoverypb.NodeRecoveryStatus{
			NodeID:        s.nodeIDContainer.Get(),
			PendingPlanID: planID,
		},
	}, nil
}

func (s Server) Verify(
	ctx context.Context, request *serverpb.RecoveryVerifyRequest,
) (*serverpb.RecoveryVerifyResponse, error) {
	var nss []loqrecoverypb.NodeRecoveryStatus
	err := s.visitNodes(ctx, replicaInfoStreamRetryOptions,
		func(nodeID roachpb.NodeID, client serverpb.AdminClient) error {
			res, err := client.RecoveryNodeStatus(ctx, &serverpb.RecoveryNodeStatusRequest{})
			if err != nil {
				return errors.Mark(errors.Wrapf(err, "failed to retrieve status of n%d", nodeID), errMarkRetry)
			}
			if res.Status != nil {
				nss = append(nss, *res.Status)
			}
			return nil
		})
	if err != nil {
		return nil, err
	}

	// TODO: retrieve status of requested nodes (for decommission check)
	// TODO: retrieve unavailable ranges report
	return &serverpb.RecoveryVerifyResponse{
		Statuses: nss,
	}, nil
}

func makeVisitAvailableNodes(
	g *gossip.Gossip, loc roachpb.Locality, rpcCtx *rpc.Context,
) visitNodesFn {
	return func(ctx context.Context, retryOpts retry.Options,
		visitor func(nodeID roachpb.NodeID, client serverpb.AdminClient) error,
	) error {
		collectNodeWithRetry := func(node roachpb.NodeDescriptor) error {
			var err error
			for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
				log.Infof(ctx, "visiting node n%d, attempt %d", node.NodeID, r.CurrentAttempt())
				addr := node.AddressForLocality(loc)
				conn, err := rpcCtx.GRPCDialNode(addr.String(), node.NodeID, rpc.DefaultClass).Connect(ctx)
				client := serverpb.NewAdminClient(conn)
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
				err = visitor(node.NodeID, client)
				if err == nil {
					return nil
				}
				log.Infof(ctx, "failed calling a visitor for node n%d: %s", node.NodeID, err)
				if !IsRetryableError(err) {
					// For non retryable errors abort immediately.
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
			if d.NodeID != 0 {
				nodes = append(nodes, d)
			}

			return nil
		}); err != nil {
			return err
		}

		for _, node := range nodes {
			if err := collectNodeWithRetry(node); err != nil {
				return err
			}
		}
		return nil
	}
}
