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
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
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

// ClusterAdminClient allows Server to talk to all remote nodes without having
// a dependence on Admin Server.
type ClusterAdminClient interface {
	// VisitAvailableNodesWithRetry calls visitor with an admin client for every
	// known cluster node.
	// If visitor returns retryVisit error then node would be dialed again and
	// revisited until visitor succeeds or retry attempts are exhausted. In latter
	// case cause of retryVisit is returned.
	// If visitor returns StopIteration() error, rest of the nodes would be
	// skipped.
	// Returns error if visitor failed or if error happened when processing node
	// data.
	VisitAvailableNodesWithRetry(ctx context.Context, retryOpts retry.Options,
		visitor func(nodeID roachpb.NodeID, client serverpb.AdminClient) error,
	) error
}

type Server struct {
	stores               *kvserver.Stores
	metadataQueryTimeout time.Duration
	forwardReplicaFilter func(*serverpb.RecoveryCollectLocalReplicaInfoResponse) error
}

func NewServer(stores *kvserver.Stores, knobs base.ModuleTestingKnobs) *Server {
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
		stores:               stores,
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
	clusterAdminClient ClusterAdminClient,
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
	return clusterAdminClient.VisitAvailableNodesWithRetry(ctx,
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
