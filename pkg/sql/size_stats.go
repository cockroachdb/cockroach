// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/errors"
)

// AdminTableStats is an endpoint that returns disk usage and replication
// statistics for the specified table.
func (p *planner) AdminTableApproximateSize(
	ctx context.Context, databaseName string, schemaQualifiedTable string,
) (uint64, error) {
	// Simply call into the SQL server that we have access to from the planner.
	//p.execCfg.SQLServer.TableStats()
	sessionData := p.EvalContext().SessionData
	return p.execCfg.SQLServer.TableStats(ctx,
		sessionData.User(),
		sessionData.Database,
		&serverpb.TableStatsRequest{Database: databaseName,
			Table: schemaQualifiedTable})
}

func (s *Server) TableStats(
	ctx context.Context,
	username security.SQLUsername,
	database string,
	req *serverpb.TableStatsRequest,
) (uint64, error) {
	escQualTable, err := parser.GetFullyQualifiedTableName(req.Database, req.Table)
	if err != nil {
		return 0, err
	}

	// tenantid/1/2/hello -> world

	row, err := s.cfg.InternalExecutor.QueryRowEx(
		ctx, "admin-resolve-name", nil,
		sessiondata.InternalExecutorOverride{User: username, Database: database},
		"SELECT $1::regclass::oid", escQualTable,
	)
	if err != nil {
		return 0, err
	}
	if row == nil {
		return 0, errors.Newf("failed to resolve %q as a table name", escQualTable)
	}
	tableID := descpb.ID(tree.MustBeDOid(row[0]).DInt)
	tableStartKey := s.cfg.Codec.TablePrefix(uint32(tableID))
	tableEndKey := tableStartKey.PrefixEnd()
	span := roachpb.Span{Key: tableStartKey, EndKey: tableEndKey}
	resp, err := s.StatsForSpan(ctx, span)
	if err != nil {
		return 0, err
	}
	return resp.ApproximateDiskBytes, nil
}

func (s *Server) StatsForSpan(
	ctx context.Context, span roachpb.Span,
) (*serverpb.TableStatsResponse, error) {
	startKey, err := keys.Addr(span.Key)
	if err != nil {
		return nil, err
	}
	endKey, err := keys.Addr(span.EndKey)
	if err != nil {
		return nil, err
	}

	// Get current range descriptors for table. This is done by scanning over
	// meta2 keys for the range. A special case occurs if we wish to include
	// the meta1 key range itself, in which case we'll get KeyMin back and that
	// cannot be scanned (due to range-local addressing confusion). This is
	// handled appropriately by adjusting the bounds to grab the descriptors
	// for all ranges (including range1, which is not only gossiped but also
	// persisted in meta1).
	startMetaKey := keys.RangeMetaKey(startKey)
	if bytes.Equal(startMetaKey, roachpb.RKeyMin) {
		// This is the special case described above. The following key instructs
		// the code below to scan all of the addressing, i.e. grab all of the
		// descriptors including that for r1.
		startMetaKey = keys.RangeMetaKey(keys.MustAddr(keys.Meta2Prefix))
	}

	rangeDescKVs, err := s.cfg.DB.Scan(ctx, startMetaKey, keys.RangeMetaKey(endKey), 0)
	if err != nil {
		return nil, err
	}

	// This map will store the nodes we need to fan out to.
	nodeIDs := make(map[roachpb.NodeID]struct{})
	for _, kv := range rangeDescKVs {
		var rng roachpb.RangeDescriptor
		if err := kv.Value.GetProto(&rng); err != nil {
			return nil, err
		}
		for _, repl := range rng.Replicas().Descriptors() {
			nodeIDs[repl.NodeID] = struct{}{}
		}
	}

	// Construct TableStatsResponse by sending an RPC to every node involved.
	tableStatResponse := serverpb.TableStatsResponse{
		NodeCount: int64(len(nodeIDs)),
		// TODO(mrtracy): The "RangeCount" returned by TableStats is more
		// accurate than the "RangeCount" returned by TableDetails, because this
		// method always consistently queries the meta2 key range for the table;
		// in contrast, TableDetails uses a method on the DistSender, which
		// queries using a range metadata cache and thus may return stale data
		// for tables that are rapidly splitting. However, one potential
		// *advantage* of using the DistSender is that it will populate the
		// DistSender's range metadata cache in the case where meta2 information
		// for this table is not already present; the query used by TableStats
		// does not populate the DistSender cache. We should consider plumbing
		// TableStats' meta2 query through the DistSender so that it will share
		// the advantage of populating the cache (without the disadvantage of
		// potentially returning stale data).
		// See Github #5435 for some discussion.
		RangeCount: int64(len(rangeDescKVs)),
	}
	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *serverpb.SpanStatsResponse
		err    error
	}

	// Send a SpanStats query to each node.
	responses := make(chan nodeResponse, len(nodeIDs))
	for nodeID := range nodeIDs {
		nodeID := nodeID // avoid data race
		if err := s.cfg.DistSQLSrv.Stopper.RunAsyncTask(
			ctx, "server.adminServer: requesting remote stats",
			func(ctx context.Context) {
				// Set a generous timeout on the context for each individual query.
				var spanResponse *serverpb.SpanStatsResponse
				err := contextutil.RunWithTimeout(ctx, "request remote stats", 5*base.NetworkTimeout,
					func(ctx context.Context) error {
						conn, err := s.cfg.NodeDialer.Dial(ctx, nodeID, rpc.DefaultClass)
						if err != nil {
							return err
						}
						client := serverpb.NewStatusClient(conn)
						req := serverpb.SpanStatsRequest{
							StartKey: startKey,
							EndKey:   endKey,
							NodeID:   nodeID.String(),
						}
						spanResponse, err = client.SpanStats(ctx, &req)
						return err
					})

				// Channel is buffered, can always write.
				responses <- nodeResponse{
					nodeID: nodeID,
					resp:   spanResponse,
					err:    err,
				}
			}); err != nil {
			return nil, err
		}
	}
	for remainingResponses := len(nodeIDs); remainingResponses > 0; remainingResponses-- {
		select {
		case resp := <-responses:
			// For nodes which returned an error, note that the node's data
			// is missing. For successful calls, aggregate statistics.
			if resp.err != nil {
				tableStatResponse.MissingNodes = append(
					tableStatResponse.MissingNodes,
					serverpb.TableStatsResponse_MissingNode{
						NodeID:       resp.nodeID.String(),
						ErrorMessage: resp.err.Error(),
					},
				)
			} else {
				tableStatResponse.Stats.Add(resp.resp.TotalStats)
				tableStatResponse.ReplicaCount += int64(resp.resp.RangeCount)
				tableStatResponse.ApproximateDiskBytes += resp.resp.ApproximateDiskBytes
			}
		case <-ctx.Done():
			// Caller gave up, stop doing work.
			return nil, ctx.Err()
		}
	}

	return &tableStatResponse, nil
}
