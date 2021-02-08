// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// makePreNodeZipRequests defines the zipRequests (API requests) that are to be
// performed once per node.
func makePerNodeZipRequests(
	prefix, id string, admin serverpb.AdminClient, status serverpb.StatusClient,
) []zipRequest {
	return []zipRequest{
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return status.Details(ctx, &serverpb.DetailsRequest{NodeId: id})
			},
			pathName: prefix + "/details",
		},
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return status.Gossip(ctx, &serverpb.GossipRequest{NodeId: id})
			},
			pathName: prefix + "/gossip",
		},
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return status.EngineStats(ctx, &serverpb.EngineStatsRequest{NodeId: id})
			},
			pathName: prefix + "/enginestats",
		},
	}
}

// Tables collected from each node in a debug zip using SQL.
var debugZipTablesPerNode = []string{
	"crdb_internal.feature_usage",

	"crdb_internal.gossip_alerts",
	"crdb_internal.gossip_liveness",
	"crdb_internal.gossip_network",
	"crdb_internal.gossip_nodes",

	"crdb_internal.leases",

	"crdb_internal.node_build_info",
	"crdb_internal.node_inflight_trace_spans",
	"crdb_internal.node_metrics",
	"crdb_internal.node_queries",
	"crdb_internal.node_runtime_info",
	"crdb_internal.node_sessions",
	"crdb_internal.node_statement_statistics",
	"crdb_internal.node_transaction_statistics",
	"crdb_internal.node_transactions",
	"crdb_internal.node_txn_stats",
}

// collectCPUProfiles collects CPU profiles in parallel over all nodes
// (this is useful since these profiles contain profiler labels, which
// can then be correlated across nodes).
//
// This is called first and in isolation, before other zip operations
// possibly influence the nodes.
func (zc *debugZipContext) collectCPUProfiles(
	ctx context.Context, nodeList []statuspb.NodeStatus, livenessByNodeID nodeLivenesses,
) error {
	if zipCtx.cpuProfDuration <= 0 {
		// Nothing to do; return early.
		return nil
	}

	var wg sync.WaitGroup
	type profData struct {
		data []byte
		err  error
	}

	// NB: this takes care not to produce non-deterministic log output.
	resps := make([]profData, len(nodeList))
	for i := range nodeList {
		if livenessByNodeID[nodeList[i].Desc.NodeID] == livenesspb.NodeLivenessStatus_DECOMMISSIONED {
			continue
		}
		wg.Add(1)
		go func(ctx context.Context, i int) {
			defer wg.Done()

			secs := int32(zipCtx.cpuProfDuration / time.Second)
			if secs < 1 {
				secs = 1
			}

			var pd profData
			err := contextutil.RunWithTimeout(ctx, "fetch cpu profile", zc.timeout+zipCtx.cpuProfDuration, func(ctx context.Context) error {
				resp, err := zc.status.Profile(ctx, &serverpb.ProfileRequest{
					NodeId:  fmt.Sprintf("%d", nodeList[i].Desc.NodeID),
					Type:    serverpb.ProfileRequest_CPU,
					Seconds: secs,
				})
				if err != nil {
					return err
				}
				pd = profData{data: resp.Data}
				return nil
			})
			if err != nil {
				resps[i] = profData{err: err}
			} else {
				resps[i] = pd
			}
		}(ctx, i)
	}

	fmt.Print("requesting CPU profiles... ")
	wg.Wait()
	fmt.Println("ok")

	for i, pd := range resps {
		if len(pd.data) == 0 && pd.err == nil {
			continue // skipped node
		}
		prefix := fmt.Sprintf("%s/%s", nodesPrefix, fmt.Sprintf("%d", nodeList[i].Desc.NodeID))
		if err := zc.z.createRawOrError(prefix+"/cpu.pprof", pd.data, pd.err); err != nil {
			return err
		}
	}
	return nil
}

func (zc *debugZipContext) collectPerNodeData(
	ctx context.Context, node statuspb.NodeStatus, livenessByNodeID nodeLivenesses,
) error {
	nodeID := node.Desc.NodeID

	liveness := livenessByNodeID[nodeID]
	if liveness == livenesspb.NodeLivenessStatus_DECOMMISSIONED {
		// Decommissioned + process terminated. Let's not waste time
		// on this node.
		//
		// NB: we still inspect DECOMMISSIONING nodes (marked as
		// decommissioned but the process is still alive) to get a
		// chance to collect their log files.
		//
		// NB: we still inspect DEAD nodes because even though they
		// don't heartbeat their liveness record their process might
		// still be up and willing to deliver some log files.
		return nil
	}

	id := fmt.Sprintf("%d", nodeID)
	prefix := fmt.Sprintf("%s/%s", nodesPrefix, id)

	if !zipCtx.nodes.isIncluded(nodeID) {
		if err := zc.z.createRaw(prefix+".skipped",
			[]byte(fmt.Sprintf("skipping excluded node %d\n", nodeID))); err != nil {
			return err
		}
		return nil
	}

	if err := zc.z.createJSON(prefix+"/status.json", node); err != nil {
		return err
	}

	// Don't use sqlConn because that's only for is the node `debug
	// zip` was pointed at, but here we want to connect to nodes
	// individually to grab node- local SQL tables. Try to guess by
	// replacing the host in the connection string; this may or may
	// not work and if it doesn't, we let the invalid curSQLConn get
	// used anyway so that anything that does *not* need it will
	// still happen.
	sqlAddr := node.Desc.CheckedSQLAddress()
	curSQLConn := guessNodeURL(zc.firstNodeSQLConn.url, sqlAddr.AddressField)
	fmt.Printf("using SQL connection URL for node %s: %s\n", id, curSQLConn.url)

	for _, table := range debugZipTablesPerNode {
		selectClause, ok := customSelectClause[table]
		if !ok {
			selectClause = "*"
		}
		if err := zc.dumpTableDataForZip(curSQLConn, prefix, table, selectClause); err != nil {
			return errors.Wrapf(err, "fetching %s", table)
		}
	}

	perNodeZipRequests := makePerNodeZipRequests(prefix, id, zc.admin, zc.status)

	for _, r := range perNodeZipRequests {
		if err := zc.runZipRequest(ctx, r); err != nil {
			return err
		}
	}

	var stacksData []byte
	requestErr := zc.runZipFn(ctx, "requesting stacks for node "+id,
		func(ctx context.Context) error {
			stacks, err := zc.status.Stacks(ctx, &serverpb.StacksRequest{
				NodeId: id,
				Type:   serverpb.StacksType_GOROUTINE_STACKS,
			})
			if err == nil {
				stacksData = stacks.Data
			}
			return err
		})
	if err := zc.z.createRawOrError(prefix+"/stacks.txt", stacksData, requestErr); err != nil {
		return err
	}

	var threadData []byte
	requestErr = zc.runZipFn(ctx, "requesting threads for node "+id,
		func(ctx context.Context) error {
			threads, err := zc.status.Stacks(ctx, &serverpb.StacksRequest{
				NodeId: id,
				Type:   serverpb.StacksType_THREAD_STACKS,
			})
			if err == nil {
				threadData = threads.Data
			}
			return err
		})
	if err := zc.z.createRawOrError(prefix+"/threads.txt", threadData, requestErr); err != nil {
		return err
	}

	var heapData []byte
	requestErr = zc.runZipFn(ctx, "requesting heap profile for node "+id,
		func(ctx context.Context) error {
			heap, err := zc.status.Profile(ctx, &serverpb.ProfileRequest{
				NodeId: id,
				Type:   serverpb.ProfileRequest_HEAP,
			})
			if err == nil {
				heapData = heap.Data
			}
			return err
		})
	if err := zc.z.createRawOrError(prefix+"/heap.pprof", heapData, requestErr); err != nil {
		return err
	}

	var profiles *serverpb.GetFilesResponse
	if requestErr := zc.runZipFn(ctx, "requesting heap files for node "+id,
		func(ctx context.Context) error {
			var err error
			profiles, err = zc.status.GetFiles(ctx, &serverpb.GetFilesRequest{
				NodeId:   id,
				Type:     serverpb.FileType_HEAP,
				Patterns: []string{"*"},
			})
			return err
		}); requestErr != nil {
		if err := zc.z.createError(prefix+"/heapprof", requestErr); err != nil {
			return err
		}
	} else {
		fmt.Printf("%d found\n", len(profiles.Files))
		for _, file := range profiles.Files {
			fName := maybeAddProfileSuffix(file.Name)
			name := prefix + "/heapprof/" + fName
			if err := zc.z.createRaw(name, file.Contents); err != nil {
				return err
			}
		}
	}

	var goroutinesResp *serverpb.GetFilesResponse
	if requestErr := zc.runZipFn(ctx, "requesting goroutine files for node "+id,
		func(ctx context.Context) error {
			var err error
			goroutinesResp, err = zc.status.GetFiles(ctx, &serverpb.GetFilesRequest{
				NodeId:   id,
				Type:     serverpb.FileType_GOROUTINES,
				Patterns: []string{"*"},
			})
			return err
		}); requestErr != nil {
		if err := zc.z.createError(prefix+"/goroutines", requestErr); err != nil {
			return err
		}
	} else {
		fmt.Printf("%d found\n", len(goroutinesResp.Files))
		for _, file := range goroutinesResp.Files {
			// NB: the files have a .txt.gz suffix already.
			name := prefix + "/goroutines/" + file.Name
			if err := zc.z.createRaw(name, file.Contents); err != nil {
				return err
			}
		}
	}

	var logs *serverpb.LogFilesListResponse
	if requestErr := zc.runZipFn(ctx, "requesting log files list",
		func(ctx context.Context) error {
			var err error
			logs, err = zc.status.LogFilesList(
				ctx, &serverpb.LogFilesListRequest{NodeId: id})
			return err
		}); requestErr != nil {
		if err := zc.z.createError(prefix+"/logs", requestErr); err != nil {
			return err
		}
	} else {
		fmt.Printf("%d found\n", len(logs.Files))
		for _, file := range logs.Files {
			name := prefix + "/logs/" + file.Name
			var entries *serverpb.LogEntriesResponse
			if requestErr := zc.runZipFn(ctx, fmt.Sprintf("requesting log file %s", file.Name),
				func(ctx context.Context) error {
					var err error
					entries, err = zc.status.LogFile(
						ctx, &serverpb.LogFileRequest{
							NodeId: id, File: file.Name, Redact: zipCtx.redactLogs,
						})
					return err
				}); requestErr != nil {
				if err := zc.z.createError(name, requestErr); err != nil {
					return err
				}
				continue
			}
			warnRedactLeak := false
			if err := func() error {
				// Use a closure so that the zipper is only locked once per
				// created log file.
				zc.z.Lock()
				defer zc.z.Unlock()

				logOut, err := zc.z.createLocked(name, timeutil.Unix(0, file.ModTimeNanos))
				if err != nil {
					return err
				}
				for _, e := range entries.Entries {
					// If the user requests redaction, and some non-redactable
					// data was found in the log, *despite KeepRedactable
					// being set*, this means that this zip client is talking
					// to a node that doesn't yet know how to redact. This
					// also means that node may be leaking sensitive data.
					//
					// In that case, we do the redaction work ourselves in the
					// most conservative way possible. (It's not great that
					// possibly confidential data flew over the network, but
					// at least it stops here.)
					if zipCtx.redactLogs && !e.Redactable {
						e.Message = "REDACTEDBYZIP"
						// We're also going to print a warning at the end.
						warnRedactLeak = true
					}
					if err := log.FormatLegacyEntry(e, logOut); err != nil {
						return err
					}
				}
				return nil
			}(); err != nil {
				return err
			}
			if warnRedactLeak {
				// Defer the warning, so that it does not get "drowned" as
				// part of the main zip output.
				defer func(fileName string) {
					fmt.Fprintf(stderr, "WARNING: server-side redaction failed for %s, completed client-side (--redact-logs=true)\n", fileName)
				}(file.Name)
			}
		}
	}

	var ranges *serverpb.RangesResponse
	if requestErr := zc.runZipFn(ctx, "requesting ranges", func(ctx context.Context) error {
		var err error
		ranges, err = zc.status.Ranges(ctx, &serverpb.RangesRequest{NodeId: id})
		return err
	}); requestErr != nil {
		if err := zc.z.createError(prefix+"/ranges", requestErr); err != nil {
			return err
		}
	} else {
		fmt.Printf("%d found\n", len(ranges.Ranges))
		sort.Slice(ranges.Ranges, func(i, j int) bool {
			return ranges.Ranges[i].State.Desc.RangeID <
				ranges.Ranges[j].State.Desc.RangeID
		})
		for _, r := range ranges.Ranges {
			name := fmt.Sprintf("%s/ranges/%s", prefix, r.State.Desc.RangeID)
			if err := zc.z.createJSON(name+".json", r); err != nil {
				return err
			}
		}
	}
	return nil
}

func guessNodeURL(workingURL string, hostport string) *sqlConn {
	u, err := url.Parse(workingURL)
	if err != nil {
		u = &url.URL{Host: "invalid"}
	}
	u.Host = hostport
	return makeSQLConn(u.String())
}
