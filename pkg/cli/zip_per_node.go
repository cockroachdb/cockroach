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
	"os"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// makePerNodeZipRequests defines the zipRequests (API requests) that are to be
// performed once per node.
func makePerNodeZipRequests(prefix, id string, status serverpb.StatusClient) []zipRequest {
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

// collectCPUProfiles collects CPU profiles in parallel over all nodes
// (this is useful since these profiles contain profiler labels, which
// can then be correlated across nodes).
//
// Note that the request for CPU profiles is always issued at maximum
// concurrency, because we wish profiling to be enabled on all nodes
// simultaneously. This ensures that profiling data flows across RPCs.
//
// This is called first and in isolation, before other zip operations
// possibly influence the nodes.
func (zc *debugZipContext) collectCPUProfiles(
	ctx context.Context, ni nodesInfo, livenessByNodeID nodeLivenesses,
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

	zc.clusterPrinter.info("requesting CPU profiles")

	if ni.nodesListResponse == nil {
		return errors.AssertionFailedf("nodes list is empty; nothing to do")
	}

	nodeList := ni.nodesListResponse.Nodes
	// NB: this takes care not to produce non-deterministic log output.
	resps := make([]profData, len(nodeList))
	for i := range nodeList {
		nodeID := roachpb.NodeID(nodeList[i].NodeID)
		if livenessByNodeID[nodeID] == livenesspb.NodeLivenessStatus_DECOMMISSIONED {
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
					NodeId:  fmt.Sprintf("%d", nodeID),
					Type:    serverpb.ProfileRequest_CPU,
					Seconds: secs,
					Labels:  true,
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

	wg.Wait()
	zc.clusterPrinter.info("profiles generated")

	for i, pd := range resps {
		if len(pd.data) == 0 && pd.err == nil {
			continue // skipped node
		}
		nodeID := nodeList[i].NodeID
		prefix := fmt.Sprintf("%s/%s", nodesPrefix, fmt.Sprintf("%d", nodeID))
		s := zc.clusterPrinter.start("profile for node %d", nodeID)
		if err := zc.z.createRawOrError(s, prefix+"/cpu.pprof", pd.data, pd.err); err != nil {
			return err
		}
	}
	return nil
}

func (zc *debugZipContext) collectPerNodeData(
	ctx context.Context,
	nodeDetails serverpb.NodeDetails,
	nodeStatus *statuspb.NodeStatus,
	livenessByNodeID nodeLivenesses,
) error {
	nodeID := roachpb.NodeID(nodeDetails.NodeID)

	if livenessByNodeID != nil {
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
	}
	nodePrinter := zipCtx.newZipReporter("node %d", nodeID)
	id := fmt.Sprintf("%d", nodeID)
	prefix := fmt.Sprintf("%s/%s", nodesPrefix, id)

	if !zipCtx.nodes.isIncluded(nodeID) {
		if err := zc.z.createRaw(nodePrinter.start("skipping node"), prefix+".skipped",
			[]byte(fmt.Sprintf("skipping excluded node %d\n", nodeID))); err != nil {
			return err
		}
		return nil
	}
	if nodeStatus != nil {
		// Use nodeStatus to populate the status.json file as it contains more data for a KV node.
		if err := zc.z.createJSON(nodePrinter.start("node status"), prefix+"/status.json", *nodeStatus); err != nil {
			return err
		}
	} else {
		if err := zc.z.createJSON(nodePrinter.start("node status"), prefix+"/status.json", nodeDetails); err != nil {
			return err
		}
	}

	// Don't use sqlConn because that's only for is the node `debug
	// zip` was pointed at, but here we want to connect to nodes
	// individually to grab node- local SQL tables. Try to guess by
	// replacing the host in the connection string; this may or may
	// not work and if it doesn't, we let the invalid curSQLConn get
	// used anyway so that anything that does *not* need it will
	// still happen.
	sqlAddr := nodeDetails.SQLAddress
	if sqlAddr.IsEmpty() {
		sqlAddr = nodeDetails.Address
	}
	curSQLConn := guessNodeURL(zc.firstNodeSQLConn.GetURL(), sqlAddr.AddressField)
	nodePrinter.info("using SQL connection URL: %s", curSQLConn.GetURL())

	for _, table := range zipInternalTablesPerNode.GetTables() {
		query, err := zipInternalTablesPerNode.QueryForTable(table, zipCtx.redact)
		if err != nil {
			return err
		}
		if err := zc.dumpTableDataForZip(nodePrinter, curSQLConn, prefix, table, query); err != nil {
			return errors.Wrapf(err, "fetching %s", table)
		}
	}

	perNodeZipRequests := makePerNodeZipRequests(prefix, id, zc.status)

	for _, r := range perNodeZipRequests {
		if err := zc.runZipRequest(ctx, nodePrinter, r); err != nil {
			return err
		}
	}

	var stacksData []byte
	s := nodePrinter.start("requesting stacks")
	requestErr := zc.runZipFn(ctx, s,
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
	if err := zc.z.createRawOrError(s, prefix+"/stacks.txt", stacksData, requestErr); err != nil {
		return err
	}

	var stacksDataWithLabels []byte
	s = nodePrinter.start("requesting stacks with labels")
	// This condition is added to workaround https://github.com/cockroachdb/cockroach/issues/74133.
	// Please make the call to retrieve stacks unconditional after the issue is fixed.
	if util.RaceEnabled {
		stacksDataWithLabels = []byte("disabled in race mode, see 74133")
	} else {
		requestErr = zc.runZipFn(ctx, s,
			func(ctx context.Context) error {
				stacks, err := zc.status.Stacks(ctx, &serverpb.StacksRequest{
					NodeId: id,
					Type:   serverpb.StacksType_GOROUTINE_STACKS_DEBUG_1,
				})
				if err == nil {
					stacksDataWithLabels = stacks.Data
				}
				return err
			})
	}
	if err := zc.z.createRawOrError(s, prefix+"/stacks_with_labels.txt", stacksDataWithLabels, requestErr); err != nil {
		return err
	}

	var heapData []byte
	s = nodePrinter.start("requesting heap profile")
	requestErr = zc.runZipFn(ctx, s,
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
	if err := zc.z.createRawOrError(s, prefix+"/heap.pprof", heapData, requestErr); err != nil {
		return err
	}

	var profiles *serverpb.GetFilesResponse
	s = nodePrinter.start("requesting heap file list")
	if requestErr := zc.runZipFn(ctx, s,
		func(ctx context.Context) error {
			var err error
			profiles, err = zc.status.GetFiles(ctx, &serverpb.GetFilesRequest{
				NodeId:   id,
				Type:     serverpb.FileType_HEAP,
				Patterns: zipCtx.files.retrievalPatterns(),
				ListOnly: true,
			})
			return err
		}); requestErr != nil {
		if err := zc.z.createError(s, prefix+"/heapprof", requestErr); err != nil {
			return err
		}
	} else {
		s.done()

		// Now filter the list of files and for each file selected,
		// retrieve it.
		//
		// We retrieve the files one by one to avoid loading up multiple
		// files' worth of data server-side in one response in RAM. This
		// sequential processing is not significantly slower than
		// requesting multiple files at once, because these files are
		// large and the transfer time is mostly incurred in the data
		// transmission, not the request-response roundtrip latency.
		// Additionally, cross-node concurrency is parallelizing these
		// transfers somehow.

		nodePrinter.info("%d heap profiles found", len(profiles.Files))
		for _, file := range profiles.Files {
			ctime := extractTimeFromFileName(file.Name)
			if !zipCtx.files.isIncluded(file.Name, ctime, ctime) {
				nodePrinter.info("skipping excluded heap profile: %s", file.Name)
				continue
			}

			fName := maybeAddProfileSuffix(file.Name)
			name := prefix + "/heapprof/" + fName
			fs := nodePrinter.start("retrieving %s", file.Name)
			var oneprof *serverpb.GetFilesResponse
			if fileErr := zc.runZipFn(ctx, fs, func(ctx context.Context) error {
				var err error
				oneprof, err = zc.status.GetFiles(ctx, &serverpb.GetFilesRequest{
					NodeId:   id,
					Type:     serverpb.FileType_HEAP,
					Patterns: []string{file.Name},
					ListOnly: false, // Retrieve the file contents.
				})
				return err
			}); fileErr != nil {
				if err := zc.z.createError(fs, name, fileErr); err != nil {
					return err
				}
			} else {
				fs.done()

				if len(oneprof.Files) < 1 {
					// This is possible if the file was removed in-between
					// the list request above and the content retrieval request.
					continue
				}
				file := oneprof.Files[0]
				if err := zc.z.createRaw(nodePrinter.start("writing profile"), name, file.Contents); err != nil {
					return err
				}
			}
		}
	}

	var goroutinesResp *serverpb.GetFilesResponse
	s = nodePrinter.start("requesting goroutine dump list")
	if requestErr := zc.runZipFn(ctx, s,
		func(ctx context.Context) error {
			var err error
			goroutinesResp, err = zc.status.GetFiles(ctx, &serverpb.GetFilesRequest{
				NodeId:   id,
				Type:     serverpb.FileType_GOROUTINES,
				Patterns: zipCtx.files.retrievalPatterns(),
				ListOnly: true,
			})
			return err
		}); requestErr != nil {
		if err := zc.z.createError(s, prefix+"/goroutines", requestErr); err != nil {
			return err
		}
	} else {
		s.done()

		// Now filter the list of files and for each file selected,
		// retrieve it.
		//
		// We retrieve the files one by one to avoid loading up multiple
		// files' worth of data server-side in one response in RAM. This
		// sequential processing is not significantly slower than
		// requesting multiple files at once, because these files are
		// large and the transfer time is mostly incurred in the data
		// transmission, not the request-response roundtrip latency.
		// Additionally, cross-node concurrency is parallelizing these
		// transfers somehow.

		nodePrinter.info("%d goroutine dumps found", len(goroutinesResp.Files))
		for _, file := range goroutinesResp.Files {
			ctime := extractTimeFromFileName(file.Name)
			if !zipCtx.files.isIncluded(file.Name, ctime, ctime) {
				nodePrinter.info("skipping excluded goroutine dump: %s", file.Name)
				continue
			}

			// NB: the files have a .txt.gz suffix already.
			name := prefix + "/goroutines/" + file.Name

			fs := nodePrinter.start("retrieving %s", file.Name)
			var onedump *serverpb.GetFilesResponse
			if fileErr := zc.runZipFn(ctx, fs, func(ctx context.Context) error {
				var err error
				onedump, err = zc.status.GetFiles(ctx, &serverpb.GetFilesRequest{
					NodeId:   id,
					Type:     serverpb.FileType_GOROUTINES,
					Patterns: []string{file.Name},
					ListOnly: false, // Retrieve the file contents.
				})
				return err
			}); fileErr != nil {
				if err := zc.z.createError(fs, name, fileErr); err != nil {
					return err
				}
			} else {
				fs.done()

				if len(onedump.Files) < 1 {
					// This is possible if the file was removed in-between
					// the list request above and the content retrieval request.
					continue
				}
				file := onedump.Files[0]
				if err := zc.z.createRaw(nodePrinter.start("writing dump"), name, file.Contents); err != nil {
					return err
				}
			}
		}
	}

	var logs *serverpb.LogFilesListResponse
	s = nodePrinter.start("requesting log files list")
	if requestErr := zc.runZipFn(ctx, s,
		func(ctx context.Context) error {
			var err error
			logs, err = zc.status.LogFilesList(
				ctx, &serverpb.LogFilesListRequest{NodeId: id})
			return err
		}); requestErr != nil {
		if err := zc.z.createError(s, prefix+"/logs", requestErr); err != nil {
			return err
		}
	} else {
		s.done()

		// Now filter the list of files and for each file selected,
		// retrieve it.
		//
		// We retrieve the files one by one to avoid loading up multiple
		// files' worth of data server-side in one response in RAM. This
		// sequential processing is not significantly slower than
		// requesting multiple files at once, because these files are
		// large and the transfer time is mostly incurred in the data
		// transmission, not the request-response roundtrip latency.
		// Additionally, cross-node concurrency is parallelizing these
		// transfers somehow.

		nodePrinter.info("%d log files found", len(logs.Files))
		for _, file := range logs.Files {
			ctime := extractTimeFromFileName(file.Name)
			mtime := timeutil.Unix(0, file.ModTimeNanos)
			if !zipCtx.files.isIncluded(file.Name, ctime, mtime) {
				nodePrinter.info("skipping excluded log file: %s", file.Name)
				continue
			}

			logPrinter := nodePrinter.withPrefix("log file: %s", file.Name)
			name := prefix + "/logs/" + file.Name
			var entries *serverpb.LogEntriesResponse
			sf := logPrinter.start("requesting file")
			if requestErr := zc.runZipFn(ctx, sf,
				func(ctx context.Context) error {
					var err error
					entries, err = zc.status.LogFile(
						ctx, &serverpb.LogFileRequest{
							NodeId: id, File: file.Name, Redact: zipCtx.redact,
						})
					return err
				}); requestErr != nil {
				if err := zc.z.createError(sf, name, requestErr); err != nil {
					return err
				}
				continue
			}
			sf.progress("writing output: %s", name)
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
					if zipCtx.redact && !e.Redactable {
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
				return sf.fail(err)
			}
			sf.done()
			if warnRedactLeak {
				// Defer the warning, so that it does not get "drowned" as
				// part of the main zip output.
				defer func(fileName string) {
					fmt.Fprintf(stderr, "WARNING: server-side redaction failed for %s, completed client-side (--redact=true)\n", fileName)
				}(file.Name)
			}
		}
	}

	var ranges *serverpb.RangesResponse
	s = nodePrinter.start("requesting ranges")
	if requestErr := zc.runZipFn(ctx, s, func(ctx context.Context) error {
		var err error
		ranges, err = zc.status.Ranges(ctx, &serverpb.RangesRequest{NodeId: id})
		return err
	}); requestErr != nil {
		if err := zc.z.createError(s, prefix+"/ranges", requestErr); err != nil {
			return err
		}
	} else {
		s.done()
		nodePrinter.info("%d ranges found", len(ranges.Ranges))
		sort.Slice(ranges.Ranges, func(i, j int) bool {
			return ranges.Ranges[i].State.Desc.RangeID <
				ranges.Ranges[j].State.Desc.RangeID
		})
		for _, r := range ranges.Ranges {
			s := nodePrinter.start("writing range %d", r.State.Desc.RangeID)
			name := fmt.Sprintf("%s/ranges/%s", prefix, r.State.Desc.RangeID)
			if err := zc.z.createJSON(s, name+".json", r); err != nil {
				return err
			}
		}
	}
	return nil
}

func guessNodeURL(workingURL string, hostport string) clisqlclient.Conn {
	u, err := url.Parse(workingURL)
	if err != nil {
		u = &url.URL{Host: "invalid"}
	}
	u.Host = hostport
	return sqlConnCtx.MakeSQLConn(os.Stdout, stderr, u.String())
}
