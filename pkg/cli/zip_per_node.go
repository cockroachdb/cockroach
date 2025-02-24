// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var redactedAddress = fmt.Sprintf("%s=%s", rpc.RemoteAddressTag, redact.RedactedMarker())

const (
	regexpOfRemoteAddress   = "[[:alnum:].:-]+"
	stacksFileName          = "stacks.txt"
	stacksWithLabelFileName = "stacks_with_labels.txt"
	heapPprofFileName       = "heap.pprof"
	lsmFileName             = "lsm.txt"
	rangesInfoFileName      = "ranges.json"
	detailsFileName         = "details.json"
	gossipFileName          = "gossip.json"
	statusFileName          = "status.json"
	cpuProfileFileName      = "cpu.pprof"
	detailsName             = "details"
	gossipName              = "gossip"
)

func (zc *debugZipContext) collectPerNodeData(
	ctx context.Context,
	nodeDetails serverpb.NodeDetails,
	nodeStatus *statuspb.NodeStatus,
	livenessByNodeID nodeLivenesses,
	redactedNodeDetails serverpb.NodeDetails,
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

	nodePrinter := zipCtx.newZipReporter(redact.Sprintf("node %d", nodeID))
	id := fmt.Sprintf("%d", nodeID)
	prefix := fmt.Sprintf("%s%s/%s", zc.prefix, nodesPrefix, id)

	if !zipCtx.nodes.isIncluded(nodeID) {
		nodePrinter.info("skipping excluded node")
		return nil
	}
	err := zc.getNodeStatus(nodeStatus, nodePrinter, prefix, redactedNodeDetails)
	if err != nil {
		return err
	}

	err = zc.getInternalTablesPerNode(nodeDetails, nodePrinter, prefix)
	if err != nil {
		return err
	}

	err = zc.getPerNodeMetadata(ctx, prefix, id, nodePrinter)
	if err != nil {
		return err
	}

	err = zc.getStackInformation(ctx, nodePrinter, id, prefix)
	if err != nil {
		return err
	}

	err = zc.getCurrentHeapProfile(ctx, nodePrinter, id, prefix)
	if err != nil {
		return err
	}

	err = zc.getEngineStats(ctx, nodePrinter, id, prefix)
	if err != nil {
		return err
	}

	err = zc.getProfiles(ctx, nodePrinter, id, prefix)
	if err != nil {
		return err
	}

	err = zc.getLogFiles(ctx, nodePrinter, id, prefix)
	if err != nil {
		return err
	}

	err = zc.getRangeInformation(ctx, nodePrinter, id, prefix)
	if err != nil {
		return err
	}
	return nil
}

// makePerNodeZipRequests defines the zipRequests (API requests) that are to be
// performed once per node.
func makePerNodeZipRequests(
	zr *zipReporter, prefix, id string, status serverpb.StatusClient,
) []zipRequest {
	var zipRequests []zipRequest

	if zipCtx.files.shouldIncludeFile(detailsFileName) {
		zipRequests = append(zipRequests, zipRequest{
			fn: func(ctx context.Context) (interface{}, error) {
				return status.Details(ctx, &serverpb.DetailsRequest{NodeId: id, Redact: zipCtx.redact})
			},

			pathName: path.Join(prefix, detailsName),
		})
	} else {
		zr.info("skipping %s due to file filters", detailsFileName)
	}

	if zipCtx.files.shouldIncludeFile(gossipFileName) {
		zipRequests = append(zipRequests, zipRequest{
			fn: func(ctx context.Context) (interface{}, error) {
				return status.Gossip(ctx, &serverpb.GossipRequest{NodeId: id, Redact: zipCtx.redact})
			},
			pathName: path.Join(prefix, gossipName),
		})
	} else {
		zr.info("skipping %s due to file filters", gossipFileName)
	}
	return zipRequests
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
	ctx context.Context, ni *serverpb.NodesListResponse, livenessByNodeID nodeLivenesses,
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
	if !zipCtx.files.shouldIncludeFile(cpuProfileFileName) {
		zc.clusterPrinter.info("skipping %s due to file filters", cpuProfileFileName)
		return nil
	}

	if ni == nil {
		return errors.AssertionFailedf("nodes list is empty; nothing to do")
	}

	nodeList := ni.Nodes
	// NB: this takes care not to produce non-deterministic log output.
	resps := make([]profData, len(nodeList))
	for i := range nodeList {
		nodeID := roachpb.NodeID(nodeList[i].NodeID)
		if livenessByNodeID[nodeID] == livenesspb.NodeLivenessStatus_DECOMMISSIONED {
			continue
		}
		if !zipCtx.nodes.isIncluded(nodeID) {
			zc.clusterPrinter.info(fmt.Sprintf("skipping excluded node %d", nodeID))
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
			err := timeutil.RunWithTimeout(ctx, "fetch cpu profile", zc.timeout+zipCtx.cpuProfDuration, func(ctx context.Context) error {
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
		prefix := fmt.Sprintf("%s%s/%s", zc.prefix, nodesPrefix, fmt.Sprintf("%d", nodeID))
		s := zc.clusterPrinter.start(redact.Sprintf("profile for node %d", nodeID))
		if err := zc.z.createRawOrError(s, prefix+"/"+cpuProfileFileName, pd.data, pd.err); err != nil {
			return err
		}
	}
	return nil
}

// collectFileList is a helper that retrieves all relevant files of the
// specified FileType.
func (zc *debugZipContext) collectFileList(
	ctx context.Context, nodePrinter *zipReporter, id, prefix string, fileType serverpb.FileType,
) error {
	var fileKind string
	switch fileType {
	case serverpb.FileType_HEAP:
		fileKind = "heap profile"
		prefix = prefix + "/heapprof"
	case serverpb.FileType_GOROUTINES:
		fileKind = "goroutine dump"
		prefix = prefix + "/goroutines"
	case serverpb.FileType_CPU:
		fileKind = "cpu profile"
		prefix = prefix + "/cpuprof"
	default:
		return errors.AssertionFailedf("unknown file type: %v", fileType)
	}

	var files *serverpb.GetFilesResponse
	s := nodePrinter.start(redact.Sprintf("requesting %s list", fileKind))
	if requestErr := zc.runZipFn(ctx, s,
		func(ctx context.Context) error {
			var err error
			files, err = zc.status.GetFiles(ctx, &serverpb.GetFilesRequest{
				NodeId:   id,
				Type:     fileType,
				Patterns: zipCtx.files.retrievalPatterns(),
				ListOnly: true,
			})
			return err
		}); requestErr != nil {
		if err := zc.z.createError(s, prefix, requestErr); err != nil {
			return err
		}
	} else {
		s.done()

		// Now filter the list of files and for each file selected, retrieve it.
		//
		// We retrieve the files one by one to avoid loading up multiple files'
		// worth of data server-side in one response in RAM. This sequential
		// processing is not significantly slower than requesting multiple files
		// at once, because these files are large and the transfer time is
		// mostly incurred in the data transmission, not the request-response
		// round-trip latency. Additionally, cross-node concurrency is
		// parallelizing these transfers somehow.
		nodePrinter.info("%d %ss found", len(files.Files), fileKind)
		for _, file := range files.Files {
			ctime := extractTimeFromFileName(file.Name)
			if !zipCtx.files.isIncluded(file.Name, ctime, ctime) {
				nodePrinter.info("skipping excluded %s: %s due to file filters", fileKind, file.Name)
				continue
			}

			// NB: for goroutine dumps, the files have a .txt.gz suffix already.
			name := prefix + "/" + file.Name
			fs := nodePrinter.start(redact.Sprintf("retrieving %s", file.Name))
			var onefile *serverpb.GetFilesResponse
			if fileErr := zc.runZipFn(ctx, fs, func(ctx context.Context) error {
				var err error
				onefile, err = zc.status.GetFiles(ctx, &serverpb.GetFilesRequest{
					NodeId:   id,
					Type:     fileType,
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

				if len(onefile.Files) < 1 {
					// This is possible if the file was removed in-between the
					// list request above and the content retrieval request.
					continue
				}
				file := onefile.Files[0]
				if err := zc.z.createRaw(nodePrinter.start("writing profile"), name, file.Contents); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (zc *debugZipContext) getRangeInformation(
	ctx context.Context, nodePrinter *zipReporter, id string, prefix string,
) error {
	if zipCtx.includeRangeInfo {
		if !zipCtx.files.shouldIncludeFile(rangesInfoFileName) {
			nodePrinter.info("skipping %s due to file filters", rangesInfoFileName)
			return nil
		}
		var ranges *serverpb.RangesResponse
		s := nodePrinter.start("requesting ranges")
		if requestErr := zc.runZipFn(ctx, s, func(ctx context.Context) error {
			var err error
			ranges, err = zc.status.Ranges(ctx, &serverpb.RangesRequest{NodeId: id, Redact: zipCtx.redact})
			return err
		}); requestErr != nil {
			if err := zc.z.createError(s, prefix+"/ranges", requestErr); err != nil {
				return err
			}
		} else {
			s.done()
			sort.Slice(ranges.Ranges, func(i, j int) bool {
				return ranges.Ranges[i].State.Desc.RangeID <
					ranges.Ranges[j].State.Desc.RangeID
			})
			s := nodePrinter.start("writing ranges")
			name := fmt.Sprintf("%s/%s", prefix, rangesInfoFileName)
			if err := zc.z.createJSON(s, name, ranges.Ranges); err != nil {
				return err
			}
		}
	}
	return nil
}

func (zc *debugZipContext) getLogFiles(
	ctx context.Context, nodePrinter *zipReporter, id string, prefix string,
) error {
	var logs *serverpb.LogFilesListResponse
	s := nodePrinter.start("requesting log files list")
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
				nodePrinter.info("skipping excluded log file: %s due to file filters", file.Name)
				continue
			}

			logPrinter := nodePrinter.withPrefix(redact.Sprintf("log file: %s", file.Name))
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
			// Log the list of errors that occurred during log entries request.
			if len(entries.ParseErrors) > 0 {
				sf.shout("%d parsing errors occurred:", len(entries.ParseErrors))
				parseErr := fmt.Errorf("%d errors occurred:\n%s", len(entries.ParseErrors), strings.Join(entries.ParseErrors, "\n"))
				if err := zc.z.createError(sf, name, parseErr); err != nil {
					return err
				}
			}
			sf = logPrinter.start("writing output")
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
				//nolint:deferloop TODO(#137605)
				defer func(fileName string) {
					fmt.Fprintf(stderr, "WARNING: server-side redaction failed for %s, completed client-side (--redact=true)\n", fileName)
				}(file.Name)
			}
		}
	}
	return nil
}

func (zc *debugZipContext) getProfiles(
	ctx context.Context, nodePrinter *zipReporter, id string, prefix string,
) error {
	// Collect all relevant heap profiles.
	if err := zc.collectFileList(ctx, nodePrinter, id, prefix, serverpb.FileType_HEAP); err != nil {
		return err
	}

	// Collect all relevant goroutine dumps.
	if err := zc.collectFileList(ctx, nodePrinter, id, prefix, serverpb.FileType_GOROUTINES); err != nil {
		return err
	}

	// Collect all relevant cpu profiles.
	if err := zc.collectFileList(ctx, nodePrinter, id, prefix, serverpb.FileType_CPU); err != nil {
		return err
	}
	return nil
}

func (zc *debugZipContext) getEngineStats(
	ctx context.Context, nodePrinter *zipReporter, id string, prefix string,
) error {
	if !zipCtx.files.shouldIncludeFile(lsmFileName) {
		nodePrinter.info("skipping %s due to file filters", lsmFileName)
		return nil
	}
	// Collect storage engine metrics using the same format as the /debug/lsm route.
	var lsmStats string
	s := nodePrinter.start("requesting engine stats")
	requestErr := zc.runZipFn(ctx, s,
		func(ctx context.Context) error {
			resp, err := zc.status.EngineStats(ctx, &serverpb.EngineStatsRequest{NodeId: id})
			if err == nil {
				lsmStats = debug.FormatLSMStats(resp.StatsByStoreId)
			}
			return err
		})
	if err := zc.z.createRawOrError(s, prefix+"/"+lsmFileName, []byte(lsmStats), requestErr); err != nil {
		return err
	}
	return nil
}

func (zc *debugZipContext) getCurrentHeapProfile(
	ctx context.Context, nodePrinter *zipReporter, id string, prefix string,
) error {
	if !zipCtx.files.shouldIncludeFile(heapPprofFileName) {
		nodePrinter.info("skipping %s due to file filters", heapPprofFileName)
		return nil
	}
	var heapData []byte
	s := nodePrinter.start("requesting heap profile")
	requestErr := zc.runZipFn(ctx, s,
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
	if err := zc.z.createRawOrError(s, prefix+"/"+heapPprofFileName, heapData, requestErr); err != nil {
		return err
	}
	return nil
}

func (zc *debugZipContext) getStackInformation(
	ctx context.Context, nodePrinter *zipReporter, id string, prefix string,
) error {
	if zipCtx.includeStacks {
		if zipCtx.files.shouldIncludeFile(stacksFileName) {
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
			if err := zc.z.createRawOrError(s, prefix+"/"+stacksFileName, stacksData, requestErr); err != nil {
				return err
			}
		} else {
			nodePrinter.info("skipping %s due to file filters", stacksFileName)
		}

		var stacksDataWithLabels []byte
		if zipCtx.files.shouldIncludeFile(stacksWithLabelFileName) {
			s := nodePrinter.start("requesting stacks with labels")
			requestErr := zc.runZipFn(ctx, s,
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
			if zipCtx.redact {
				stacksDataWithLabels = redactStackTrace(stacksDataWithLabels)
			}
			if err := zc.z.createRawOrError(s, prefix+"/"+stacksWithLabelFileName, stacksDataWithLabels, requestErr); err != nil {
				return err
			}
		} else {
			nodePrinter.info("skipping %s due to file filters", stacksWithLabelFileName)
		}
	} else {
		nodePrinter.info("Skipping fetching goroutine stacks. Enable via the --" + cliflags.ZipIncludeGoroutineStacks.Name + " flag.")
	}
	return nil
}

func (zc *debugZipContext) getPerNodeMetadata(
	ctx context.Context, prefix string, id string, nodePrinter *zipReporter,
) error {
	perNodeZipRequests := makePerNodeZipRequests(nodePrinter, prefix, id, zc.status)
	for _, r := range perNodeZipRequests {
		if err := zc.runZipRequest(ctx, nodePrinter, r); err != nil {
			return err
		}
	}
	return nil
}

func (zc *debugZipContext) getInternalTablesPerNode(
	nodeDetails serverpb.NodeDetails, nodePrinter *zipReporter, prefix string,
) error {
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
	return nil
}

func (zc *debugZipContext) getNodeStatus(
	nodeStatus *statuspb.NodeStatus,
	nodePrinter *zipReporter,
	prefix string,
	redactedNodeDetails serverpb.NodeDetails,
) error {
	if !zipCtx.files.shouldIncludeFile(statusFileName) {
		nodePrinter.info("skipping %s due to file filters", statusFileName)
		return nil
	}
	if nodeStatus != nil {
		// Use nodeStatus to populate the status.json file as it contains more data for a KV node.
		if err := zc.z.createJSON(nodePrinter.start("node status"), prefix+"/"+statusFileName, *nodeStatus); err != nil {
			return err
		}
	} else {
		if err := zc.z.createJSON(nodePrinter.start("node status"), prefix+"/"+statusFileName, redactedNodeDetails); err != nil {
			return err
		}
	}
	return nil
}

func redactStackTrace(stacksDataWithLabels []byte) []byte {
	re := regexp.MustCompile(fmt.Sprintf("%s=%s", rpc.RemoteAddressTag, regexpOfRemoteAddress))
	data := re.ReplaceAll(stacksDataWithLabels, []byte(redactedAddress))
	return data
}

func guessNodeURL(workingURL string, hostport string) clisqlclient.Conn {
	u, err := url.Parse(workingURL)
	if err != nil {
		u = &url.URL{Host: "invalid"}
	}
	u.Host = hostport
	return sqlConnCtx.MakeSQLConn(os.Stdout, stderr, u.String())
}
