// Copyright 2017 The Cockroach Authors.
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
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/spf13/cobra"
)

var debugZipCmd = &cobra.Command{
	Use:   "zip <file>",
	Short: "gather cluster debug data into a zip file",
	Long: `

Gather cluster debug data into a zip file. Data includes cluster events, node
liveness, node status, range status, node stack traces, node engine stats, log
files, and SQL schema.

Retrieval of per-node details (status, stack traces, range status, engine stats)
requires the node to be live and operating properly. Retrieval of SQL data
requires the cluster to be live.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugZip),
}

// Tables containing cluster-wide info that are collected in a debug zip.
var debugZipTablesPerCluster = []string{
	"crdb_internal.cluster_queries",
	"crdb_internal.cluster_sessions",
	"crdb_internal.cluster_settings",
	"crdb_internal.cluster_transactions",

	"crdb_internal.jobs",
	"system.jobs",       // get the raw, restorable jobs records too.
	"system.descriptor", // descriptors also contain job-like mutation state.
	"system.namespace",
	"system.namespace2", // TODO(sqlexec): consider removing in 20.2 or later.

	"crdb_internal.kv_node_status",
	"crdb_internal.kv_store_status",

	"crdb_internal.schema_changes",
	"crdb_internal.partitions",
	"crdb_internal.zones",
}

// Tables collected from each node in a debug zip.
var debugZipTablesPerNode = []string{
	"crdb_internal.feature_usage",

	"crdb_internal.gossip_alerts",
	"crdb_internal.gossip_liveness",
	"crdb_internal.gossip_network",
	"crdb_internal.gossip_nodes",

	"crdb_internal.leases",

	"crdb_internal.node_build_info",
	"crdb_internal.node_metrics",
	"crdb_internal.node_queries",
	"crdb_internal.node_runtime_info",
	"crdb_internal.node_sessions",
	"crdb_internal.node_statement_statistics",
	"crdb_internal.node_transactions",
	"crdb_internal.node_txn_stats",
}

// Override for the default SELECT * when dumping the table.
var customSelectClause = map[string]string{
	"system.jobs":       "*, to_hex(payload) AS hex_payload, to_hex(progress) AS hex_progress",
	"system.descriptor": "*, to_hex(descriptor) AS hex_descriptor",
}

type zipper struct {
	f *os.File
	z *zip.Writer
}

func newZipper(f *os.File) *zipper {
	return &zipper{
		f: f,
		z: zip.NewWriter(f),
	}
}

func (z *zipper) close() error {
	err1 := z.z.Close()
	err2 := z.f.Close()
	return errors.CombineErrors(err1, err2)
}

func (z *zipper) create(name string, mtime time.Time) (io.Writer, error) {
	fmt.Printf("writing: %s\n", name)
	if mtime.IsZero() {
		mtime = timeutil.Now()
	}
	return z.z.CreateHeader(&zip.FileHeader{
		Name:     name,
		Method:   zip.Deflate,
		Modified: mtime,
	})
}

func (z *zipper) createRaw(name string, b []byte) error {
	w, err := z.create(name, time.Time{})
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func (z *zipper) createJSON(name string, m interface{}) error {
	if !strings.HasSuffix(name, ".json") {
		return errors.Errorf("%s does not have .json suffix", name)
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return z.createRaw(name, b)
}

func (z *zipper) createError(name string, e error) error {
	w, err := z.create(name+".err.txt", time.Time{})
	if err != nil {
		return err
	}
	fmt.Printf("  ^- resulted in %s\n", e)
	fmt.Fprintf(w, "%s\n", e)
	return nil
}

func (z *zipper) createJSONOrError(name string, m interface{}, e error) error {
	if e != nil {
		return z.createError(name, e)
	}
	return z.createJSON(name, m)
}

func (z *zipper) createRawOrError(name string, b []byte, e error) error {
	if filepath.Ext(name) == "" {
		return errors.Errorf("%s has no extension", name)
	}
	if e != nil {
		return z.createError(name, e)
	}
	return z.createRaw(name, b)
}

type zipRequest struct {
	fn       func(ctx context.Context) (interface{}, error)
	pathName string
}

func guessNodeURL(workingURL string, hostport string) *sqlConn {
	u, err := url.Parse(workingURL)
	if err != nil {
		u = &url.URL{Host: "invalid"}
	}
	u.Host = hostport
	return makeSQLConn(u.String())
}

func runZipRequestWithTimeout(
	ctx context.Context,
	requestName string,
	timeout time.Duration,
	fn func(ctx context.Context) error,
) error {
	fmt.Printf("%s... ", requestName)
	return contextutil.RunWithTimeout(ctx, requestName, timeout, fn)
}

func runDebugZip(cmd *cobra.Command, args []string) (retErr error) {
	const (
		base          = "debug"
		eventsName    = base + "/events"
		livenessName  = base + "/liveness"
		nodesPrefix   = base + "/nodes"
		rangelogName  = base + "/rangelog"
		reportsPrefix = base + "/reports"
		schemaPrefix  = base + "/schema"
		settingsName  = base + "/settings"
	)

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Printf("establishing RPC connection to %s...\n", serverCfg.AdvertiseAddr)
	conn, _, finish, err := getClientGRPCConn(baseCtx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	status := serverpb.NewStatusClient(conn)
	admin := serverpb.NewAdminClient(conn)

	fmt.Println("retrieving the node status to get the SQL address...")
	nodeD, err := status.Details(baseCtx, &serverpb.DetailsRequest{NodeId: "local"})
	if err != nil {
		return err
	}
	sqlAddr := nodeD.SQLAddress
	if sqlAddr.IsEmpty() {
		// No SQL address: either a pre-19.2 node, or same address for both
		// SQL and RPC.
		sqlAddr = nodeD.Address
	}
	fmt.Printf("using SQL address: %s\n", sqlAddr.AddressField)
	cliCtx.clientConnHost, cliCtx.clientConnPort, err = net.SplitHostPort(sqlAddr.AddressField)
	if err != nil {
		return err
	}

	// We're going to use the SQL code, but in non-interactive mode.
	// Override whatever terminal-driven defaults there may be out there.
	cliCtx.isInteractive = false
	cliCtx.terminalOutput = false
	sqlCtx.showTimes = false
	// Use a streaming format to avoid accumulating all rows in RAM.
	cliCtx.tableDisplayFormat = tableDisplayTSV

	sqlConn, err := makeSQLClient("cockroach zip", useSystemDb)
	if err != nil {
		log.Warningf(baseCtx, "unable to open a SQL session. Debug information will be incomplete: %s", err)
	}
	defer sqlConn.Close()
	// Note: we're not printing "connection established" because the driver we're using
	// does late binding.
	if sqlConn != nil {
		fmt.Printf("using SQL connection URL: %s\n", sqlConn.url)
	}

	name := args[0]
	out, err := os.Create(name)
	if err != nil {
		return err
	}
	fmt.Printf("writing %s\n", name)

	z := newZipper(out)
	defer func() {
		cErr := z.close()
		retErr = errors.CombineErrors(retErr, cErr)
	}()

	timeout := 10 * time.Second
	if cliCtx.cmdTimeout != 0 {
		timeout = cliCtx.cmdTimeout
	}

	var runZipRequest = func(r zipRequest) error {
		var data interface{}
		err = runZipRequestWithTimeout(baseCtx, "requesting data for "+r.pathName, timeout, func(ctx context.Context) error {
			data, err = r.fn(ctx)
			return err
		})
		return z.createJSONOrError(r.pathName+".json", data, err)
	}

	for _, r := range []zipRequest{
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return admin.Events(ctx, &serverpb.EventsRequest{})
			},
			pathName: eventsName,
		},
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return admin.RangeLog(ctx, &serverpb.RangeLogRequest{})
			},
			pathName: rangelogName,
		},
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return admin.Liveness(ctx, &serverpb.LivenessRequest{})
			},
			pathName: livenessName,
		},
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return admin.Settings(ctx, &serverpb.SettingsRequest{})
			},
			pathName: settingsName,
		},
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return status.ProblemRanges(ctx, &serverpb.ProblemRangesRequest{})
			},
			pathName: reportsPrefix + "/problemranges",
		},
	} {
		if err := runZipRequest(r); err != nil {
			return err
		}
	}

	for _, table := range debugZipTablesPerCluster {
		selectClause, ok := customSelectClause[table]
		if !ok {
			selectClause = "*"
		}
		if err := dumpTableDataForZip(z, sqlConn, timeout, base, table, selectClause); err != nil {
			return errors.Wrapf(err, "fetching %s", table)
		}
	}

	{
		var nodes *serverpb.NodesResponse
		err := runZipRequestWithTimeout(baseCtx, "requesting nodes", timeout, func(ctx context.Context) error {
			nodes, err = status.Nodes(ctx, &serverpb.NodesRequest{})
			return err
		})
		if cErr := z.createJSONOrError(base+"/nodes.json", nodes, err); cErr != nil {
			return cErr
		}

		// In case nodes came up back empty (the Nodes() RPC failed), we
		// still want to inspect the per-node endpoints on the head
		// node. As per the above, we were able to connect at least to
		// that.
		nodeList := []statuspb.NodeStatus{{Desc: roachpb.NodeDescriptor{
			NodeID:     nodeD.NodeID,
			Address:    nodeD.Address,
			SQLAddress: nodeD.SQLAddress,
		}}}
		if nodes != nil {
			// If the nodes were found, use that instead.
			nodeList = nodes.Nodes
		}

		// We'll want livenesses to decide whether a node is decommissioned.
		var lresponse *serverpb.LivenessResponse
		err = runZipRequestWithTimeout(baseCtx, "requesting liveness", timeout, func(ctx context.Context) error {
			lresponse, err = admin.Liveness(ctx, &serverpb.LivenessRequest{})
			return err
		})
		if cErr := z.createJSONOrError(base+"/liveness.json", nodes, err); cErr != nil {
			return cErr
		}
		livenessByNodeID := map[roachpb.NodeID]kvserverpb.NodeLivenessStatus{}
		if lresponse != nil {
			livenessByNodeID = lresponse.Statuses
		}

		for _, node := range nodeList {
			nodeID := node.Desc.NodeID

			liveness := livenessByNodeID[nodeID]
			if liveness == kvserverpb.NodeLivenessStatus_DECOMMISSIONED {
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
				continue
			}

			id := fmt.Sprintf("%d", nodeID)
			prefix := fmt.Sprintf("%s/%s", nodesPrefix, id)

			if !zipCtx.nodes.isIncluded(nodeID) {
				if err := z.createRaw(prefix+".skipped",
					[]byte(fmt.Sprintf("skipping excluded node %d\n", nodeID))); err != nil {
					return err
				}
				continue
			}

			// Don't use sqlConn because that's only for is the node `debug
			// zip` was pointed at, but here we want to connect to nodes
			// individually to grab node- local SQL tables. Try to guess by
			// replacing the host in the connection string; this may or may
			// not work and if it doesn't, we let the invalid curSQLConn get
			// used anyway so that anything that does *not* need it will
			// still happen.
			sqlAddr := node.Desc.SQLAddress
			if sqlAddr.IsEmpty() {
				// No SQL address: either a pre-19.2 node, or same address for both
				// SQL and RPC.
				sqlAddr = node.Desc.Address
			}
			curSQLConn := guessNodeURL(sqlConn.url, sqlAddr.AddressField)
			if err := z.createJSON(prefix+"/status.json", node); err != nil {
				return err
			}
			fmt.Printf("using SQL connection URL for node %s: %s\n", id, curSQLConn.url)

			for _, table := range debugZipTablesPerNode {
				selectClause, ok := customSelectClause[table]
				if !ok {
					selectClause = "*"
				}
				if err := dumpTableDataForZip(z, curSQLConn, timeout, prefix, table, selectClause); err != nil {
					return errors.Wrapf(err, "fetching %s", table)
				}
			}

			for _, r := range []zipRequest{
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
			} {
				if err := runZipRequest(r); err != nil {
					return err
				}
			}

			var stacksData []byte
			err = runZipRequestWithTimeout(baseCtx, "requesting stacks for node "+id, timeout,
				func(ctx context.Context) error {
					stacks, err := status.Stacks(ctx, &serverpb.StacksRequest{
						NodeId: id,
						Type:   serverpb.StacksType_GOROUTINE_STACKS,
					})
					if err == nil {
						stacksData = stacks.Data
					}
					return err
				})
			if err := z.createRawOrError(prefix+"/stacks.txt", stacksData, err); err != nil {
				return err
			}

			var threadData []byte
			err = runZipRequestWithTimeout(baseCtx, "requesting threads for node "+id, timeout,
				func(ctx context.Context) error {
					threads, err := status.Stacks(ctx, &serverpb.StacksRequest{
						NodeId: id,
						Type:   serverpb.StacksType_THREAD_STACKS,
					})
					if err == nil {
						threadData = threads.Data
					}
					return err
				})
			if err := z.createRawOrError(prefix+"/threads.txt", threadData, err); err != nil {
				return err
			}

			var heapData []byte
			err = runZipRequestWithTimeout(baseCtx, "requesting heap profile for node "+id, timeout,
				func(ctx context.Context) error {
					heap, err := status.Profile(ctx, &serverpb.ProfileRequest{
						NodeId: id,
						Type:   serverpb.ProfileRequest_HEAP,
					})
					if err == nil {
						heapData = heap.Data
					}
					return err
				})
			if err := z.createRawOrError(prefix+"/heap.pprof", heapData, err); err != nil {
				return err
			}

			var profiles *serverpb.GetFilesResponse
			if err := runZipRequestWithTimeout(baseCtx, "requesting heap files for node "+id, timeout,
				func(ctx context.Context) error {
					profiles, err = status.GetFiles(ctx, &serverpb.GetFilesRequest{
						NodeId:   id,
						Type:     serverpb.FileType_HEAP,
						Patterns: []string{"*"},
					})
					return err
				}); err != nil {
				if err := z.createError(prefix+"/heapprof", err); err != nil {
					return err
				}
			} else {
				fmt.Printf("%d found\n", len(profiles.Files))
				for _, file := range profiles.Files {
					name := prefix + "/heapprof/" + file.Name + ".pprof"
					if err := z.createRaw(name, file.Contents); err != nil {
						return err
					}
				}
			}

			var goroutinesResp *serverpb.GetFilesResponse
			if err := runZipRequestWithTimeout(baseCtx, "requesting goroutine files for node "+id, timeout,
				func(ctx context.Context) error {
					goroutinesResp, err = status.GetFiles(ctx, &serverpb.GetFilesRequest{
						NodeId:   id,
						Type:     serverpb.FileType_GOROUTINES,
						Patterns: []string{"*"},
					})
					return err
				}); err != nil {
				if err := z.createError(prefix+"/goroutines", err); err != nil {
					return err
				}
			} else {
				fmt.Printf("%d found\n", len(goroutinesResp.Files))
				for _, file := range goroutinesResp.Files {
					// NB: the files have a .txt.gz suffix already.
					name := prefix + "/goroutines/" + file.Name
					if err := z.createRawOrError(name, file.Contents, err); err != nil {
						return err
					}
				}
			}

			var logs *serverpb.LogFilesListResponse
			if err := runZipRequestWithTimeout(baseCtx, "requesting log files list", timeout,
				func(ctx context.Context) error {
					logs, err = status.LogFilesList(
						ctx, &serverpb.LogFilesListRequest{NodeId: id})
					return err
				}); err != nil {
				if err := z.createError(prefix+"/logs", err); err != nil {
					return err
				}
			} else {
				fmt.Printf("%d found\n", len(logs.Files))
				for _, file := range logs.Files {
					name := prefix + "/logs/" + file.Name
					var entries *serverpb.LogEntriesResponse
					if err := runZipRequestWithTimeout(baseCtx, fmt.Sprintf("requesting log file %s", file.Name), timeout,
						func(ctx context.Context) error {
							entries, err = status.LogFile(
								ctx, &serverpb.LogFileRequest{
									NodeId: id, File: file.Name, Redact: zipCtx.redactLogs, KeepRedactable: true,
								})
							return err
						}); err != nil {
						if err := z.createError(name, err); err != nil {
							return err
						}
						continue
					}
					logOut, err := z.create(name, timeutil.Unix(0, file.ModTimeNanos))
					if err != nil {
						return err
					}
					warnRedactLeak := false
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
						if err := e.Format(logOut); err != nil {
							return err
						}
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
			if err := runZipRequestWithTimeout(baseCtx, "requesting ranges", timeout, func(ctx context.Context) error {
				ranges, err = status.Ranges(ctx, &serverpb.RangesRequest{NodeId: id})
				return err
			}); err != nil {
				if err := z.createError(prefix+"/ranges", err); err != nil {
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
					if err := z.createJSON(name+".json", r); err != nil {
						return err
					}
				}
			}
		}
	}

	{
		var databases *serverpb.DatabasesResponse
		if err := runZipRequestWithTimeout(baseCtx, "requesting list of SQL databases", timeout, func(ctx context.Context) error {
			databases, err = admin.Databases(ctx, &serverpb.DatabasesRequest{})
			return err
		}); err != nil {
			if err := z.createError(schemaPrefix, err); err != nil {
				return err
			}
		} else {
			fmt.Printf("%d found\n", len(databases.Databases))
			var dbEscaper fileNameEscaper
			for _, dbName := range databases.Databases {
				prefix := schemaPrefix + "/" + dbEscaper.escape(dbName)
				var database *serverpb.DatabaseDetailsResponse
				requestErr := runZipRequestWithTimeout(baseCtx, fmt.Sprintf("requesting database details for %s", dbName), timeout,
					func(ctx context.Context) error {
						database, err = admin.DatabaseDetails(ctx, &serverpb.DatabaseDetailsRequest{Database: dbName})
						return err
					})
				if err := z.createJSONOrError(prefix+"@details.json", database, requestErr); err != nil {
					return err
				}
				if requestErr != nil {
					continue
				}

				fmt.Printf("%d tables found\n", len(database.TableNames))
				var tbEscaper fileNameEscaper
				for _, tableName := range database.TableNames {
					name := prefix + "/" + tbEscaper.escape(tableName)
					var table *serverpb.TableDetailsResponse
					err := runZipRequestWithTimeout(baseCtx, fmt.Sprintf("requesting table details for %s.%s", dbName, tableName), timeout,
						func(ctx context.Context) error {
							table, err = admin.TableDetails(ctx, &serverpb.TableDetailsRequest{Database: dbName, Table: tableName})
							return err
						})
					if err := z.createJSONOrError(name+".json", table, err); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

type fileNameEscaper struct {
	counters map[string]int
}

// escape ensures that f is stripped of characters that
// may be invalid in file names. The characters are also lowercased
// to ensure proper normalization in case-insensitive filesystems.
func (fne *fileNameEscaper) escape(f string) string {
	f = strings.ToLower(f)
	var out strings.Builder
	for _, c := range f {
		if c < 127 && (unicode.IsLetter(c) || unicode.IsDigit(c)) {
			out.WriteRune(c)
		} else {
			out.WriteByte('_')
		}
	}
	objName := out.String()
	result := objName

	if fne.counters == nil {
		fne.counters = make(map[string]int)
	}
	cnt := fne.counters[objName]
	if cnt > 0 {
		result += fmt.Sprintf("-%d", cnt)
	}
	cnt++
	fne.counters[objName] = cnt
	return result
}

func dumpTableDataForZip(
	z *zipper, conn *sqlConn, timeout time.Duration, base, table, selectClause string,
) error {
	query := fmt.Sprintf(`SET statement_timeout = '%s'; SELECT %s FROM %s`, timeout, selectClause, table)
	baseName := base + "/" + table

	fmt.Printf("retrieving SQL data for %s... ", table)
	const maxRetries = 5
	suffix := ""
	for numRetries := 1; numRetries <= maxRetries; numRetries++ {
		name := baseName + suffix + ".txt"
		w, err := z.create(name, time.Time{})
		if err != nil {
			return err
		}
		// Pump the SQL rows directly into the zip writer, to avoid
		// in-RAM buffering.
		if err := runQueryAndFormatResults(conn, w, makeQuery(query)); err != nil {
			if cErr := z.createError(name, err); cErr != nil {
				return cErr
			}
			var pqErr *pq.Error
			if !errors.As(err, &pqErr) {
				// Not a SQL error. Nothing to retry.
				break
			}
			if pgcode.MakeCode(string(pqErr.Code)) != pgcode.SerializationFailure {
				// A non-retry error. We've printed the error, and
				// there's nothing to retry. Stop here.
				break
			}
			// We've encountered a retry error. Add a suffix then loop.
			suffix = fmt.Sprintf(".%d", numRetries)
			continue
		}
		break
	}
	return nil
}

type nodeSelection struct {
	inclusive     rangeSelection
	exclusive     rangeSelection
	includedCache map[int]struct{}
	excludedCache map[int]struct{}
}

func (n *nodeSelection) isIncluded(nodeID roachpb.NodeID) bool {
	// Avoid recomputing the maps on every call.
	if n.includedCache == nil {
		n.includedCache = n.inclusive.items()
	}
	if n.excludedCache == nil {
		n.excludedCache = n.exclusive.items()
	}

	// If the included cache is empty, then we're assuming the node is included.
	isIncluded := true
	if len(n.includedCache) > 0 {
		_, isIncluded = n.includedCache[int(nodeID)]
	}
	// Then filter out excluded IDs.
	if _, excluded := n.excludedCache[int(nodeID)]; excluded {
		isIncluded = false
	}
	return isIncluded
}

type rangeSelection struct {
	input  string
	ranges []vrange
}

type vrange struct {
	a, b int
}

func (r *rangeSelection) String() string { return r.input }

func (r *rangeSelection) Type() string {
	return "a-b,c,d-e,..."
}

func (r *rangeSelection) Set(v string) error {
	r.input = v
	for _, rs := range strings.Split(v, ",") {
		var thisRange vrange
		if strings.Contains(rs, "-") {
			ab := strings.SplitN(rs, "-", 2)
			a, err := strconv.Atoi(ab[0])
			if err != nil {
				return err
			}
			b, err := strconv.Atoi(ab[1])
			if err != nil {
				return err
			}
			if b < a {
				return errors.New("invalid range")
			}
			thisRange = vrange{a, b}
		} else {
			a, err := strconv.Atoi(rs)
			if err != nil {
				return err
			}
			thisRange = vrange{a, a}
		}
		r.ranges = append(r.ranges, thisRange)
	}
	return nil
}

// items returns the values selected by the range selection
func (r *rangeSelection) items() map[int]struct{} {
	s := map[int]struct{}{}
	for _, vr := range r.ranges {
		for i := vr.a; i <= vr.b; i++ {
			s[i] = struct{}{}
		}
	}
	return s
}
