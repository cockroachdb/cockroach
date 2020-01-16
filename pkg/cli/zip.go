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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
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

	"crdb_internal.jobs",
	"system.jobs",       // get the raw, restorable jobs records too.
	"system.descriptor", // descriptors also contain job-like mutation state.
	"system.namespace",

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
	"crdb_internal.node_txn_stats",
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

func (z *zipper) close() {
	_ = z.z.Close()
	_ = z.f.Close()
}

func (z *zipper) create(name string, mtime time.Time) (io.Writer, error) {
	fmt.Printf("  %s\n", name)
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
	w, err := z.create(name, time.Time{})
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

func runDebugZip(cmd *cobra.Command, args []string) error {
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

	conn, _, finish, err := getClientGRPCConn(baseCtx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	status := serverpb.NewStatusClient(conn)
	admin := serverpb.NewAdminClient(conn)

	// Retrieve the node status to get the SQL address.
	nodeS, err := status.Node(baseCtx, &serverpb.NodeRequest{NodeId: "local"})
	if err != nil {
		return err
	}
	sqlAddr := nodeS.Desc.SQLAddress
	if sqlAddr.IsEmpty() {
		// No SQL address: either a pre-19.2 node, or same address for both
		// SQL and RPC.
		sqlAddr = nodeS.Desc.Address
	}
	cliCtx.clientConnHost, cliCtx.clientConnPort, err = net.SplitHostPort(sqlAddr.AddressField)
	if err != nil {
		return err
	}

	sqlConn, err := makeSQLClient("cockroach zip", useSystemDb)
	if err != nil {
		log.Warningf(baseCtx, "unable to open a SQL session. Debug information will be incomplete: %s", err)
	}
	defer sqlConn.Close()

	name := args[0]
	out, err := os.Create(name)
	if err != nil {
		return err
	}
	fmt.Printf("writing %s\n", name)

	z := newZipper(out)
	defer z.close()

	timeout := 10 * time.Second
	if cliCtx.cmdTimeout != 0 {
		timeout = cliCtx.cmdTimeout
	}

	var runZipRequest = func(r zipRequest) error {
		var data interface{}
		err = contextutil.RunWithTimeout(baseCtx, "request "+r.pathName, timeout, func(ctx context.Context) error {
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
		query := fmt.Sprintf(`SELECT * FROM %s`, table)
		if err := dumpTableDataForZip(z, sqlConn, query, base+"/"+table+".txt"); err != nil {
			return errors.Wrap(err, table)
		}
	}

	{
		var nodes *serverpb.NodesResponse
		if err := contextutil.RunWithTimeout(baseCtx, "request nodes", timeout, func(ctx context.Context) error {
			nodes, err = status.Nodes(ctx, &serverpb.NodesRequest{})
			return err
		}); err != nil {
			if err := z.createError(nodesPrefix, err); err != nil {
				return err
			}
		} else {
			for _, node := range nodes.Nodes {
				id := fmt.Sprintf("%d", node.Desc.NodeID)
				prefix := fmt.Sprintf("%s/%s", nodesPrefix, id)
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

				for _, table := range debugZipTablesPerNode {
					query := fmt.Sprintf(`SELECT * FROM %s`, table)
					if err := dumpTableDataForZip(z, curSQLConn, query, prefix+"/"+table+".txt"); err != nil {
						return errors.Wrap(err, table)
					}
				}

				for _, r := range []zipRequest{
					{
						fn: func(ctx context.Context) (interface{}, error) {
							return status.Details(ctx, &serverpb.DetailsRequest{NodeId: id, Ready: false})
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
				err = contextutil.RunWithTimeout(baseCtx, "request stacks", timeout,
					func(ctx context.Context) error {
						stacks, err := status.Stacks(ctx, &serverpb.StacksRequest{NodeId: id})
						if err == nil {
							stacksData = stacks.Data
						}
						return err
					})
				if err := z.createRawOrError(prefix+"/stacks.txt", stacksData, err); err != nil {
					return err
				}

				var heapData []byte
				err = contextutil.RunWithTimeout(baseCtx, "request heap profile", timeout,
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
				if err := contextutil.RunWithTimeout(baseCtx, "request heap files", timeout,
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
					for _, file := range profiles.Files {
						name := prefix + "/heapprof/" + file.Name + ".pprof"
						if err := z.createRaw(name, file.Contents); err != nil {
							return err
						}
					}
				}

				var goroutinesResp *serverpb.GetFilesResponse
				if err := contextutil.RunWithTimeout(baseCtx, "request goroutine files", timeout,
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
					for _, file := range goroutinesResp.Files {
						// NB: the files have a .txt.gz suffix already.
						name := prefix + "/goroutines/" + file.Name
						if err := z.createRawOrError(name, file.Contents, err); err != nil {
							return err
						}
					}
				}

				var logs *serverpb.LogFilesListResponse
				if err := contextutil.RunWithTimeout(baseCtx, "request logs", timeout,
					func(ctx context.Context) error {
						logs, err = status.LogFilesList(
							ctx, &serverpb.LogFilesListRequest{NodeId: id})
						return err
					}); err != nil {
					if err := z.createError(prefix+"/logs", err); err != nil {
						return err
					}
				} else {
					for _, file := range logs.Files {
						name := prefix + "/logs/" + file.Name
						var entries *serverpb.LogEntriesResponse
						if err := contextutil.RunWithTimeout(baseCtx, fmt.Sprintf("request log %s", file.Name), timeout,
							func(ctx context.Context) error {
								entries, err = status.LogFile(
									ctx, &serverpb.LogFileRequest{NodeId: id, File: file.Name})
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
						for _, e := range entries.Entries {
							if err := e.Format(logOut); err != nil {
								return err
							}
						}
					}
				}

				var ranges *serverpb.RangesResponse
				if err := contextutil.RunWithTimeout(baseCtx, "request ranges", timeout, func(ctx context.Context) error {
					ranges, err = status.Ranges(ctx, &serverpb.RangesRequest{NodeId: id})
					return err
				}); err != nil {
					if err := z.createError(prefix+"/ranges", err); err != nil {
						return err
					}
				} else {
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
	}

	{
		var databases *serverpb.DatabasesResponse
		if err := contextutil.RunWithTimeout(baseCtx, "request databases", timeout, func(ctx context.Context) error {
			databases, err = admin.Databases(ctx, &serverpb.DatabasesRequest{})
			return err
		}); err != nil {
			if err := z.createError(schemaPrefix, err); err != nil {
				return err
			}
		} else {
			for _, dbName := range databases.Databases {
				prefix := schemaPrefix + "/" + dbName
				var database *serverpb.DatabaseDetailsResponse
				requestErr := contextutil.RunWithTimeout(baseCtx, fmt.Sprintf("request database %s", dbName), timeout,
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

				for _, tableName := range database.TableNames {
					name := prefix + "/" + tableName
					var table *serverpb.TableDetailsResponse
					err := contextutil.RunWithTimeout(baseCtx, fmt.Sprintf("request table %s", tableName), timeout,
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

func dumpTableDataForZip(z *zipper, conn *sqlConn, query string, name string) error {
	if !strings.HasSuffix(name, ".txt") {
		return errors.Errorf("%s does not have .txt suffix", name)
	}
	var buf bytes.Buffer

	err := runQueryAndFormatResults(conn, &buf, makeQuery(query))
	if err != nil {
		return z.createError(name, err)
	}
	w, err := z.create(name, time.Time{})
	if err != nil {
		return err
	}
	_, err = io.Copy(w, bytes.NewReader(buf.Bytes()))
	return err
}
