// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package cli

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
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
liveness, node status, range status, node stack traces, log files, and SQL
schema.

Retrieval of per-node details (status, stack traces, range status) requires the
node to be live and operating properly. Retrieval of SQL data requires the
cluster to be live.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugZip),
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
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return z.createRaw(name, b)
}

func (z *zipper) createError(name string, e error) error {
	fmt.Printf("  %s: %s\n", name, e)
	w, err := z.create(name, time.Time{})
	if err != nil {
		return err
	}
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
	if e != nil {
		return z.createError(name, e)
	}
	return z.createRaw(name, b)
}

type zipRequest struct {
	fn       func(ctx context.Context) (interface{}, error)
	pathName string
}

func runDebugZip(cmd *cobra.Command, args []string) error {
	const (
		base               = "debug"
		eventsName         = base + "/events"
		gossipLivenessName = base + "/gossip/liveness"
		gossipNetworkName  = base + "/gossip/network"
		gossipNodesName    = base + "/gossip/nodes"
		alertsName         = base + "/alerts"
		livenessName       = base + "/liveness"
		metricsName        = base + "/metrics"
		nodesPrefix        = base + "/nodes"
		rangelogName       = base + "/rangelog"
		reportsPrefix      = base + "/reports"
		schemaPrefix       = base + "/schema"
		settingsName       = base + "/settings"
	)

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, _, finish, err := getClientGRPCConn(baseCtx)
	if err != nil {
		return err
	}
	defer finish()

	status := serverpb.NewStatusClient(conn)
	admin := serverpb.NewAdminClient(conn)

	sqlConn, err := getPasswordAndMakeSQLClient("cockroach sql")
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
		return z.createJSONOrError(r.pathName, data, err)
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

	for _, item := range []struct {
		query, name string
	}{
		{"SELECT * FROM crdb_internal.gossip_liveness;", gossipLivenessName},
		{"SELECT * FROM crdb_internal.gossip_network;", gossipNetworkName},
		{"SELECT * FROM crdb_internal.gossip_nodes;", gossipNodesName},
		{"SELECT * FROM crdb_internal.node_metrics;", metricsName},
		{"SELECT * FROM crdb_internal.gossip_alerts;", alertsName},
	} {
		if err := dumpTableDataForZip(z, sqlConn, item.query, item.name); err != nil {
			return errors.Wrap(err, item.name)
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
				if err := z.createJSON(prefix+"/status", node); err != nil {
					return err
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
				} {
					if err := runZipRequest(r); err != nil {
						return err
					}
				}

				var stacksData []byte
				err = contextutil.RunWithTimeout(baseCtx, "request stacks", timeout,
					func(ctx context.Context) error {
						stacks, err := status.Stacks(ctx, &serverpb.StacksRequest{NodeId: id})
						if err != nil {
							stacksData = stacks.Data
						}
						return err
					})
				if err := z.createRawOrError(prefix+"/stacks", stacksData, err); err != nil {
					return err
				}

				var heapData []byte
				err = contextutil.RunWithTimeout(baseCtx, "request heap profile", timeout,
					func(ctx context.Context) error {
						heap, err := status.Profile(ctx, &serverpb.ProfileRequest{
							NodeId: id,
							Type:   serverpb.ProfileRequest_HEAP,
						})
						heapData = heap.Data
						return err
					})
				if err := z.createRawOrError(prefix+"/heap", heapData, err); err != nil {
					return err
				}

				var profiles *serverpb.GetFilesResponse
				if err := contextutil.RunWithTimeout(baseCtx, "request files", timeout,
					func(ctx context.Context) error {
						profiles, err = status.GetFiles(ctx, &serverpb.GetFilesRequest{
							NodeId:   id,
							Type:     serverpb.FileType_HEAP,
							Patterns: []string{"*"},
						})
						return err
					}); err != nil {
					if err := z.createError(prefix+"/heapfiles", err); err != nil {
						return err
					}
				} else {
					for _, file := range profiles.Files {
						name := prefix + "/heapprof/" + file.Name
						if err := z.createRaw(name, file.Contents); err != nil {
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
						if err := z.createJSON(name, r); err != nil {
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
				if err := z.createJSONOrError(prefix+"@details", database, requestErr); err != nil {
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
					if err := z.createJSONOrError(name, table, err); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func dumpTableDataForZip(z *zipper, conn *sqlConn, query string, name string) error {
	w, err := z.create(name, time.Time{})
	if err != nil {
		return err
	}

	if err = runQueryAndFormatResults(conn, w, makeQuery(query)); err != nil {
		if err := z.createError(name, err); err != nil {
			return err
		}
	}

	return nil
}
