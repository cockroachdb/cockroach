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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

var debugZipCmd = &cobra.Command{
	Use:   "zip [file]",
	Short: "gather cluster debug data into a zip file",
	Long: `

Gather cluster debug data into a zip file. Data includes cluster events, node
liveness, node status, range status, node stack traces, log files, and SQL
schema.

Retrieval of per-node details (status, stack traces, range status) requires the
node to be live and operating properly. Retrieval of SQL data requires the
cluster to be live.
`,
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

func (z *zipper) create(name string) (io.Writer, error) {
	fmt.Printf("  %s\n", name)
	return z.z.Create(name)
}

func (z *zipper) createRaw(name string, b []byte) error {
	w, err := z.create(name)
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
	w, err := z.z.Create(name)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s\n", e)
	return nil
}

func runDebugZip(cmd *cobra.Command, args []string) error {
	const (
		base         = "debug"
		eventsName   = base + "/events"
		livenessName = base + "/liveness"
		nodesPrefix  = base + "/nodes"
		schemaPrefix = base + "/schema"
		settingsName = base + "/settings"
	)

	if len(args) != 1 {
		return errors.New("exactly one argument is required")
	}

	conn, _, stopper, err := getClientGRPCConn()
	if err != nil {
		return err
	}
	ctx := stopperContext(stopper)
	defer stopper.Stop(ctx)

	status := serverpb.NewStatusClient(conn)
	admin := serverpb.NewAdminClient(conn)

	name := args[0]
	out, err := os.Create(name)
	if err != nil {
		return err
	}
	fmt.Printf("writing %s\n", name)

	z := newZipper(out)
	defer z.close()

	if events, err := admin.Events(ctx, &serverpb.EventsRequest{}); err != nil {
		if err := z.createError(eventsName, err); err != nil {
			return err
		}
	} else {
		if err := z.createJSON(eventsName, events); err != nil {
			return err
		}
	}

	if liveness, err := admin.Liveness(ctx, &serverpb.LivenessRequest{}); err != nil {
		if err := z.createError(livenessName, err); err != nil {
			return err
		}
	} else {
		if err := z.createJSON(livenessName, liveness); err != nil {
			return err
		}
	}

	if settings, err := admin.Settings(ctx, &serverpb.SettingsRequest{}); err != nil {
		if err := z.createError(settingsName, err); err != nil {
			return err
		}
	} else {
		if err := z.createJSON(settingsName, settings); err != nil {
			return err
		}
	}

	if nodes, err := status.Nodes(ctx, &serverpb.NodesRequest{}); err != nil {
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

			if gossip, err := status.Gossip(ctx, &serverpb.GossipRequest{NodeId: id}); err != nil {
				if err := z.createError(prefix+"/gossip", err); err != nil {
					return err
				}
			} else if err := z.createJSON(prefix+"/gossip", gossip); err != nil {
				return err
			}

			if stacks, err := status.Stacks(ctx, &serverpb.StacksRequest{NodeId: id}); err != nil {
				if err := z.createError(prefix+"/stacks", err); err != nil {
					return err
				}
			} else if err := z.createRaw(prefix+"/stacks", stacks.Data); err != nil {
				return err
			}

			if logs, err := status.LogFilesList(
				ctx, &serverpb.LogFilesListRequest{NodeId: id}); err != nil {
				if err := z.createError(prefix+"/logs", err); err != nil {
					return err
				}
			} else {
				for _, file := range logs.Files {
					name := prefix + "/logs/" + file.Name
					entries, err := status.LogFile(
						ctx, &serverpb.LogFileRequest{NodeId: id, File: file.Name})
					if err != nil {
						if err := z.createError(name, err); err != nil {
							return err
						}
						continue
					}
					logOut, err := z.create(name)
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

			if ranges, err := status.Ranges(ctx, &serverpb.RangesRequest{NodeId: id}); err != nil {
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

	if databases, err := admin.Databases(ctx, &serverpb.DatabasesRequest{}); err != nil {
		if err := z.createError(schemaPrefix, err); err != nil {
			return err
		}
	} else {
		for _, dbName := range databases.Databases {
			prefix := schemaPrefix + "/" + dbName
			database, err := admin.DatabaseDetails(
				ctx, &serverpb.DatabaseDetailsRequest{Database: dbName})
			if err != nil {
				if err := z.createError(prefix, err); err != nil {
					return err
				}
				continue
			}
			if err := z.createJSON(prefix+"@details", database); err != nil {
				return err
			}

			for _, tableName := range database.TableNames {
				name := prefix + "/" + tableName
				table, err := admin.TableDetails(
					ctx, &serverpb.TableDetailsRequest{Database: dbName, Table: tableName})
				if err != nil {
					if err := z.createError(name, err); err != nil {
						return err
					}
					continue
				}
				if err := z.createJSON(name, table); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
