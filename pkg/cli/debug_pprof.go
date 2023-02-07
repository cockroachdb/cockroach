// Copyright 2023 The Cockroach Authors.
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
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugProfile = &cobra.Command{
	Use:   "profile <outputfile> <node_id> [<node_id> ...]",
	Short: "continuous profile collection to zip file",
	Long: ` TODO
`,
	Args: cobra.MinimumNArgs(2),
	RunE: clierrorplus.MaybeDecorateError(runDebugProfile),
}

/*
	ids, err := func() ([]roachpb.NodeID, error) {
		conn, err := makeSQLClient("cockroach debug pprof", useSystemDb)
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		rows, err := conn.Query(ctx, `SELECT node_id FROM crdb_internal.gossip_nodes WHERE is_live`)
		if err != nil {
			return nil, err
		}
		var ns []roachpb.NodeID
		var n int64
		for {
			err := rows.Next([]driver.Value{&n})
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			ns = append(ns, roachpb.NodeID(n))
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		return ns, nil
	}()
*/

func runDebugProfile(_ *cobra.Command, args []string) (retErr error) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	ids := args[1:]
	f, err := os.Create(args[0])
	if err != nil {
		return err
	}
	z := zip.NewWriter(f)
	defer func() {
		retErr = errors.CombineErrors(retErr, z.Close())
		retErr = errors.CombineErrors(retErr, f.Close())
	}()

	conn, _, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	status := serverpb.NewStatusClient(conn)

	g := ctxgroup.WithContext(ctx)

	type nodeResp struct {
		ts     time.Time
		nodeID string
		resp   *serverpb.JSONResponse
	}
	ch := make(chan nodeResp, 10)

	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case resp := <-ch:
				name := fmt.Sprintf("profile_%s_%s.pb.gz", resp.nodeID, resp.ts.Format(`2006-01-02T15:04:05.999`))
				w, err := z.Create(name)
				if err != nil {
					return err
				}
				i, err := io.Copy(w, bytes.NewReader(resp.resp.Data))
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "%s: %s\n", name, humanizeutil.IBytes(i))
			}
		}
	})

	for _, id := range ids {
		req := &serverpb.ProfileRequest{
			NodeId:  id,
			Type:    serverpb.ProfileRequest_CPU,
			Seconds: 10,
			Labels:  true,
		}
		g.GoCtx(func(ctx context.Context) error {
			for {
				c, err := status.ProfileLoop(ctx, req)
				if err == nil {
					for {
						var resp *serverpb.JSONResponse
						resp, err = c.Recv()
						if err != nil {
							break
						}
						ch <- nodeResp{
							ts:     timeutil.Now(),
							nodeID: req.NodeId,
							resp:   resp,
						}
					}
				}

				if ctx.Err() != nil {
					return ctx.Err()
				}

				fmt.Fprintf(os.Stderr, "%s: retrying in 1s after err: %s\n", req.NodeId, err)
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	return g.Wait()
}
