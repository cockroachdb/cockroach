// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
)

func registerTenantSpanStatsMixedVersion(r registry.Registry) {
	testSpec := registry.TestSpec{
		Name:              "tenant-span-stats/mixed-version",
		Owner:             registry.OwnerClusterObs,
		Cluster:           r.MakeClusterSpec(4),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Timeout:           30 * time.Minute,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			v, err := version.Parse("v23.2.0")
			if err != nil {
				t.Fatalf("unable to parse version correctly %v", err)
			}
			if t.BuildVersion().AtLeast(v) {
				t.Fatal("predecessor binary already includes RPC protobuf changes, this test is no longer needed -- remove me")
			}

			mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All())

			tableName := "test"
			var startKey string
			var endKey string
			var res *install.RunResultDetails
			var errOutput errorOutput

			mvt.OnStartup("fetch span stats - start", func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
				// Create a test table.
				err = h.Exec(rng, fmt.Sprintf(`CREATE TABLE %s (num int)`, tableName))
				if err != nil {
					return err
				}
				// Get the table's span keys.
				row := h.QueryRow(rng,
					fmt.Sprintf(`SELECT crdb_internal.pretty_key(crdb_internal.table_span('%s'::regclass::oid::int)[1], 0)`, tableName))
				err = row.Scan(&startKey)
				if err != nil {
					return err
				}
				row = h.QueryRow(rng,
					fmt.Sprintf(`SELECT crdb_internal.pretty_key(crdb_internal.table_span('%s'::regclass::oid::int)[2], 0)`, tableName))
				err = row.Scan(&endKey)
				if err != nil {
					return err
				}
				// Dial a node for span stats on startup. All nodes will be on the previous version, this is a sanity check.
				l.Printf("Fetch span stats from node (start).")
				res, err = fetchSpanStatsFromNode(ctx, l, c, c.Node(1), oldReqBody(startKey, endKey))
				if err != nil {
					return err
				}
				// Ensure the result can be marshalled into a valid span stats response.
				var spanStats roachpb.SpanStatsResponse
				return json.Unmarshal([]byte(res.Stdout), &spanStats)
			})

			mvt.InMixedVersion("fetch span stats - mixed", func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
				// Skip finalizing state.
				if h.Context().Finalizing {
					return nil
				}

				nodeID := h.RandomNode(rng, c.All())

				// Fetch span stats from random node.
				l.Printf("Fetch span stats from random node (%v).", nodeID)
				res, err = fetchSpanStatsFromNode(ctx, l, c, c.Node(nodeID), createReqBody(rng, startKey, endKey))
				if err != nil {
					return err
				}
				// Expect an error in the stdout.
				// Ensure the result can be marshalled into a valid error response.
				err = json.Unmarshal([]byte(res.Stdout), &errOutput)
				if err != nil {
					return err
				}
				// Ensure error output is not empty.
				if errOutput.Error == "" {
					return errors.Newf("expected an error but got empty")
				}
				return nil
			})

			mvt.AfterUpgradeFinalized("fetch span stats - finalized", func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
				// Fanout on finalized version, sanity check.
				l.Printf("Fanout from current version node (finalized).")
				res, err = fetchSpanStatsFromNode(ctx, l, c, c.Node(1), newReqBody(startKey, endKey))
				if err != nil {
					return err
				}
				// Ensure the result can be marshalled into a valid span stats response.
				var spanStats roachpb.SpanStatsResponse
				return json.Unmarshal([]byte(res.Stdout), &spanStats)
			})
			mvt.Run()
		},
	}
	r.Add(testSpec)
}

func fetchSpanStatsFromNode(
	ctx context.Context,
	l *logger.Logger,
	c cluster.Cluster,
	node option.NodeListOption,
	reqBody string,
) (*install.RunResultDetails, error) {
	adminAddrs, err := c.InternalAdminUIAddr(ctx, l, node)
	if err != nil {
		return nil, errors.Wrap(err, "error getting internal admin ui address")
	}
	loginCmd := fmt.Sprintf(
		"%s auth-session login root --certs-dir ./certs --format raw",
		mixedversion.CurrentCockroachPath,
	)
	res, err := c.RunWithDetailsSingleNode(ctx, l, node, loginCmd)
	if err != nil {
		return nil, errors.Wrap(err, "failed to authenticate")
	}

	var sessionCookie string
	for _, line := range strings.Split(res.Stdout, "\n") {
		if strings.HasPrefix(line, "session=") {
			sessionCookie = line
		}
	}
	if sessionCookie == "" {
		return nil, fmt.Errorf("failed to find session cookie in `login` output")
	}

	curlCmd := roachtestutil.NewCommand("curl").
		Option("k").
		Flag("X", "POST").
		Flag("H", "'Content-Type: application/json'").
		Flag("cookie", fmt.Sprintf("'%s'", sessionCookie)).
		Flag("d", fmt.Sprintf("'%s'", reqBody)).
		Arg("https://%s/_status/span", adminAddrs[0]).
		String()
	res, err = c.RunWithDetailsSingleNode(ctx, l, node, curlCmd)
	if err != nil {
		return nil, errors.Wrap(err, "error getting span statistics")
	}
	l.Printf("fetchSpanStatsFromNode - curl result:\n%s", res.Stdout)
	return &res, nil
}

// createReqBody randomly chooses to build a request body using the old or new format.
func createReqBody(prng *rand.Rand, startKey string, endKey string) string {
	if prng.Intn(2) == 0 {
		return oldReqBody(startKey, endKey)
	}
	return newReqBody(startKey, endKey)
}

func oldReqBody(startKey string, endKey string) string {
	return fmt.Sprintf(`{"node_id": "%v", "start_key": "%v", "end_key": "%v"}`, 0, startKey, endKey)
}

func newReqBody(startKey string, endKey string) string {
	return fmt.Sprintf(`{"node_id": "%v", "spans": [{"key": "%v", "end_key": "%v"}]}`, 0, startKey, endKey)
}

type errorOutput struct {
	Error   string        `json:"error"`
	Code    int           `json:"code"`
	Message string        `json:"message"`
	Details []interface{} `json:"details"`
}
