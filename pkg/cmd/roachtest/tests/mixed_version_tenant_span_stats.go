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
				res, err = fetchSpanStatsFromNode(ctx, l, c, c.Node(1), oldReqBody(2, startKey, endKey))
				if err != nil {
					return err
				}
				// Ensure the result can be marshalled into a valid span stats response.
				var spanStats roachpb.SpanStatsResponse
				return json.Unmarshal([]byte(res.Stdout), &spanStats)
			})

			mvt.InMixedVersion("fetch span stats - mixed", func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
				mixedVersionReqError := "unable to service a mixed version request"
				unknownFieldError := "unknown field"

				// If we have nodes in mixed versions.
				if len(h.Context().FromVersionNodes) > 0 && len(h.Context().ToVersionNodes) > 0 {
					prevVersNodeID := h.Context().FromVersionNodes[0]
					currVersNodeID := h.Context().ToVersionNodes[0]

					// Fetch span stats from previous version node, dialing to a current version node.
					l.Printf("Fetch span stats from previous version node (%v), dialing to a current version node (%v).", prevVersNodeID, currVersNodeID)
					res, err = fetchSpanStatsFromNode(ctx, l, c, c.Node(prevVersNodeID), oldReqBody(currVersNodeID, startKey, endKey))
					if err != nil {
						return err
					}
					// Expect an error in the stdout - mixed version error.
					// Ensure the result can be marshalled into a valid error response.
					err = json.Unmarshal([]byte(res.Stdout), &errOutput)
					if err != nil {
						return err
					}
					// Ensure we get the expected error.
					expected := assertExpectedError(errOutput.Error, mixedVersionReqError)
					if !expected {
						return errors.Newf("expected '%s' in error message, got: '%v'", mixedVersionReqError, errOutput.Error)
					}

					// Fetch span stats from current version node, dialing to a previous version node.
					l.Printf("Fetch span stats from current version node (%v), dialing to a previous version node (%v).", currVersNodeID, prevVersNodeID)
					res, err = fetchSpanStatsFromNode(ctx, l, c, c.Node(currVersNodeID), newReqBody(prevVersNodeID, startKey, endKey))
					if err != nil {
						return err
					}
					// Expect an error in the stdout - mixed version error.
					// Ensure the result can be marshalled into a valid error response.
					err = json.Unmarshal([]byte(res.Stdout), &errOutput)
					if err != nil {
						return err
					}
					// Ensure we get the expected error.
					expectedCurrToPrev := assertExpectedError(errOutput.Message, mixedVersionReqError)
					expectedUnknown := assertExpectedError(errOutput.Message, unknownFieldError)
					if !expectedCurrToPrev && !expectedUnknown {
						return errors.Newf("expected '%s' or '%s' in error message, got: '%v'", mixedVersionReqError, expectedUnknown, errOutput.Error)
					}

					// Fanout from current version node.
					l.Printf("Fanout from current version node (mixed).")
					res, err = fetchSpanStatsFromNode(ctx, l, c, c.Node(currVersNodeID), newReqBody(0, startKey, endKey))
					if err != nil {
						return err
					}
					// Expect an error in the stdout - mixed version error.
					// Ensure the result can be marshalled into a valid error response.
					err = json.Unmarshal([]byte(res.Stdout), &errOutput)
					if err != nil {
						return err
					}
					// Ensure we get the expected error.
					expectedCurrToPrev = assertExpectedError(errOutput.Message, mixedVersionReqError)
					expectedUnknown = assertExpectedError(errOutput.Message, unknownFieldError)
					if !expectedCurrToPrev && !expectedUnknown {
						return errors.Newf("expected '%s' or '%s' in error message, got: '%v'", mixedVersionReqError, expectedUnknown, errOutput.Error)
					}
				} else {
					// All nodes are on one version, but we're in mixed state (i.e. cluster version is on a different version)
					var issueNodeID int
					var dialNodeID int
					// All nodes on current version
					if len(h.Context().ToVersionNodes) == 4 {
						issueNodeID = h.Context().ToVersionNodes[0]
						dialNodeID = h.Context().ToVersionNodes[1]
					} else {
						// All nodes on previous version
						issueNodeID = h.Context().FromVersionNodes[0]
						dialNodeID = h.Context().FromVersionNodes[1]
					}
					// Dial a node for span stats.
					l.Printf("Dial a node for span stats (different cluster version).")
					res, err = fetchSpanStatsFromNode(ctx, l, c, c.Node(issueNodeID), newReqBody(dialNodeID, startKey, endKey))
					if err != nil {
						return err
					}
					// Expect an error in the stdout - mixed version error.
					// Ensure the result can be marshalled into a valid error response.
					err = json.Unmarshal([]byte(res.Stdout), &errOutput)
					if err != nil {
						return err
					}
					// Ensure we get the expected error.
					mixedClusterVersionErr := assertExpectedError(errOutput.Message, mixedVersionReqError)
					expectedUnknown := assertExpectedError(errOutput.Message, unknownFieldError)
					if !mixedClusterVersionErr && !expectedUnknown {
						return errors.Newf("expected '%s' or '%s' in error message, got: '%v'", mixedVersionReqError, unknownFieldError, errOutput.Error)
					}
				}
				return nil
			})

			mvt.AfterUpgradeFinalized("fetch span stats - finalized", func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
				// Fanout on finalized version, sanity check.
				l.Printf("Fanout from current version node (finalized).")
				res, err = fetchSpanStatsFromNode(ctx, l, c, c.Node(1), newReqBody(0, startKey, endKey))
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
	res, err := c.RunWithDetailsSingleNode(ctx, l, node,
		"curl", "-X", "POST", fmt.Sprintf("http://%s/_status/span",
			adminAddrs[0]), "-H", "'Content-Type: application/json'", "-d", fmt.Sprintf("'%s'", reqBody))
	if err != nil {
		return nil, errors.Wrap(err, "error getting span statistics")
	}
	l.Printf("fetchSpanStatsFromNode - curl result", res.Stdout)
	return &res, nil
}

func assertExpectedError(errorMessage string, expectedError string) bool {
	return strings.Contains(errorMessage, expectedError)
}

func oldReqBody(nodeID int, startKey string, endKey string) string {
	return fmt.Sprintf(`{"node_id": "%v", "start_key": "%v", "end_key": "%v"}`, nodeID, startKey, endKey)
}

func newReqBody(nodeID int, startKey string, endKey string) string {
	return fmt.Sprintf(`{"node_id": "%v", "spans": [{"key": "%v", "end_key": "%v"}]}`, nodeID, startKey, endKey)
}

type errorOutput struct {
	Error   string        `json:"error"`
	Code    int           `json:"code"`
	Message string        `json:"message"`
	Details []interface{} `json:"details"`
}
