// Copyright 2021 The Cockroach Authors.
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
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
)

// getDiskUsageInBytes does what's on the tin. nodeIdx starts at one.
func getDiskUsageInBytes(
	ctx context.Context, c cluster.Cluster, logger *logger.Logger, nodeIdx int,
) (int, error) {
	var out []byte
	for {
		var err error
		// `du` can warn if files get removed out from under it (which
		// happens during RocksDB compactions, for example). Discard its
		// stderr to avoid breaking Atoi later.
		// TODO(bdarnell): Refactor this stack to not combine stdout and
		// stderr so we don't need to do this (and the Warning check
		// below).
		out, err = c.RunWithBuffer(
			ctx,
			logger,
			c.Node(nodeIdx),
			"du -sk {store-dir} 2>/dev/null | grep -oE '^[0-9]+'",
		)
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			// If `du` fails, retry.
			// TODO(bdarnell): is this worth doing? It was originally added
			// because of the "files removed out from under it" problem, but
			// that doesn't result in a command failure, just a stderr
			// message.
			logger.Printf("retrying disk usage computation after spurious error: %s", err)
			continue
		}
		break
	}

	str := string(out)
	// We need this check because sometimes the first line of the roachprod output is a warning
	// about adding an ip to a list of known hosts.
	if strings.Contains(str, "Warning") {
		str = strings.Split(str, "\n")[1]
	}

	size, err := strconv.Atoi(strings.TrimSpace(str))
	if err != nil {
		return 0, err
	}

	return size * 1024, nil
}
