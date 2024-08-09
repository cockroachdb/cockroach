// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateNotify(n *tree.Notify) (tree.Statement, error) {
	stmt := fmt.Sprintf(
		`UPSERT INTO system.notifications (channel, payload, pid) VALUES (%s, %s, %d)`,
		lexbase.EscapeSQLString(n.ChannelName.String()),
		lexbase.EscapeSQLString(n.Payload.String()),
		// TODO: should we make pid something like server+pid? the pgwire protocol mandates an i32 for this so maybe not feasible.
		os.Getpid(),
	)
	return d.parse(stmt)
}
