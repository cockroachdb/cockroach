// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateNotify(n *tree.Notify) (tree.Statement, error) {
	return parse(
		fmt.Sprintf("UPSERT INTO system.pg_notifications (chan_name, message, node_id)"+
			"VALUES (%s, %s, %d)",
			lexbase.EscapeSQLString(n.ChanName.String()),
			lexbase.EscapeSQLString(n.Message.RawString()),
			d.evalCtx.NodeID.SQLInstanceID(),
		))
}
