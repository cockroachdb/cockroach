// Copyright 2024 The Cockroach Authors.
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

// NOTE: the value we send for `pid` coresponds to the value of
// `pg_backend_pid()` that the user can call. However in reality crdb session
// ids are uint128/64s and will never fit into the int32 value that the postgres
// protocol defines for this field. This limits the usefulness of this feature
// somewhat, as a known use case is to check whether the returned pid ==
// pg_backend_pid() to see if the receiver is also the sender. In crdb this
// check would return true sometimes when that was not the case. We should
// mention this limitation in the documentation for this feature.
//
// Alternatively, we could set it to 0/-1/some special value and not support
// this facet of the feature at all, to avoid users doing the wrong thing.
func (d *delegator) delegateNotify(n *tree.Notify) (tree.Statement, error) {
	return d.parse(fmt.Sprintf(
		`UPSERT INTO system.notifications (channel, payload, pid) SELECT %s, %s, pg_backend_pid()`,
		lexbase.EscapeSQLString(n.ChannelName.String()),
		lexbase.EscapeSQLString(n.Payload.RawString()),
	))
}
