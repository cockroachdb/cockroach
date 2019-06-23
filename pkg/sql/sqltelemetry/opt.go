// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltelemetry

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
)

// OptNodeCounter should be incremented every time a node of the given
// type is encountered at the end of the query optimization (i.e. it
// counts the nodes actually used for physical planning).
func OptNodeCounter(nodeType string) telemetry.Counter {
	return telemetry.GetCounterOnce(fmt.Sprintf("sql.plan.opt.node.%s", nodeType))
}
