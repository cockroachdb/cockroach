// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
