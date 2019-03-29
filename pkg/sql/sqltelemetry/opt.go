// Copyright 2019 The Cockroach Authors.
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
// permissions and limitations under the License.

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
