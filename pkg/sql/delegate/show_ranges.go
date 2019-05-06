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

package delegate

import (
	"encoding/hex"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// delegateShowRanges implements the SHOW EXPERIMENTAL_RANGES statement:
//   SHOW EXPERIMENTAL_RANGES FROM TABLE t
//   SHOW EXPERIMENTAL_RANGES FROM INDEX t@idx
//
// These statements show the ranges corresponding to the given table or index,
// along with the list of replicas and the lease holder.
func (d *delegator) delegateShowRanges(n *tree.ShowRanges) (tree.Statement, error) {
	idx, err := cat.ResolveTableIndex(
		d.ctx, d.catalog, cat.Flags{AvoidDescriptorCaches: true}, &n.TableOrIndex,
	)
	if err != nil {
		return nil, err
	}
	if err := d.catalog.CheckPrivilege(d.ctx, idx.Table(), privilege.SELECT); err != nil {
		return nil, err
	}

	span := idx.Span()
	startKey := hex.EncodeToString([]byte(span.Key))
	endKey := hex.EncodeToString([]byte(span.EndKey))
	return parse(fmt.Sprintf(`
SELECT 
  CASE WHEN r.start_key <= x'%s' THEN NULL ELSE crdb_internal.pretty_key(r.start_key, 2) END AS start_key,
  CASE WHEN r.end_key >= x'%s' THEN NULL ELSE crdb_internal.pretty_key(r.end_key, 2) END AS end_key,
  range_id,
  replicas,
  lease_holder
FROM crdb_internal.ranges AS r
WHERE (r.start_key < x'%s')
  AND (r.end_key   > x'%s')`,
		startKey, endKey, endKey, startKey,
	))
}
