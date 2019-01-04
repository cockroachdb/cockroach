// Copyright 2015 The Cockroach Authors.
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

package sql

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ShowRanges implements the SHOW EXPERIMENTAL_RANGES statement:
//   SHOW EXPERIMENTAL_RANGES FROM TABLE t
//   SHOW EXPERIMENTAL_RANGES FROM INDEX t@idx
//
// These statements show the ranges corresponding to the given table or index,
// along with the list of replicas and the lease holder.
func (p *planner) ShowRanges(ctx context.Context, n *tree.ShowRanges) (planNode, error) {
	desc, index, err := p.getTableAndIndex(ctx, n.Table, n.Index, privilege.SELECT)
	if err != nil {
		return nil, err
	}
	span := desc.IndexSpan(index.ID)
	if desc.ID < keys.MaxSystemConfigDescID {
		span = keys.SystemConfigSpan
	}
	startKey := hex.EncodeToString([]byte(span.Key))
	endKey := hex.EncodeToString([]byte(span.EndKey))
	return p.delegateQuery(ctx, "SHOW RANGES",
		fmt.Sprintf(`
SELECT 
  CASE WHEN r.start_key <= x'%s' THEN NULL ELSE crdb_internal.pretty_key(r.start_key, 2) END AS start_key,
  CASE WHEN r.end_key >= x'%s' THEN NULL ELSE crdb_internal.pretty_key(r.end_key, 2) END AS end_key,
  range_id,
  replicas,
  lease_holder
FROM crdb_internal.ranges AS r
WHERE (r.start_key < x'%s')
  AND (r.end_key   > x'%s')
`, startKey, endKey, endKey, startKey), nil, nil)
}

// ScanMetaKVs returns the meta KVs for the ranges that touch the given span.
func ScanMetaKVs(
	ctx context.Context, txn *client.Txn, span roachpb.Span,
) ([]client.KeyValue, error) {
	metaStart := keys.RangeMetaKey(keys.MustAddr(span.Key).Next())
	metaEnd := keys.RangeMetaKey(keys.MustAddr(span.EndKey))

	kvs, err := txn.Scan(ctx, metaStart, metaEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 || !kvs[len(kvs)-1].Key.Equal(metaEnd.AsRawKey()) {
		// Normally we need to scan one more KV because the ranges are addressed by
		// the end key.
		extraKV, err := txn.Scan(ctx, metaEnd, keys.Meta2Prefix.PrefixEnd(), 1 /* one result */)
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, extraKV[0])
	}
	return kvs, nil
}
