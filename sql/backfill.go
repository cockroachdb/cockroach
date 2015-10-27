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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
)

func (p *planner) backfillBatch(b *client.Batch, tableName *parser.QualifiedName, tableDesc *TableDescriptor, indexDescs ...IndexDescriptor) error {
	// Get all the rows affected.
	// TODO(vivek): Avoid going through Select.
	// TODO(tamird): Support partial indexes?
	row, err := p.Select(&parser.Select{
		Exprs: parser.SelectExprs{parser.StarSelectExpr()},
		From:  parser.TableExprs{&parser.AliasedTableExpr{Expr: tableName}},
	})
	if err != nil {
		return err
	}

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex := map[ColumnID]int{}
	for i, name := range row.Columns() {
		c, err := tableDesc.FindColumnByName(name)
		if err != nil {
			return err
		}
		colIDtoRowIndex[c.ID] = i
	}

	// TODO(tamird): This will fall down in production use. We need to do
	// something better (see #2036). In particular, this implementation
	// has the following problems:
	// - Very large tables will generate an enormous batch here. This
	// isn't really a problem in itself except that it will exacerbate
	// the other issue:
	// - Any non-quiescent table that this runs against will end up with
	// an inconsistent index. This is because as inserts/updates continue
	// to roll in behind this operation's read front, the written index
	// will become incomplete/stale before it's written.

	for row.Next() {
		rowVals := row.Values()

		for _, indexDesc := range indexDescs {
			secondaryIndexEntries, err := encodeSecondaryIndexes(
				tableDesc.ID, []IndexDescriptor{indexDesc}, colIDtoRowIndex, rowVals)
			if err != nil {
				return err
			}

			for _, secondaryIndexEntry := range secondaryIndexEntries {
				if log.V(2) {
					log.Infof("CPut %s -> %v", prettyKey(secondaryIndexEntry.key, 0),
						secondaryIndexEntry.value)
				}
				b.CPut(secondaryIndexEntry.key, secondaryIndexEntry.value, nil)
			}
		}
	}

	return row.Err()
}
