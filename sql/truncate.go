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
//
// Author: XisiHuang (cockhuangxh@163.com)

package sql

import (
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/log"
)

// Truncate deletes all rows from a table.
// Privileges: DROP on table.
//   Notes: postgres requires TRUNCATE.
//          mysql requires DROP (for mysql >= 5.1.16, DELETE before that).
func (p *planner) Truncate(n *parser.Truncate) (planNode, *roachpb.Error) {
	b := client.Batch{}
	for _, tableQualifiedName := range n.Tables {
		tableDesc, pErr := p.getTableLease(tableQualifiedName)
		if pErr != nil {
			return nil, pErr
		}

		if err := p.checkPrivilege(&tableDesc, privilege.DROP); err != nil {
			return nil, roachpb.NewError(err)
		}

		tablePrefix := keys.MakeTablePrefix(uint32(tableDesc.ID))

		// Delete rows and indexes starting with the table's prefix.
		tableStartKey := roachpb.Key(tablePrefix)
		tableEndKey := tableStartKey.PrefixEnd()
		if log.V(2) {
			log.Infof("DelRange %s - %s", tableStartKey, tableEndKey)
		}
		b.DelRange(tableStartKey, tableEndKey, false)
	}

	if pErr := p.txn.Run(&b); pErr != nil {
		return nil, pErr
	}

	return &emptyNode{}, nil
}
