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
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/gogo/protobuf/proto"
)

// Future home of the aysynchronous schema changer that picks up
// queued schema changes and processes them.
//
// applyMutations applies the queued mutations for a table.
// TODO(vivek): Eliminate the need to pass in tableName.
func (p *planner) applyMutations(tableDesc *TableDescriptor, tableName *parser.QualifiedName) error {
	if len(tableDesc.Mutations) == 0 {
		return nil
	}
	newTableDesc := proto.Clone(tableDesc).(*TableDescriptor)
	// Make all mutations active.
	for _, mutation := range newTableDesc.Mutations {
		newTableDesc.makeMutationComplete(mutation)
	}
	newTableDesc.Mutations = nil
	if err := newTableDesc.Validate(); err != nil {
		return err
	}

	b := client.Batch{}
	if err := p.backfillBatch(&b, tableName, tableDesc, newTableDesc); err != nil {
		return err
	}
	newTableDesc.Version++

	b.Put(MakeDescMetadataKey(newTableDesc.GetID()), wrapDescriptor(newTableDesc))

	if err := p.txn.Run(&b); err != nil {
		return convertBatchError(newTableDesc, b, err)
	}
	p.notifyCompletedSchemaChange(newTableDesc.ID)
	return nil
}
