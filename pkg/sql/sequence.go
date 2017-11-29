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
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"golang.org/x/net/context"
)

// IncrementSequence implements the tree.SequenceAccessor interface.
func (p *planner) IncrementSequence(ctx context.Context, seqName *tree.TableName) (int64, error) {
	descriptor, err := getSequenceDesc(ctx, p.txn, p.getVirtualTabler(), seqName)
	if err != nil {
		return 0, err
	}
	seqValueKey := keys.MakeSequenceKey(uint32(descriptor.ID))
	val, err := client.IncrementValRetryable(
		ctx, p.txn.DB(), seqValueKey, descriptor.SequenceOpts.Increment)
	if err != nil {
		return 0, err
	}

	p.session.mu.Lock()
	defer p.session.mu.Unlock()
	p.session.mu.LastSequenceValue = val
	p.session.mu.NextValEverCalled = true

	return val, nil
}

// GetLastSequenceValue implements the tree.SequenceAccessor interface.
func (p *planner) GetLastSequenceValue(ctx context.Context) (int64, error) {
	p.session.mu.RLock()
	defer p.session.mu.RUnlock()

	if !p.session.mu.NextValEverCalled {
		return 0, pgerror.NewError(
			pgerror.CodeObjectNotInPrerequisiteStateError, "lastval is not yet defined in this session")
	}

	return p.session.mu.LastSequenceValue, nil
}

// GetSequenceValue implements the tree.SequenceAccessor interface.
func (p *planner) GetSequenceValue(ctx context.Context, seqName *tree.TableName) (int64, error) {
	descriptor, err := getSequenceDesc(ctx, p.txn, p.getVirtualTabler(), seqName)
	if err != nil {
		return 0, err
	}
	seqValueKey := keys.MakeSequenceKey(uint32(descriptor.ID))
	val, err := p.txn.Get(ctx, seqValueKey)
	if err != nil {
		return 0, err
	}
	return val.ValueInt(), nil
}

// SetSequenceValue implements the tree.SequenceAccessor interface.
func (p *planner) SetSequenceValue(
	ctx context.Context, seqName *tree.TableName, newVal int64,
) error {
	descriptor, err := getSequenceDesc(ctx, p.txn, p.getVirtualTabler(), seqName)
	if err != nil {
		return err
	}
	seqValueKey := keys.MakeSequenceKey(uint32(descriptor.ID))
	return p.txn.Put(ctx, seqValueKey, newVal)
}
