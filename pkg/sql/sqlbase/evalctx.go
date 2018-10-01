// Copyright 2016 The Cockroach Authors.
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

package sqlbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// DummySequenceOperators implements the tree.SequenceOperators interface by
// returning errors.
type DummySequenceOperators struct{}

var _ tree.EvalDatabase = &DummySequenceOperators{}

var errSequenceOperators = errors.New("cannot backfill such sequence operation")

// ParseQualifiedTableName is part of the tree.EvalDatabase interface.
func (so *DummySequenceOperators) ParseQualifiedTableName(
	ctx context.Context, sql string,
) (*tree.TableName, error) {
	return nil, errSequenceOperators
}

// ResolveTableName is part of the tree.EvalDatabase interface.
func (so *DummySequenceOperators) ResolveTableName(ctx context.Context, tn *tree.TableName) error {
	return errSequenceOperators
}

// LookupSchema is part of the tree.EvalDatabase interface.
func (so *DummySequenceOperators) LookupSchema(
	ctx context.Context, dbName, scName string,
) (bool, tree.SchemaMeta, error) {
	return false, nil, errSequenceOperators
}

// IncrementSequence is part of the tree.SequenceOperators interface.
func (so *DummySequenceOperators) IncrementSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	return 0, errSequenceOperators
}

// GetLatestValueInSessionForSequence implements the tree.SequenceOperators
// interface.
func (so *DummySequenceOperators) GetLatestValueInSessionForSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	return 0, errSequenceOperators
}

// SetSequenceValue implements the tree.SequenceOperators interface.
func (so *DummySequenceOperators) SetSequenceValue(
	ctx context.Context, seqName *tree.TableName, newVal int64, isCalled bool,
) error {
	return errSequenceOperators
}

// DummyEvalPlanner implements the tree.EvalPlanner interface by returning
// errors.
type DummyEvalPlanner struct{}

var _ tree.EvalPlanner = &DummyEvalPlanner{}

var errEvalPlanner = errors.New("cannot backfill such evaluated expression")

// ParseQualifiedTableName is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) ParseQualifiedTableName(
	ctx context.Context, sql string,
) (*tree.TableName, error) {
	return nil, errEvalPlanner
}

// LookupSchema is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) LookupSchema(
	ctx context.Context, dbName, scName string,
) (bool, tree.SchemaMeta, error) {
	return false, nil, errEvalPlanner
}

// ResolveTableName is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) ResolveTableName(ctx context.Context, tn *tree.TableName) error {
	return errEvalPlanner
}

// ParseType is part of the tree.EvalPlanner interface.
func (ep *DummyEvalPlanner) ParseType(sql string) (coltypes.CastTargetType, error) {
	return nil, errEvalPlanner
}

// EvalSubquery is part of the tree.EvalPlanner interface.
func (ep *DummyEvalPlanner) EvalSubquery(expr *tree.Subquery) (tree.Datum, error) {
	return nil, errEvalPlanner
}
