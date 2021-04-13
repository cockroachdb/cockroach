// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// TODO(ajwerner): Add privilege checking.

// Dependencies are non-stateful objects needed for planning schema
// changes.
type Dependencies struct {
	// TODO(ajwerner): Inject a better interface than this.
	res     resolver.SchemaResolver
	semaCtx *tree.SemaContext
	evalCtx *tree.EvalContext
	descs   *descs.Collection
}

// BuildContext is the entry point for planning schema changes. From AST nodes
// for DDL statements, it constructs targets which represent schema changes to
// be performed.
//
// The builder itself is essentially stateless aside from the dependencies it
// needs to resolve (immutable) descriptors, evaluate expressions, etc. The
// methods in its API take schema change graph nodes (i.e., targets and their
// current states) and DDL statement AST nodes, and output new schema change
// graph nodes that incorporate targets that were added or changed.
type BuildContext struct {
	Dependencies

	// outputNodes contains the internal state when building targets for an individual
	// statement.
	outputNodes []*scpb.Node
}

type notImplementedError struct {
	n      tree.NodeFormatter
	detail string
}

// TODO(ajwerner): Deal with redaction.

var _ error = (*notImplementedError)(nil)

// HasNotImplemented returns true if the error indicates that the builder does
// not support the provided statement.
func HasNotImplemented(err error) bool {
	return errors.HasType(err, (*notImplementedError)(nil))
}

func (e *notImplementedError) Error() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%T not implemented in the new schema changer", e.n)
	if e.detail != "" {
		fmt.Fprintf(&buf, ": %s", e.detail)
	}
	return buf.String()
}

// ConcurrentSchemaChangeError indicates that building the schema change plan
// is not currently possible because there are other concurrent schema changes
// on one of the descriptors.
type ConcurrentSchemaChangeError struct {
	// TODO(ajwerner): Instead of waiting for one descriptor at a time, we should
	// get all the IDs of the descriptors we might be waiting for and return them
	// from the builder.
	descID descpb.ID
}

func (e *ConcurrentSchemaChangeError) Error() string {
	return fmt.Sprintf("descriptor %d is undergoing another schema change", e.descID)
}

// DescriptorID is the ID of the descriptor undergoing concurrent schema
// changes.
func (e *ConcurrentSchemaChangeError) DescriptorID() descpb.ID {
	return e.descID
}

// NewBuilder creates a new BuildContext.
func NewBuilder(
	res resolver.SchemaResolver,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	descs *descs.Collection,
	initialNodes []*scpb.Node,
) *BuildContext {
	return &BuildContext{
		Dependencies: Dependencies{
			res:     res,
			semaCtx: semaCtx,
			evalCtx: evalCtx,
			descs:   descs,
		},
		outputNodes: initialNodes,
	}
}

// Build builds targets and transforms the provided schema change nodes
// accordingly, given a statement.
//
// TODO(ajwerner): Clarify whether the nodes will be mutated. Potentially just
// clone them defensively here. Similarly, close the statement as some schema
// changes mutate the AST. It's best if this method had a clear contract that
// it did not mutate its arguments.
func (b *BuildContext) Build(
	ctx context.Context, n tree.Statement,
) (err error, outputNodes []*scpb.Node) {
	defer func() {
		if recErr := recover(); recErr != nil {
			if errObj, ok := recErr.(error); ok {
				err = errObj
			} else {
				err = errors.Errorf("Unknown error encountered while building schema change plan %s", recErr)
			}
		}
	}()
	switch n := n.(type) {
	case *tree.DropSequence:
		b.dropSequence(ctx, n)
	case *tree.AlterTable:
		b.alterTable(ctx, n)
	default:
		return &notImplementedError{n: n}, nil
	}
	return err, b.outputNodes
}

func (b *BuildContext) addNode(dir scpb.Target_Direction, elem scpb.Element) {
	var s scpb.State
	switch dir {
	case scpb.Target_ADD:
		s = scpb.State_ABSENT
	case scpb.Target_DROP:
		s = scpb.State_PUBLIC
	default:
		panic(errors.Errorf("unknown direction %s", dir))
	}
	b.outputNodes = append(b.outputNodes, &scpb.Node{
		Target: scpb.NewTarget(dir, elem),
		State:  s,
	})
}

// getTableDescriptorForLockingChange returns a table descriptor that is
// guaranteed to have no concurrent running schema changes and can therefore
// undergo a "locking" change, or else a ConcurrentSchemaChangeError if the
// table is not currently in the required state. Locking changes roughly
// correspond to schema changes with mutations, which must be serialized and
// (in the new schema changer) require mutual exclusion.
func (b *BuildContext) getTableDescriptorForLockingChange(
	ctx context.Context, tn *tree.TableName,
) (catalog.TableDescriptor, error) {
	table, err := b.getTableDescriptor(ctx, tn)
	if err != nil {
		return nil, err
	}
	if HasConcurrentSchemaChanges(table) {
		return nil, &ConcurrentSchemaChangeError{descID: table.GetID()}
	}
	return table, nil
}

func (b *BuildContext) getTableDescriptor(
	ctx context.Context, tn *tree.TableName,
) (catalog.TableDescriptor, error) {
	// This will return an error for dropped and offline tables, but it's possible
	// that later iterations of the builder will want to handle those cases
	// in a different way.
	return resolver.ResolveExistingTableObject(ctx, b.res, tn,
		tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				Required:    true,
				AvoidCached: true,
			},
		},
	)
}

// HasConcurrentSchemaChanges returns whether the table descriptor is undergoing
// concurrent schema changes.
func HasConcurrentSchemaChanges(table catalog.TableDescriptor) bool {
	// TODO(ajwerner): For now we simply check for the absence of mutations. Once
	// we start implementing schema changes with ops to be executed during
	// statement execution, we'll have to take into account mutations that were
	// written in this transaction.
	return len(table.AllMutations()) > 0
}
