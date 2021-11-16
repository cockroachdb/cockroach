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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// buildContext is the entry point for planning schema changes. From AST nodes
// for DDL statements, it constructs targets which represent schema changes to
// be performed.
//
// The builder itself is essentially stateless aside from the dependencies it
// needs to resolve (immutable) descriptors, evaluate expressions, etc. The
// methods in its API take schema change graph nodes (i.e., targets and their
// current states) and DDL statement AST nodes, and output new schema change
// graph nodes that incorporate targets that were added or changed.
type buildContext struct {
	Dependencies

	// output contains the internal state when building targets for an individual
	// statement.
	output scpb.State

	statementMetadata
}

// statementMetadata metadata which identifies,
// the statements and tracking parent elements. This
// allows us to detailed tracking for things like event
// logging.
type statementMetadata struct {
	// statementMetaData indices that will be used to identify
	// the statements responsible for any elements.
	statementMetaData scpb.TargetMetadata
	// sourceElementID tracks the parent elements responsible
	// for any new elements added. This is used for detailed
	// tracking during cascade operations.
	sourceElementID scpb.SourceElementID
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

// Build constructs a new set state from an initial state and a statement.
func Build(
	ctx context.Context, dependencies Dependencies, initial scpb.State, n tree.Statement,
) (built scpb.State, err error) {
	buildContext := &buildContext{
		Dependencies: dependencies,
		output:       initial.Clone(),
	}
	return buildContext.build(ctx, n)
}

// build builds targets and transforms the provided schema change nodes
// accordingly, given a statement.
func (b *buildContext) build(ctx context.Context, n tree.Statement) (output scpb.State, err error) {
	defer func() {
		if recErr := recover(); recErr != nil {
			if errObj, ok := recErr.(error); ok {
				err = errObj
			} else {
				err = errors.Errorf("unexpected error encountered while building schema change plan %s", recErr)
			}
		}
	}()
	// Set up the metadata associated with the current statement,
	// which will be used to track information about what generated
	// the current set of elements.
	b.updateStatementMetadata(n)
	switch n := n.(type) {
	case *tree.DropTable:
		b.dropTable(ctx, n)
	case *tree.DropView:
		b.dropView(ctx, n)
	case *tree.DropSequence:
		b.dropSequence(ctx, n)
	case *tree.DropType:
		b.dropType(ctx, n)
	case *tree.DropSchema:
		b.dropSchema(ctx, n)
	case *tree.DropDatabase:
		b.dropDatabase(ctx, n)
	case *tree.AlterTable:
		b.alterTable(ctx, n)
	case *tree.CreateIndex:
		b.createIndex(ctx, n)
	default:
		return scpb.State{}, &notImplementedError{n: n}
	}
	return b.output, nil
}

// checkIfNodeExists checks if an existing node is already there,
// in any direction.
func (b *buildContext) checkIfNewColumnExistsByName(tableID descpb.ID, name tree.Name) bool {
	// Check if any existing node matches the new node we are
	// trying to add.
	for _, node := range b.output.Nodes {
		if node.Status != scpb.Status_ABSENT {
			continue
		}
		column, ok := node.Element().(*scpb.Column)
		if ok &&
			column.TableID == tableID &&
			column.Column.Name == string(name) {
			return true
		}
	}
	return false
}

// checkIfNodeExists checks if an existing node is already there,
// in any direction.
func (b *buildContext) checkIfNodeExists(
	dir scpb.Target_Direction, elem scpb.Element,
) (exists bool, index int) {
	// Check if any existing node matches the new node we are
	// trying to add.
	for idx, node := range b.output.Nodes {
		if screl.EqualElements(node.Element(), elem) {
			return true, idx
		}
	}
	return false, -1
}

func (b *buildContext) updateStatementMetadata(n tree.Statement) {
	b.output.Statements = append(b.output.Statements,
		&scpb.Statement{
			Statement: n.String(),
		},
	)
	b.output.Authorization = scpb.Authorization{
		AppName:  b.SessionData().ApplicationName,
		Username: b.SessionData().SessionUser().Normalized(),
	}
	b.statementMetaData = scpb.TargetMetadata{
		SubWorkID:       1,
		SourceElementID: b.newSourceElementID(),
		StatementID:     uint32(len(b.output.Statements) - 1),
	}
}

func (b *buildContext) addNode(dir scpb.Target_Direction, elem scpb.Element) {
	var s scpb.Status
	switch dir {
	case scpb.Target_ADD:
		s = scpb.Status_ABSENT
	case scpb.Target_DROP:
		s = scpb.Status_PUBLIC
	default:
		panic(errors.Errorf("unknown direction %s", dir))
	}

	if exists, _ := b.checkIfNodeExists(dir, elem); exists {
		panic(errors.Errorf("attempted to add duplicate element %s", elem))
	}
	b.output.Nodes = append(b.output.Nodes, &scpb.Node{
		Target: scpb.NewTarget(dir, elem, &b.statementMetaData),
		Status: s,
	})
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

// incrementSubWorkID increments the current subwork ID used for tracking
// when an statement does operations on multiple objects.
func (b *buildContext) incrementSubWorkID() {
	b.statementMetaData.SubWorkID++
}

// incrementSourceElementID increments the source element ID,
// which will be inherited by any new elements created after
// the current one.
// Note: This ID is independent of descriptors intentionally,
// 			 since from a parent-child relationship the objects
//		   can be anything.
func (b *buildContext) newSourceElementID() scpb.SourceElementID {
	b.sourceElementID++
	return b.sourceElementID
}

// setSourceElementID indicates that all nodes added
// after this will be child nodes of this source element ID.
func (b *buildContext) setSourceElementID(
	sourceElementID scpb.SourceElementID,
) scpb.SourceElementID {
	lastID := b.statementMetaData.SourceElementID
	b.statementMetaData.SourceElementID = sourceElementID
	return lastID
}

func onErrPanic(err error) {
	if err != nil {
		panic(err)
	}
}
