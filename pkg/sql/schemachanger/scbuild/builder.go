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
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// AuthorizationAccessor for checking authorization (e.g. desc privileges).
type AuthorizationAccessor interface {
	// CheckPrivilege verifies that the current user has `privilege` on `descriptor`.
	CheckPrivilege(
		ctx context.Context, descriptor catalog.Descriptor, privilege privilege.Kind,
	) error
	// HasAdminRole verifies if a user has an admin role
	HasAdminRole(ctx context.Context) (bool, error)
	// HasOwnership returns if the role or any role the role is a member of
	// has ownership privilege of the desc.
	HasOwnership(ctx context.Context, descriptor catalog.Descriptor) (bool, error)
}

// Dependencies are non-stateful objects needed for planning schema
// changes.
type Dependencies struct {
	// TODO(ajwerner): Inject a better interface than this.
	Res          resolver.SchemaResolver
	SemaCtx      *tree.SemaContext
	EvalCtx      *tree.EvalContext
	Descs        *descs.Collection
	AuthAccessor AuthorizationAccessor
}

// FIXME: Stupid hack, we should pick up the event log stuff..

// BuildMetaData metadata for generated nodes, used information not inside
// the nodes themselves.
type BuildMetaData struct {
	CreatedDescriptors catalog.DescriptorIDSet
}

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
	output        scpb.State
	buildMetaData BuildMetaData
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
) (built scpb.State, metaData BuildMetaData, err error) {
	buildContext := &buildContext{
		Dependencies:  dependencies,
		output:        cloneState(initial),
		buildMetaData: metaData,
	}
	return buildContext.build(ctx, n)
}

func cloneState(state scpb.State) scpb.State {
	clone := make(scpb.State, len(state))
	for i, n := range state {
		clone[i] = &scpb.Node{
			Target: protoutil.Clone(n.Target).(*scpb.Target),
			Status: n.Status,
		}
	}
	return clone
}

// build builds targets and transforms the provided schema change nodes
// accordingly, given a statement.
func (b *buildContext) build(
	ctx context.Context, n tree.Statement,
) (output scpb.State, buildMeta BuildMetaData, err error) {
	defer func() {
		if recErr := recover(); recErr != nil {
			if errObj, ok := recErr.(error); ok {
				err = errObj
			} else {
				err = errors.Errorf("unexpected error encountered while building schema change plan %s", recErr)
			}
		}
	}()
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
	case *tree.CreateSequence:
		b.createSequence(ctx, n)
	default:
		return nil, b.buildMetaData, &notImplementedError{n: n}
	}
	// Optimize the generated nodes to apply simplification
	// rules to avoid useless operations.
	b.optimizeNodes(ctx)
	return b.output, b.buildMetaData, nil
}

func (b *buildContext) forEachNodeOfType(
	dir scpb.Target_Direction, elementType reflect.Type, elementFunc func(element scpb.Element) error,
) error {
	for _, node := range b.output {
		if node.Target.Direction == dir &&
			reflect.TypeOf(node.Element()) == elementType {
			if err := elementFunc(node.Element()); err != nil {
				return err
			}
		}
	}
	return nil
}

// addIfDuplicateDoesNotExistForDir only adds a node if a duplicate
// in the matching direction does not exist for drops or static
// additions.
func (b *buildContext) addIfDuplicateDoesNotExistForDir(
	dir scpb.Target_Direction, elem scpb.Element,
) {
	duplicateObjectsAllowed := dir == scpb.Target_DROP ||
		dir == scpb.Target_STATIC
	if exists, _ := b.checkIfNodeExists(dir, elem); exists && duplicateObjectsAllowed {
		return
	}
	b.addNode(dir, elem)
}

// checkIfNodeExists checks if an existing node is already there,
// in any direction.
func (b *buildContext) checkIfNodeExists(
	dir scpb.Target_Direction, elem scpb.Element,
) (exists bool, index int) {
	// Check if any existing node matches the new node we are
	// trying to add.
	for idx, node := range b.output {
		if screl.EqualElements(node.Element(), elem) {
			return true, idx
		}
	}
	return false, -1
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
	b.output = append(b.output, &scpb.Node{
		Target: scpb.NewTarget(dir, elem),
		Status: s,
	})
}

// getTableDescriptorForLockingChange returns a table descriptor that is
// guaranteed to have no concurrent running schema changes and can therefore
// undergo a "locking" change, or else a ConcurrentSchemaChangeError if the
// table is not currently in the required state. Locking changes roughly
// correspond to schema changes with mutations, which must be serialized and
// (in the new schema changer) require mutual exclusion.
func (b *buildContext) getTableDescriptorForLockingChange(
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

func (b *buildContext) getTableDescriptor(
	ctx context.Context, tn *tree.TableName,
) (catalog.TableDescriptor, error) {
	// This will return an error for dropped and offline tables, but it's possible
	// that later iterations of the builder will want to handle those cases
	// in a different way.
	_, table, err := resolver.ResolveExistingTableObject(ctx, b.Res, tn,
		tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				Required:    true,
				AvoidCached: true,
			},
		},
	)
	return table, err
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

func (b *buildContext) optimizeNodes(ctx context.Context) {
	droppedDescs := make(map[descpb.ID]bool)
	// Detect and track all dropped descriptors first.
	for _, node := range b.output {
		if node.Target.Direction == scpb.Target_DROP {
			if node.Target.Table != nil ||
				node.Target.View != nil ||
				node.Target.Sequence != nil {
				droppedDescs[screl.GetDescID(node.Element())] = true
			}
		}
	}
	// Drop any non-top level objects referring to dropped
	// descriptors.
	modifiedOutput := make(scpb.State, 0, len(b.output))
	for _, node := range b.output {
		if node.Target.Direction == scpb.Target_DROP &&
			(node.Target.PrimaryIndex != nil ||
				node.Target.SecondaryIndex != nil ||
				node.Target.Column != nil ||
				node.Target.ColumnName != nil ||
				node.Target.DefaultExpression != nil ||
				node.Target.InForeignKey != nil ||
				node.Target.OutForeignKey != nil ||
				node.Target.CheckConstraint != nil ||
				node.Target.UniqueConstraint != nil ||
				node.Target.Owner != nil ||
				node.Target.UserPrivileges != nil ||
				node.Target.Locality != nil) {
			_, found := droppedDescs[screl.GetDescID(node.Element())]
			if found {
				continue
			}
		}
		modifiedOutput = append(modifiedOutput, node)
	}
	b.output = modifiedOutput
}
