// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schematelemetry

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// CollectClusterSchemaForTelemetry returns a projection of the cluster's SQL
// schema as of the provided system time, suitably filtered for the purposes of
// schema telemetry.
//
// The projection may be truncated, in which case a pseudo-random subset of
// records is selected. The seed for the randomization is derived from the UUID.
//
// This function is tested in the systemschema package.
//
// TODO(postamar): monitor memory usage
func CollectClusterSchemaForTelemetry(
	ctx context.Context,
	cfg *sql.ExecutorConfig,
	asOf hlc.Timestamp,
	snapshotID uuid.UUID,
	maxRecordsInSnapshot int,
) ([]logpb.EventPayload, error) {
	// Scrape the raw catalog.
	var raw nstree.Catalog
	if err := sql.DescsTxn(ctx, cfg, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
		err := txn.SetFixedTimestamp(ctx, asOf)
		if err != nil {
			return err
		}
		raw, err = col.Direct().GetCatalogUnvalidated(ctx, txn)
		return err
	}); err != nil {
		return nil, err
	}
	// Determine which parts of the catalog are in this snapshot.
	rng := rand.New(rand.NewSource(int64(snapshotID.ToUint128().Lo ^ snapshotID.ToUint128().Hi)))
	nsKeys, orphanedDescIDs, descIDsInSnapshot := truncatedCatalogKeys(raw, maxRecordsInSnapshot, rng)
	meta := &eventpb.SchemaSnapshotMetadata{
		CommonEventDetails: logpb.CommonEventDetails{
			Timestamp: asOf.WallTime,
		},
		SnapshotID:    snapshotID.String(),
		AsOfTimestamp: asOf.WallTime,
		NumRecords:    uint32(len(nsKeys) + orphanedDescIDs.Len()),
	}
	events := make([]logpb.EventPayload, 1, 1+meta.NumRecords)
	events[0] = meta
	newEvent := func(id descpb.ID) *eventpb.SchemaDescriptor {
		return &eventpb.SchemaDescriptor{
			CommonEventDetails: meta.CommonEventDetails,
			SnapshotID:         meta.SnapshotID,
			DescID:             uint32(id),
		}
	}
	// Redact the descriptors.
	redacted := make(map[descpb.ID]*eventpb.SchemaDescriptor, descIDsInSnapshot.Len())
	_ = raw.ForEachDescriptorEntry(func(rd catalog.Descriptor) error {
		if !descIDsInSnapshot.Contains(rd.GetID()) {
			return nil
		}
		ev := newEvent(rd.GetID())
		redacted[rd.GetID()] = ev
		var redactErrs []error
		// Redact parts of the catalog which may contain PII.
		{
			mut := rd.NewBuilder().BuildCreatedMutable()
			switch d := mut.(type) {
			case *tabledesc.Mutable:
				redactErrs = redactTableDescriptor(d.TableDesc())
			case *typedesc.Mutable:
				redactTypeDescriptor(d.TypeDesc())
			}
			ev.Desc = mut.DescriptorProto()
		}
		// Add all errors to the snapshot metadata event.
		for _, err := range redactErrs {
			if err != nil {
				err = errors.Wrapf(err, " %s %q (%d)", rd.DescriptorType(), rd.GetName(), rd.GetID())
				log.Errorf(ctx, "error during schema telemetry event generation: %v", err)
				meta.Errors = append(meta.Errors, err.Error())
			}
		}
		return nil
	})
	// Add the log events for the descriptor entries with no namespace entry.
	orphanedDescIDs.ForEach(func(id descpb.ID) {
		events = append(events, redacted[id])
	})
	// Add the log events for each of the selected namespace entries.
	_ = raw.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		if _, found := nsKeys[descpb.NameInfo{
			ParentID:       e.GetParentID(),
			ParentSchemaID: e.GetParentSchemaID(),
			Name:           e.GetName(),
		}]; !found {
			return nil
		}
		ev := newEvent(e.GetID())
		ev.ParentDatabaseID = uint32(e.GetParentID())
		ev.ParentSchemaID = uint32(e.GetParentSchemaID())
		ev.Name = e.GetName()
		if src, ok := redacted[e.GetID()]; ok {
			ev.Desc = src.Desc
		}
		events = append(events, ev)
		return nil
	})
	return events, nil
}

func truncatedCatalogKeys(
	raw nstree.Catalog, maxJoinedRecords int, rng *rand.Rand,
) (
	namespaceKeys map[descpb.NameInfo]struct{},
	orphanedDescIDs, descIDsInSnapshot catalog.DescriptorIDSet,
) {
	// Collect all the joined record keys using the input catalog.
	descIDs := raw.OrderedDescriptorIDs()
	type joinedRecordKey struct {
		nsKey catalog.NameKey
		id    descpb.ID
	}
	keys := make([]joinedRecordKey, 0, len(descIDs))
	{
		var idsInNamespace catalog.DescriptorIDSet
		_ = raw.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
			idsInNamespace.Add(e.GetID())
			keys = append(keys, joinedRecordKey{
				nsKey: e,
				id:    e.GetID(),
			})
			return nil
		})
		_ = raw.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
			if !idsInNamespace.Contains(desc.GetID()) {
				keys = append(keys, joinedRecordKey{id: desc.GetID()})
			}
			return nil
		})
	}
	// Truncate the input catalog if necessary.
	// Discard any excess keys at random.
	if len(keys) > maxJoinedRecords {
		rng.Shuffle(len(keys), func(i, j int) {
			k := keys[i]
			keys[i] = keys[j]
			keys[j] = k
		})
		keys = keys[:maxJoinedRecords]
	}
	// Return namespace keys and orphaned descriptor IDs.
	namespaceKeys = make(map[descpb.NameInfo]struct{}, len(keys))
	for _, k := range keys {
		descIDsInSnapshot.Add(k.id)
		if k.nsKey == nil {
			orphanedDescIDs.Add(k.id)
		} else {
			namespaceKeys[descpb.NameInfo{
				ParentID:       k.nsKey.GetParentID(),
				ParentSchemaID: k.nsKey.GetParentSchemaID(),
				Name:           k.nsKey.GetName(),
			}] = struct{}{}
		}
	}
	return namespaceKeys, orphanedDescIDs, descIDsInSnapshot
}

func redactTableDescriptor(d *descpb.TableDescriptor) (errs []error) {
	handleErr := func(err error) {
		errs = append(errs, err)
	}
	if d.ViewQuery != "" {
		handleErr(errors.Wrap(redactQuery(&d.ViewQuery), "view query"))
	}
	if d.CreateQuery != "" {
		handleErr(errors.Wrap(redactQuery(&d.CreateQuery), "create query"))
	}
	handleErr(redactIndex(&d.PrimaryIndex))
	for i := range d.Indexes {
		idx := &d.Indexes[i]
		handleErr(errors.Wrapf(redactIndex(idx), "index #%d", idx.ID))
	}
	for i := range d.Columns {
		col := &d.Columns[i]
		for _, err := range redactColumn(col) {
			handleErr(errors.Wrapf(err, "column #%d", col.ID))
		}
	}
	for i := range d.Checks {
		chk := d.Checks[i]
		handleErr(errors.Wrapf(redactCheckConstraints(chk), "constraint #%d", chk.ConstraintID))
	}
	for i := range d.UniqueWithoutIndexConstraints {
		uwi := &d.UniqueWithoutIndexConstraints[i]
		handleErr(errors.Wrapf(redactUniqueWithoutIndexConstraint(uwi), "constraint #%d", uwi.ConstraintID))
	}
	for _, m := range d.Mutations {
		if idx := m.GetIndex(); idx != nil {
			handleErr(errors.Wrapf(redactIndex(idx), "index #%d", idx.ID))
		} else if col := m.GetColumn(); col != nil {
			for _, err := range redactColumn(col) {
				handleErr(errors.Wrapf(err, "column #%d", col.ID))
			}
		} else if ctu := m.GetConstraint(); ctu != nil {
			switch ctu.ConstraintType {
			case descpb.ConstraintToUpdate_CHECK:
				chk := &ctu.Check
				handleErr(errors.Wrapf(redactCheckConstraints(chk), "constraint #%d", chk.ConstraintID))
			case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
				uwi := &ctu.UniqueWithoutIndexConstraint
				handleErr(errors.Wrapf(redactUniqueWithoutIndexConstraint(uwi), "constraint #%d", uwi.ConstraintID))
			}
		}
	}
	if scs := d.DeclarativeSchemaChangerState; scs != nil {
		for i := range scs.RelevantStatements {
			stmt := &scs.RelevantStatements[i]
			stmt.Statement.Statement = stmt.Statement.RedactedStatement
		}
		for i := range scs.Targets {
			t := &scs.Targets[i]
			handleErr(errors.Wrapf(redactElement(t.Element()), "element #%d", i))
		}
	}
	return errs
}

func redactQuery(sql *string) error {
	q, err := parser.ParseOne(*sql)
	if err != nil {
		*sql = "_"
		return err
	}
	fmtCtx := tree.NewFmtCtx(tree.FmtHideConstants)
	q.AST.Format(fmtCtx)
	*sql = fmtCtx.String()
	return nil
}

func redactIndex(idx *descpb.IndexDescriptor) error {
	redactPartitioning(&idx.Partitioning)
	return errors.Wrap(redactExprStr(&idx.Predicate), "partial predicate")
}

func redactColumn(col *descpb.ColumnDescriptor) (errs []error) {
	handleErr := func(err error) {
		errs = append(errs, err)
	}
	if ce := col.ComputeExpr; ce != nil {
		handleErr(errors.Wrap(redactExprStr(ce), "compute expr"))
	}
	if de := col.DefaultExpr; de != nil {
		handleErr(errors.Wrap(redactExprStr(de), "default expr"))
	}
	if ue := col.OnUpdateExpr; ue != nil {
		handleErr(errors.Wrap(redactExprStr(ue), "on-update expr"))
	}
	return errs
}

func redactCheckConstraints(chk *descpb.TableDescriptor_CheckConstraint) error {
	return redactExprStr(&chk.Expr)
}

func redactUniqueWithoutIndexConstraint(uwi *descpb.UniqueWithoutIndexConstraint) error {
	return redactExprStr(&uwi.Predicate)
}

func redactTypeDescriptor(d *descpb.TypeDescriptor) {
	for i := range d.EnumMembers {
		e := &d.EnumMembers[i]
		e.LogicalRepresentation = "_"
		e.PhysicalRepresentation = []byte("_")
	}
}

// redactElement redacts literals which may contain PII from elements.
func redactElement(element scpb.Element) error {
	switch e := element.(type) {
	case *scpb.EnumTypeValue:
		e.LogicalRepresentation = "_"
		e.PhysicalRepresentation = []byte("_")
	case *scpb.IndexPartitioning:
		redactPartitioning(&e.PartitioningDescriptor)
	case *scpb.SecondaryIndexPartial:
		return redactExpr(&e.Expression.Expr)
	case *scpb.CheckConstraint:
		return redactExpr(&e.Expression.Expr)
	case *scpb.ColumnDefaultExpression:
		return redactExpr(&e.Expression.Expr)
	case *scpb.ColumnOnUpdateExpression:
		return redactExpr(&e.Expression.Expr)
	case *scpb.ColumnType:
		if e.ComputeExpr != nil {
			return redactExpr(&e.ComputeExpr.Expr)
		}
	}
	return nil
}

func redactPartitioning(p *catpb.PartitioningDescriptor) {
	for i := range p.List {
		l := &p.List[i]
		for j := range l.Values {
			l.Values[j] = []byte("_")
		}
		redactPartitioning(&l.Subpartitioning)
	}
	for i := range p.Range {
		r := &p.Range[i]
		r.FromInclusive = []byte("_")
		r.ToExclusive = []byte("_")
	}
}

func redactExpr(expr *catpb.Expression) error {
	str := string(*expr)
	err := redactExprStr(&str)
	*expr = catpb.Expression(str)
	return err
}

func redactExprStr(expr *string) error {
	if *expr == "" {
		return nil
	}
	parsedExpr, err := parser.ParseExpr(*expr)
	if err != nil {
		*expr = "_"
		return err
	}
	fmtCtx := tree.NewFmtCtx(tree.FmtHideConstants)
	parsedExpr.Format(fmtCtx)
	*expr = fmtCtx.String()
	return nil
}
