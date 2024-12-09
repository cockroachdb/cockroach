// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func CreateSequence(b BuildCtx, n *tree.CreateSequence) {
	doCreateSequence(b, n)
}

// doCreateSequence creates a sequence and returns the sequence element that
// has been created.
func doCreateSequence(b BuildCtx, n *tree.CreateSequence) *scpb.Sequence {
	dbElts, scElts := b.ResolveTargetObject(n.Name.ToUnresolvedObjectName(), privilege.CREATE)
	_, _, schemaElem := scpb.FindSchema(scElts)
	_, _, dbElem := scpb.FindDatabase(dbElts)
	_, _, scName := scpb.FindNamespace(scElts)
	_, _, dbname := scpb.FindNamespace(dbElts)
	n.Name.SchemaName = tree.Name(scName.Name)
	n.Name.CatalogName = tree.Name(dbname.Name)
	n.Name.ExplicitCatalog = true
	n.Name.ExplicitSchema = true
	owner := b.CurrentUser()

	// Detect duplicate sequence names.
	ers := b.ResolveRelation(n.Name.ToUnresolvedObjectName(),
		ResolveParams{
			IsExistenceOptional: true,
			RequiredPrivilege:   privilege.USAGE,
			WithOffline:         true, // We search sequence with provided name, including offline ones.
			ResolveTypes:        true, // Check for collisions with type names.
		})
	if ers != nil && !ers.IsEmpty() {
		if n.IfNotExists {
			return nil
		}
		panic(sqlerrors.NewRelationAlreadyExistsError(n.Name.FQString()))
	}

	if n.Persistence.IsTemporary() {
		if !b.SessionData().TempTablesEnabled {
			panic(errors.WithTelemetry(
				pgerror.WithCandidateCode(
					errors.WithHint(
						errors.WithIssueLink(
							errors.Newf("temporary tables are only supported experimentally"),
							errors.IssueLink{IssueURL: build.MakeIssueURL(46260)},
						),
						"You can enable temporary tables by running `SET experimental_enable_temp_tables = 'on'`.",
					),
					pgcode.ExperimentalFeature,
				),
				"sql.schema.temp_tables_disabled",
			))
		}
		// Resolve the temporary schema element.
		scElts = MaybeCreateOrResolveTemporarySchema(b)
		schemaElem = scElts.FilterSchema().MustGetOneElement()
	}

	// Sanity check for duplication options on the sequence.
	optionsSeen := map[string]bool{}
	var sequenceOwnedBy *tree.ColumnItem
	var restartWith *int64
	for _, opt := range n.Options {
		_, seenBefore := optionsSeen[opt.Name]
		if seenBefore {
			panic(pgerror.New(pgcode.Syntax, "conflicting or redundant options"))
		}
		optionsSeen[opt.Name] = true
		if opt.Name == tree.SeqOptOwnedBy {
			sequenceOwnedBy = opt.ColumnItemVal
		}
		if opt.Name == tree.SeqOptRestart {
			restartWith = opt.IntVal
		}
	}
	// If the database is multi-region then CREATE SEQUENCE will fallback.
	if _, _, dbRegionConfig := scpb.FindDatabaseRegionConfig(dbElts); dbRegionConfig != nil {
		panic(scerrors.NotImplementedErrorf(n, "create sequence unsupported"+
			"on multi-region clusters, since locality will not be set"))
	}
	// Parse the sequence options before and validate they
	// are supported.
	defaultIntSize := b.SessionData().DefaultIntSize
	tempSequenceOpts := descpb.TableDescriptor_SequenceOpts{
		Increment: 1,
	}
	if err := schemaexpr.AssignSequenceOptions(
		&tempSequenceOpts,
		n.Options,
		defaultIntSize,
		true, /*setDefaults*/
		nil,  /*existingType*/
	); err != nil {
		panic(pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue))
	}
	// Generate the sequence elements.
	sequenceID := b.GenerateUniqueDescID()
	sequenceElem := &scpb.Sequence{
		SequenceID:  sequenceID,
		IsTemporary: n.Persistence.IsTemporary(),
	}
	if restartWith != nil {
		sequenceElem.RestartWith = *restartWith
		sequenceElem.UseRestartWith = true
	}
	b.Add(sequenceElem)
	// Setup the namespace entry.
	sequenceNamespace := &scpb.Namespace{
		DatabaseID:   dbElem.DatabaseID,
		SchemaID:     schemaElem.SchemaID,
		DescriptorID: sequenceID,
		Name:         string(n.Name.ObjectName),
	}
	b.Add(sequenceNamespace)
	// Set up a schema child entry. This will be a no-op for relations.
	sequenceSchemaChild := &scpb.SchemaChild{
		ChildObjectID: sequenceID,
		SchemaID:      schemaElem.SchemaID,
	}
	b.Add(sequenceSchemaChild)
	// Add a table data element, this go public with the descriptor.
	tableData := &scpb.TableData{
		TableID:    sequenceID,
		DatabaseID: dbElem.DatabaseID,
	}
	b.Add(tableData)
	// Add any sequence options.
	options := scdecomp.GetSequenceOptions(sequenceElem.SequenceID, &tempSequenceOpts)
	for _, opt := range options {
		b.Add(opt)
	}
	// Add any sequence owned by element.
	if sequenceOwnedBy != nil {
		maybeAssignSequenceOwner(b, sequenceNamespace, sequenceOwnedBy)
	}
	// Add the single column for a sequence.
	b.Add(&scpb.Column{
		TableID:  sequenceID,
		ColumnID: tabledesc.SequenceColumnID,
	})
	b.Add(&scpb.ColumnType{
		TableID:                 sequenceID,
		ColumnID:                tabledesc.SequenceColumnID,
		TypeT:                   newTypeT(types.Int),
		ElementCreationMetadata: scdecomp.NewElementCreationMetadata(b.EvalCtx().Settings.Version.ActiveVersion(b)),
	})
	b.Add(&scpb.ColumnNotNull{
		TableID:  sequenceID,
		ColumnID: tabledesc.SequenceColumnID,
	})
	b.Add(&scpb.ColumnName{
		TableID:  sequenceID,
		ColumnID: tabledesc.SequenceColumnID,
		Name:     tabledesc.SequenceColumnName,
	})
	// Setup the primary index on the value column.
	b.Add(&scpb.PrimaryIndex{
		Index: scpb.Index{
			TableID:  sequenceID,
			IndexID:  keys.SequenceIndexID,
			IsUnique: true,
		},
	})
	b.Add(&scpb.IndexName{
		TableID: sequenceID,
		IndexID: keys.SequenceIndexID,
		Name:    tabledesc.LegacyPrimaryKeyIndexName,
	})
	b.Add(&scpb.IndexColumn{
		TableID:       sequenceID,
		IndexID:       keys.SequenceIndexID,
		ColumnID:      tabledesc.SequenceColumnID,
		OrdinalInKind: 0,
		Kind:          scpb.IndexColumn_KEY,
		Direction:     catenumpb.IndexColumn_ASC,
	})
	// Setup ownership elements.
	ownerElem, userPrivsElems :=
		b.BuildUserPrivilegesFromDefaultPrivileges(dbElem, schemaElem, sequenceID, privilege.Sequences, owner)
	b.Add(ownerElem)
	for _, userPrivsElem := range userPrivsElems {
		b.Add(userPrivsElem)
	}
	// Log the creation of this sequence.
	b.LogEventForExistingTarget(sequenceElem)
	return sequenceElem
}

func maybeAssignSequenceOwner(b BuildCtx, sequence *scpb.Namespace, owner *tree.ColumnItem) {
	if owner.TableName == nil {
		panic(errors.WithHint(pgerror.New(pgcode.Syntax, "invalid OWNED BY option"),
			"Specify OWNED BY table.column or OWNED BY NONE."))
	}
	// Resolve table first to validate it's sane.
	tableElts := b.ResolveTable(owner.TableName, ResolveParams{})
	_, _, tbl := scpb.FindTable(tableElts)
	_, _, tblNamespace := scpb.FindNamespace(tableElts)
	if tblNamespace.DatabaseID != sequence.DatabaseID {
		if err := b.CanCreateCrossDBSequenceOwnerRef(); err != nil {
			panic(err)
		}
	}
	// Next resolve the column.
	colElts := b.ResolveColumn(tbl.TableID, owner.ColumnName, ResolveParams{})
	_, _, col := scpb.FindColumn(colElts)
	// Create a sequence owner element
	b.Add(&scpb.SequenceOwner{
		SequenceID: sequence.DescriptorID,
		TableID:    tbl.TableID,
		ColumnID:   col.ColumnID,
	})
}
