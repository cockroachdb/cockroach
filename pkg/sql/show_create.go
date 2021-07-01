// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catformat"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

type shouldOmitFKClausesFromCreate int

const (
	_ shouldOmitFKClausesFromCreate = iota
	// OmitFKClausesFromCreate will not include any foreign key information in the
	// create statement.
	OmitFKClausesFromCreate
	// IncludeFkClausesInCreate will include foreign key information in the create
	// statement, and error if a FK cannot be resolved.
	IncludeFkClausesInCreate
	// OmitMissingFKClausesFromCreate will include foreign key information only if they
	// can be resolved. If not, it will ignore those constraints.
	// This is used in the case when showing the create statement for
	// tables stored in backups. Not all relevant tables may have been
	// included in the back up, so some foreign key information may be
	// impossible to retrieve.
	OmitMissingFKClausesFromCreate
)

// ShowCreateDisplayOptions is a container struct holding the options that
// ShowCreate uses to determine how much information should be included in the
// CREATE statement.
type ShowCreateDisplayOptions struct {
	FKDisplayMode shouldOmitFKClausesFromCreate
	// Comment resolution requires looking up table data from system.comments
	// table. This is sometimes not possible. For example, in the context of a
	// SHOW BACKUP which may resolve the create statement, there is no mechanism
	// to read any table data from the backup (nor is there a guarantee that the
	// system.comments table is included in the backup at all).
	IgnoreComments bool
}

// ShowCreateTable returns a valid SQL representation of the CREATE
// TABLE statement used to create the given table.
//
// The names of the tables references by foreign keys, and the
// interleaved parent if any, are prefixed by their own database name
// unless it is equal to the given dbPrefix. This allows us to elide
// the prefix when the given table references other tables in the
// current database.
func ShowCreateTable(
	ctx context.Context,
	p PlanHookState,
	tn *tree.TableName,
	dbPrefix string,
	desc catalog.TableDescriptor,
	lCtx simpleSchemaResolver,
	displayOptions ShowCreateDisplayOptions,
) (string, error) {
	a := &rowenc.DatumAlloc{}

	f := p.ExtendedEvalContext().FmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	if desc.IsTemporary() {
		f.WriteString("TEMP ")
	}
	f.WriteString("TABLE ")
	f.FormatNode(tn)
	f.WriteString(" (")
	// Inaccessible columns are not displayed in SHOW CREATE TABLE.
	for i, col := range desc.AccessibleColumns() {
		if i != 0 {
			f.WriteString(",")
		}
		f.WriteString("\n\t")
		colstr, err := schemaexpr.FormatColumnForDisplay(ctx, desc, col, &p.RunParams(ctx).p.semaCtx)
		if err != nil {
			return "", err
		}
		f.WriteString(colstr)
	}

	if desc.IsPhysicalTable() {
		f.WriteString(",\n\tCONSTRAINT ")
		formatQuoteNames(&f.Buffer, desc.GetPrimaryIndex().GetName())
		f.WriteString(" ")
		f.WriteString(desc.PrimaryKeyString())
	}

	// TODO (lucy): Possibly include FKs in the mutations list here, or else
	// exclude check mutations below, for consistency.
	if displayOptions.FKDisplayMode != OmitFKClausesFromCreate {
		if err := desc.ForeachOutboundFK(func(fk *descpb.ForeignKeyConstraint) error {
			fkCtx := tree.NewFmtCtx(tree.FmtSimple)
			fkCtx.WriteString(",\n\tCONSTRAINT ")
			fkCtx.FormatNameP(&fk.Name)
			fkCtx.WriteString(" ")
			// Passing in EmptySearchPath causes the schema name to show up in the
			// constraint definition, which we need for `cockroach dump` output to be
			// usable.
			if err := showForeignKeyConstraint(
				&fkCtx.Buffer,
				dbPrefix,
				desc,
				fk,
				lCtx,
				sessiondata.EmptySearchPath,
			); err != nil {
				if displayOptions.FKDisplayMode == OmitMissingFKClausesFromCreate {
					return nil
				}
				// When FKDisplayMode == IncludeFkClausesInCreate.
				return err
			}
			f.WriteString(fkCtx.String())
			return nil
		}); err != nil {
			return "", err
		}
	}
	allIdx := append(
		append([]catalog.Index{}, desc.PublicNonPrimaryIndexes()...),
		desc.GetPrimaryIndex())
	for _, idx := range allIdx {
		// Only add indexes to the create_statement column, and not to the
		// create_nofks column if they are not associated with an INTERLEAVE
		// statement.
		// Initialize to false if Interleave has no ancestors, indicating that the
		// index is not interleaved at all.
		includeInterleaveClause := idx.NumInterleaveAncestors() == 0
		if displayOptions.FKDisplayMode != OmitFKClausesFromCreate {
			// The caller is instructing us to not omit FK clauses from inside the CREATE.
			// (i.e. the caller does not want them as separate DDL.)
			// Since we're including FK clauses, we need to also include the PARTITION and INTERLEAVE
			// clauses as well.
			includeInterleaveClause = true
		}
		if !idx.Primary() && includeInterleaveClause {
			// Showing the primary index is handled above.

			// Build the PARTITION BY clause.
			var partitionBuf bytes.Buffer
			if err := ShowCreatePartitioning(
				a, p.ExecCfg().Codec, desc, idx, idx.GetPartitioning(), &partitionBuf, 1 /* indent */, 0, /* colOffset */
			); err != nil {
				return "", err
			}

			// Add interleave or Foreign Key indexes only to the create_table columns,
			// and not the create_nofks column.
			var interleaveBuf bytes.Buffer
			if includeInterleaveClause {
				// TODO(mgartner): The logic in showCreateInterleave can be
				// moved to catformat.IndexForDisplay.
				if err := showCreateInterleave(idx, &interleaveBuf, dbPrefix, lCtx); err != nil {
					return "", err
				}
			}

			f.WriteString(",\n\t")
			idxStr, err := catformat.IndexForDisplay(
				ctx,
				desc,
				&descpb.AnonymousTable,
				idx,
				partitionBuf.String(),
				interleaveBuf.String(),
				p.RunParams(ctx).p.SemaCtx(),
			)
			if err != nil {
				return "", err
			}
			f.WriteString(idxStr)

		}
	}

	// Create the FAMILY and CONSTRAINTs of the CREATE statement
	showFamilyClause(desc, f)
	if err := showConstraintClause(ctx, desc, &p.RunParams(ctx).p.semaCtx, f); err != nil {
		return "", err
	}

	if err := showCreateInterleave(desc.GetPrimaryIndex(), &f.Buffer, dbPrefix, lCtx); err != nil {
		return "", err
	}
	if err := ShowCreatePartitioning(
		a, p.ExecCfg().Codec, desc, desc.GetPrimaryIndex(), desc.GetPrimaryIndex().GetPartitioning(), &f.Buffer, 0 /* indent */, 0, /* colOffset */
	); err != nil {
		return "", err
	}

	if err := showCreateLocality(desc, f); err != nil {
		return "", err
	}

	if !displayOptions.IgnoreComments {
		if err := showComments(tn, desc, selectComment(ctx, p, desc.GetID()), &f.Buffer); err != nil {
			return "", err
		}
	}

	return f.CloseAndGetString(), nil
}

// formatQuoteNames quotes and adds commas between names.
func formatQuoteNames(buf *bytes.Buffer, names ...string) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	for i := range names {
		if i > 0 {
			f.WriteString(", ")
		}
		f.FormatNameP(&names[i])
	}
	buf.WriteString(f.CloseAndGetString())
}

// ShowCreate returns a valid SQL representation of the CREATE
// statement used to create the descriptor passed in.
//
// The names of the tables references by foreign keys, and the
// interleaved parent if any, are prefixed by their own database name
// unless it is equal to the given dbPrefix. This allows us to elide
// the prefix when the given table references other tables in the
// current database.
func (p *planner) ShowCreate(
	ctx context.Context,
	dbPrefix string,
	allDescs []descpb.Descriptor,
	desc catalog.TableDescriptor,
	displayOptions ShowCreateDisplayOptions,
) (string, error) {
	var stmt string
	var err error
	tn := tree.MakeUnqualifiedTableName(tree.Name(desc.GetName()))
	if desc.IsView() {
		stmt, err = ShowCreateView(ctx, &p.RunParams(ctx).p.semaCtx, &tn, desc)
	} else if desc.IsSequence() {
		stmt, err = ShowCreateSequence(ctx, &tn, desc)
	} else {
		lCtx, lErr := newInternalLookupCtxFromDescriptors(ctx, allDescs, nil /* want all tables */)
		if lErr != nil {
			return "", lErr
		}
		// Overwrite desc with hydrated descriptor.
		desc, err = lCtx.getTableByID(desc.GetID())
		if err != nil {
			return "", err
		}
		stmt, err = ShowCreateTable(ctx, p, &tn, dbPrefix, desc, lCtx, displayOptions)
	}

	return stmt, err
}
