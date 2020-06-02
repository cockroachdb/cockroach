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

	"github.com/cockroachdb/cockroach/pkg/sql/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	tn *tree.Name,
	dbPrefix string,
	desc *sqlbase.ImmutableTableDescriptor,
	lCtx simpleSchemaResolver,
	displayOptions ShowCreateDisplayOptions,
) (string, error) {
	a := &sqlbase.DatumAlloc{}

	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	if desc.Temporary {
		f.WriteString("TEMP ")
	}
	f.WriteString("TABLE ")
	f.FormatNode(tn)
	f.WriteString(" (")
	primaryKeyIsOnVisibleColumn := false
	visibleCols := desc.VisibleColumns()
	for i := range visibleCols {
		col := &visibleCols[i]
		if i != 0 {
			f.WriteString(",")
		}
		f.WriteString("\n\t")
		colstr, err := schemaexpr.FormatColumnForDisplay(ctx, &p.RunParams(ctx).p.semaCtx, desc, col)
		if err != nil {
			return "", err
		}
		f.WriteString(colstr)
		if desc.IsPhysicalTable() && desc.PrimaryIndex.ColumnIDs[0] == col.ID {
			// Only set primaryKeyIsOnVisibleColumn to true if the primary key
			// is on a visible column (not rowid).
			primaryKeyIsOnVisibleColumn = true
		}
	}
	if primaryKeyIsOnVisibleColumn ||
		(desc.IsPhysicalTable() && desc.PrimaryIndex.IsSharded()) {
		f.WriteString(",\n\tCONSTRAINT ")
		formatQuoteNames(&f.Buffer, desc.PrimaryIndex.Name)
		f.WriteString(" ")
		f.WriteString(desc.PrimaryKeyString())
	}
	// TODO (lucy): Possibly include FKs in the mutations list here, or else
	// exclude check mutations below, for consistency.
	if displayOptions.FKDisplayMode != OmitFKClausesFromCreate {
		for i := range desc.OutboundFKs {
			fkCtx := tree.NewFmtCtx(tree.FmtSimple)
			fk := &desc.OutboundFKs[i]
			fkCtx.WriteString(",\n\tCONSTRAINT ")
			fkCtx.FormatNameP(&fk.Name)
			fkCtx.WriteString(" ")
			if err := showForeignKeyConstraint(&fkCtx.Buffer, dbPrefix, desc, fk, lCtx); err != nil {
				if displayOptions.FKDisplayMode == OmitMissingFKClausesFromCreate {
					continue
				} else { // When FKDisplayMode == IncludeFkClausesInCreate.
					return "", err
				}
			}
			f.WriteString(fkCtx.String())
		}
	}
	allIdx := append(desc.Indexes, desc.PrimaryIndex)
	for i := range allIdx {
		idx := &allIdx[i]
		// Only add indexes to the create_statement column, and not to the
		// create_nofks column if they are not associated with an INTERLEAVE
		// statement.
		// Initialize to false if Interleave has no ancestors, indicating that the
		// index is not interleaved at all.
		includeInterleaveClause := len(idx.Interleave.Ancestors) == 0
		if displayOptions.FKDisplayMode != OmitFKClausesFromCreate {
			// The caller is instructing us to not omit FK clauses from inside the CREATE.
			// (i.e. the caller does not want them as separate DDL.)
			// Since we're including FK clauses, we need to also include the PARTITION and INTERLEAVE
			// clauses as well.
			includeInterleaveClause = true
		}
		if idx.ID != desc.PrimaryIndex.ID && includeInterleaveClause {
			// Showing the primary index is handled above.
			f.WriteString(",\n\t")
			f.WriteString(idx.SQLString(&sqlbase.AnonymousTable))
			// Showing the INTERLEAVE and PARTITION BY for the primary index are
			// handled last.

			// Add interleave or Foreign Key indexes only to the create_table columns,
			// and not the create_nofks column.
			if includeInterleaveClause {
				if err := showCreateInterleave(idx, &f.Buffer, dbPrefix, lCtx); err != nil {
					return "", err
				}
			}
			if err := ShowCreatePartitioning(
				a, p.ExecCfg().Codec, desc, idx, &idx.Partitioning, &f.Buffer, 1 /* indent */, 0, /* colOffset */
			); err != nil {
				return "", err
			}
		}
	}

	// Create the FAMILY and CONSTRAINTs of the CREATE statement
	showFamilyClause(desc, f)
	if err := showConstraintClause(ctx, desc, &p.RunParams(ctx).p.semaCtx, f); err != nil {
		return "", err
	}

	if err := showCreateInterleave(&desc.PrimaryIndex, &f.Buffer, dbPrefix, lCtx); err != nil {
		return "", err
	}
	if err := ShowCreatePartitioning(
		a, p.ExecCfg().Codec, desc, &desc.PrimaryIndex, &desc.PrimaryIndex.Partitioning, &f.Buffer, 0 /* indent */, 0, /* colOffset */
	); err != nil {
		return "", err
	}

	if !displayOptions.IgnoreComments {
		if err := showComments(desc, selectComment(ctx, p, desc.ID), &f.Buffer); err != nil {
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
// statement used to create the descriptor passed in. The
//
// The names of the tables references by foreign keys, and the
// interleaved parent if any, are prefixed by their own database name
// unless it is equal to the given dbPrefix. This allows us to elide
// the prefix when the given table references other tables in the
// current database.
func (p *planner) ShowCreate(
	ctx context.Context,
	dbPrefix string,
	allDescs []sqlbase.Descriptor,
	desc *sqlbase.ImmutableTableDescriptor,
	displayOptions ShowCreateDisplayOptions,
) (string, error) {
	var stmt string
	var err error
	tn := (*tree.Name)(&desc.Name)
	if desc.IsView() {
		stmt, err = ShowCreateView(ctx, tn, desc)
	} else if desc.IsSequence() {
		stmt, err = ShowCreateSequence(ctx, tn, desc)
	} else {
		lCtx := newInternalLookupCtxFromDescriptors(allDescs, nil /* want all tables */)
		stmt, err = ShowCreateTable(ctx, p, tn, dbPrefix, desc, lCtx, displayOptions)
	}

	return stmt, err
}
