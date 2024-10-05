// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catformat"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	// RedactableValues causes all constants, literals, and other user-provided
	// values to be surrounded with redaction markers.
	RedactableValues bool
}

// ShowCreateTable returns a valid SQL representation of the CREATE
// TABLE statement used to create the given table.
//
// The names of the tables referenced by foreign keys are prefixed by their own
// database name unless it is equal to the given dbPrefix. This allows us to
// elide the prefix when the given table references other tables in the
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
	ctx, sp := tracing.ChildSpan(ctx, "sql.ShowCreateTable")
	defer sp.Finish()

	a := &tree.DatumAlloc{}

	fmtFlags := tree.FmtSimple
	if displayOptions.RedactableValues {
		fmtFlags |= tree.FmtMarkRedactionNode | tree.FmtOmitNameRedaction
	}
	f := p.ExtendedEvalContext().FmtCtx(fmtFlags)
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
		colstr, err := schemaexpr.FormatColumnForDisplay(
			ctx, desc, col, &p.RunParams(ctx).p.semaCtx, p.RunParams(ctx).p.SessionData(),
			displayOptions.RedactableValues,
		)
		if err != nil {
			return "", err
		}
		f.WriteString(colstr)
	}

	if desc.IsPhysicalTable() {
		f.WriteString(",\n\tCONSTRAINT ")
		formatQuoteNames(&f.Buffer, desc.GetPrimaryIndex().GetName())
		f.WriteString(" ")
		f.WriteString(tabledesc.PrimaryKeyString(desc))
	}

	// TODO (lucy): Possibly include FKs in the mutations list here, or else
	// exclude check mutations below, for consistency.
	if displayOptions.FKDisplayMode != OmitFKClausesFromCreate {
		for _, fk := range desc.OutboundForeignKeys() {
			fkCtx := tree.NewFmtCtx(tree.FmtSimple)
			fkCtx.WriteString(",\n\tCONSTRAINT ")
			fkCtx.FormatName(fk.GetName())
			fkCtx.WriteString(" ")
			// Passing in EmptySearchPath causes the schema name to show up in the
			// constraint definition, which we need for `cockroach dump` output to be
			// usable.
			if err := showForeignKeyConstraint(
				&fkCtx.Buffer,
				dbPrefix,
				desc,
				fk.ForeignKeyDesc(),
				lCtx,
				sessiondata.EmptySearchPath,
			); err != nil {
				if displayOptions.FKDisplayMode == OmitMissingFKClausesFromCreate {
					continue
				}
				// When FKDisplayMode == IncludeFkClausesInCreate.
				return "", err
			}
			f.WriteString(fkCtx.String())
		}
	}
	for _, idx := range desc.PublicNonPrimaryIndexes() {
		// Showing the primary index is handled above.

		// Build the PARTITION BY clause.
		var partitionBuf bytes.Buffer
		if err := ShowCreatePartitioning(
			a, p.ExecCfg().Codec, desc, idx, idx.GetPartitioning(), &partitionBuf, 1, /* indent */
			0 /* colOffset */, displayOptions.RedactableValues,
		); err != nil {
			return "", err
		}

		f.WriteString(",\n\t")
		idxStr, err := catformat.IndexForDisplay(
			ctx,
			desc,
			&descpb.AnonymousTable,
			idx,
			partitionBuf.String(),
			fmtFlags,
			p.RunParams(ctx).p.SemaCtx(),
			p.RunParams(ctx).p.SessionData(),
			catformat.IndexDisplayDefOnly,
		)
		if err != nil {
			return "", err
		}
		f.WriteString(idxStr)
	}

	// Create the FAMILY and CONSTRAINTs of the CREATE statement
	showFamilyClause(desc, f)
	if err := showConstraintClause(ctx, desc, &p.RunParams(ctx).p.semaCtx, p.RunParams(ctx).p.SessionData(), f); err != nil {
		return "", err
	}

	if err := ShowCreatePartitioning(
		a, p.ExecCfg().Codec, desc, desc.GetPrimaryIndex(), desc.GetPrimaryIndex().GetPartitioning(),
		&f.Buffer, 0 /* indent */, 0 /* colOffset */, displayOptions.RedactableValues,
	); err != nil {
		return "", err
	}

	if storageParams := desc.GetStorageParams(true /* spaceBetweenEqual */); len(storageParams) > 0 {
		f.Buffer.WriteString(` WITH (`)
		f.Buffer.WriteString(strings.Join(storageParams, ", "))
		f.Buffer.WriteString(`)`)
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
// The names of the tables references by foreign keys are prefixed by their own
// database name unless it is equal to the given dbPrefix. This allows us to
// elide the prefix when the given table references other tables in the current
// database.
func (p *planner) ShowCreate(
	ctx context.Context,
	dbPrefix string,
	allHydratedDescs []catalog.Descriptor,
	desc catalog.TableDescriptor,
	displayOptions ShowCreateDisplayOptions,
) (string, error) {
	ctx, sp := tracing.ChildSpan(ctx, "sql.ShowCreate")
	defer sp.Finish()

	tn := tree.MakeUnqualifiedTableName(tree.Name(desc.GetName()))
	if desc.IsView() {
		return ShowCreateView(
			ctx, &p.RunParams(ctx).p.semaCtx, p.RunParams(ctx).p.SessionData(), &tn, desc,
			displayOptions.RedactableValues,
		)
	}
	if desc.IsSequence() {
		return ShowCreateSequence(ctx, &tn, desc)
	}
	lCtx := newInternalLookupCtx(allHydratedDescs, nil /* prefix */)
	// Overwrite desc with hydrated descriptor.
	var err error
	desc, err = lCtx.getTableByID(desc.GetID())
	if err != nil {
		return "", err
	}
	return ShowCreateTable(ctx, p, &tn, dbPrefix, desc, lCtx, displayOptions)
}
