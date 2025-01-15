// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	plpgsql "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

// createViewNode represents a CREATE VIEW statement.
type createViewNode struct {
	zeroInputPlanNode
	createView *tree.CreateView
	// viewQuery contains the view definition, with all table names fully
	// qualified.
	viewQuery string
	dbDesc    catalog.DatabaseDescriptor
	columns   colinfo.ResultColumns

	// planDeps tracks which tables and views the view being created
	// depends on. This is collected during the construction of
	// the view query's logical plan.
	planDeps planDependencies

	// typeDeps tracks which types the view being created
	// depends on. This is collected during the construction of
	// the view query's logical plan.
	typeDeps typeDependencies
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because CREATE VIEW performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *createViewNode) ReadingOwnWrites() {}

func (n *createViewNode) startExec(params runParams) error {
	// Check if the parent object is a replicated PCR descriptor, which will block
	// schema changes.
	if n.dbDesc.GetReplicatedPCRVersion() != 0 {
		return pgerror.Newf(pgcode.ReadOnlySQLTransaction, "schema changes are not allowed on a reader catalog")
	}
	createView := n.createView
	tableType := tree.GetTableType(
		false /* isSequence */, true /* isView */, createView.Materialized,
	)
	if createView.Replace {
		telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter(fmt.Sprintf("or_replace_%s", tableType)))
	} else {
		telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter(tableType))
	}

	viewName := createView.Name.Object()
	log.VEventf(params.ctx, 2, "dependencies for view %s:\n%s", viewName, n.planDeps.String())

	// Check that the view does not contain references to other databases.
	if !allowCrossDatabaseViews.Get(&params.p.execCfg.Settings.SV) {
		for _, dep := range n.planDeps {
			if dbID := dep.desc.GetParentID(); dbID != n.dbDesc.GetID() && dbID != keys.SystemDatabaseID {
				return errors.WithHint(
					pgerror.Newf(pgcode.FeatureNotSupported,
						"the view cannot refer to other databases; (see the '%s' cluster setting)",
						allowCrossDatabaseViewsSetting),
					crossDBReferenceDeprecationHint(),
				)
			}
		}
	}

	// First check the backrefs and see if any of them are temporary.
	// If so, promote this view to temporary.
	backRefMutables := make(map[descpb.ID]*tabledesc.Mutable, len(n.planDeps))
	hasTempBackref := false
	{
		var ids catalog.DescriptorIDSet
		for id := range n.planDeps {
			ids.Add(id)
		}
		// Lookup the dependent tables in bulk to minimize round-trips to KV.
		if _, err := params.p.Descriptors().ByIDWithoutLeased(params.p.Txn()).WithoutNonPublic().WithoutSynthetic().Get().Descs(params.ctx, ids.Ordered()); err != nil {
			return err
		}
		for id := range n.planDeps {
			backRefMutable, err := params.p.Descriptors().MutableByID(params.p.Txn()).Table(params.ctx, id)
			if err != nil {
				return err
			}
			if !createView.Persistence.IsTemporary() && backRefMutable.Temporary {
				hasTempBackref = true
			}
			backRefMutables[id] = backRefMutable
		}
	}
	if hasTempBackref {
		createView.Persistence = tree.PersistenceTemporary
		// This notice is sent from pg, let's imitate.
		params.p.BufferClientNotice(
			params.ctx,
			pgnotice.Newf(`view "%s" will be a temporary view`, viewName),
		)
	}

	var replacingDesc *tabledesc.Mutable
	schema, err := getSchemaForCreateTable(params, n.dbDesc, createView.Persistence, &createView.Name,
		tree.ResolveRequireViewDesc, createView.IfNotExists)
	if err != nil && !sqlerrors.IsRelationAlreadyExistsError(err) {
		return err
	}
	if err != nil {
		switch {
		case createView.IfNotExists:
			return nil
		case createView.Replace:
			// If we are replacing an existing view see if what we are
			// replacing is actually a view.
			id, err := params.p.Descriptors().LookupObjectID(
				params.ctx,
				params.p.txn,
				n.dbDesc.GetID(),
				schema.GetID(),
				createView.Name.Table(),
			)
			if err != nil {
				return err
			}
			desc, err := params.p.Descriptors().MutableByID(params.p.txn).Table(params.ctx, id)
			if err != nil {
				return err
			}
			if err := params.p.CheckPrivilege(params.ctx, desc, privilege.DROP); err != nil {
				return err
			}
			if !desc.IsView() {
				return pgerror.Newf(pgcode.WrongObjectType, `%q is not a view`, viewName)
			}
			replacingDesc = desc
		default:
			return err
		}
	}

	if createView.Persistence.IsTemporary() {
		telemetry.Inc(sqltelemetry.CreateTempViewCounter)
	}

	privs, err := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		n.dbDesc.GetDefaultPrivilegeDescriptor(),
		schema.GetDefaultPrivilegeDescriptor(),
		n.dbDesc.GetID(),
		params.SessionData().User(),
		privilege.Tables,
	)
	if err != nil {
		return err
	}

	var newDesc *tabledesc.Mutable
	applyGlobalMultiRegionZoneConfig := false

	var retErr error
	params.p.runWithOptions(resolveFlags{contextDatabaseID: n.dbDesc.GetID()}, func() {
		retErr = func() error {
			// If replacingDesc != nil, we found an existing view while resolving
			// the name for our view. So instead of creating a new view, replace
			// the existing one.
			if replacingDesc != nil {
				newDesc, err = params.p.replaceViewDesc(
					params.ctx,
					params.p,
					n,
					replacingDesc,
					backRefMutables,
				)
				if err != nil {
					return err
				}
			} else {
				// If we aren't replacing anything, make a new table descriptor.
				id, err := params.EvalContext().DescIDGenerator.
					GenerateUniqueDescID(params.ctx)
				if err != nil {
					return err
				}
				// creationTime is initialized to a zero value and populated at read time.
				// See the comment in desc.MaybeIncrementVersion.
				//
				// TODO(ajwerner): remove the timestamp from MakeViewTableDesc, it's
				// currently relied on in import and restore code and tests.
				var creationTime hlc.Timestamp
				desc, err := makeViewTableDesc(
					params.ctx,
					viewName,
					n.viewQuery,
					n.dbDesc.GetID(),
					schema.GetID(),
					id,
					n.columns,
					creationTime,
					privs,
					&params.p.semaCtx,
					params.p.EvalContext(),
					params.p.EvalContext().Settings,
					createView.Persistence,
					n.dbDesc.IsMultiRegion(),
					params.p)
				if err != nil {
					return err
				}

				if createView.Materialized {
					// If the view is materialized, set up some more state on the view descriptor.
					// In particular,
					// * mark the descriptor as a materialized view
					// * mark the state as adding and remember the AsOf time to perform
					//   the view query
					// * use AllocateIDs to give the view descriptor a primary key
					desc.IsMaterializedView = true
					// If the materialized view has been created WITH NO DATA option, mark
					// the table descriptor as requiring a REFRESH VIEW to indicate the view
					// should only be accessed after a REFRESH VIEW operation has been called
					// on it.
					desc.RefreshViewRequired = !createView.WithData
					desc.State = descpb.DescriptorState_ADD
					version := params.ExecCfg().Settings.Version.ActiveVersion(params.ctx)
					if err := desc.AllocateIDs(params.ctx, version); err != nil {
						return err
					}
					// For multi-region databases, we want this descriptor to be GLOBAL instead.
					if n.dbDesc.IsMultiRegion() {
						desc.SetTableLocalityGlobal()
						applyGlobalMultiRegionZoneConfig = true
					}
				}

				// Collect all the tables/views this view depends on.
				orderedDependsOn := catalog.DescriptorIDSet{}
				for backrefID := range n.planDeps {
					orderedDependsOn.Add(backrefID)
				}
				desc.DependsOn = append(desc.DependsOn, orderedDependsOn.Ordered()...)

				// Collect all types this view depends on.
				orderedTypeDeps := catalog.DescriptorIDSet{}
				for backrefID := range n.typeDeps {
					orderedTypeDeps.Add(backrefID)
				}
				desc.DependsOnTypes = append(desc.DependsOnTypes, orderedTypeDeps.Ordered()...)
				newDesc = &desc

				if err = params.p.createDescriptor(
					params.ctx,
					newDesc,
					tree.AsStringWithFQNames(n.createView, params.Ann()),
				); err != nil {
					return err
				}
			}

			// Persist the back-references in all referenced table descriptors.
			for id, updated := range n.planDeps {
				backRefMutable := backRefMutables[id]
				// In case that we are replacing a view that already depends on
				// this table, remove all existing references so that we don't leave
				// any out of date references. Then, add the new references.
				backRefMutable.DependedOnBy = removeMatchingReferences(
					backRefMutable.DependedOnBy,
					newDesc.ID,
				)
				for _, dep := range updated.deps {
					// The logical plan constructor merely registered the dependencies.
					// It did not populate the "ID" field of TableDescriptor_Reference,
					// because the ID of the newly created view descriptor was not
					// yet known.
					// We need to do it here.
					dep.ID = newDesc.ID
					dep.ByID = updated.desc.IsSequence()
					backRefMutable.DependedOnBy = append(backRefMutable.DependedOnBy, dep)
				}
				if err := params.p.writeSchemaChange(
					params.ctx,
					backRefMutable,
					descpb.InvalidMutationID,
					fmt.Sprintf("updating view reference %q in table %s(%d)", &createView.Name,
						updated.desc.GetName(), updated.desc.GetID(),
					),
				); err != nil {
					return err
				}
			}

			// Add back references for the type dependencies.
			for id := range n.typeDeps {
				jobDesc := fmt.Sprintf("updating type back reference %d for table %d", id, newDesc.ID)
				if err := params.p.addTypeBackReference(params.ctx, id, newDesc.ID, jobDesc); err != nil {
					return err
				}
			}

			if err := validateDescriptor(params.ctx, params.p, newDesc); err != nil {
				return err
			}

			if applyGlobalMultiRegionZoneConfig {
				regionConfig, err := SynthesizeRegionConfig(params.ctx, params.p.txn, n.dbDesc.GetID(), params.p.Descriptors())
				if err != nil {
					return err
				}
				if err := ApplyZoneConfigForMultiRegionTable(
					params.ctx,
					params.p.InternalSQLTxn(),
					params.p.ExecCfg(),
					params.p.extendedEvalCtx.Tracing.KVTracingEnabled(),
					regionConfig,
					newDesc,
					applyZoneConfigForMultiRegionTableOptionTableNewConfig(
						tabledesc.LocalityConfigGlobal(),
					),
				); err != nil {
					return err
				}
			}

			// Log Create View event. This is an auditable log event and is
			// recorded in the same transaction as the table descriptor update.
			return params.p.logEvent(params.ctx,
				newDesc.ID,
				&eventpb.CreateView{
					ViewName:  createView.Name.FQString(),
					ViewQuery: n.viewQuery,
				})
		}()
	})
	return retErr
}

func (*createViewNode) Next(runParams) (bool, error) { return false, nil }
func (*createViewNode) Values() tree.Datums          { return tree.Datums{} }
func (n *createViewNode) Close(ctx context.Context)  {}

// makeViewTableDesc returns the table descriptor for a new view.
//
// It creates the descriptor directly in the PUBLIC state rather than
// the ADDING state because back-references are added to the view's
// dependencies in the same transaction that the view is created and it
// doesn't matter if reads/writes use a cached descriptor that doesn't
// include the back-references.
func makeViewTableDesc(
	ctx context.Context,
	viewName string,
	viewQuery string,
	parentID descpb.ID,
	schemaID descpb.ID,
	id descpb.ID,
	resultColumns []colinfo.ResultColumn,
	creationTime hlc.Timestamp,
	privileges *catpb.PrivilegeDescriptor,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	st *cluster.Settings,
	persistence tree.Persistence,
	isMultiRegion bool,
	sc resolver.SchemaResolver,
) (tabledesc.Mutable, error) {
	desc := tabledesc.InitTableDescriptor(
		id,
		parentID,
		schemaID,
		viewName,
		creationTime,
		privileges,
		persistence,
	)
	desc.ViewQuery = viewQuery
	if isMultiRegion {
		desc.SetTableLocalityRegionalByTable(tree.PrimaryRegionNotSpecifiedName)
	}

	if sc != nil {
		sequenceReplacedQuery, err := replaceSeqNamesWithIDs(ctx, sc, viewQuery, false /* multiStmt */)
		if err != nil {
			return tabledesc.Mutable{}, err
		}
		desc.ViewQuery = sequenceReplacedQuery
	}

	typeReplacedQuery, err := serializeUserDefinedTypes(ctx, semaCtx, desc.ViewQuery,
		false /* multiStmt */, "view queries")
	if err != nil {
		return tabledesc.Mutable{}, err
	}
	desc.ViewQuery = typeReplacedQuery

	if err := addResultColumns(ctx, semaCtx, evalCtx, st, &desc, resultColumns); err != nil {
		return tabledesc.Mutable{}, err
	}

	return desc, nil
}

// replaceSeqNamesWithIDsLang walks the query in queryStr, replacing any
// sequence names with their IDs and returning a new query string with the names
// replaced. It assumes that the query is in the SQL language.
// TODO (Chengxiong): move this to a better place.
func replaceSeqNamesWithIDs(
	ctx context.Context, sc resolver.SchemaResolver, queryStr string, multiStmt bool,
) (string, error) {
	return replaceSeqNamesWithIDsLang(ctx, sc, queryStr, multiStmt, catpb.Function_SQL)
}

// replaceSeqNamesWithIDsLang walks the query in queryStr, replacing any
// sequence names with their IDs and returning a new query string with the names
// replaced. Queries may be in either the SQL or PLpgSQL language, indicated by
// lang.
func replaceSeqNamesWithIDsLang(
	ctx context.Context,
	sc resolver.SchemaResolver,
	queryStr string,
	multiStmt bool,
	lang catpb.Function_Language,
) (string, error) {
	replaceSeqFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if expr == nil {
			return false, expr, nil
		}
		seqIdentifiers, err := seqexpr.GetUsedSequences(expr)
		if err != nil {
			return false, expr, err
		}
		seqNameToID := make(map[string]descpb.ID)
		for _, seqIdentifier := range seqIdentifiers {
			seqDesc, err := GetSequenceDescFromIdentifier(ctx, sc, seqIdentifier)
			if err != nil {
				return false, expr, err
			}
			seqNameToID[seqIdentifier.SeqName] = seqDesc.ID
		}
		newExpr, err = seqexpr.ReplaceSequenceNamesWithIDs(expr, seqNameToID)
		if err != nil {
			return false, expr, err
		}
		return false, newExpr, nil
	}

	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	switch lang {
	case catpb.Function_SQL:
		var stmts tree.Statements
		if multiStmt {
			parsedStmtd, err := parser.Parse(queryStr)
			if err != nil {
				return "", errors.Wrap(err, "failed to parse query string")
			}
			for _, s := range parsedStmtd {
				stmts = append(stmts, s.AST)
			}
		} else {
			stmt, err := parser.ParseOne(queryStr)
			if err != nil {
				return "", errors.Wrap(err, "failed to parse query string")
			}
			stmts = tree.Statements{stmt.AST}
		}

		for i, stmt := range stmts {
			newStmt, err := tree.SimpleStmtVisit(stmt, replaceSeqFunc)
			if err != nil {
				return "", err
			}
			if i > 0 {
				fmtCtx.WriteString("\n")
			}
			fmtCtx.FormatNode(newStmt)
			if multiStmt {
				fmtCtx.WriteString(";")
			}
		}
	case catpb.Function_PLPGSQL:
		var stmts plpgsqltree.Statement
		plstmt, err := plpgsql.Parse(queryStr)
		if err != nil {
			return "", errors.Wrap(err, "failed to parse query string")
		}
		stmts = plstmt.AST

		v := plpgsqltree.SQLStmtVisitor{Fn: replaceSeqFunc}
		newStmt := plpgsqltree.Walk(&v, stmts)
		fmtCtx.FormatNode(newStmt)
	}

	return fmtCtx.String(), nil
}

// serializeUserDefinedTypes walks the given query and serializes any
// user defined types as IDs, so that renaming the type does not cause
// corruption, and returns a new query string containing the replacement IDs.
// It assumes that the query language is SQL.
func serializeUserDefinedTypes(
	ctx context.Context, semaCtx *tree.SemaContext, queries string, multiStmt bool, parentType string,
) (string, error) {
	return serializeUserDefinedTypesLang(ctx, semaCtx, queries, multiStmt, parentType, catpb.Function_SQL)
}

// serializeUserDefinedTypesLang walks the given query and serializes any
// user defined types as IDs, so that renaming the type does not cause
// corruption, and returns a new query string containing the replacement IDs.
// The query may be in either the SQL or PLpgSQL language, indicated by lang.
func serializeUserDefinedTypesLang(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	queries string,
	multiStmt bool,
	parentType string,
	lang catpb.Function_Language,
) (string, error) {
	// replaceFunc is a visitor function that replaces user defined types in SQL
	// expressions with their IDs.
	replaceFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if expr == nil {
			return false, expr, nil
		}
		var innerExpr tree.Expr
		var typRef tree.ResolvableTypeReference
		switch n := expr.(type) {
		case *tree.CastExpr:
			innerExpr = n.Expr
			typRef = n.Type
		case *tree.AnnotateTypeExpr:
			innerExpr = n.Expr
			typRef = n.Type
		default:
			return true, expr, nil
		}
		// semaCtx may be nil if this is a virtual view being created at
		// init time.
		var typeResolver tree.TypeReferenceResolver
		if semaCtx != nil {
			typeResolver = semaCtx.TypeResolver
		}
		var typ *types.T
		typ, err = tree.ResolveType(ctx, typRef, typeResolver)
		if err != nil {
			return false, expr, err
		}
		if !typ.UserDefined() {
			return true, expr, nil
		}
		{
			// We cannot type-check subqueries without using optbuilder, so we
			// currently do not support casting expressions with subqueries to
			// UDTs.
			context := "casts to enums within " + parentType
			defer semaCtx.Properties.Restore(semaCtx.Properties)
			semaCtx.Properties.Require(context, tree.RejectSubqueries)
		}
		texpr, err := innerExpr.TypeCheck(ctx, semaCtx, typ)
		if err != nil {
			return false, expr, err
		}
		s := tree.Serialize(texpr)
		parsedExpr, err := parser.ParseExpr(s)
		if err != nil {
			return false, expr, err
		}
		return false, parsedExpr, nil
	}
	// replaceTypeFunc is a visitor function that replaces type annotations
	// containing user defined types with their IDs. This is currently only
	// necessary for some kinds of PLpgSQL statements.
	replaceTypeFunc := func(typ tree.ResolvableTypeReference) (newTyp tree.ResolvableTypeReference, err error) {
		if typ == nil {
			return typ, nil
		}
		// semaCtx may be nil if this is a virtual view being created at
		// init time.
		var typeResolver tree.TypeReferenceResolver
		if semaCtx != nil {
			typeResolver = semaCtx.TypeResolver
		}
		var t *types.T
		t, err = tree.ResolveType(ctx, typ, typeResolver)
		if err != nil {
			return typ, err
		}
		if !t.UserDefined() {
			return typ, nil
		}
		return &tree.OIDTypeReference{OID: t.Oid()}, nil
	}

	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	switch lang {
	case catpb.Function_SQL:
		var stmts tree.Statements
		if multiStmt {
			parsedStmts, err := parser.Parse(queries)
			if err != nil {
				return "", errors.Wrap(err, "failed to parse query")
			}
			stmts = make(tree.Statements, len(parsedStmts))
			for i, stmt := range parsedStmts {
				stmts[i] = stmt.AST
			}
		} else {
			stmt, err := parser.ParseOne(queries)
			if err != nil {
				return "", errors.Wrap(err, "failed to parse query")
			}
			stmts = tree.Statements{stmt.AST}
		}

		for i, stmt := range stmts {
			newStmt, err := tree.SimpleStmtVisit(stmt, replaceFunc)
			if err != nil {
				return "", err
			}
			if i > 0 {
				fmtCtx.WriteString("\n")
			}
			fmtCtx.FormatNode(newStmt)
			if multiStmt {
				fmtCtx.WriteString(";")
			}
		}
	case catpb.Function_PLPGSQL:
		var stmts plpgsqltree.Statement
		plstmt, err := plpgsql.Parse(queries)
		if err != nil {
			return "", errors.Wrap(err, "failed to parse query string")
		}
		stmts = plstmt.AST

		v := plpgsqltree.SQLStmtVisitor{Fn: replaceFunc}
		newStmt := plpgsqltree.Walk(&v, stmts)
		// Some PLpgSQL statements (i.e., declarations), may contain type
		// annotations containing the UDT. We need to walk the AST to replace them,
		// too.
		v2 := plpgsqltree.TypeRefVisitor{Fn: replaceTypeFunc}
		newStmt = plpgsqltree.Walk(&v2, newStmt)
		fmtCtx.FormatNode(newStmt)
	}

	return fmtCtx.CloseAndGetString(), nil
}

// replaceViewDesc modifies and returns the input view descriptor changed
// to hold the new view represented by n. Note that back references from
// tables that the new view depends on still need to be added. This function
// will additionally drop backreferences from tables the old view depended
// on that the new view no longer depends on.
func (p *planner) replaceViewDesc(
	ctx context.Context,
	sc resolver.SchemaResolver,
	n *createViewNode,
	toReplace *tabledesc.Mutable,
	backRefMutables map[descpb.ID]*tabledesc.Mutable,
) (*tabledesc.Mutable, error) {
	// Set the query to the new query.
	toReplace.ViewQuery = n.viewQuery

	if sc != nil {
		updatedQuery, err := replaceSeqNamesWithIDs(ctx, sc, n.viewQuery, false /* multiStmt */)
		if err != nil {
			return nil, err
		}
		toReplace.ViewQuery = updatedQuery
	}

	typeReplacedQuery, err := serializeUserDefinedTypes(ctx, p.SemaCtx(), toReplace.ViewQuery,
		false /* multiStmt */, "view queries")
	if err != nil {
		return nil, err
	}
	toReplace.ViewQuery = typeReplacedQuery

	// Check that the new view has at least as many columns as the old view before
	// adding result columns.
	if len(n.columns) < len(toReplace.ClusterVersion().Columns) {
		return nil, pgerror.Newf(pgcode.InvalidTableDefinition, "cannot drop columns from view")
	}
	// Reset the columns to add the new result columns onto.
	toReplace.Columns = make([]descpb.ColumnDescriptor, 0, len(n.columns))
	toReplace.NextColumnID = 0
	if err := addResultColumns(ctx, &p.semaCtx, p.EvalContext(), p.EvalContext().Settings, toReplace, n.columns); err != nil {
		return nil, err
	}

	// Compare toReplace against its clusterVersion to verify if
	// its new set of columns is valid for a replacement view.
	if err := verifyReplacingViewColumns(
		toReplace.ClusterVersion().Columns,
		toReplace.Columns,
	); err != nil {
		return nil, err
	}

	// Remove the back reference from all tables that the view depended on.
	for _, id := range toReplace.DependsOn {
		desc, ok := backRefMutables[id]
		if !ok {
			var err error
			desc, err = p.Descriptors().MutableByID(p.txn).Table(ctx, id)
			if err != nil {
				return nil, err
			}
			backRefMutables[id] = desc
		}

		// If n.planDeps doesn't contain id, then the new view definition doesn't
		// reference this table anymore, so we can remove all existing references.
		if _, ok := n.planDeps[id]; !ok {
			desc.DependedOnBy = removeMatchingReferences(desc.DependedOnBy, toReplace.ID)
			if err := p.writeSchemaChange(
				ctx,
				desc,
				descpb.InvalidMutationID,
				fmt.Sprintf("removing view reference for %q from %s(%d)", &n.createView.Name,
					desc.Name, desc.ID,
				),
			); err != nil {
				return nil, err
			}
		}
	}

	// For each old type dependency (i.e. before replacing the view),
	// see if we still depend on it. If not, then remove the back reference.
	var outdatedTypeRefs []descpb.ID
	for _, id := range toReplace.DependsOnTypes {
		if _, ok := n.typeDeps[id]; !ok {
			outdatedTypeRefs = append(outdatedTypeRefs, id)
		}
	}
	jobDesc := fmt.Sprintf("updating type back references %d for table %d", outdatedTypeRefs, toReplace.ID)
	if err := p.removeTypeBackReferences(ctx, outdatedTypeRefs, toReplace.ID, jobDesc); err != nil {
		return nil, err
	}

	// Since the view query has been replaced, the dependencies that this
	// table descriptor had are gone.
	toReplace.DependsOn = make([]descpb.ID, 0, len(n.planDeps))
	for backrefID := range n.planDeps {
		toReplace.DependsOn = append(toReplace.DependsOn, backrefID)
	}
	toReplace.DependsOnTypes = make([]descpb.ID, 0, len(n.typeDeps))
	for backrefID := range n.typeDeps {
		toReplace.DependsOnTypes = append(toReplace.DependsOnTypes, backrefID)
	}

	// Since we are replacing an existing view here, we need to write the new
	// descriptor into place.
	if err := p.writeSchemaChange(ctx, toReplace, descpb.InvalidMutationID,
		fmt.Sprintf("CREATE OR REPLACE VIEW %q AS %q", &n.createView.Name, n.viewQuery),
	); err != nil {
		return nil, err
	}
	return toReplace, nil
}

// addResultColumns adds the resultColumns as actual column
// descriptors onto desc.
func addResultColumns(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	st *cluster.Settings,
	desc *tabledesc.Mutable,
	resultColumns colinfo.ResultColumns,
) error {
	for _, colRes := range resultColumns {
		colTyp := colRes.Typ
		if colTyp.Family() == types.UnknownFamily {
			colTyp = types.String
		}
		columnTableDef := tree.ColumnTableDef{Name: tree.Name(colRes.Name), Type: colTyp}
		// Nullability constraints do not need to exist on the view, since they are
		// already enforced on the source data.
		columnTableDef.Nullable.Nullability = tree.SilentNull
		// The new types in the CREATE VIEW column specs never use
		// SERIAL so we need not process SERIAL types here.
		cdd, err := tabledesc.MakeColumnDefDescs(ctx, &columnTableDef, semaCtx, evalCtx, tree.ColumnDefaultExprInNewView)
		if err != nil {
			return err
		}
		desc.AddColumn(cdd.ColumnDescriptor)
	}
	version := st.Version.ActiveVersionOrEmpty(ctx)
	if err := desc.AllocateIDs(ctx, version); err != nil {
		return err
	}
	return nil
}

// verifyReplacingViewColumns ensures that the new set of view columns must
// have at least the same prefix of columns as the old view. We attempt to
// match the postgres error message in each of the error cases below.
func verifyReplacingViewColumns(oldColumns, newColumns []descpb.ColumnDescriptor) error {
	if len(newColumns) < len(oldColumns) {
		return pgerror.Newf(pgcode.InvalidTableDefinition, "cannot drop columns from view")
	}
	for i := range oldColumns {
		oldCol, newCol := &oldColumns[i], &newColumns[i]
		if oldCol.Name != newCol.Name {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot change name of view column %q to %q`,
				oldCol.Name,
				newCol.Name,
			)
		}
		if !newCol.Type.Identical(oldCol.Type) {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot change type of view column %q from %s to %s`,
				oldCol.Name,
				oldCol.Type.String(),
				newCol.Type.String(),
			)
		}
		if newCol.Hidden != oldCol.Hidden {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot change visibility of view column %q`,
				oldCol.Name,
			)
		}
		if newCol.Nullable != oldCol.Nullable {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot change nullability of view column %q`,
				oldCol.Name,
			)
		}
	}
	return nil
}

func overrideColumnNames(cols colinfo.ResultColumns, newNames tree.NameList) colinfo.ResultColumns {
	res := append(colinfo.ResultColumns(nil), cols...)
	for i := range res {
		res[i].Name = string(newNames[i])
	}
	return res
}

func crossDBReferenceDeprecationHint() string {
	return fmt.Sprintf("Note that cross-database references will be removed in future releases. See: %s",
		docs.ReleaseNotesURL(`#deprecations`))
}
