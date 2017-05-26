// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type createDatabaseNode struct {
	p *planner
	n *parser.CreateDatabase
}

// CreateDatabase creates a database.
// Privileges: security.RootUser user.
//   Notes: postgres requires superuser or "CREATEDB".
//          mysql uses the mysqladmin command.
func (p *planner) CreateDatabase(n *parser.CreateDatabase) (planNode, error) {
	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	if tmpl := n.Template; tmpl != "" {
		// See https://www.postgresql.org/docs/current/static/manage-ag-templatedbs.html
		if !strings.EqualFold(tmpl, "template0") {
			return nil, fmt.Errorf("unsupported template: %s", tmpl)
		}
	}

	if enc := n.Encoding; enc != "" {
		// We only support UTF8 (and aliases for UTF8).
		if !(strings.EqualFold(enc, "UTF8") ||
			strings.EqualFold(enc, "UTF-8") ||
			strings.EqualFold(enc, "UNICODE")) {
			return nil, fmt.Errorf("unsupported encoding: %s", enc)
		}
	}

	if col := n.Collate; col != "" {
		// We only support C and C.UTF-8.
		if col != "C" && col != "C.UTF-8" {
			return nil, fmt.Errorf("unsupported collation: %s", col)
		}
	}

	if ctype := n.CType; ctype != "" {
		// We only support C and C.UTF-8.
		if ctype != "C" && ctype != "C.UTF-8" {
			return nil, fmt.Errorf("unsupported character classification: %s", ctype)
		}
	}

	if err := p.RequireSuperUser("CREATE DATABASE"); err != nil {
		return nil, err
	}

	return &createDatabaseNode{p: p, n: n}, nil
}

func (n *createDatabaseNode) Start(ctx context.Context) error {
	desc := makeDatabaseDesc(n.n)

	created, err := n.p.createDatabase(ctx, &desc, n.n.IfNotExists)
	if err != nil {
		return err
	}
	if created {
		// Log Create Database event. This is an auditable log event and is
		// recorded in the same transaction as the table descriptor update.
		if err := MakeEventLogger(n.p.LeaseMgr()).InsertEventRecord(
			ctx,
			n.p.txn,
			EventLogCreateDatabase,
			int32(desc.ID),
			int32(n.p.evalCtx.NodeID),
			struct {
				DatabaseName string
				Statement    string
				User         string
			}{n.n.Name.String(), n.n.String(), n.p.session.User},
		); err != nil {
			return err
		}
	}
	return nil
}

func (*createDatabaseNode) Next(context.Context) (bool, error) { return false, nil }
func (*createDatabaseNode) Close(context.Context)              {}
func (*createDatabaseNode) Columns() sqlbase.ResultColumns     { return make(sqlbase.ResultColumns, 0) }
func (*createDatabaseNode) Ordering() orderingInfo             { return orderingInfo{} }
func (*createDatabaseNode) Values() parser.Datums              { return parser.Datums{} }
func (*createDatabaseNode) DebugValues() debugValues           { return debugValues{} }
func (*createDatabaseNode) MarkDebug(mode explainMode)         {}

func (*createDatabaseNode) Spans(context.Context) (_, _ roachpb.Spans, _ error) {
	panic("unimplemented")
}

type createIndexNode struct {
	p         *planner
	n         *parser.CreateIndex
	tableDesc *sqlbase.TableDescriptor
}

// CreateIndex creates an index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires INDEX on the table.
func (p *planner) CreateIndex(ctx context.Context, n *parser.CreateIndex) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	tableDesc, err := mustGetTableDesc(ctx, p.txn, p.getVirtualTabler(), tn)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &createIndexNode{p: p, tableDesc: tableDesc, n: n}, nil
}

func (n *createIndexNode) Start(ctx context.Context) error {
	status, i, err := n.tableDesc.FindIndexByName(n.n.Name)
	if err == nil {
		if status == sqlbase.DescriptorIncomplete {
			switch n.tableDesc.Mutations[i].Direction {
			case sqlbase.DescriptorMutation_DROP:
				return fmt.Errorf("index %q being dropped, try again later", string(n.n.Name))

			case sqlbase.DescriptorMutation_ADD:
				// Noop, will fail in AllocateIDs below.
			}
		}
		if n.n.IfNotExists {
			return nil
		}
	}

	indexDesc := sqlbase.IndexDescriptor{
		Name:             string(n.n.Name),
		Unique:           n.n.Unique,
		StoreColumnNames: n.n.Storing.ToStrings(),
	}
	if err := indexDesc.FillColumns(n.n.Columns); err != nil {
		return err
	}

	mutationIdx := len(n.tableDesc.Mutations)
	n.tableDesc.AddIndexMutation(indexDesc, sqlbase.DescriptorMutation_ADD)
	mutationID, err := n.tableDesc.FinalizeMutation()
	if err != nil {
		return err
	}
	if err := n.tableDesc.AllocateIDs(); err != nil {
		return err
	}

	if n.n.Interleave != nil {
		index := n.tableDesc.Mutations[mutationIdx].GetIndex()
		if err := n.p.addInterleave(ctx, n.tableDesc, index, n.n.Interleave); err != nil {
			return err
		}
		if err := n.p.finalizeInterleave(ctx, n.tableDesc, *index); err != nil {
			return err
		}
	}

	if err := n.p.txn.Put(
		ctx,
		sqlbase.MakeDescMetadataKey(n.tableDesc.GetID()),
		sqlbase.WrapDescriptor(n.tableDesc),
	); err != nil {
		return err
	}

	// Record index creation in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	if err := MakeEventLogger(n.p.LeaseMgr()).InsertEventRecord(
		ctx,
		n.p.txn,
		EventLogCreateIndex,
		int32(n.tableDesc.ID),
		int32(n.p.evalCtx.NodeID),
		struct {
			TableName  string
			IndexName  string
			Statement  string
			User       string
			MutationID uint32
		}{n.tableDesc.Name, n.n.Name.String(), n.n.String(), n.p.session.User, uint32(mutationID)},
	); err != nil {
		return err
	}
	n.p.notifySchemaChange(n.tableDesc.ID, mutationID)

	return nil
}

func (*createIndexNode) Next(context.Context) (bool, error) { return false, nil }
func (*createIndexNode) Close(context.Context)              {}
func (*createIndexNode) Columns() sqlbase.ResultColumns     { return make(sqlbase.ResultColumns, 0) }
func (*createIndexNode) Ordering() orderingInfo             { return orderingInfo{} }
func (*createIndexNode) Values() parser.Datums              { return parser.Datums{} }
func (*createIndexNode) DebugValues() debugValues           { return debugValues{} }
func (*createIndexNode) MarkDebug(mode explainMode)         {}

func (*createIndexNode) Spans(context.Context) (_, _ roachpb.Spans, _ error) {
	panic("unimplemented")
}

type createUserNode struct {
	p        *planner
	n        *parser.CreateUser
	password string
}

// CreateUser creates a user.
// Privileges: INSERT on system.users.
//   notes: postgres allows the creation of users with an empty password. We do
//          as well, but disallow password authentication for these users.
func (p *planner) CreateUser(ctx context.Context, n *parser.CreateUser) (planNode, error) {
	if n.Name == "" {
		return nil, errors.New("no username specified")
	}

	tDesc, err := getTableDesc(ctx, p.txn, p.getVirtualTabler(), &parser.TableName{DatabaseName: "system", TableName: "users"})
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(tDesc, privilege.INSERT); err != nil {
		return nil, err
	}

	var resolvedPassword string
	if n.HasPassword() {
		resolvedPassword = *n.Password
		if resolvedPassword == "" {
			return nil, security.ErrEmptyPassword
		}
	}

	return &createUserNode{p: p, n: n, password: resolvedPassword}, nil
}

const usernameHelp = "usernames are case insensitive, must start with a letter " +
	"or underscore, may contain letters, digits or underscores, and must not exceed 63 characters"

var usernameRE = regexp.MustCompile(`^[\p{Ll}_][\p{Ll}0-9_]{0,62}$`)

var blacklistedUsernames = map[string]struct{}{
	security.NodeUser: {},
}

// NormalizeAndValidateUsername case folds the specified username and verifies
// it validates according to the usernameRE regular expression.
func NormalizeAndValidateUsername(username string) (string, error) {
	username = parser.Name(username).Normalize()
	if !usernameRE.MatchString(username) {
		return "", errors.Errorf("username %q invalid; %s", username, usernameHelp)
	}
	if _, ok := blacklistedUsernames[username]; ok {
		return "", errors.Errorf("username %q reserved", username)
	}
	return username, nil
}

func (n *createUserNode) Start(ctx context.Context) error {
	var hashedPassword []byte
	if n.password != "" {
		var err error
		hashedPassword, err = security.HashPassword(n.password)
		if err != nil {
			return err
		}
	}

	normalizedUsername, err := NormalizeAndValidateUsername(string(n.n.Name))
	if err != nil {
		return err
	}

	internalExecutor := InternalExecutor{LeaseManager: n.p.LeaseMgr()}
	rowsAffected, err := internalExecutor.ExecuteStatementInTransaction(
		ctx,
		"create-user",
		n.p.txn,
		"INSERT INTO system.users VALUES ($1, $2);",
		normalizedUsername,
		hashedPassword,
	)
	if err != nil {
		if sqlbase.IsUniquenessConstraintViolationError(err) {
			err = errors.Errorf("user %s already exists", normalizedUsername)
		}
		return err
	} else if rowsAffected != 1 {
		return errors.Errorf(
			"%d rows affected by user creation; expected exactly one row affected", rowsAffected,
		)
	}

	return nil
}

func (*createUserNode) Next(context.Context) (bool, error) { return false, nil }
func (*createUserNode) Close(context.Context)              {}
func (*createUserNode) Columns() sqlbase.ResultColumns     { return make(sqlbase.ResultColumns, 0) }
func (*createUserNode) Ordering() orderingInfo             { return orderingInfo{} }
func (*createUserNode) Values() parser.Datums              { return parser.Datums{} }
func (*createUserNode) DebugValues() debugValues           { return debugValues{} }
func (*createUserNode) MarkDebug(mode explainMode)         {}

func (*createUserNode) Spans(context.Context) (_, _ roachpb.Spans, _ error) {
	panic("unimplemented")
}

type createViewNode struct {
	p           *planner
	n           *parser.CreateView
	dbDesc      *sqlbase.DatabaseDescriptor
	sourcePlan  planNode
	sourceQuery string
}

// CreateView creates a view.
// Privileges: CREATE on database plus SELECT on all the selected columns.
//   notes: postgres requires CREATE on database plus SELECT on all the
//						selected columns.
//          mysql requires CREATE VIEW plus SELECT on all the selected columns.
func (p *planner) CreateView(ctx context.Context, n *parser.CreateView) (planNode, error) {
	name, err := n.Name.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	dbDesc, err := MustGetDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), name.Database())
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	// To avoid races with ongoing schema changes to tables that the view
	// depends on, make sure we use the most recent versions of table
	// descriptors rather than the copies in the lease cache.
	p.avoidCachedDescriptors = true
	sourcePlan, err := p.Select(ctx, n.AsSource, []parser.Type{})
	if err != nil {
		p.avoidCachedDescriptors = false
		return nil, err
	}

	var result *createViewNode
	defer func() {
		// Ensure that we clean up after ourselves if an error occurs, because if
		// construction of the planNode fails, Close() won't be called on it.
		if result == nil {
			sourcePlan.Close(ctx)
			p.avoidCachedDescriptors = false
		}
	}()

	numColNames := len(n.ColumnNames)
	numColumns := len(sourcePlan.Columns())
	if numColNames != 0 && numColNames != numColumns {
		return nil, sqlbase.NewSyntaxError(fmt.Sprintf(
			"CREATE VIEW specifies %d column name%s, but data source has %d column%s",
			numColNames, util.Pluralize(int64(numColNames)),
			numColumns, util.Pluralize(int64(numColumns))))
	}

	var queryBuf bytes.Buffer
	var fmtErr error
	n.AsSource.Format(
		&queryBuf,
		parser.FmtReformatTableNames(
			parser.FmtParsable,
			func(t *parser.NormalizableTableName, buf *bytes.Buffer, f parser.FmtFlags) {
				tn, err := p.QualifyWithDatabase(ctx, t)
				if err != nil {
					log.Warningf(ctx, "failed to qualify table name %q with database name: %v", t, err)
					fmtErr = err
					t.TableNameReference.Format(buf, f)
				}
				tn.Format(buf, f)
			},
		),
	)
	if fmtErr != nil {
		return nil, fmtErr
	}

	// TODO(a-robinson): Support star expressions as soon as we can (#10028).
	if p.planContainsStar(ctx, sourcePlan) {
		return nil, fmt.Errorf("views do not currently support * expressions")
	}

	// Set result rather than just returnning to ensure the defer'ed cleanup
	// doesn't trigger.
	result = &createViewNode{
		p:           p,
		n:           n,
		dbDesc:      dbDesc,
		sourcePlan:  sourcePlan,
		sourceQuery: queryBuf.String(),
	}
	return result, nil
}

func (n *createViewNode) Start(ctx context.Context) error {
	tKey := tableKey{parentID: n.dbDesc.ID, name: n.n.Name.TableName().Table()}
	key := tKey.Key()
	if exists, err := descExists(ctx, n.p.txn, key); err == nil && exists {
		// TODO(a-robinson): Support CREATE OR REPLACE commands.
		return sqlbase.NewRelationAlreadyExistsError(tKey.Name())
	} else if err != nil {
		return err
	}

	id, err := GenerateUniqueDescID(ctx, n.p.txn)
	if err != nil {
		return nil
	}

	// Inherit permissions from the database descriptor.
	privs := n.dbDesc.GetPrivileges()

	affected := make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	desc, err := n.makeViewTableDesc(
		ctx, n.n, n.dbDesc.ID, id, n.sourcePlan.Columns(), privs, affected, &n.p.evalCtx)
	if err != nil {
		return err
	}

	err = desc.ValidateTable()
	if err != nil {
		return err
	}

	err = n.p.createDescriptorWithID(ctx, key, id, &desc)
	if err != nil {
		return err
	}

	// Persist the back-references in all referenced table descriptors.
	for _, updated := range affected {
		if err := n.p.saveNonmutationAndNotify(ctx, updated); err != nil {
			return err
		}
	}
	if desc.Adding() {
		n.p.notifySchemaChange(desc.ID, sqlbase.InvalidMutationID)
	}
	if err := desc.Validate(ctx, n.p.txn); err != nil {
		return err
	}

	// Log Create View event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	if err := MakeEventLogger(n.p.LeaseMgr()).InsertEventRecord(
		ctx,
		n.p.txn,
		EventLogCreateView,
		int32(desc.ID),
		int32(n.p.evalCtx.NodeID),
		struct {
			ViewName  string
			Statement string
			User      string
		}{n.n.Name.String(), n.n.String(), n.p.session.User},
	); err != nil {
		return err
	}

	return nil
}

func (n *createViewNode) Close(ctx context.Context) {
	n.sourcePlan.Close(ctx)
	n.sourcePlan = nil
	n.p.avoidCachedDescriptors = false
}

func (*createViewNode) Next(context.Context) (bool, error) { return false, nil }
func (*createViewNode) Columns() sqlbase.ResultColumns     { return make(sqlbase.ResultColumns, 0) }
func (*createViewNode) Ordering() orderingInfo             { return orderingInfo{} }
func (*createViewNode) Values() parser.Datums              { return parser.Datums{} }
func (*createViewNode) DebugValues() debugValues           { return debugValues{} }
func (*createViewNode) MarkDebug(mode explainMode)         {}

func (*createViewNode) Spans(context.Context) (_, _ roachpb.Spans, _ error) {
	panic("unimplemented")
}

type createTableNode struct {
	p          *planner
	n          *parser.CreateTable
	dbDesc     *sqlbase.DatabaseDescriptor
	sourcePlan planNode
	count      int
}

// CreateTable creates a table.
// Privileges: CREATE on database.
//   Notes: postgres/mysql require CREATE on database.
func (p *planner) CreateTable(ctx context.Context, n *parser.CreateTable) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	dbDesc, err := MustGetDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), tn.Database())
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	hoistConstraints(n)
	for _, def := range n.Defs {
		switch t := def.(type) {
		case *parser.ForeignKeyConstraintTableDef:
			if _, err := t.Table.NormalizeWithDatabaseName(p.session.Database); err != nil {
				return nil, err
			}
		}
	}

	var sourcePlan planNode
	if n.As() {
		// The sourcePlan is needed to determine the set of columns to use
		// to populate the new table descriptor in Start() below. We
		// instantiate the sourcePlan as early as here so that EXPLAIN has
		// something useful to show about CREATE TABLE .. AS ...
		sourcePlan, err = p.Select(ctx, n.AsSource, []parser.Type{})
		if err != nil {
			return nil, err
		}
		numColNames := len(n.AsColumnNames)
		numColumns := len(sourcePlan.Columns())
		if numColNames != 0 && numColNames != numColumns {
			sourcePlan.Close(ctx)
			return nil, sqlbase.NewSyntaxError(fmt.Sprintf(
				"CREATE TABLE specifies %d column name%s, but data source has %d column%s",
				numColNames, util.Pluralize(int64(numColNames)),
				numColumns, util.Pluralize(int64(numColumns))))
		}
	}

	return &createTableNode{p: p, n: n, dbDesc: dbDesc, sourcePlan: sourcePlan}, nil
}

func hoistConstraints(n *parser.CreateTable) {
	for _, d := range n.Defs {
		if col, ok := d.(*parser.ColumnTableDef); ok {
			for _, checkExpr := range col.CheckExprs {
				n.Defs = append(n.Defs,
					&parser.CheckConstraintTableDef{
						Expr: checkExpr.Expr,
						Name: checkExpr.ConstraintName,
					},
				)
			}
			col.CheckExprs = nil
			if col.HasFKConstraint() {
				var targetCol parser.NameList
				if col.References.Col != "" {
					targetCol = append(targetCol, col.References.Col)
				}
				n.Defs = append(n.Defs, &parser.ForeignKeyConstraintTableDef{
					Table:    col.References.Table,
					FromCols: parser.NameList{col.Name},
					ToCols:   targetCol,
					Name:     col.References.ConstraintName,
				})
				col.References.Table = parser.NormalizableTableName{}
			}
		}
	}
}

func (n *createTableNode) Start(ctx context.Context) error {
	tKey := tableKey{parentID: n.dbDesc.ID, name: n.n.Table.TableName().Table()}
	key := tKey.Key()
	if exists, err := descExists(ctx, n.p.txn, key); err == nil && exists {
		if n.n.IfNotExists {
			return nil
		}
		return sqlbase.NewRelationAlreadyExistsError(tKey.Name())
	} else if err != nil {
		return err
	}

	id, err := GenerateUniqueDescID(ctx, n.p.txn)
	if err != nil {
		return err
	}

	// If a new system table is being created (which should only be doable by
	// an internal user account), make sure it gets the correct privileges.
	privs := n.dbDesc.GetPrivileges()
	if n.dbDesc.ID == keys.SystemDatabaseID {
		privs = sqlbase.NewDefaultPrivilegeDescriptor()
	}

	var desc sqlbase.TableDescriptor
	var affected map[sqlbase.ID]*sqlbase.TableDescriptor
	if n.n.As() {
		desc, err = makeTableDescIfAs(n.n, n.dbDesc.ID, id, n.sourcePlan.Columns(), privs, &n.p.evalCtx)
	} else {
		affected = make(map[sqlbase.ID]*sqlbase.TableDescriptor)
		desc, err = n.p.makeTableDesc(ctx, n.n, n.dbDesc.ID, id, privs, affected)
	}
	if err != nil {
		return err
	}

	// We need to validate again after adding the FKs.
	// Only validate the table because backreferences aren't created yet.
	// Everything is validated below.
	err = desc.ValidateTable()
	if err != nil {
		return err
	}

	if err := n.p.createDescriptorWithID(ctx, key, id, &desc); err != nil {
		return err
	}

	for _, updated := range affected {
		if err := n.p.saveNonmutationAndNotify(ctx, updated); err != nil {
			return err
		}
	}
	if desc.Adding() {
		n.p.notifySchemaChange(desc.ID, sqlbase.InvalidMutationID)
	}

	for _, index := range desc.AllNonDropIndexes() {
		if len(index.Interleave.Ancestors) > 0 {
			if err := n.p.finalizeInterleave(ctx, &desc, index); err != nil {
				return err
			}
		}
	}

	if err := desc.Validate(ctx, n.p.txn); err != nil {
		return err
	}

	// Log Create Table event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	if err := MakeEventLogger(n.p.LeaseMgr()).InsertEventRecord(
		ctx,
		n.p.txn,
		EventLogCreateTable,
		int32(desc.ID),
		int32(n.p.evalCtx.NodeID),
		struct {
			TableName string
			Statement string
			User      string
		}{n.n.Table.String(), n.n.String(), n.p.session.User},
	); err != nil {
		return err
	}

	if n.n.As() {
		// TODO(knz): Ideally we would want to plug the sourcePlan which
		// was already computed as a data source into the insertNode. Now
		// unfortunately this is not so easy: when this point is reached,
		// expandPlan() has already been called on sourcePlan (for
		// EXPLAIN), and expandPlan() on insertPlan (via optimizePlan)
		// below would cause a 2nd invocation and cause a panic. So
		// instead we close this sourcePlan and let the insertNode create
		// it anew from the AsSource syntax node.
		n.sourcePlan.Close(ctx)
		n.sourcePlan = nil

		insert := &parser.Insert{
			Table:     &n.n.Table,
			Rows:      n.n.AsSource,
			Returning: parser.AbsentReturningClause,
		}
		insertPlan, err := n.p.Insert(ctx, insert, nil /* desiredTypes */)
		if err != nil {
			return err
		}
		defer insertPlan.Close(ctx)
		insertPlan, err = n.p.optimizePlan(ctx, insertPlan, allColumns(insertPlan))
		if err != nil {
			return err
		}
		if err = n.p.startPlan(ctx, insertPlan); err != nil {
			return err
		}
		// This driver function call is done here instead of in the Next
		// method since CREATE TABLE is a DDL statement and Executor only
		// runs Next() for statements with type "Rows".
		count, err := countRowsAffected(ctx, insertPlan)
		if err != nil {
			return err
		}
		// Passing the affected rows num back
		n.count = count
	}
	return nil
}

func (n *createTableNode) Close(ctx context.Context) {
	if n.sourcePlan != nil {
		n.sourcePlan.Close(ctx)
		n.sourcePlan = nil
	}
}

func (*createTableNode) Next(context.Context) (bool, error) { return false, nil }
func (*createTableNode) Columns() sqlbase.ResultColumns     { return make(sqlbase.ResultColumns, 0) }
func (*createTableNode) Ordering() orderingInfo             { return orderingInfo{} }
func (*createTableNode) Values() parser.Datums              { return parser.Datums{} }
func (*createTableNode) DebugValues() debugValues           { return debugValues{} }
func (*createTableNode) MarkDebug(mode explainMode)         {}

func (*createTableNode) Spans(context.Context) (_, _ roachpb.Spans, _ error) {
	panic("unimplemented")
}

type indexMatch bool

const (
	matchExact  indexMatch = true
	matchPrefix indexMatch = false
)

// Referenced cols must be unique, thus referenced indexes must match exactly.
// Referencing cols have no uniqueness requirement and thus may match a strict
// prefix of an index.
func matchesIndex(
	cols []sqlbase.ColumnDescriptor, idx sqlbase.IndexDescriptor, exact indexMatch,
) bool {
	if len(cols) > len(idx.ColumnIDs) || (exact && len(cols) != len(idx.ColumnIDs)) {
		return false
	}

	for i := range cols {
		if cols[i].ID != idx.ColumnIDs[i] {
			return false
		}
	}
	return true
}

func (p *planner) resolveFK(
	ctx context.Context,
	tbl *sqlbase.TableDescriptor,
	d *parser.ForeignKeyConstraintTableDef,
	backrefs map[sqlbase.ID]*sqlbase.TableDescriptor,
	mode sqlbase.ConstraintValidity,
) error {
	return resolveFK(ctx, p.txn, &p.session.virtualSchemas, tbl, d, backrefs, mode)
}

// resolveFK looks up the tables and columns mentioned in a `REFERENCES`
// constraint and adds metadata representing that constraint to the descriptor.
// It may, in doing so, add to or alter descriptors in the passed in `backrefs`
// map of other tables that need to be updated when this table is created.
// Constraints that are not known to hold for existing data are created
// "unvalidated", but when table is empty (e.g. during creation), no existing
// data imples no existing violations, and thus the constraint can be created
// without the unvalidated flag.
func resolveFK(
	ctx context.Context,
	txn *client.Txn,
	vt VirtualTabler,
	tbl *sqlbase.TableDescriptor,
	d *parser.ForeignKeyConstraintTableDef,
	backrefs map[sqlbase.ID]*sqlbase.TableDescriptor,
	mode sqlbase.ConstraintValidity,
) error {
	targetTable := d.Table.TableName()
	target, err := getTableDesc(ctx, txn, vt, targetTable)
	if err != nil {
		return err
	}
	// Special-case: self-referencing FKs (i.e. referencing another col in the
	// same table) will reference a table name that doesn't exist yet (since we
	// are creating it).
	if target == nil {
		if targetTable.Table() == tbl.Name {
			target = tbl
		} else {
			return fmt.Errorf("referenced table %q not found", targetTable.String())
		}
	} else {
		// Since this FK is referencing another table, this table must be created in
		// a non-public "ADD" state and made public only after all leases on the
		// other table are updated to include the backref.
		if mode == sqlbase.ConstraintValidity_Validated {
			tbl.State = sqlbase.TableDescriptor_ADD
			if err := tbl.SetUpVersion(); err != nil {
				return err
			}
		}

		// When adding a self-ref FK to an _existing_ table, we want to make sure
		// we edit the same copy.
		if target.ID == tbl.ID {
			target = tbl
		} else {
			// If we resolve the same table more than once, we only want to edit a
			// single instance of it, so replace target with previously resolved table.
			if prev, ok := backrefs[target.ID]; ok {
				target = prev
			} else {
				backrefs[target.ID] = target
			}
		}
	}

	srcCols, err := tbl.FindActiveColumnsByNames(d.FromCols)
	if err != nil {
		return err
	}

	targetColNames := d.ToCols
	// If no columns are specified, attempt to default to PK.
	if len(targetColNames) == 0 {
		targetColNames = make(parser.NameList, len(target.PrimaryIndex.ColumnNames))
		for i, n := range target.PrimaryIndex.ColumnNames {
			targetColNames[i] = parser.Name(n)
		}
	}

	targetCols, err := target.FindActiveColumnsByNames(targetColNames)
	if err != nil {
		return err
	}

	if len(targetCols) != len(srcCols) {
		return fmt.Errorf("%d columns must reference exactly %d columns in referenced table (found %d)",
			len(srcCols), len(srcCols), len(targetCols))
	}

	for i := range srcCols {
		if s, t := srcCols[i], targetCols[i]; s.Type.Kind != t.Type.Kind {
			return fmt.Errorf("type of %q (%s) does not match foreign key %q.%q (%s)",
				s.Name, s.Type.Kind, target.Name, t.Name, t.Type.Kind)
		}
	}

	constraintName := string(d.Name)
	if constraintName == "" {
		constraintName = fmt.Sprintf("fk_%s_ref_%s", string(d.FromCols[0]), target.Name)
	}

	var targetIdx *sqlbase.IndexDescriptor
	if matchesIndex(targetCols, target.PrimaryIndex, matchExact) {
		targetIdx = &target.PrimaryIndex
	} else {
		found := false
		// Find the index corresponding to the referenced column.
		for i, idx := range target.Indexes {
			if idx.Unique && matchesIndex(targetCols, idx, matchExact) {
				targetIdx = &target.Indexes[i]
				found = true
				break
			}
		}
		if !found {
			return pgerror.NewErrorf(
				pgerror.CodeInvalidForeignKeyError,
				"there is no unique constraint matching given keys for referenced table %s",
				targetTable.String(),
			)
		}
	}

	ref := sqlbase.ForeignKeyReference{
		Table:           target.ID,
		Index:           targetIdx.ID,
		Name:            constraintName,
		SharedPrefixLen: int32(len(srcCols)),
	}
	if mode == sqlbase.ConstraintValidity_Unvalidated {
		ref.Validity = sqlbase.ConstraintValidity_Unvalidated
	}
	backref := sqlbase.ForeignKeyReference{Table: tbl.ID}

	if matchesIndex(srcCols, tbl.PrimaryIndex, matchPrefix) {
		if tbl.PrimaryIndex.ForeignKey.IsSet() {
			return pgerror.NewErrorf(pgerror.CodeInvalidForeignKeyError,
				"columns cannot be used by multiple foreign key constraints")
		}
		tbl.PrimaryIndex.ForeignKey = ref
		backref.Index = tbl.PrimaryIndex.ID
	} else {
		found := false
		for i := range tbl.Indexes {
			if matchesIndex(srcCols, tbl.Indexes[i], matchPrefix) {
				if tbl.Indexes[i].ForeignKey.IsSet() {
					return pgerror.NewErrorf(pgerror.CodeInvalidForeignKeyError,
						"columns cannot be used by multiple foreign key constraints")
				}
				tbl.Indexes[i].ForeignKey = ref
				backref.Index = tbl.Indexes[i].ID
				found = true
				break
			}
		}
		if !found {
			// Avoid unexpected index builds from ALTER TABLE ADD CONSTRAINT.
			if mode == sqlbase.ConstraintValidity_Unvalidated {
				return pgerror.NewErrorf(pgerror.CodeInvalidForeignKeyError,
					"foreign key requires an existing index on columns %s", colNames(srcCols))
			}
			added, err := addIndexForFK(tbl, srcCols, constraintName, ref)
			if err != nil {
				return err
			}
			backref.Index = added
		}
	}
	targetIdx.ReferencedBy = append(targetIdx.ReferencedBy, backref)
	return nil
}

// Adds an index to a table descriptor (that is in the process of being created)
// that will support using `srcCols` as the referencing (src) side of an FK.
func addIndexForFK(
	tbl *sqlbase.TableDescriptor,
	srcCols []sqlbase.ColumnDescriptor,
	constraintName string,
	ref sqlbase.ForeignKeyReference,
) (sqlbase.IndexID, error) {
	// No existing index for the referencing columns found, so we add one.
	idx := sqlbase.IndexDescriptor{
		Name:             fmt.Sprintf("%s_auto_index_%s", tbl.Name, constraintName),
		ColumnNames:      make([]string, len(srcCols)),
		ColumnDirections: make([]sqlbase.IndexDescriptor_Direction, len(srcCols)),
		ForeignKey:       ref,
	}
	for i, c := range srcCols {
		idx.ColumnDirections[i] = sqlbase.IndexDescriptor_ASC
		idx.ColumnNames[i] = c.Name
	}
	if err := tbl.AddIndex(idx, false); err != nil {
		return 0, err
	}
	if err := tbl.AllocateIDs(); err != nil {
		return 0, err
	}

	added := tbl.Indexes[len(tbl.Indexes)-1]

	// Since we just added the index, we can assume it is the last one rather than
	// searching all the indexes again. That said, we sanity check that it matches
	// in case a refactor ever violates that assumption.
	if !matchesIndex(srcCols, added, matchPrefix) {
		panic("no matching index and auto-generated index failed to match")
	}

	return added.ID, nil
}

// colNames converts a []colDesc to a human-readable string for use in error messages.
func colNames(cols []sqlbase.ColumnDescriptor) string {
	var s bytes.Buffer
	s.WriteString(`("`)
	for i, c := range cols {
		if i != 0 {
			s.WriteString(`", "`)
		}
		s.WriteString(c.Name)
	}
	s.WriteString(`")`)
	return s.String()
}

func (p *planner) saveNonmutationAndNotify(ctx context.Context, td *sqlbase.TableDescriptor) error {
	if err := td.SetUpVersion(); err != nil {
		return err
	}
	if err := td.ValidateTable(); err != nil {
		return err
	}
	if err := p.writeTableDesc(ctx, td); err != nil {
		return err
	}
	p.notifySchemaChange(td.ID, sqlbase.InvalidMutationID)
	return nil
}

func (p *planner) addInterleave(
	ctx context.Context,
	desc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	interleave *parser.InterleaveDef,
) error {
	return addInterleave(ctx, p.txn, &p.session.virtualSchemas, desc, index, interleave, p.session.Database)
}

// addInterleave marks an index as one that is interleaved in some parent data
// according to the given definition.
func addInterleave(
	ctx context.Context,
	txn *client.Txn,
	vt VirtualTabler,
	desc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	interleave *parser.InterleaveDef,
	sessionDB string,
) error {
	if interleave.DropBehavior != parser.DropDefault {
		return util.UnimplementedWithIssueErrorf(
			7854, "unsupported shorthand %s", interleave.DropBehavior)
	}

	tn, err := interleave.Parent.NormalizeWithDatabaseName(sessionDB)
	if err != nil {
		return err
	}

	parentTable, err := mustGetTableDesc(ctx, txn, vt, tn)
	if err != nil {
		return err
	}
	parentIndex := parentTable.PrimaryIndex

	if len(interleave.Fields) != len(parentIndex.ColumnIDs) {
		return fmt.Errorf("interleaved columns must match parent")
	}
	if len(interleave.Fields) > len(index.ColumnIDs) {
		return fmt.Errorf("declared columns must match index being interleaved")
	}
	for i, targetColID := range parentIndex.ColumnIDs {
		targetCol, err := parentTable.FindColumnByID(targetColID)
		if err != nil {
			return err
		}
		col, err := desc.FindColumnByID(index.ColumnIDs[i])
		if err != nil {
			return err
		}
		if interleave.Fields[i].Normalize() != parser.ReNormalizeName(col.Name) {
			return fmt.Errorf("declared columns must match index being interleaved")
		}
		if !reflect.DeepEqual(col.Type, targetCol.Type) ||
			index.ColumnDirections[i] != parentIndex.ColumnDirections[i] {

			return fmt.Errorf("interleaved columns must match parent")
		}
	}

	ancestorPrefix := append(
		[]sqlbase.InterleaveDescriptor_Ancestor(nil), parentIndex.Interleave.Ancestors...)
	intl := sqlbase.InterleaveDescriptor_Ancestor{
		TableID:         parentTable.ID,
		IndexID:         parentIndex.ID,
		SharedPrefixLen: uint32(len(parentIndex.ColumnIDs)),
	}
	for _, ancestor := range ancestorPrefix {
		intl.SharedPrefixLen -= ancestor.SharedPrefixLen
	}
	index.Interleave = sqlbase.InterleaveDescriptor{Ancestors: append(ancestorPrefix, intl)}

	desc.State = sqlbase.TableDescriptor_ADD
	return nil
}

// finalizeInterleave creats backreferences from an interleaving parent to the
// child data being interleaved.
func (p *planner) finalizeInterleave(
	ctx context.Context, desc *sqlbase.TableDescriptor, index sqlbase.IndexDescriptor,
) error {
	// TODO(dan): This is similar to finalizeFKs. Consolidate them
	if len(index.Interleave.Ancestors) == 0 {
		return nil
	}
	// Only the last ancestor needs the backreference.
	ancestor := index.Interleave.Ancestors[len(index.Interleave.Ancestors)-1]
	var ancestorTable *sqlbase.TableDescriptor
	if ancestor.TableID == desc.ID {
		ancestorTable = desc
	} else {
		var err error
		ancestorTable, err = sqlbase.GetTableDescFromID(ctx, p.txn, ancestor.TableID)
		if err != nil {
			return err
		}
	}
	ancestorIndex, err := ancestorTable.FindIndexByID(ancestor.IndexID)
	if err != nil {
		return err
	}
	ancestorIndex.InterleavedBy = append(ancestorIndex.InterleavedBy,
		sqlbase.ForeignKeyReference{Table: desc.ID, Index: index.ID})

	if err := p.saveNonmutationAndNotify(ctx, ancestorTable); err != nil {
		return err
	}

	if desc.State == sqlbase.TableDescriptor_ADD {
		desc.State = sqlbase.TableDescriptor_PUBLIC

		if err := p.saveNonmutationAndNotify(ctx, desc); err != nil {
			return err
		}
	}

	return nil
}

// makeViewTableDesc returns the table descriptor for a new view.
//
// It creates the descriptor directly in the PUBLIC state rather than
// the ADDING state because back-references are added to the view's
// dependencies in the same transaction that the view is created and it
// doesn't matter if reads/writes use a cached descriptor that doesn't
// include the back-references.
func (n *createViewNode) makeViewTableDesc(
	ctx context.Context,
	p *parser.CreateView,
	parentID sqlbase.ID,
	id sqlbase.ID,
	resultColumns []sqlbase.ResultColumn,
	privileges *sqlbase.PrivilegeDescriptor,
	affected map[sqlbase.ID]*sqlbase.TableDescriptor,
	evalCtx *parser.EvalContext,
) (sqlbase.TableDescriptor, error) {
	desc := sqlbase.TableDescriptor{
		ID:            id,
		ParentID:      parentID,
		FormatVersion: sqlbase.FamilyFormatVersion,
		Version:       1,
		Privileges:    privileges,
		ViewQuery:     n.sourceQuery,
	}
	viewName, err := p.Name.Normalize()
	if err != nil {
		return desc, err
	}
	desc.Name = viewName.Table()
	for i, colRes := range resultColumns {
		colType, err := parser.DatumTypeToColumnType(colRes.Typ)
		if err != nil {
			return desc, err
		}
		columnTableDef := parser.ColumnTableDef{Name: parser.Name(colRes.Name), Type: colType}
		if len(p.ColumnNames) > i {
			columnTableDef.Name = p.ColumnNames[i]
		}
		// We pass an empty search path here because there are no names to resolve.
		col, _, err := sqlbase.MakeColumnDefDescs(&columnTableDef, nil, evalCtx)
		if err != nil {
			return desc, err
		}
		desc.AddColumn(*col)
	}

	n.resolveViewDependencies(ctx, &desc, affected)

	return desc, desc.AllocateIDs()
}

// makeTableDescIfAs is the MakeTableDesc method for when we have a table
// that is created with the CREATE AS format.
func makeTableDescIfAs(
	p *parser.CreateTable,
	parentID, id sqlbase.ID,
	resultColumns []sqlbase.ResultColumn,
	privileges *sqlbase.PrivilegeDescriptor,
	evalCtx *parser.EvalContext,
) (desc sqlbase.TableDescriptor, err error) {
	desc = sqlbase.TableDescriptor{
		ID:            id,
		ParentID:      parentID,
		FormatVersion: sqlbase.InterleavedFormatVersion,
		Version:       1,
		Privileges:    privileges,
	}
	tableName, err := p.Table.Normalize()
	if err != nil {
		return desc, err
	}
	desc.Name = tableName.Table()
	for i, colRes := range resultColumns {
		colType, err := parser.DatumTypeToColumnType(colRes.Typ)
		if err != nil {
			return desc, err
		}
		columnTableDef := parser.ColumnTableDef{Name: parser.Name(colRes.Name), Type: colType}
		columnTableDef.Nullable.Nullability = parser.SilentNull
		if len(p.AsColumnNames) > i {
			columnTableDef.Name = p.AsColumnNames[i]
		}
		// We pass an empty search path here because we do not have any expressions to resolve.
		col, _, err := sqlbase.MakeColumnDefDescs(&columnTableDef, nil, evalCtx)
		if err != nil {
			return desc, err
		}
		desc.AddColumn(*col)
	}

	return desc, desc.AllocateIDs()
}

// MakeTableDesc creates a table descriptor from a CreateTable statement.
func MakeTableDesc(
	ctx context.Context,
	txn *client.Txn,
	vt VirtualTabler,
	searchPath parser.SearchPath,
	n *parser.CreateTable,
	parentID, id sqlbase.ID,
	privileges *sqlbase.PrivilegeDescriptor,
	affected map[sqlbase.ID]*sqlbase.TableDescriptor,
	sessionDB string,
	evalCtx *parser.EvalContext,
) (sqlbase.TableDescriptor, error) {
	desc := sqlbase.TableDescriptor{
		ID:            id,
		ParentID:      parentID,
		FormatVersion: sqlbase.InterleavedFormatVersion,
		Version:       1,
		Privileges:    privileges,
	}
	tableName, err := n.Table.Normalize()
	if err != nil {
		return desc, err
	}
	desc.Name = tableName.Table()

	for _, def := range n.Defs {
		if d, ok := def.(*parser.ColumnTableDef); ok {
			if !desc.IsVirtualTable() {
				if _, ok := d.Type.(*parser.ArrayColType); ok {
					return desc, util.UnimplementedWithIssueErrorf(2115, "ARRAY column types are unsupported")
				}
				if _, ok := d.Type.(*parser.VectorColType); ok {
					return desc, util.UnimplementedWithIssueErrorf(2115, "VECTOR column types are unsupported")
				}
			}

			col, idx, err := sqlbase.MakeColumnDefDescs(d, searchPath, evalCtx)
			if err != nil {
				return desc, err
			}
			desc.AddColumn(*col)
			if idx != nil {
				if err := desc.AddIndex(*idx, d.PrimaryKey); err != nil {
					return desc, err
				}
			}
			if d.HasColumnFamily() {
				// Pass true for `create` and `ifNotExists` because when we're creating
				// a table, we always want to create the specified family if it doesn't
				// exist.
				err := desc.AddColumnToFamilyMaybeCreate(col.Name, string(d.Family.Name), true, true)
				if err != nil {
					return desc, err
				}
			}
		}
	}

	var primaryIndexColumnSet map[string]struct{}
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *parser.ColumnTableDef:
			// pass, handled above.

		case *parser.IndexTableDef:
			idx := sqlbase.IndexDescriptor{
				Name:             string(d.Name),
				StoreColumnNames: d.Storing.ToStrings(),
			}
			if err := idx.FillColumns(d.Columns); err != nil {
				return desc, err
			}
			if err := desc.AddIndex(idx, false); err != nil {
				return desc, err
			}
			if d.Interleave != nil {
				return desc, util.UnimplementedWithIssueErrorf(9148, "use CREATE INDEX to make interleaved indexes")
			}
		case *parser.UniqueConstraintTableDef:
			idx := sqlbase.IndexDescriptor{
				Name:             string(d.Name),
				Unique:           true,
				StoreColumnNames: d.Storing.ToStrings(),
			}
			if err := idx.FillColumns(d.Columns); err != nil {
				return desc, err
			}
			if err := desc.AddIndex(idx, d.PrimaryKey); err != nil {
				return desc, err
			}
			if d.PrimaryKey {
				primaryIndexColumnSet = make(map[string]struct{})
				for _, c := range d.Columns {
					primaryIndexColumnSet[c.Column.Normalize()] = struct{}{}
				}
			}
			if d.Interleave != nil {
				return desc, util.UnimplementedWithIssueErrorf(9148, "use CREATE INDEX to make interleaved indexes")
			}

		case *parser.CheckConstraintTableDef, *parser.ForeignKeyConstraintTableDef, *parser.FamilyTableDef:
			// pass, handled below.

		default:
			return desc, errors.Errorf("unsupported table def: %T", def)
		}
	}

	if primaryIndexColumnSet != nil {
		// Primary index columns are not nullable.
		for i := range desc.Columns {
			if _, ok := primaryIndexColumnSet[parser.ReNormalizeName(desc.Columns[i].Name)]; ok {
				desc.Columns[i].Nullable = false
			}
		}
	}

	// Now that all columns are in place, add any explicit families (this is done
	// here, rather than in the constraint pass below since we want to pick up
	// explicit allocations before AllocateIDs adds implicit ones).
	for _, def := range n.Defs {
		if d, ok := def.(*parser.FamilyTableDef); ok {
			fam := sqlbase.ColumnFamilyDescriptor{
				Name:        string(d.Name),
				ColumnNames: d.Columns.ToStrings(),
			}
			desc.AddFamily(fam)
		}
	}

	if err := desc.AllocateIDs(); err != nil {
		return desc, err
	}

	if n.Interleave != nil {
		if err := addInterleave(ctx, txn, vt, &desc, &desc.PrimaryIndex, n.Interleave, sessionDB); err != nil {
			return desc, err
		}
	}

	// With all structural elements in place and IDs allocated, we can resolve the
	// constraints and qualifications.
	// FKs are resolved after the descriptor is otherwise complete and IDs have
	// been allocated since the FKs will reference those IDs. Resolution also
	// accumulates updates to other tables (adding backreferences) in the passed
	// map -- anything in that map should be saved when the table is created.
	generatedNames := map[string]struct{}{}
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *parser.ColumnTableDef, *parser.IndexTableDef, *parser.UniqueConstraintTableDef, *parser.FamilyTableDef:
			// pass, handled above.

		case *parser.CheckConstraintTableDef:
			ck, err := makeCheckConstraint(desc, d, generatedNames, searchPath)
			if err != nil {
				return desc, err
			}
			desc.Checks = append(desc.Checks, ck)

		case *parser.ForeignKeyConstraintTableDef:
			if err := resolveFK(ctx, txn, vt, &desc, d, affected, sqlbase.ConstraintValidity_Validated); err != nil {
				return desc, err
			}
		default:
			return desc, errors.Errorf("unsupported table def: %T", def)
		}
	}

	// Multiple FKs from the same column would potentially result in ambiguous or
	// unexpected behavior with conflicting CASCADE/RESTRICT/etc behaviors.
	colsInFKs := make(map[sqlbase.ColumnID]struct{})
	for _, idx := range desc.Indexes {
		if idx.ForeignKey.IsSet() {
			numCols := len(idx.ColumnIDs)
			if idx.ForeignKey.SharedPrefixLen > 0 {
				numCols = int(idx.ForeignKey.SharedPrefixLen)
			}
			for i := 0; i < numCols; i++ {
				if _, ok := colsInFKs[idx.ColumnIDs[i]]; ok {
					return desc, fmt.Errorf(
						"column %q cannot be used by multiple foreign key constraints", idx.ColumnNames[i])
				}
				colsInFKs[idx.ColumnIDs[i]] = struct{}{}
			}
		}
	}

	return desc, desc.AllocateIDs()
}

// makeTableDesc creates a table descriptor from a CreateTable statement.
func (p *planner) makeTableDesc(
	ctx context.Context,
	n *parser.CreateTable,
	parentID, id sqlbase.ID,
	privileges *sqlbase.PrivilegeDescriptor,
	affected map[sqlbase.ID]*sqlbase.TableDescriptor,
) (sqlbase.TableDescriptor, error) {
	return MakeTableDesc(
		ctx,
		p.txn,
		&p.session.virtualSchemas,
		p.session.SearchPath,
		n,
		parentID,
		id,
		privileges,
		affected,
		p.session.Database,
		&p.evalCtx,
	)
}

// dummyColumnItem is used in makeCheckConstraint to construct an expression
// that can be both type-checked and examined for variable expressions.
type dummyColumnItem struct {
	typ parser.Type
}

// String implements the Stringer interface.
func (d dummyColumnItem) String() string {
	return fmt.Sprintf("<%s>", d.typ)
}

// Format implements the NodeFormatter interface.
func (d dummyColumnItem) Format(buf *bytes.Buffer, _ parser.FmtFlags) {
	buf.WriteString(d.String())
}

// Walk implements the Expr interface.
func (d dummyColumnItem) Walk(_ parser.Visitor) parser.Expr {
	return d
}

// TypeCheck implements the Expr interface.
func (d dummyColumnItem) TypeCheck(
	_ *parser.SemaContext, desired parser.Type,
) (parser.TypedExpr, error) {
	return d, nil
}

// Eval implements the TypedExpr interface.
func (dummyColumnItem) Eval(_ *parser.EvalContext) (parser.Datum, error) {
	panic("dummyColumnItem.Eval() is undefined")
}

// ResolvedType implements the TypedExpr interface.
func (d dummyColumnItem) ResolvedType() parser.Type {
	return d.typ
}

func makeCheckConstraint(
	desc sqlbase.TableDescriptor,
	d *parser.CheckConstraintTableDef,
	inuseNames map[string]struct{},
	searchPath parser.SearchPath,
) (*sqlbase.TableDescriptor_CheckConstraint, error) {
	// CHECK expressions seem to vary across databases. Wikipedia's entry on
	// Check_constraint (https://en.wikipedia.org/wiki/Check_constraint) says
	// that if the constraint refers to a single column only, it is possible to
	// specify the constraint as part of the column definition. Postgres allows
	// specifying them anywhere about any columns, but it moves all constraints to
	// the table level (i.e., columns never have a check constraint themselves). We
	// will adhere to the stricter definition.

	var nameBuf bytes.Buffer
	name := string(d.Name)

	generateName := name == ""
	if generateName {
		nameBuf.WriteString("check")
	}

	preFn := func(expr parser.Expr) (err error, recurse bool, newExpr parser.Expr) {
		vBase, ok := expr.(parser.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return nil, true, expr
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return err, false, nil
		}

		c, ok := v.(*parser.ColumnItem)
		if !ok {
			return nil, true, expr
		}

		col, err := desc.FindActiveColumnByName(c.ColumnName)
		if err != nil {
			return fmt.Errorf("column %q not found for constraint %q",
				c.ColumnName, d.Expr.String()), false, nil
		}
		if generateName {
			nameBuf.WriteByte('_')
			nameBuf.WriteString(col.Name)
		}
		// Convert to a dummy node of the correct type.
		return nil, false, dummyColumnItem{col.Type.ToDatumType()}
	}

	expr, err := parser.SimpleVisit(d.Expr, preFn)
	if err != nil {
		return nil, err
	}

	var p parser.Parser
	if err := p.AssertNoAggregationOrWindowing(expr, "CHECK expressions", searchPath); err != nil {
		return nil, err
	}

	if _, err := sqlbase.SanitizeVarFreeExpr(expr, parser.TypeBool, "CHECK", searchPath); err != nil {
		return nil, err
	}
	if generateName {
		name = nameBuf.String()

		// If generated name isn't unique, attempt to add a number to the end to
		// get a unique name.
		if _, ok := inuseNames[name]; ok {
			i := 1
			for {
				appended := fmt.Sprintf("%s%d", name, i)
				if _, ok := inuseNames[appended]; !ok {
					name = appended
					break
				}
				i++
			}
		}
		if inuseNames != nil {
			inuseNames[name] = struct{}{}
		}
	}
	return &sqlbase.TableDescriptor_CheckConstraint{Expr: parser.Serialize(d.Expr), Name: name}, nil
}

// resolveViewDependencies looks up the tables included in a view's query
// and adds metadata representing those dependencies to both the new view's
// descriptor and the dependend-upon tables' descriptors. The modified table
// descriptors are put into the backrefs map of other tables so that they can
// be updated when this view is created.
func (n *createViewNode) resolveViewDependencies(
	ctx context.Context,
	tbl *sqlbase.TableDescriptor,
	backrefs map[sqlbase.ID]*sqlbase.TableDescriptor,
) {
	n.p.populateViewBackrefs(ctx, n.sourcePlan, tbl, backrefs)

	// Also create the forward references in the new view's descriptor.
	tbl.DependsOn = make([]sqlbase.ID, 0, len(backrefs))
	for _, backref := range backrefs {
		tbl.DependsOn = append(tbl.DependsOn, backref.ID)
	}
}

// populateViewBackrefs adds back-references to the descriptor for each referenced
// table / view in the plan.
func (p *planner) populateViewBackrefs(
	ctx context.Context,
	plan planNode,
	tbl *sqlbase.TableDescriptor,
	backrefs map[sqlbase.ID]*sqlbase.TableDescriptor,
) {
	b := &backrefCollector{p: p, tbl: tbl, backrefs: backrefs}
	_ = walkPlan(ctx, plan, planObserver{enterNode: b.enterNode})
}

type backrefCollector struct {
	p        *planner
	tbl      *sqlbase.TableDescriptor
	backrefs map[sqlbase.ID]*sqlbase.TableDescriptor
}

// enterNode is used by a planObserver.
func (b *backrefCollector) enterNode(ctx context.Context, _ string, plan planNode) bool {
	// I was initially concerned about doing type assertions on every node in
	// the tree, but it's actually faster than a string comparison on the name
	// returned by ExplainPlan, judging by a mini-benchmark run on my laptop
	// with go 1.7.1.
	if sel, ok := plan.(*renderNode); ok {
		// If this is a view, we don't want to resolve the underlying scan(s).
		// We instead prefer to track the dependency on the view itself rather
		// than on its indirect dependencies.
		if sel.source.info.viewDesc != nil {
			populateViewBackrefFromViewDesc(sel.source.info.viewDesc, b.tbl, b.backrefs)
			// Return early to avoid processing the view's underlying query.
			return false
		}
	} else if join, ok := plan.(*joinNode); ok {
		if join.left.info.viewDesc != nil {
			populateViewBackrefFromViewDesc(join.left.info.viewDesc, b.tbl, b.backrefs)
		} else {
			b.p.populateViewBackrefs(ctx, join.left.plan, b.tbl, b.backrefs)
		}
		if join.right.info.viewDesc != nil {
			populateViewBackrefFromViewDesc(join.right.info.viewDesc, b.tbl, b.backrefs)
		} else {
			b.p.populateViewBackrefs(ctx, join.right.plan, b.tbl, b.backrefs)
		}
		// Return early to avoid re-processing the children.
		return false
	} else if scan, ok := plan.(*scanNode); ok {
		desc, ok := b.backrefs[scan.desc.ID]
		if !ok {
			desc = &scan.desc
			b.backrefs[desc.ID] = desc
		}
		ref := sqlbase.TableDescriptor_Reference{
			ID:        b.tbl.ID,
			ColumnIDs: make([]sqlbase.ColumnID, 0, len(scan.cols)),
		}
		if scan.index != nil && scan.isSecondaryIndex {
			ref.IndexID = scan.index.ID
		}
		for i := range scan.cols {
			// Only include the columns that are actually needed.
			if scan.valNeededForCol[i] {
				ref.ColumnIDs = append(ref.ColumnIDs, scan.cols[i].ID)
			}
		}
		desc.DependedOnBy = append(desc.DependedOnBy, ref)
	}
	return true
}

func populateViewBackrefFromViewDesc(
	dependency *sqlbase.TableDescriptor,
	tbl *sqlbase.TableDescriptor,
	backrefs map[sqlbase.ID]*sqlbase.TableDescriptor,
) {
	desc, ok := backrefs[dependency.ID]
	if !ok {
		desc = dependency
		backrefs[desc.ID] = desc
	}
	ref := sqlbase.TableDescriptor_Reference{ID: tbl.ID}
	desc.DependedOnBy = append(desc.DependedOnBy, ref)
}

// planContainsStar returns true if one of the render nodes in the
// plan contains a star expansion.
func (p *planner) planContainsStar(ctx context.Context, plan planNode) bool {
	s := &starDetector{}
	_ = walkPlan(ctx, plan, planObserver{enterNode: s.enterNode})
	return s.foundStar
}

// starDetector supports planContainsStar().
type starDetector struct {
	foundStar bool
}

// enterNode implements the planObserver interface.
func (s *starDetector) enterNode(_ context.Context, _ string, plan planNode) bool {
	if s.foundStar {
		return false
	}
	if sel, ok := plan.(*renderNode); ok {
		if sel.isStar {
			s.foundStar = true
			return false
		}
	}
	return true
}
