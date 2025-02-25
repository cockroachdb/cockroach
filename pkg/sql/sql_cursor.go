// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// DeclareCursor implements the DECLARE statement.
// See https://www.postgresql.org/docs/current/sql-declare.html for details.
func (p *planner) DeclareCursor(ctx context.Context, s *tree.DeclareCursor) (planNode, error) {
	if s.Binary {
		return nil, unimplemented.NewWithIssue(77099, "DECLARE BINARY CURSOR")
	}
	if s.Scroll == tree.Scroll {
		return nil, unimplemented.NewWithIssue(77102, "DECLARE SCROLL CURSOR")
	}

	return &delayedNode{
		name: s.String(),
		constructor: func(ctx context.Context, p *planner) (_ planNode, _ error) {
			if p.extendedEvalCtx.TxnImplicit && !s.Hold {
				return nil, pgerror.Newf(pgcode.NoActiveSQLTransaction, "DECLARE CURSOR can only be used in transaction blocks")
			}

			sd := p.SessionData()
			// This session variable was introduced as a workaround to #96322.
			// Today, if a timeout is set, FETCH's timeout is from the point
			// DECLARE CURSOR is executed rather than the FETCH itself.
			// The setting allows us to override the setting without affecting
			// third-party applications.
			if !p.SessionData().DeclareCursorStatementTimeoutEnabled {
				sd = sd.Clone()
				sd.StmtTimeout = 0
			}
			// We avoid using the internal executor provided by p.InternalSQLTxn()
			// since we want to customize the session data used by the cursor.
			ief := p.ExecCfg().InternalDB
			ie := MakeInternalExecutor(ief.server, ief.memMetrics, ief.monitor)
			ie.SetSessionData(sd)
			ie.extraTxnState = &extraTxnState{
				txn:                p.Txn(),
				descCollection:     p.Descriptors(),
				jobs:               p.extendedEvalCtx.jobs,
				schemaChangerState: p.extendedEvalCtx.SchemaChangerState,
			}
			if err := p.checkIfCursorExists(s.Name); err != nil {
				return nil, err
			}

			// Try to plan the cursor query to make sure that it's valid.
			stmt := makeStatement(statements.Statement[tree.Statement]{AST: s.Select}, clusterunique.ID{},
				tree.FmtFlags(queryFormattingForFingerprintsMask.Get(&p.execCfg.Settings.SV)))
			pt := planTop{}
			pt.init(&stmt, &p.instrumentation)
			opc := &p.optPlanningCtx
			opc.p.stmt = stmt
			opc.reset(ctx)

			memo, err := opc.buildExecMemo(ctx)
			if err != nil {
				return nil, err
			}
			if err := opc.runExecBuilder(
				ctx,
				&pt,
				&stmt,
				newExecFactory(ctx, p),
				memo,
				p.SemaCtx(),
				p.EvalContext(),
				p.autoCommit,
				false, /* disableTelemetryAndPlanGists */
			); err != nil {
				return nil, err
			}
			if s.Hold && pt.flags.IsSet(planFlagContainsLocking) {
				return nil, errors.WithDetail(
					pgerror.Newf(pgcode.FeatureNotSupported,
						"DECLARE CURSOR WITH HOLD must not contain locking"),
					"Holdable cursors must be READ ONLY.",
				)
			}
			if pt.flags.IsSet(planFlagContainsMutation) {
				// Cursors with mutations are invalid.
				return nil, pgerror.Newf(pgcode.FeatureNotSupported,
					"DECLARE CURSOR must not contain data-modifying statements in WITH")
			}

			statement := formatWithPlaceholders(ctx, s.Select, p.EvalContext())
			itCtx := context.Background()
			rows, err := ie.QueryIterator(itCtx, "sql-cursor", p.txn, statement)
			if err != nil {
				return nil, errors.Wrap(err, "failed to DECLARE CURSOR")
			}
			cursor := &sqlCursor{
				Rows:       rows,
				readSeqNum: p.txn.GetReadSeqNum(),
				txn:        p.txn,
				statement:  statement,
				created:    timeutil.Now(),
				withHold:   s.Hold,
			}
			if err := p.sqlCursors.addCursor(s.Name, cursor); err != nil {
				// This case shouldn't happen because cursor names are scoped to a session,
				// and sessions can't have more than one statement running at once. But
				// let's be diligent and clean up if it somehow does happen anyway.
				_ = cursor.Close()
				return nil, err
			}
			return newZeroNode(nil /* columns */), nil
		},
	}, nil
}

// checkIfCursorExists checks whether a cursor or portal with the given name
// already exists, and returns an error if one does.
func (p *planner) checkIfCursorExists(name tree.Name) error {
	if cursor := p.sqlCursors.getCursor(name); cursor != nil {
		return pgerror.Newf(pgcode.DuplicateCursor, "cursor %q already exists", name)
	}
	if p.extendedEvalCtx.PreparedStatementState.HasPortal(string(name)) {
		return pgerror.Newf(pgcode.DuplicateCursor, "cursor %q already exists as portal", name)
	}
	return nil
}

var errBackwardScan = pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "cursor can only scan forward")

// FetchCursor implements the FETCH and MOVE statements.
// See https://www.postgresql.org/docs/current/sql-fetch.html for details.
func (p *planner) FetchCursor(_ context.Context, s *tree.CursorStmt) (planNode, error) {
	return p.newFetchNode(s)
}

// newFetchNode creates a new fetchNode, which implements FETCH and MOVE
// statements.
func (p *planner) newFetchNode(s *tree.CursorStmt) (*fetchNode, error) {
	cursor := p.sqlCursors.getCursor(s.Name)
	if cursor == nil {
		return nil, pgerror.Newf(
			pgcode.InvalidCursorName, "cursor %q does not exist", s.Name,
		)
	}
	if s.Count < 0 || s.FetchType == tree.FetchBackwardAll {
		return nil, errBackwardScan
	}
	node := &fetchNode{
		n:         s.Count,
		fetchType: s.FetchType,
		cursor:    cursor,
	}
	if s.FetchType != tree.FetchNormal {
		node.n = 0
		node.offset = s.Count
	}
	return node, nil
}

type fetchNode struct {
	zeroInputPlanNode
	cursor *sqlCursor
	// n is the number of rows requested.
	n int64
	// offset is the number of rows to read first, when in relative or absolute
	// mode.
	offset    int64
	fetchType tree.FetchType

	seeked bool

	// origTxnSeqNum is the transaction sequence number of the user's transaction
	// before the fetch began.
	origTxnSeqNum enginepb.TxnSeq
}

func (f *fetchNode) startInternal() error {
	if !f.cursor.persisted {
		// We need to make sure that we're reading at the same read sequence number
		// that we had when we created the cursor, to preserve the "sensitivity"
		// semantics of cursors, which demand that data written after the cursor
		// was declared is not visible to the cursor.
		f.origTxnSeqNum = f.cursor.txn.GetReadSeqNum()
		return f.cursor.txn.SetReadSeqNum(f.cursor.readSeqNum)
	}
	// If persisted is set, the cursor has already been fully read into a row
	// container, so there is no need to set the read sequence number.
	return nil
}

func (f *fetchNode) nextInternal(ctx context.Context) (bool, error) {
	if f.fetchType == tree.FetchAll {
		return f.cursor.Next(ctx)
	}

	if !f.seeked {
		// FIRST, LAST, ABSOLUTE, and RELATIVE require seeking before returning
		// values. Do that first.
		f.seeked = true
		switch f.fetchType {
		case tree.FetchFirst:
			switch f.cursor.curRow {
			case 0:
				_, err := f.cursor.Next(ctx)
				return true, err
			case 1:
				return true, nil
			}
			return false, errBackwardScan
		case tree.FetchLast:
			return false, errBackwardScan
		case tree.FetchAbsolute:
			if f.cursor.curRow > f.offset {
				return false, errBackwardScan
			}
			if f.offset == 0 {
				// ABSOLUTE 0 is positioned before the first row.
				return false, nil
			}
			for f.cursor.curRow < f.offset {
				more, err := f.cursor.Next(ctx)
				if !more || err != nil {
					return more, err
				}
			}
			return true, nil
		case tree.FetchRelative:
			for i := int64(0); i < f.offset; i++ {
				more, err := f.cursor.Next(ctx)
				if !more || err != nil {
					return more, err
				}
			}
			return true, nil
		}
	}
	if f.n <= 0 {
		return false, nil
	}
	f.n--
	return f.cursor.Next(ctx)
}

func (f *fetchNode) startExec(params runParams) error {
	return f.startInternal()
}

func (f *fetchNode) Next(params runParams) (bool, error) {
	return f.nextInternal(params.ctx)
}

func (f fetchNode) Values() tree.Datums {
	return f.cursor.Cur()
}

func (f fetchNode) Close(ctx context.Context) {
	// We explicitly do not pass through the Close to our Rows, because
	// running FETCH on a CURSOR does not close it.
	if !f.cursor.persisted {
		// Reset the transaction's read sequence number to what it was before the
		// fetch began, so that subsequent reads in the transaction can still see
		// writes from that transaction.
		if err := f.cursor.txn.SetReadSeqNum(f.origTxnSeqNum); err != nil {
			log.Warningf(ctx, "error resetting transaction read seq num after CURSOR operation: %v", err)
		}
	}
}

// CloseCursor implements the FETCH statement.
// See https://www.postgresql.org/docs/current/sql-close.html for details.
func (p *planner) CloseCursor(ctx context.Context, n *tree.CloseCursor) (planNode, error) {
	return &delayedNode{
		name: n.String(),
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			if n.All {
				return newZeroNode(nil /* columns */), p.sqlCursors.closeAll(cursorCloseForExplicitClose)
			}
			return newZeroNode(nil /* columns */), p.sqlCursors.closeCursor(n.Name)
		},
	}, nil
}

// GenUniqueCursorName implements the eval.Planner interface.
func (p *planner) GenUniqueCursorName() tree.Name {
	return p.sqlCursors.genUniqueName()
}

// PLpgSQLCloseCursor implements the eval.Planner interface.
func (p *planner) PLpgSQLCloseCursor(cursorName tree.Name) error {
	return p.sqlCursors.closeCursor(cursorName)
}

// PLpgSQLFetchCursor returns the next row from the cursor with the given name
// after seeking past the specified number of rows. It is used to implement the
// PLpgSQL FETCH and MOVE statements. When there are no rows left to return,
// PLpgSQLFetchCursor returns nil.
//
// Note: when called with the FORWARD ALL option, PLpgSQLFetchCursor will only
// return the last row. This is compatible with PLpgSQL, as FORWARD ALL can only
// be used with PLpgSQL MOVE, which ignores all returned rows.
func (p *planner) PLpgSQLFetchCursor(
	ctx context.Context, cursorStmt *tree.CursorStmt,
) (res tree.Datums, err error) {
	cursor, err := p.newFetchNode(cursorStmt)
	if err != nil {
		return nil, err
	}
	if err = cursor.startInternal(); err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var hasNext bool
	hasNext, err = cursor.nextInternal(ctx)
	for err == nil && hasNext {
		res = cursor.Values()
		hasNext, err = cursor.nextInternal(ctx)
	}
	return res, err
}

type sqlCursor struct {
	isql.Rows
	// txn is the transaction object that the internal executor for this cursor
	// is running with.
	txn *kv.Txn
	// readSeqNum is the sequence number of the transaction that the cursor was
	// initialized with.
	readSeqNum enginepb.TxnSeq
	statement  string
	created    time.Time
	curRow     int64
	withHold   bool
	// persisted indicates that the cursor's query was executed to completion and
	// the result stored in a row container. If true, there is no need to set the
	// transaction sequence number, since the query is no longer active.
	// In addition, the cursor need not be closed when its parent transaction
	// closes.
	persisted bool
	// committed is set when the transaction that created the cursor has
	// successfully committed. It is only used for cursors declared using
	// WITH HOLD. It is used to ensure that aborting a transaction only closes
	// cursors that were opened by that transaction.
	committed bool
}

// Next implements the Rows interface.
func (s *sqlCursor) Next(ctx context.Context) (bool, error) {
	more, err := s.Rows.Next(ctx)
	if err == nil {
		s.curRow++
	}
	return more, err
}

// sqlCursors contains a set of active cursors for a session.
type sqlCursors interface {
	// closeAll closes cursors in the set according to the following rules:
	// * Explicit CLOSE ALL or session end: close all cursors.
	// * txnCommit: close non-holdable cursors from the current txn and persist
	//   holdable cursors from the current txn.
	// * txnRollback: close all cursors from the current txn.
	// * txnPrepare: close all cursors from the current txn and return an error
	//   if there were any holdable cursors from the current txn.
	closeAll(reason cursorCloseReason) error
	// closeCursor closes the named cursor, returning an error if that cursor
	// didn't exist in the set.
	closeCursor(tree.Name) error
	// getCursor returns the named cursor, returning nil if that cursor
	// didn't exist in the set.
	getCursor(tree.Name) *sqlCursor
	// addCursor adds a new cursor with the given name to the set, returning an
	// error if the cursor already existed in the set.
	addCursor(tree.Name, *sqlCursor) error
	// list returns all open cursors in the set.
	list() map[tree.Name]*sqlCursor
	// genUniqueName is used to generate a name for an unnamed PLpgSQL cursor that
	// will not conflict with other cursors currently defined on the session.
	genUniqueName() tree.Name
}

// emptySqlCursors is the default impl used by the planner when the
// connExecutor is not available.
type emptySqlCursors struct{}

var _ sqlCursors = emptySqlCursors{}

func (e emptySqlCursors) closeAll(cursorCloseReason) error {
	return errors.AssertionFailedf("closeAll not supported in emptySqlCursors")
}

func (e emptySqlCursors) closeCursor(tree.Name) error {
	return errors.AssertionFailedf("closeCursor not supported in emptySqlCursors")
}

func (e emptySqlCursors) getCursor(tree.Name) *sqlCursor {
	return nil
}

func (e emptySqlCursors) addCursor(tree.Name, *sqlCursor) error {
	return errors.AssertionFailedf("addCursor not supported in emptySqlCursors")
}

func (e emptySqlCursors) list() map[tree.Name]*sqlCursor {
	return nil
}

func (e emptySqlCursors) genUniqueName() tree.Name {
	return ""
}

// cursorMap is a sqlCursors that's backed by an actual map.
type cursorMap struct {
	cursors map[tree.Name]*sqlCursor
	// nameCounter is used to help generate unique names for unnamed PLpgSQL
	// cursors.
	nameCounter int
}

type cursorCloseReason uint8

const (
	cursorCloseForTxnCommit cursorCloseReason = iota
	cursorCloseForTxnRollback
	cursorCloseForTxnPrepare
	cursorCloseForExplicitClose
)

func (c *cursorMap) closeAll(p *planner, reason cursorCloseReason) error {
	for n, curs := range c.cursors {
		switch reason {
		case cursorCloseForTxnCommit:
			if curs.withHold {
				// Cursors declared using WITH HOLD are not closed at transaction
				// commit, and become the responsibility of the session.
				curs.committed = true
				if !curs.persisted {
					// Execute the cursor's query to completion and persist the result so
					// that it can survive the transaction's commit.
					if err := persistCursor(p, curs); err != nil {
						return err
					}
				}
				continue
			}
		case cursorCloseForTxnRollback:
			if curs.committed {
				// Transaction rollback should only remove cursors that were created in
				// the current transaction.
				continue
			}
		case cursorCloseForTxnPrepare:
			if curs.withHold && !curs.committed {
				// Disallow preparing a transaction that has created a cursor WITH HOLD.
				// It's fine for previous transactions to have created holdable cursors.
				// NOTE: Postgres also disallows this.
				//
				// Make sure to close all cursors before returning the error.
				err := c.closeAll(p, cursorCloseForTxnRollback)
				return errors.CombineErrors(
					pgerror.New(pgcode.FeatureNotSupported,
						"cannot PREPARE a transaction that has created a cursor WITH HOLD"),
					err,
				)
			}
		}
		if err := curs.Close(); err != nil {
			return err
		}
		delete(c.cursors, n)
	}
	if reason == cursorCloseForExplicitClose {
		// All cursors are closed for explicit close, so we can lose the reference
		// to the map.
		c.cursors = nil
	}
	return nil
}

func (c *cursorMap) closeCursor(s tree.Name) error {
	cursor, ok := c.cursors[s]
	if !ok {
		return pgerror.Newf(pgcode.InvalidCursorName, "cursor %q does not exist", s)
	}
	err := cursor.Close()
	delete(c.cursors, s)
	return err
}

func (c *cursorMap) getCursor(s tree.Name) *sqlCursor {
	return c.cursors[s]
}

func (c *cursorMap) addCursor(s tree.Name, cursor *sqlCursor) error {
	if c.cursors == nil {
		c.cursors = make(map[tree.Name]*sqlCursor)
	}
	if _, ok := c.cursors[s]; ok {
		return pgerror.Newf(pgcode.DuplicateCursor, "cursor %q already exists", s)
	}
	c.cursors[s] = cursor
	return nil
}

func (c *cursorMap) list() map[tree.Name]*sqlCursor {
	return c.cursors
}

func (c *cursorMap) genUniqueName() tree.Name {
	for {
		c.nameCounter++
		name := tree.Name(fmt.Sprintf("<unnamed portal %d>", c.nameCounter))
		if _, ok := c.cursors[name]; !ok {
			// This name is unique.
			return name
		}
	}
}

// connExCursorAccessor is a sqlCursors that delegates to a connExecutor's
// extraTxnState.
type connExCursorAccessor struct {
	ex *connExecutor
}

func (c connExCursorAccessor) closeAll(reason cursorCloseReason) error {
	return c.ex.extraTxnState.sqlCursors.closeAll(&c.ex.planner, reason)
}

func (c connExCursorAccessor) closeCursor(s tree.Name) error {
	return c.ex.extraTxnState.sqlCursors.closeCursor(s)
}

func (c connExCursorAccessor) getCursor(s tree.Name) *sqlCursor {
	return c.ex.extraTxnState.sqlCursors.getCursor(s)
}

func (c connExCursorAccessor) addCursor(s tree.Name, cursor *sqlCursor) error {
	return c.ex.extraTxnState.sqlCursors.addCursor(s, cursor)
}

func (c connExCursorAccessor) list() map[tree.Name]*sqlCursor {
	return c.ex.extraTxnState.sqlCursors.list()
}

func (c connExCursorAccessor) genUniqueName() tree.Name {
	return c.ex.extraTxnState.sqlCursors.genUniqueName()
}

// checkNoConflictingCursors returns an error if the input schema changing
// statement conflicts with any open SQL cursors in the current planner.
func (p *planner) checkNoConflictingCursors(stmt tree.Statement) error {
	// TODO(jordan): this is stricter than Postgres is. Postgres permits
	// concurrent schema changes within a transaction where a cursor is already
	// open if the open cursors do not depend on schema objects being changed.
	// We could improve this by matching the memo metadata's list of dependent
	// schema objects in each open cursor with the objects being changed in the
	// schema change.
	if len(p.sqlCursors.list()) > 0 {
		return unimplemented.NewWithIssue(74608, "cannot run schema change "+
			"in a transaction with open DECLARE cursors")
	}
	return nil
}

// persistCursor runs the given cursor to completion and stores the result in a
// row container that can outlive the cursor's transaction.
func persistCursor(p *planner, cursor *sqlCursor) error {
	// Use context.Background() because the cursor can outlive the context in
	// which it was created.
	helper := persistedCursorHelper{
		ctx:          context.Background(),
		resultCols:   cursor.Types(),
		rowsAffected: cursor.RowsAffected(),
	}
	mon := p.sessionMonitor
	if mon == nil {
		return errors.AssertionFailedf("cannot persist cursor without an active session")
	}
	helper.container.InitWithParentMon(
		helper.ctx,
		getTypesFromResultColumns(helper.resultCols),
		mon,
		p.ExtendedEvalContextCopy(),
		"persisted_cursor", /* opName */
	)
	for {
		ok, err := cursor.Next(helper.ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if err = helper.container.AddRow(helper.ctx, cursor.Cur()); err != nil {
			return err
		}
	}
	helper.iter = newRowContainerIterator(helper.ctx, helper.container)
	if err := cursor.Rows.Close(); err != nil {
		return err
	}
	cursor.Rows = &helper
	return nil
}

// persistedCursorHelper wraps a row container in order to feed the results of
// executing a SQL statement to a SQL cursor. Note that the SQL statement is not
// lazily executed; its entire result is written to the container.
type persistedCursorHelper struct {
	ctx context.Context

	// Fields related to implementing the isql.Rows interface.
	container    rowContainerHelper
	iter         *rowContainerIterator
	resultCols   colinfo.ResultColumns
	lastRow      tree.Datums
	rowsAffected int
}

var _ isql.Rows = &persistedCursorHelper{}

// Next implements the isql.Rows interface.
func (h *persistedCursorHelper) Next(_ context.Context) (bool, error) {
	row, err := h.iter.Next()
	if err != nil || row == nil {
		return false, err
	}
	// Shallow-copy the row to ensure that it is safe to hold on to after Next()
	// and Close() calls - see the isql.Rows interface.
	h.lastRow = make(tree.Datums, len(row))
	copy(h.lastRow, row)
	h.rowsAffected++
	return true, nil
}

// Cur implements the isql.Rows interface.
func (h *persistedCursorHelper) Cur() tree.Datums {
	return h.lastRow
}

// RowsAffected implements the isql.Rows interface.
func (h *persistedCursorHelper) RowsAffected() int {
	return h.rowsAffected
}

// Close implements the isql.Rows interface.
func (h *persistedCursorHelper) Close() error {
	if h.iter != nil {
		h.iter.Close()
		h.iter = nil
	}
	h.container.Close(h.ctx)
	return nil
}

// Types implements the isql.Rows interface.
func (h *persistedCursorHelper) Types() colinfo.ResultColumns {
	return h.resultCols
}

// HasResults implements the isql.Rows interface.
func (h *persistedCursorHelper) HasResults() bool {
	return h.lastRow != nil
}
