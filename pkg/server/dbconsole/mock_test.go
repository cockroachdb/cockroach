// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"context"
	"net/http"
	"net/http/httptest"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/redact"
)

// mockExecutor implements isql.Executor with configurable function fields
// for the methods used by dbconsole handlers.
type mockExecutor struct {
	queryBufferedExFn func(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		session sessiondata.InternalExecutorOverride,
		stmt string,
		qargs ...interface{},
	) ([]tree.Datums, error)

	queryRowExFn func(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		session sessiondata.InternalExecutorOverride,
		stmt string,
		qargs ...interface{},
	) (tree.Datums, error)

	execExFn func(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		o sessiondata.InternalExecutorOverride,
		stmt string,
		qargs ...interface{},
	) (int, error)
}

func (m *mockExecutor) QueryBufferedEx(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, error) {
	if m.queryBufferedExFn != nil {
		return m.queryBufferedExFn(ctx, opName, txn, session, stmt, qargs...)
	}
	return nil, nil
}

func (m *mockExecutor) QueryRowEx(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	if m.queryRowExFn != nil {
		return m.queryRowExFn(ctx, opName, txn, session, stmt, qargs...)
	}
	return nil, nil
}

func (m *mockExecutor) ExecEx(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	o sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (int, error) {
	if m.execExFn != nil {
		return m.execExFn(ctx, opName, txn, o, stmt, qargs...)
	}
	return 0, nil
}

// Unused Executor interface methods.

func (m *mockExecutor) Exec(
	_ context.Context, _ redact.RedactableString, _ *kv.Txn, _ string, _ ...interface{},
) (int, error) {
	panic("unimplemented")
}

func (m *mockExecutor) ExecParsed(
	_ context.Context,
	_ redact.RedactableString,
	_ *kv.Txn,
	_ sessiondata.InternalExecutorOverride,
	_ statements.Statement[tree.Statement],
	_ ...interface{},
) (int, error) {
	panic("unimplemented")
}

func (m *mockExecutor) QueryRow(
	_ context.Context, _ redact.RedactableString, _ *kv.Txn, _ string, _ ...interface{},
) (tree.Datums, error) {
	panic("unimplemented")
}

func (m *mockExecutor) QueryRowExParsed(
	_ context.Context,
	_ redact.RedactableString,
	_ *kv.Txn,
	_ sessiondata.InternalExecutorOverride,
	_ statements.Statement[tree.Statement],
	_ ...interface{},
) (tree.Datums, error) {
	panic("unimplemented")
}

func (m *mockExecutor) QueryRowExWithCols(
	_ context.Context,
	_ redact.RedactableString,
	_ *kv.Txn,
	_ sessiondata.InternalExecutorOverride,
	_ string,
	_ ...interface{},
) (tree.Datums, colinfo.ResultColumns, error) {
	panic("unimplemented")
}

func (m *mockExecutor) QueryBuffered(
	_ context.Context, _ redact.RedactableString, _ *kv.Txn, _ string, _ ...interface{},
) ([]tree.Datums, error) {
	panic("unimplemented")
}

func (m *mockExecutor) QueryIterator(
	_ context.Context, _ redact.RedactableString, _ *kv.Txn, _ string, _ ...interface{},
) (isql.Rows, error) {
	panic("unimplemented")
}

func (m *mockExecutor) QueryIteratorEx(
	_ context.Context,
	_ redact.RedactableString,
	_ *kv.Txn,
	_ sessiondata.InternalExecutorOverride,
	_ string,
	_ ...interface{},
) (isql.Rows, error) {
	panic("unimplemented")
}

func (m *mockExecutor) QueryBufferedExWithCols(
	_ context.Context,
	_ redact.RedactableString,
	_ *kv.Txn,
	_ sessiondata.InternalExecutorOverride,
	_ string,
	_ ...interface{},
) ([]tree.Datums, colinfo.ResultColumns, error) {
	panic("unimplemented")
}

func (m *mockExecutor) WithSyntheticDescriptors(_ []catalog.Descriptor, _ func() error) error {
	panic("unimplemented")
}

// mockInternalDB implements isql.DB, returning a configurable mock executor.
type mockInternalDB struct {
	executor *mockExecutor
}

func (m *mockInternalDB) Executor(_ ...isql.ExecutorOption) isql.Executor {
	return m.executor
}

func (m *mockInternalDB) KV() *kv.DB {
	panic("unimplemented")
}

func (m *mockInternalDB) Txn(
	_ context.Context, _ func(context.Context, isql.Txn) error, _ ...isql.TxnOption,
) error {
	panic("unimplemented")
}

func (m *mockInternalDB) Session(
	_ context.Context, _ string, _ ...isql.ExecutorOption,
) (isql.Session, error) {
	panic("unimplemented")
}

// newAuthenticatedRequest creates an httptest request with authentication
// context injected for the given user.
func newAuthenticatedRequest(method, url string, body *http.Request) *http.Request {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, url, body.Body)
	} else {
		req = httptest.NewRequest(method, url, nil)
	}
	ctx := authserver.ContextWithHTTPAuthInfo(req.Context(), "testuser", 0)
	return req.WithContext(ctx)
}

// Datum construction helpers to reduce test boilerplate.

func dString(s string) tree.Datum {
	return tree.NewDString(s)
}

func dInt(i int64) tree.Datum {
	return tree.NewDInt(tree.DInt(i))
}

func dBool(b bool) tree.Datum {
	return tree.MakeDBool(tree.DBool(b))
}

func dBytes(b []byte) tree.Datum {
	return tree.NewDBytes(tree.DBytes(b))
}

func dStringArray(vals ...string) tree.Datum {
	arr := tree.NewDArray(types.String)
	for _, v := range vals {
		_ = arr.Append(tree.NewDString(v))
	}
	return arr
}

// makeStaticQueryBufferedEx returns a queryBufferedExFn that always returns
// the given rows and error, ignoring opName.
func makeStaticQueryBufferedEx(
	rows []tree.Datums, err error,
) func(context.Context, redact.RedactableString, *kv.Txn, sessiondata.InternalExecutorOverride, string, ...interface{}) ([]tree.Datums, error) {
	return func(
		_ context.Context,
		_ redact.RedactableString,
		_ *kv.Txn,
		_ sessiondata.InternalExecutorOverride,
		_ string,
		_ ...interface{},
	) ([]tree.Datums, error) {
		return rows, err
	}
}

// makeStaticQueryRowEx returns a queryRowExFn that always returns the given
// row and error.
func makeStaticQueryRowEx(
	row tree.Datums, err error,
) func(context.Context, redact.RedactableString, *kv.Txn, sessiondata.InternalExecutorOverride, string, ...interface{}) (tree.Datums, error) {
	return func(
		_ context.Context,
		_ redact.RedactableString,
		_ *kv.Txn,
		_ sessiondata.InternalExecutorOverride,
		_ string,
		_ ...interface{},
	) (tree.Datums, error) {
		return row, err
	}
}

// makeStaticExecEx returns an execExFn that always returns the given count
// and error.
func makeStaticExecEx(
	count int, err error,
) func(context.Context, redact.RedactableString, *kv.Txn, sessiondata.InternalExecutorOverride, string, ...interface{}) (int, error) {
	return func(
		_ context.Context,
		_ redact.RedactableString,
		_ *kv.Txn,
		_ sessiondata.InternalExecutorOverride,
		_ string,
		_ ...interface{},
	) (int, error) {
		return count, err
	}
}
