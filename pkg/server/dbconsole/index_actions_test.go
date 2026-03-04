// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestDropSchemaIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var capturedStmt string
		exec := &mockExecutor{
			execExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				o sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) (int, error) {
				capturedStmt = stmt
				return 1, nil
			},
		}
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: exec},
		}

		body, _ := json.Marshal(DropSchemaIndexRequest{
			Database: "mydb",
			Schema:   "public",
			Table:    "users",
			Index:    "idx_name",
		})

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/insights/schema/drop-index",
			httptest.NewRequest("POST", "/", bytes.NewReader(body)),
		)
		w := httptest.NewRecorder()
		api.DropSchemaIndex(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(
			t, `DROP INDEX public.users@idx_name`, capturedStmt,
		)
	})

	t.Run("sql execution failure", func(t *testing.T) {
		exec := &mockExecutor{
			execExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				o sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) (int, error) {
				return 0, errors.New("index does not exist")
			},
		}
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: exec},
		}

		body, _ := json.Marshal(DropSchemaIndexRequest{
			Database: "mydb",
			Schema:   "public",
			Table:    "users",
			Index:    "idx_bad",
		})

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/insights/schema/drop-index",
			httptest.NewRequest("POST", "/", bytes.NewReader(body)),
		)
		w := httptest.NewRecorder()
		api.DropSchemaIndex(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
		require.Contains(t, w.Body.String(), "index does not exist")
	})

	t.Run("missing fields", func(t *testing.T) {
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: &mockExecutor{}},
		}

		body, _ := json.Marshal(DropSchemaIndexRequest{
			Database: "mydb",
			Schema:   "public",
			// Table and Index missing.
		})

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/insights/schema/drop-index",
			httptest.NewRequest("POST", "/", bytes.NewReader(body)),
		)
		w := httptest.NewRecorder()
		api.DropSchemaIndex(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Contains(t, w.Body.String(), "required")
	})

	t.Run("wrong method", func(t *testing.T) {
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: &mockExecutor{}},
		}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/insights/schema/drop-index", nil,
		)
		w := httptest.NewRecorder()
		api.DropSchemaIndex(w, req)

		require.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})

	t.Run("invalid body", func(t *testing.T) {
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: &mockExecutor{}},
		}

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/insights/schema/drop-index",
			httptest.NewRequest(
				"POST", "/", bytes.NewReader([]byte("not json")),
			),
		)
		w := httptest.NewRecorder()
		api.DropSchemaIndex(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("reserved word quoting", func(t *testing.T) {
		var capturedStmt string
		exec := &mockExecutor{
			execExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				o sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) (int, error) {
				capturedStmt = stmt
				return 1, nil
			},
		}
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: exec},
		}

		body, _ := json.Marshal(DropSchemaIndexRequest{
			Database: "mydb",
			Schema:   "public",
			Table:    "select",
			Index:    "primary",
		})

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/insights/schema/drop-index",
			httptest.NewRequest("POST", "/", bytes.NewReader(body)),
		)
		w := httptest.NewRecorder()
		api.DropSchemaIndex(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		// Reserved words should be quoted by the AST formatter.
		require.Contains(t, capturedStmt, `"select"`)
	})
}

func TestCreateSchemaIndex(t *testing.T) {
	const recommendation = "CREATE INDEX ON users (name)"

	t.Run("success", func(t *testing.T) {
		var capturedExecStmt string
		exec := &mockExecutor{
			queryRowExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				session sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) (tree.Datums, error) {
				return tree.Datums{
					dStringArray(recommendation),
				}, nil
			},
			execExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				o sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) (int, error) {
				capturedExecStmt = stmt
				return 1, nil
			},
		}
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: exec},
		}

		body, _ := json.Marshal(CreateSchemaIndexRequest{
			FingerprintID:  "abc123",
			Recommendation: recommendation,
			Database:       "mydb",
		})

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/insights/schema/create-index",
			httptest.NewRequest("POST", "/", bytes.NewReader(body)),
		)
		w := httptest.NewRecorder()
		api.CreateSchemaIndex(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, recommendation, capturedExecStmt)
	})

	t.Run("fingerprint not found", func(t *testing.T) {
		exec := &mockExecutor{
			queryRowExFn: makeStaticQueryRowEx(nil, nil),
		}
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: exec},
		}

		body, _ := json.Marshal(CreateSchemaIndexRequest{
			FingerprintID:  "notfound",
			Recommendation: recommendation,
			Database:       "mydb",
		})

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/insights/schema/create-index",
			httptest.NewRequest("POST", "/", bytes.NewReader(body)),
		)
		w := httptest.NewRecorder()
		api.CreateSchemaIndex(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Contains(t, w.Body.String(), "fingerprint not found")
	})

	t.Run("recommendation not in array", func(t *testing.T) {
		exec := &mockExecutor{
			queryRowExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				session sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) (tree.Datums, error) {
				return tree.Datums{
					dStringArray("CREATE INDEX ON other_table (col)"),
				}, nil
			},
		}
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: exec},
		}

		body, _ := json.Marshal(CreateSchemaIndexRequest{
			FingerprintID:  "abc123",
			Recommendation: recommendation,
			Database:       "mydb",
		})

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/insights/schema/create-index",
			httptest.NewRequest("POST", "/", bytes.NewReader(body)),
		)
		w := httptest.NewRecorder()
		api.CreateSchemaIndex(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Contains(
			t, w.Body.String(), "recommendation not found",
		)
	})

	t.Run("non-CREATE INDEX recommendation rejected", func(t *testing.T) {
		dropRec := "DROP INDEX users@idx_name"
		exec := &mockExecutor{
			queryRowExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				session sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) (tree.Datums, error) {
				return tree.Datums{
					dStringArray(dropRec),
				}, nil
			},
		}
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: exec},
		}

		body, _ := json.Marshal(CreateSchemaIndexRequest{
			FingerprintID:  "abc123",
			Recommendation: dropRec,
			Database:       "mydb",
		})

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/insights/schema/create-index",
			httptest.NewRequest("POST", "/", bytes.NewReader(body)),
		)
		w := httptest.NewRecorder()
		api.CreateSchemaIndex(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Contains(
			t, w.Body.String(), "not a CREATE INDEX statement",
		)
	})

	t.Run("execution failure", func(t *testing.T) {
		exec := &mockExecutor{
			queryRowExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				session sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) (tree.Datums, error) {
				return tree.Datums{
					dStringArray(recommendation),
				}, nil
			},
			execExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				o sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) (int, error) {
				return 0, errors.New("permission denied")
			},
		}
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: exec},
		}

		body, _ := json.Marshal(CreateSchemaIndexRequest{
			FingerprintID:  "abc123",
			Recommendation: recommendation,
			Database:       "mydb",
		})

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/insights/schema/create-index",
			httptest.NewRequest("POST", "/", bytes.NewReader(body)),
		)
		w := httptest.NewRecorder()
		api.CreateSchemaIndex(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
		require.Contains(t, w.Body.String(), "permission denied")
	})

	t.Run("missing fields", func(t *testing.T) {
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: &mockExecutor{}},
		}

		body, _ := json.Marshal(CreateSchemaIndexRequest{
			FingerprintID: "abc123",
			// Recommendation and Database missing.
		})

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/insights/schema/create-index",
			httptest.NewRequest("POST", "/", bytes.NewReader(body)),
		)
		w := httptest.NewRecorder()
		api.CreateSchemaIndex(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Contains(t, w.Body.String(), "required")
	})

	t.Run("wrong method", func(t *testing.T) {
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: &mockExecutor{}},
		}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/insights/schema/create-index", nil,
		)
		w := httptest.NewRecorder()
		api.CreateSchemaIndex(w, req)

		require.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})

	t.Run("null recommendations", func(t *testing.T) {
		exec := &mockExecutor{
			queryRowExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				session sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) (tree.Datums, error) {
				return tree.Datums{tree.DNull}, nil
			},
		}
		api := &ApiV2DBConsole{
			InternalDB: &mockInternalDB{executor: exec},
		}

		body, _ := json.Marshal(CreateSchemaIndexRequest{
			FingerprintID:  "abc123",
			Recommendation: recommendation,
			Database:       "mydb",
		})

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/insights/schema/create-index",
			httptest.NewRequest("POST", "/", bytes.NewReader(body)),
		)
		w := httptest.NewRecorder()
		api.CreateSchemaIndex(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Contains(
			t, w.Body.String(), "no index recommendations found",
		)
	})
}
