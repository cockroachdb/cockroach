// Fix for Issue #149103
// sql: disallow PL/pgSQL txn control statements in a simple protocol query batch

```json
{
  "root_cause": "CockroachDB improperly handles PL/pgSQL transaction control statements within multi-statement queries executed via the simple protocol. This behavior deviates from PostgreSQL, which restricts such transaction controls to ensure transaction integrity and state consistency.",
  "fix_strategy": "Enhance the SQL execution engine to detect and block transaction control statements (e.g., COMMIT, ROLLBACK) within multi-statement queries when executing via the simple protocol. This entails parsing the query batch to identify these statements and returning an error if any are found.",
  "code_changes": [
    {
      "file": "pkg/sql/conn_executor_exec.go",
      "description": "Implement transaction control statement detection and blocking in multi-statement queries",
      "before": "func (ex *connExecutor) execStmtInOpenState(ctx context.Context, stmt Statement) (fsm.Event, fsm.EventPayload)",
      "after": "func (ex *connExecutor) execStmtInOpenState(ctx context.Context, stmt Statement) (fsm.Event, fsm.EventPayload) {\n\n    // Detect transaction control statements in simple protocol\n    if ex.sessionData.Protocol == pgwirebase.ProtocolSimple && stmt.AST != nil {\n        switch stmt.AST.(type) {\n        case *tree.CommitTransaction, *tree.RollbackTransaction:\n            ex.state.mu.Lock()\n            defer ex.state.mu.Unlock()\n            return ex.makeErrEvent(errors.New(\"invalid transaction termination\"), stmt.AST)\n        }\n    }\n\n    return ex.doExecStmtInOpenState(ctx, stmt)\n}",
      "line_number": 234
    }
  ],
  "test_files": [
    {
      "path": "pkg/sql/conn_executor_exec_test.go",
      "content": "package sql\n\nimport (\n    \"context\"\n    \"testing\"\n\n    \"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase\"\n    \"github.com/cockroachdb/cockroach/pkg/sql/sem/tree\"\n)\n\nfunc TestBlockTxnControlInSimpleProtocol(t *testing.T) {\n    tests := []struct {\n        query string\n        proto pgwirebase.Protocol\n        expectErr bool\n    }{\n        {\"BEGIN; COMMIT;\", pgwirebase.ProtocolSimple, true},\n        {\"BEGIN; ROLLBACK;\", pgwirebase.ProtocolSimple, true},\n        {\"SELECT 1; COMMIT;\", pgwirebase.ProtocolSimple, true},\n        {\"SELECT 1;\", pgwirebase.ProtocolSimple, false},\n    }\n\n    for _, tt := range tests {\n        ctx := context.Background()\n        ex, _, _, _ := createTestConnExecutor(ctx, tt.proto)\n\n        stmts, _ := ex.prepStmtsForBatch(ctx, tt.query)\n        for _, stmt := range stmts {\n            event, _ := ex.execStmtInOpenState(ctx, stmt)\n            if (event == fsm.ErrEvent{}) != tt.expectErr {\n                t.Errorf(\"%q. Expected error: %v, got %v\", tt.query, tt.expectErr, event != fsm.ErrEvent{})\n            }\n        }\n    }\n}",
      "description": "Test to ensure transaction control statements in simple protocol queries are blocked"
    }
  ],
  "test_cases": [
    {
      "name": "TestBlockTxnControlInSimpleProtocol",
      "description": "Ensure transaction control statements are blocked in simple protocol mode",
      "code": "BEGIN; COMMIT;"
    },
    {
      "name": "TestAllowOtherStatementsInSimpleProtocol",
      "description": "Ensure non-transaction control statements are allowed in simple protocol mode",
      "code": "SELECT 1;"
    }
  ]
}
```

// This fix was generated automatically by the CockroachDB Issue Fixer
