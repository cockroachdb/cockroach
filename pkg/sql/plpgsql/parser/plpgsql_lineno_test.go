// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser_test

import (
	"testing"

	plpgsql "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/stretchr/testify/require"
)

// TestParseLineNumbers verifies that the parser populates correct line numbers
// on PLpgSQL statement AST nodes.
func TestParseLineNumbers(t *testing.T) {
	// Test basic block with assignment and return.
	t.Run("basic_block", func(t *testing.T) {
		input := `
		DECLARE
		  x INT;
		BEGIN
		  x := 1 + 2;
		  RETURN x;
		END`
		stmt, err := plpgsql.Parse(input)
		require.NoError(t, err)

		require.Len(t, stmt.AST.Body, 2)

		assign, ok := stmt.AST.Body[0].(*plpgsqltree.Assignment)
		require.True(t, ok, "expected Assignment, got %T", stmt.AST.Body[0])
		require.Equal(t, 5, assign.GetLineNo(), "assignment should be on line 5")

		ret, ok := stmt.AST.Body[1].(*plpgsqltree.Return)
		require.True(t, ok, "expected Return, got %T", stmt.AST.Body[1])
		require.Equal(t, 6, ret.GetLineNo(), "return should be on line 6")
	})

	// Test IF statement.
	t.Run("if_statement", func(t *testing.T) {
		input := `
		DECLARE
		  x INT := 0;
		BEGIN
		  IF x > 0 THEN
		    x := 1;
		  ELSE
		    x := 2;
		  END IF;
		  RETURN x;
		END`
		stmt, err := plpgsql.Parse(input)
		require.NoError(t, err)

		require.Len(t, stmt.AST.Body, 2)

		ifStmt, ok := stmt.AST.Body[0].(*plpgsqltree.If)
		require.True(t, ok, "expected If, got %T", stmt.AST.Body[0])
		require.Equal(t, 5, ifStmt.GetLineNo(), "IF should be on line 5")

		// Verify assignments inside IF branches have correct line numbers.
		require.Len(t, ifStmt.ThenBody, 1)
		thenAssign, ok := ifStmt.ThenBody[0].(*plpgsqltree.Assignment)
		require.True(t, ok)
		require.Equal(t, 6, thenAssign.GetLineNo(), "then-branch assignment should be on line 6")

		require.Len(t, ifStmt.ElseBody, 1)
		elseAssign, ok := ifStmt.ElseBody[0].(*plpgsqltree.Assignment)
		require.True(t, ok)
		require.Equal(t, 8, elseAssign.GetLineNo(), "else-branch assignment should be on line 8")
	})

	// Test WHILE loop.
	t.Run("while_loop", func(t *testing.T) {
		input := `
		DECLARE
		  i INT := 0;
		BEGIN
		  WHILE i < 10 LOOP
		    i := i + 1;
		  END LOOP;
		  RETURN i;
		END`
		stmt, err := plpgsql.Parse(input)
		require.NoError(t, err)

		require.Len(t, stmt.AST.Body, 2)

		whileStmt, ok := stmt.AST.Body[0].(*plpgsqltree.While)
		require.True(t, ok, "expected While, got %T", stmt.AST.Body[0])
		require.Equal(t, 5, whileStmt.GetLineNo(), "WHILE should be on line 5")
	})

	// Test RAISE statement.
	t.Run("raise_statement", func(t *testing.T) {
		input := `
		BEGIN
		  RAISE NOTICE 'hello';
		  RAISE EXCEPTION 'boom';
		END`
		stmt, err := plpgsql.Parse(input)
		require.NoError(t, err)

		require.Len(t, stmt.AST.Body, 2)

		raise1, ok := stmt.AST.Body[0].(*plpgsqltree.Raise)
		require.True(t, ok, "expected Raise, got %T", stmt.AST.Body[0])
		require.Equal(t, 3, raise1.GetLineNo(), "first RAISE should be on line 3")

		raise2, ok := stmt.AST.Body[1].(*plpgsqltree.Raise)
		require.True(t, ok, "expected Raise, got %T", stmt.AST.Body[1])
		require.Equal(t, 4, raise2.GetLineNo(), "second RAISE should be on line 4")
	})

	// Test RAISE inside IF (mimics CREATE FUNCTION body structure).
	t.Run("raise_inside_if", func(t *testing.T) {
		input := `
		BEGIN
		  IF x <= 0 THEN
		    RAISE EXCEPTION 'must be positive';
		  END IF;
		  RETURN x;
		END`
		stmt, err := plpgsql.Parse(input)
		require.NoError(t, err)

		ifStmt, ok := stmt.AST.Body[0].(*plpgsqltree.If)
		require.True(t, ok, "expected If, got %T", stmt.AST.Body[0])
		require.Equal(t, 3, ifStmt.GetLineNo(), "IF should be on line 3")

		raiseStmt, ok := ifStmt.ThenBody[0].(*plpgsqltree.Raise)
		require.True(t, ok, "expected Raise, got %T", ifStmt.ThenBody[0])
		require.Equal(t, 4, raiseStmt.GetLineNo(), "RAISE inside IF should be on line 4")
	})

	// Test with blank lines between statements.
	t.Run("blank_lines", func(t *testing.T) {
		input := `
		BEGIN


		  IF x <= 0 THEN




		    RAISE EXCEPTION 'must be positive';



		  END IF;
		  RETURN x;
		END`
		stmt, err := plpgsql.Parse(input)
		require.NoError(t, err)

		ifStmt, ok := stmt.AST.Body[0].(*plpgsqltree.If)
		require.True(t, ok, "expected If, got %T", stmt.AST.Body[0])
		require.Equal(t, 5, ifStmt.GetLineNo(), "IF should be on line 5 (after 2 blank lines)")

		raiseStmt, ok := ifStmt.ThenBody[0].(*plpgsqltree.Raise)
		require.True(t, ok, "expected Raise, got %T", ifStmt.ThenBody[0])
		require.Equal(t, 10, raiseStmt.GetLineNo(), "RAISE should be on line 10 (after 4 blank lines)")
	})

	// Test nested block.
	t.Run("nested_block", func(t *testing.T) {
		input := `
		DECLARE
		  x INT := 0;
		BEGIN
		  BEGIN
		    x := x + 1;
		  END;
		  RETURN x;
		END`
		stmt, err := plpgsql.Parse(input)
		require.NoError(t, err)

		require.Len(t, stmt.AST.Body, 2)

		nestedBlock, ok := stmt.AST.Body[0].(*plpgsqltree.Block)
		require.True(t, ok, "expected Block, got %T", stmt.AST.Body[0])
		require.Equal(t, 5, nestedBlock.GetLineNo(), "nested block should be on line 5")
	})

	// Test LOOP statement.
	t.Run("loop_statement", func(t *testing.T) {
		input := `
		DECLARE
		  i INT := 0;
		BEGIN
		  LOOP
		    EXIT WHEN i >= 5;
		    i := i + 1;
		  END LOOP;
		  RETURN i;
		END`
		stmt, err := plpgsql.Parse(input)
		require.NoError(t, err)

		require.Len(t, stmt.AST.Body, 2)

		loopStmt, ok := stmt.AST.Body[0].(*plpgsqltree.Loop)
		require.True(t, ok, "expected Loop, got %T", stmt.AST.Body[0])
		require.Equal(t, 5, loopStmt.GetLineNo(), "LOOP should be on line 5")

		require.Len(t, loopStmt.Body, 2)
		exitStmt, ok := loopStmt.Body[0].(*plpgsqltree.Exit)
		require.True(t, ok, "expected Exit, got %T", loopStmt.Body[0])
		require.Equal(t, 6, exitStmt.GetLineNo(), "EXIT should be on line 6")
	})

	// Test NULL statement.
	t.Run("null_statement", func(t *testing.T) {
		input := `
		BEGIN
		  NULL;
		  RETURN 1;
		END`
		stmt, err := plpgsql.Parse(input)
		require.NoError(t, err)

		require.Len(t, stmt.AST.Body, 2)

		nullStmt, ok := stmt.AST.Body[0].(*plpgsqltree.Null)
		require.True(t, ok, "expected Null, got %T", stmt.AST.Body[0])
		require.Equal(t, 3, nullStmt.GetLineNo(), "NULL should be on line 3")
	})
}
