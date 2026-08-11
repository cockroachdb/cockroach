// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachanger_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestDropTriggerSweepsOrphanBackref verifies the defensive cleanup added to
// UpdateTriggerBackReferencesInRelations (executed on DROP TRIGGER).
//
// Before v26.2, trigger back-references carried no TriggerID and DROP TRIGGER
// used a whole-table removal gate, so dropping one of several triggers that
// shared a target could leave an untagged (TriggerID==0) back-ref behind. Such
// an orphan is invisible to validation until the last live trigger to the target
// is dropped, at which point DROP TRIGGER fails. The fix sweeps these orphans
// during the drop, but only when it is provably safe: the source no longer has
// a live trigger referencing the target, and the target is not a sequence.
func TestDropTriggerSweepsOrphanBackref(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(s.SQLConn(t))
	sqlDB.Exec(t, `SET use_declarative_schema_changer = 'on'`)
	descDB := s.InternalDB().(descs.DB)

	tableID := func(name string) descpb.ID {
		var id int
		sqlDB.QueryRow(t, `SELECT $1::REGCLASS::OID::INT`, name).Scan(&id)
		return descpb.ID(id)
	}

	// funcID returns a user-defined function's descriptor ID. A UDF's pg_proc OID
	// is offset from its descriptor ID, so read the raw descriptor ID (the value
	// that appears in back-references) from crdb_internal.
	funcID := func(name string) descpb.ID {
		var id int
		sqlDB.QueryRow(t,
			`SELECT function_id FROM crdb_internal.create_function_statements WHERE function_name = $1`,
			name).Scan(&id)
		return descpb.ID(id)
	}

	// dependedOnBy reads the target relation's DependedOnBy slice fresh from KV.
	dependedOnBy := func(id descpb.ID) []descpb.TableDescriptor_Reference {
		var refs []descpb.TableDescriptor_Reference
		require.NoError(t, descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			tbl, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, id)
			if err != nil {
				return err
			}
			refs = append(refs, tbl.TableDesc().DependedOnBy...)
			return nil
		}))
		return refs
	}

	// injectOrphan appends a leaked pre-v26.2 trigger orphan back-ref (a
	// TriggerID==0 entry from sourceID) to the target relation, mimicking the
	// state a customer's descriptor would be in after the pre-v26.2 leak.
	injectOrphan := func(targetID, sourceID descpb.ID, colIDs ...descpb.ColumnID) {
		require.NoError(t, descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			tbl, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, targetID)
			if err != nil {
				return err
			}
			tbl.DependedOnBy = append(tbl.DependedOnBy, descpb.TableDescriptor_Reference{
				ID:        sourceID,
				ColumnIDs: colIDs,
				TriggerID: 0,
			})
			return txn.Descriptors().WriteDesc(ctx, false /* kvTrace */, tbl, txn.KV())
		}))
	}

	countRefsFrom := func(refs []descpb.TableDescriptor_Reference, sourceID descpb.ID) int {
		n := 0
		for _, r := range refs {
			if r.ID == sourceID {
				n++
			}
		}
		return n
	}

	t.Run("orphan swept when last trigger is dropped", func(t *testing.T) {
		// target is referenced by a live trigger on src, a view, and a UDF. The
		// view and UDF produce legitimate TriggerID==0 back-refs from their own
		// descriptors; only the injected orphan (from src) should be swept.
		sqlDB.Exec(t, `CREATE TABLE target1 (id INT PRIMARY KEY, data TEXT)`)
		sqlDB.Exec(t, `CREATE TABLE src1 (id INT PRIMARY KEY)`)
		sqlDB.Exec(t, `
			CREATE FUNCTION trig_fn1() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$
			BEGIN
				SELECT id FROM target1 LIMIT 1;
				RETURN NEW;
			END;
			$$`)
		sqlDB.Exec(t, `CREATE TRIGGER tr AFTER INSERT ON src1 FOR EACH ROW EXECUTE FUNCTION trig_fn1()`)
		sqlDB.Exec(t, `CREATE VIEW v1 AS SELECT id FROM target1`)
		sqlDB.Exec(t, `CREATE FUNCTION reads_target1() RETURNS INT LANGUAGE SQL AS $$ SELECT id FROM target1 LIMIT 1 $$`)

		target1, src1 := tableID("target1"), tableID("src1")
		viewID, fnID := tableID("v1"), funcID("reads_target1")

		// Sanity: before injection the view and UDF each hold a legitimate
		// TriggerID==0 back-ref on target1.
		before := dependedOnBy(target1)
		require.Equal(t, 1, countRefsFrom(before, src1), "expected a trigger back-ref on target1")
		require.Equal(t, 1, countRefsFrom(before, viewID), "expected a view back-ref on target1")
		require.Equal(t, 1, countRefsFrom(before, fnID), "expected a UDF back-ref on target1")

		injectOrphan(target1, src1)
		require.Equal(t, 2, countRefsFrom(dependedOnBy(target1), src1), "expected an orphan back-ref on target1")

		// Dropping the sole live trigger must succeed and leave the view/UDF back-refs
		// intact while removing every back-ref sourced from src1.
		sqlDB.Exec(t, `DROP TRIGGER tr ON src1`)

		after := dependedOnBy(target1)
		require.Zero(t, countRefsFrom(after, src1), "orphan and trigger back-refs from src1 should be gone")
		require.Equal(t, 1, countRefsFrom(after, viewID), "view back-ref must survive")
		require.Equal(t, 1, countRefsFrom(after, fnID), "UDF back-ref must survive")
	})

	t.Run("orphan preserved while a sibling trigger remains", func(t *testing.T) {
		// With two live triggers referencing the target, dropping one must not
		// sweep the orphan: the source still has a live trigger to the target, so
		// the orphan is still latent (and cleanup must wait for the last drop).
		sqlDB.Exec(t, `CREATE TABLE target2 (id INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE src2 (id INT PRIMARY KEY)`)
		sqlDB.Exec(t, `
			CREATE FUNCTION trig_fn2() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$
			BEGIN
				SELECT id FROM target2 LIMIT 1;
				RETURN NEW;
			END;
			$$`)
		sqlDB.Exec(t, `CREATE TRIGGER tr1 AFTER INSERT ON src2 FOR EACH ROW EXECUTE FUNCTION trig_fn2()`)
		sqlDB.Exec(t, `CREATE TRIGGER tr2 AFTER UPDATE ON src2 FOR EACH ROW EXECUTE FUNCTION trig_fn2()`)

		target2, src2 := tableID("target2"), tableID("src2")
		injectOrphan(target2, src2)

		// Two tagged trigger back-refs + one injected orphan = 3 refs from src2.
		require.Equal(t, 3, countRefsFrom(dependedOnBy(target2), src2))

		sqlDB.Exec(t, `DROP TRIGGER tr1 ON src2`)

		// tr1's tagged back-ref is removed; tr2's back-ref and the orphan remain.
		after := dependedOnBy(target2)
		require.Equal(t, 2, countRefsFrom(after, src2),
			"dropping a sibling must remove only tr1's back-ref, leaving tr2 and the orphan")
		orphanRemains := false
		for _, r := range after {
			if r.ID == src2 && r.TriggerID == 0 {
				orphanRemains = true
			}
		}
		require.True(t, orphanRemains, "orphan must survive while a sibling trigger references the target")
	})

	t.Run("sequence column-default back-ref is not swept", func(t *testing.T) {
		// A sequence back-ref from a column default also carries TriggerID==0.
		// When a trigger that references the same sequence is dropped, the guard
		// against sequences must prevent the default back-ref from being swept.
		sqlDB.Exec(t, `CREATE SEQUENCE seq3`)
		sqlDB.Exec(t, `CREATE TABLE src3 (id INT PRIMARY KEY, n INT DEFAULT nextval('seq3'))`)
		sqlDB.Exec(t, `
			CREATE FUNCTION trig_fn3() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$
			BEGIN
				SELECT nextval('seq3');
				RETURN NEW;
			END;
			$$`)
		sqlDB.Exec(t, `CREATE TRIGGER tr AFTER INSERT ON src3 FOR EACH ROW EXECUTE FUNCTION trig_fn3()`)

		seq3, src3 := tableID("seq3"), tableID("src3")

		// The sequence holds a column-default back-ref (TriggerID==0, ByID) and a
		// tagged trigger back-ref, both sourced from src3.
		hasDefaultBackref := func(refs []descpb.TableDescriptor_Reference) bool {
			for _, r := range refs {
				if r.ID == src3 && r.TriggerID == 0 {
					return true
				}
			}
			return false
		}
		before := dependedOnBy(seq3)
		require.Equal(t, 2, len(before), "sequence should have 2 back-refs before the drop")
		require.True(t, hasDefaultBackref(before),
			"sequence should have a column-default back-ref before the drop")

		sqlDB.Exec(t, `DROP TRIGGER tr ON src3`)

		// The trigger's own back-ref is gone, but the column-default back-ref
		// (which validation still requires, since src3.n defaults to nextval) is
		// preserved.
		after := dependedOnBy(seq3)
		require.Equal(t, 1, len(after), "sequence should have 1 back-ref after the drop")
		require.True(t, hasDefaultBackref(after),
			"the sequence column-default back-ref must be preserved by the IsSequence guard")
	})

	t.Run("orphan on a view target is swept", func(t *testing.T) {
		// A trigger whose function reads a view produces a table->view back-ref on
		// the view, sourced from the trigger's table. A base table's own schema
		// never references a view (columns, defaults, checks, and FKs cannot), so
		// only its triggers can -- which makes a TriggerID==0 back-ref from the
		// trigger's table to a view unambiguously a trigger orphan and safe to
		// sweep.
		sqlDB.Exec(t, `CREATE TABLE base4 (id INT PRIMARY KEY, data TEXT)`)
		sqlDB.Exec(t, `CREATE VIEW target_v4 AS SELECT id FROM base4`)
		sqlDB.Exec(t, `CREATE TABLE src4 (id INT PRIMARY KEY)`)
		sqlDB.Exec(t, `
			CREATE FUNCTION trig_fn4() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$
			BEGIN
				SELECT id FROM target_v4 LIMIT 1;
				RETURN NEW;
			END;
			$$`)
		sqlDB.Exec(t, `CREATE TRIGGER tr AFTER INSERT ON src4 FOR EACH ROW EXECUTE FUNCTION trig_fn4()`)

		targetV4, src4 := tableID("target_v4"), tableID("src4")

		// Before injection the view holds a single tagged trigger back-ref from src4.
		require.Equal(t, 1, countRefsFrom(dependedOnBy(targetV4), src4),
			"expected a trigger back-ref on the view")

		injectOrphan(targetV4, src4)
		require.Equal(t, 2, countRefsFrom(dependedOnBy(targetV4), src4),
			"expected an orphan back-ref on the view")

		// Dropping the sole live trigger must succeed and remove every back-ref
		// sourced from src4, including the orphan.
		sqlDB.Exec(t, `DROP TRIGGER tr ON src4`)

		require.Zero(t, countRefsFrom(dependedOnBy(targetV4), src4),
			"orphan and trigger back-refs from src4 should be gone from the view")
	})
}
