// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestTTLSessionOverrides exercises the foreign-key closure walk directly and
// asserts on the exact override map it produces, covering shapes that are
// awkward to prove end-to-end:
//
//   - a diamond where a child is first reached by a scan-only (NO ACTION) edge
//     and later by a CASCADE edge, which must still traverse the child so its
//     own subtree gets grants;
//   - a self-referencing foreign key (walk termination on cycles);
//   - an update-received (SET DEFAULT) child whose outbound foreign key and
//     sequence-backed default require grants on descriptors outside the
//     inbound closure;
//   - a second-hop update cascade: a SET NULL child that itself has an inbound
//     ON UPDATE foreign key, so the update chains one hop further to a
//     grandchild that must receive the UPDATE-tier grant;
//   - the SELECT-only tier for RESTRICT children.
//
// Asserting map equality also locks down the grants' upper bound: the walk
// must not hand out privileges beyond what the referential actions need.
func TestTTLSessionOverrides(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	runner := sqlutils.MakeSQLRunner(conn)
	runner.Exec(t, `CREATE USER tbl_owner`)
	runner.Exec(t, `CREATE SEQUENCE seq`)
	runner.Exec(t, `CREATE TABLE root_tbl (
		id INT PRIMARY KEY,
		parent_id INT REFERENCES root_tbl (id) ON DELETE CASCADE
	)`)
	runner.Exec(t, `ALTER TABLE root_tbl OWNER TO tbl_owner`)
	runner.Exec(t, `CREATE TABLE other_parent (id INT PRIMARY KEY)`)
	runner.Exec(t, `CREATE TABLE mid (
		id INT PRIMARY KEY,
		pid INT REFERENCES root_tbl (id) ON DELETE CASCADE
	)`)
	// diamond_child is reachable from root_tbl twice: directly via a NO ACTION
	// edge, and via mid through CASCADE edges.
	runner.Exec(t, `CREATE TABLE diamond_child (
		id INT PRIMARY KEY,
		pid_root INT REFERENCES root_tbl (id),
		pid_mid INT REFERENCES mid (id) ON DELETE CASCADE
	)`)
	runner.Exec(t, `CREATE TABLE grandchild (
		id INT PRIMARY KEY,
		pid INT REFERENCES diamond_child (id) ON DELETE CASCADE
	)`)
	// setdefault_child is updated by the cascade: rewriting pid evaluates the
	// nextval() default and re-checks the second foreign key on pid against
	// other_parent.
	runner.Exec(t, `CREATE TABLE setdefault_child (
		id INT PRIMARY KEY,
		pid INT DEFAULT nextval('seq') REFERENCES root_tbl (id) ON DELETE SET DEFAULT,
		FOREIGN KEY (pid) REFERENCES other_parent (id)
	)`)
	runner.Exec(t, `CREATE TABLE restrict_child (
		id INT PRIMARY KEY,
		pid INT REFERENCES root_tbl (id) ON DELETE RESTRICT
	)`)
	// setnull_child is update-received: ON DELETE SET NULL rewrites its pid when a
	// root_tbl row is deleted. It in turn has an inbound foreign key whose ON
	// UPDATE action cascades, so the update chains one hop further to
	// update_grandchild. This is the second-hop case setdefault_child does not
	// reach; it exercises the fkUpdate branch that follows fk.OnUpdate()
	// conservatively, without checking which columns the update actually touched.
	runner.Exec(t, `CREATE TABLE setnull_child (
		id INT PRIMARY KEY,
		pid INT REFERENCES root_tbl (id) ON DELETE SET NULL
	)`)
	runner.Exec(t, `CREATE TABLE update_grandchild (
		id INT PRIMARY KEY,
		pid INT REFERENCES setnull_child (id) ON UPDATE CASCADE
	)`)

	// simple_tbl has no inbound foreign keys and references no functions, so it
	// takes the node-user fast path.
	runner.Exec(t, `CREATE TABLE simple_tbl (id INT PRIMARY KEY, v INT)`)
	runner.Exec(t, `ALTER TABLE simple_tbl OWNER TO tbl_owner`)
	// udf_tbl also has no inbound foreign keys, but a computed column referencing
	// a UDF is user-defined code, so it must run as the owner rather than node.
	runner.Exec(t, `CREATE FUNCTION plus_one(i INT) RETURNS INT IMMUTABLE LANGUAGE SQL AS 'SELECT i + 1'`)
	runner.Exec(t, `CREATE TABLE udf_tbl (
		id INT PRIMARY KEY,
		c INT AS (plus_one(id)) STORED
	)`)
	runner.Exec(t, `ALTER TABLE udf_tbl OWNER TO tbl_owner`)

	tableID := func(name string) descpb.ID {
		var id int64
		runner.QueryRow(t, `SELECT $1::REGCLASS::OID`, name).Scan(&id)
		return descpb.ID(id)
	}
	rootID := tableID("root_tbl")

	// withDesc runs fn against the named table within a descriptor transaction.
	withDesc := func(name string, fn func(lookupTable tableLookupFn, desc catalog.TableDescriptor)) {
		db := s.InternalDB().(*sql.InternalDB)
		require.NoError(t, db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			lookupTable := makeTableLookup(txn)
			desc, err := lookupTable(ctx, tableID(name))
			require.NoError(t, err)
			fn(lookupTable, desc)
			return nil
		}))
	}

	t.Run("fast path for table with no FKs or functions", func(t *testing.T) {
		withDesc("simple_tbl", func(lookupTable tableLookupFn, desc catalog.TableDescriptor) {
			user, overrides, err := ttlSessionOverrides(ctx, lookupTable, desc, true /* runAsOwner */)
			require.NoError(t, err)
			require.True(t, user.IsNodeUser())
			require.Nil(t, overrides)
		})
	})

	t.Run("table with a UDF does not take the fast path", func(t *testing.T) {
		withDesc("udf_tbl", func(lookupTable tableLookupFn, desc catalog.TableDescriptor) {
			user, overrides, err := ttlSessionOverrides(ctx, lookupTable, desc, true /* runAsOwner */)
			require.NoError(t, err)
			require.Equal(t, "tbl_owner", user.Normalized())
			require.NotNil(t, overrides)
		})
	})

	t.Run("setting disabled falls back to node with no overrides", func(t *testing.T) {
		// Even the complex root table runs as node when the setting is off.
		withDesc("root_tbl", func(lookupTable tableLookupFn, desc catalog.TableDescriptor) {
			user, overrides, err := ttlSessionOverrides(ctx, lookupTable, desc, false /* runAsOwner */)
			require.NoError(t, err)
			require.True(t, user.IsNodeUser())
			require.Nil(t, overrides)
		})
	})

	readWrite := func(write privilege.Kind) sessiondata.DescriptorOverride {
		return sessiondata.DescriptorOverride{
			Privileges: privilege.List{privilege.SELECT, write}.ToBitField(),
			BypassRLS:  true,
		}
	}
	readOnly := sessiondata.DescriptorOverride{
		Privileges: privilege.List{privilege.SELECT}.ToBitField(),
		BypassRLS:  true,
	}

	t.Run("full foreign-key closure", func(t *testing.T) {
		withDesc("root_tbl", func(lookupTable tableLookupFn, root catalog.TableDescriptor) {
			owner, overrides, err := ttlSessionOverrides(ctx, lookupTable, root, true /* runAsOwner */)
			require.NoError(t, err)
			require.Equal(t, "tbl_owner", owner.Normalized())

			expected := map[uint32]sessiondata.DescriptorOverride{
				// Deleted tables: the TTL table itself and the CASCADE chain through
				// the diamond, including the grandchild below the twice-reached
				// diamond_child.
				uint32(rootID):                   readWrite(privilege.DELETE),
				uint32(tableID("mid")):           readWrite(privilege.DELETE),
				uint32(tableID("diamond_child")): readWrite(privilege.DELETE),
				uint32(tableID("grandchild")):    readWrite(privilege.DELETE),
				// Updated tables: SET DEFAULT rewrites setdefault_child's rows; SET
				// NULL rewrites setnull_child's, whose own ON UPDATE inbound foreign
				// key chains the update on to update_grandchild.
				uint32(tableID("setdefault_child")):  readWrite(privilege.UPDATE),
				uint32(tableID("setnull_child")):     readWrite(privilege.UPDATE),
				uint32(tableID("update_grandchild")): readWrite(privilege.UPDATE),
				// Scan-only grants: the RESTRICT child's existence check, and the
				// outbound reference re-checked when setdefault_child is rewritten.
				uint32(tableID("restrict_child")): readOnly,
				uint32(tableID("other_parent")):   readOnly,
				// The rewritten column's default evaluates nextval('seq').
				uint32(tableID("seq")): {
					Privileges: privilege.List{privilege.USAGE}.ToBitField(),
				},
				// Name resolution on the containing database and schema.
				uint32(root.GetParentID()): {
					Privileges: privilege.List{privilege.CONNECT}.ToBitField(),
				},
				uint32(root.GetParentSchemaID()): {
					Privileges: privilege.List{privilege.USAGE}.ToBitField(),
				},
			}
			require.Equal(t, expected, overrides)
		})
	})

	t.Run("update cascade chains through SET NULL to a grandchild", func(t *testing.T) {
		// Deleting a root_tbl row sets setnull_child.pid to NULL (an update), and
		// setnull_child's inbound ON UPDATE CASCADE foreign key propagates that
		// update to update_grandchild. The grandchild must receive the UPDATE-tier
		// grant even though it is reached only through the fkUpdate -> fk.OnUpdate()
		// path, never through a delete cascade. The full-closure case above already
		// pins the whole map; this isolates the second-hop update grant on its own.
		withDesc("root_tbl", func(lookupTable tableLookupFn, root catalog.TableDescriptor) {
			_, overrides, err := ttlSessionOverrides(ctx, lookupTable, root, true /* runAsOwner */)
			require.NoError(t, err)
			require.Equal(t,
				readWrite(privilege.UPDATE),
				overrides[uint32(tableID("update_grandchild"))],
			)
		})
	})
}
