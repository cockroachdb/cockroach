package builtins_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// Ensure that we can generate a bunch of rows of all of the relevant data types and then
// can cross product them all and we get different values which align to the uniformity
// expectations.
func TestCrdbInternalKeyEncode(t *testing.T) {

	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	tdb.Exec(t, `CREATE TABLE t (
    i2    INT2,
    i4    INT4,
    i8    INT8,
    f4    FLOAT4,
    f8    FLOAT8,
    s     STRING,
    c     CHAR,
    b     BYTES,
    dc    DECIMAL,
    ival  INTERVAL,
    oid   OID,
    tstz  TIMESTAMPTZ,
    ts    TIMESTAMP,
    da    DATE,
    inet  INET,
    vb    VARBIT,
    sa    STRING[],
    ia    INT[]
);`)
	tab := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "defaultdb", "t")

	// The cross-join explodes if we do more than one non-NULL row.
	const numRows = 1
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numRows; i++ {
		var row []string
		for _, col := range tab.WritableColumns() {
			if col.GetName() == "rowid" {
				continue
			}
			const nullOk = false
			d := randgen.RandDatum(r, col.GetType(), nullOk)
			row = append(row, tree.AsStringWithFlags(d, tree.FmtParsable))
		}
		tdb.Exec(t, fmt.Sprintf("INSERT INTO t VALUES (%s)", strings.Join(row, ", ")))
	}
	// Insert one NULL row for all columns
	tdb.Exec(t, "INSERT INTO t(rowid) VALUES (DEFAULT)")

	// Generate a query to cross join all of the values of the two rows
	// with also the column ordinal between them.
	crossJoinQuery := func() string {
		var b strings.Builder
		b.WriteString("SELECT ")
		for i, col := range tab.WritableColumns() {
			if col.GetName() == "rowid" {
				continue
			}
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%c.%s, %d",
				'a'+rune(i), col.GetName(), i)
		}
		b.WriteString(" FROM ")
		for i := 0; i < len(tab.WritableColumns())-1; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("t ")
			b.WriteRune('a' + rune(i))
		}
		return b.String()
	}

	// Ensure that the encoded version of each row is unique.
	tdb.CheckQueryResults(t, fmt.Sprintf(`
  SELECT c, count(c)
    FROM (
              SELECT k, count(k) AS c
                FROM (
                        SELECT crdb_internal.key_encode(input.*) AS k
                          FROM (%s) AS input
                     )
            GROUP BY k
         )
GROUP BY c;`, crossJoinQuery()), [][]string{
		// This number is somewhat magical but it's the size of the cross join.
		// We expect all rows to be unique.
		{"1", "262144"},
	})
}

// Test that some data types cannot be key encoded.
func TestCrdbInternalKeyEncodeIllegalType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	for _, val := range []string{
		"'{\"a\": 1}'::JSONB",
	} {
		t.Run(val, func(t *testing.T) {
			tdb.ExpectErr(t, ".*illegal argument.*",
				fmt.Sprintf("SELECT crdb_internal.key_encode(%s)", val))
		})
	}
}
