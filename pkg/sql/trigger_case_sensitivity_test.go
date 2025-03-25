package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestTriggerCaseSensitivity verifies whether the trigger fails when the column case does not match.
func TestTriggerCaseSensitivity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(db)

	tests := []struct {
		name       string
		tableSQL   string
		shouldFail bool
	}{
		{
			name: "unquoted column name (Postgres treats as lowercase 'last_modified')",
			tableSQL: `
				CREATE TABLE employee (
					emp_id INT NOT NULL,
					first_name STRING NOT NULL,
					last_name STRING NOT NULL,
					hire_date DATE NOT NULL,
					email STRING,
					resume BYTES,
					last_modified TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
				)`,
			shouldFail: true, // Trigger should fail because NEW.last_modified doesn't exist.
		},
		{
			name: "quoted column name (Preserves exact case 'LAST_MODIFIED')",
			tableSQL: `
				CREATE TABLE employee (
					emp_id INT NOT NULL,
					first_name STRING NOT NULL,
					last_name STRING NOT NULL,
					hire_date DATE NOT NULL,
					email STRING,
					resume BYTES,
					"LAST_MODIFIED" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
				)`,
			shouldFail: false, // Trigger should succeed.
		},
	}

	triggerSQL := `
		CREATE OR REPLACE FUNCTION set_last_modified() 
		RETURNS TRIGGER AS $$
		BEGIN
			IF TG_OP IN ('INSERT', 'UPDATE') AND SESSION_USER NOT IN ('SYS', 'ADMIN') THEN
				NEW.last_modified := CURRENT_TIMESTAMP;
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER emp_last_mod_trg
			BEFORE INSERT OR UPDATE ON employee
			FOR EACH ROW 
			EXECUTE PROCEDURE set_last_modified();
	`

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tdb.Exec(t, `DROP TABLE IF EXISTS employee CASCADE`)
			tdb.Exec(t, test.tableSQL)

			_, err := db.Exec(triggerSQL)
			if test.shouldFail {
				require.Error(t, err, "Expected trigger creation to fail")
			} else {
				require.NoError(t, err, "Trigger creation should succeed")
			}
		})
	}
}
