package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestPgSettingsUnitColumn verifies if the unit column in pg_settings table
// displays the correct unit according to the specified setting
func TestPgSettingsUnitColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)

	tests := []struct {
		name     string
		setting  string
		expected string
	}{
		{
			name:     "Verify that time related lines report the respective unit",
			setting:  "statement_timeout",
			expected: "ms", // At the moment all time related lines are using milisseconds
		},
		{
			name:     "Verify that size related lines report the respective unit",
			setting:  "join_reader_ordering_strategy_batch_size",
			expected: "KiB", // Humanized Bytes function returns this unit (for this setting default value)
		},
		{
			name:     "Verify lines with no unit report NULL",
			setting:  "application_name",
			expected: "", // NULL
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var unit string
			query := `SELECT unit FROM pg_settings WHERE name = $1`

			err := db.QueryRow(query, test.setting).Scan(&unit)

			if test.expected == "" {
				// Expect NULL (no result)
				require.Error(t, err)
			} else {
				require.NoError(t, err, "Should be able to retrieve unit for setting %s", test.setting)
				require.Equal(t, test.expected, unit, "Unit for setting %s does not match expected value", test.setting)
			}
		})
	}
}
