// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package workload

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// ValidateInitialData asserts that a cluster contains the given tables and that
// they pass some sampling-based sanity checks. The table data is required to be
// in its initial state, with no subsequent modifications.
func ValidateInitialData(ctx context.Context, db *gosql.DB, tables []Table) error {
	const samples = 100
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	for _, table := range tables {
		if err := validateInitialDataTable(ctx, db, table, rng, samples); err != nil {
			return err
		}
	}
	return nil
}

func validateInitialDataTable(
	ctx context.Context, db *gosql.DB, table Table, rng *rand.Rand, samples int,
) error {
	if table.InitialRowFn == nil {
		// Some workloads don't support initial table data, so nothing to
		// validate.
		return nil
	}

	var rows int
	query := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, table.Name)
	if err := db.QueryRow(query).Scan(&rows); err != nil {
		return errors.Wrapf(err, `counting rows in %s [%s]`, table.Name, query)
	}
	if table.InitialRowCount != rows {
		return errors.Errorf(`expected %d rows for table %s, got: %d`,
			table.InitialRowCount, table.Name, rows)
	}

	if samples > table.InitialRowCount {
		samples = table.InitialRowCount
	}
	if samples < 1 {
		samples = 1
	}

	spacing := 1.0 / float64(samples)
	for i := 0; i < samples; i++ {
		offset := (float64(i) + rng.Float64()) * spacing
		rowIdx := int(offset * float64(samples))
		if err := validateInitialDataRow(db, table, rowIdx); err != nil {
			return err
		}
	}
	return nil
}

func validateInitialDataRow(db *gosql.DB, table Table, rowIdx int) error {
	actual := make([]interface{}, len(table.InitialRowFn(rowIdx)))
	for i := range actual {
		actual[i] = new(interface{})
	}
	query := fmt.Sprintf(`SELECT * FROM "%s" ORDER BY PRIMARY KEY "%s" OFFSET %d LIMIT 1`,
		table.Name, table.Name, rowIdx)
	if err := db.QueryRow(query).Scan(actual...); err != nil {
		return errors.Wrapf(err, `fetching table %s row %d [%s]`, table.Name, rowIdx, query)
	}
	// TODO(dan): This currently validates that the row has the correct number
	// of columns. Make it validate the actual datum values once TPCC is made
	// deterministic (and so will pass).
	return nil
}
