// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package tpcc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/jackc/pgx"
)

// configureZone sets up zone configs for previously created partitions. By default it adds constraints
// in terms of racks, but if the zones flag is passed into tpcc, it will set the constraints based on the
// geographic zones provided.
func configureZone(db *pgx.ConnPool, table, partition string, constraint int, zones []string) {
	var constraints string
	if len(zones) > 0 {
		constraints = fmt.Sprintf("[+zone=%s]", zones[constraint])
	} else {
		constraints = fmt.Sprintf("[+rack=%d]", constraint)
	}

	// We are removing the EXPERIMENTAL keyword in 2.1. For compatibility
	// with 2.0 clusters we still need to try with it if the
	// syntax without EXPERIMENTAL fails.
	// TODO(knz): Remove this in 2.2.
	sql := fmt.Sprintf(`ALTER PARTITION %s OF TABLE %s CONFIGURE ZONE USING constraints = '%s'`,
		partition, table, constraints)
	_, err := db.Exec(sql)
	if err != nil && strings.Contains(err.Error(), "syntax error") {
		sql = fmt.Sprintf(`ALTER PARTITION %s OF TABLE %s EXPERIMENTAL CONFIGURE ZONE 'constraints: %s'`,
			partition, table, constraints)
		_, err = db.Exec(sql)
	}
	if err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", sql, err))
	}
}

func partitionWarehouse(db *pgx.ConnPool, wIDs []int, partitions int, zones []string) {
	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE warehouse PARTITION BY RANGE (w_id) (\n")
	for i := 0; i < partitions; i++ {
		fmt.Fprintf(&buf, "  PARTITION p%d VALUES FROM (%d) to (%d)", i, wIDs[i], wIDs[i+1])
		if i+1 < partitions {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}

	for i := 0; i < partitions; i++ {
		configureZone(db, "warehouse", fmt.Sprintf("p%d", i), i, zones)
	}
}

func partitionDistrict(db *pgx.ConnPool, wIDs []int, partitions int, zones []string) {
	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE district PARTITION BY RANGE (d_w_id) (\n")
	for i := 0; i < partitions; i++ {
		fmt.Fprintf(&buf, "  PARTITION p%d VALUES FROM (%d) to (%d)", i, wIDs[i], wIDs[i+1])
		if i+1 < partitions {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}

	for i := 0; i < partitions; i++ {
		configureZone(db, "district", fmt.Sprintf("p%d", i), i, zones)
	}
}

func partitionNewOrder(db *pgx.ConnPool, wIDs []int, partitions int, zones []string) {
	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE new_order PARTITION BY RANGE (no_w_id) (\n")
	for i := 0; i < partitions; i++ {
		fmt.Fprintf(&buf, "  PARTITION p%d VALUES FROM (%d) to (%d)", i, wIDs[i], wIDs[i+1])
		if i+1 < partitions {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}

	for i := 0; i < partitions; i++ {
		configureZone(db, "new_order", fmt.Sprintf("p%d", i), i, zones)
	}
}

func partitionOrder(db *pgx.ConnPool, wIDs []int, partitions int, zones []string) {
	targets := []string{
		`TABLE "order"`,
		`INDEX "order"@order_idx`,
		`INDEX "order"@order_o_w_id_o_d_id_o_c_id_idx`,
	}

	for j, target := range targets {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "ALTER %s PARTITION BY RANGE (o_w_id) (\n", target)
		for i := 0; i < partitions; i++ {
			fmt.Fprintf(&buf, "  PARTITION p%d_%d VALUES FROM (%d) to (%d)", j, i, wIDs[i], wIDs[i+1])
			if i+1 < partitions {
				buf.WriteString(",")
			}
			buf.WriteString("\n")
		}
		buf.WriteString(")\n")
		if _, err := db.Exec(buf.String()); err != nil {
			panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
		}
	}

	for i := 0; i < partitions; i++ {
		for j := range targets {
			configureZone(db, `"order"`, fmt.Sprintf("p%d_%d", j, i), i, zones)
		}
	}
}

func partitionOrderLine(db *pgx.ConnPool, wIDs []int, partitions int, zones []string) {
	data := []struct {
		target string
		column string
	}{
		{`TABLE order_line`, `ol_w_id`},
		{`INDEX order_line@order_line_fk`, `ol_supply_w_id`},
	}

	for j, d := range data {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "ALTER %s PARTITION BY RANGE (%s) (\n", d.target, d.column)
		for i := 0; i < partitions; i++ {
			fmt.Fprintf(&buf, "  PARTITION p%d_%d VALUES FROM (%d) to (%d)", j, i, wIDs[i], wIDs[i+1])
			if i+1 < partitions {
				buf.WriteString(",")
			}
			buf.WriteString("\n")
		}
		buf.WriteString(")\n")
		if _, err := db.Exec(buf.String()); err != nil {
			panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
		}
	}

	for i := 0; i < partitions; i++ {
		for j := range data {
			configureZone(db, `order_line`, fmt.Sprintf("p%d_%d", j, i), i, zones)
		}
	}
}

func partitionStock(db *pgx.ConnPool, wIDs []int, partitions int, zones []string) {
	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE stock PARTITION BY RANGE (s_w_id) (\n")
	for i := 0; i < partitions; i++ {
		fmt.Fprintf(&buf, "  PARTITION p%d VALUES FROM (%d) to (%d)", i, wIDs[i], wIDs[i+1])
		if i+1 < partitions {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}

	// TODO(peter): remove duplication with partitionItem().
	const nItems = 100000
	iIDs := make([]int, partitions+1)
	for i := 0; i < partitions; i++ {
		iIDs[i] = i * (nItems / partitions)
	}
	iIDs[partitions] = nItems

	buf.Reset()
	buf.WriteString("ALTER INDEX stock@stock_s_i_id_idx PARTITION BY RANGE (s_i_id) (\n")
	for i := 0; i < partitions; i++ {
		fmt.Fprintf(&buf, "  PARTITION p1_%d VALUES FROM (%d) to (%d)", i, iIDs[i], iIDs[i+1])
		if i+1 < partitions {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}

	for i := 0; i < partitions; i++ {
		configureZone(db, "stock", fmt.Sprintf("p%d", i), i, zones)
		configureZone(db, "stock", fmt.Sprintf("p1_%d", i), i, zones)
	}
}

func partitionCustomer(db *pgx.ConnPool, wIDs []int, partitions int, zones []string) {
	targets := []string{
		`TABLE customer`,
		`INDEX customer@customer_idx`,
	}

	for j, target := range targets {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "ALTER %s PARTITION BY RANGE (c_w_id) (\n", target)
		for i := 0; i < partitions; i++ {
			fmt.Fprintf(&buf, "  PARTITION p%d_%d VALUES FROM (%d) to (%d)", j, i, wIDs[i], wIDs[i+1])
			if i+1 < partitions {
				buf.WriteString(",")
			}
			buf.WriteString("\n")
		}
		buf.WriteString(")\n")
		if _, err := db.Exec(buf.String()); err != nil {
			panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
		}
	}

	for i := 0; i < partitions; i++ {
		for j := range targets {
			configureZone(db, `customer`, fmt.Sprintf("p%d_%d", j, i), i, zones)
		}
	}
}

func partitionHistory(db *pgx.ConnPool, wIDs []int, partitions int, zones []string) {
	const maxVal = math.MaxUint64
	temp := make([]byte, 16)
	rowids := make([]uuid.UUID, partitions+1)
	for i := 0; i < partitions; i++ {
		var err error

		// We're splitting the UUID rowid column evenly into N partitions. The
		// column is sorted lexicographically on the bytes of the UUID which means
		// we should put the partitioning values at the front of the UUID.
		binary.BigEndian.PutUint64(temp, uint64(i)*(maxVal/uint64(partitions)))
		rowids[i], err = uuid.FromBytes(temp)
		if err != nil {
			panic(err)
		}
	}

	rowids[partitions], _ = uuid.FromString("ffffffff-ffff-ffff-ffff-ffffffffffff")

	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE history PARTITION BY RANGE (rowid) (\n")
	for i := 0; i < partitions; i++ {
		fmt.Fprintf(&buf, "  PARTITION p%d VALUES FROM ('%s') to ('%s')", i, rowids[i], rowids[i+1])
		if i+1 < partitions {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}

	data := []struct {
		target string
		column string
	}{
		{`INDEX history@history_h_w_id_h_d_id_idx`, `h_w_id`},
		{`INDEX history@history_h_c_w_id_h_c_d_id_h_c_id_idx`, `h_c_w_id`},
	}

	for j, d := range data {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "ALTER %s PARTITION BY RANGE (%s) (\n", d.target, d.column)
		for i := 0; i < partitions; i++ {
			fmt.Fprintf(&buf, "  PARTITION p%d_%d VALUES FROM (%d) to (%d)", j, i, wIDs[i], wIDs[i+1])
			if i+1 < partitions {
				buf.WriteString(",")
			}
			buf.WriteString("\n")
		}
		buf.WriteString(")\n")
		if _, err := db.Exec(buf.String()); err != nil {
			panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
		}
	}

	for i := 0; i < partitions; i++ {
		configureZone(db, `history`, fmt.Sprintf("p%d", i), i, zones)
		for j := range data {
			configureZone(db, `history`, fmt.Sprintf("p%d_%d", j, i), i, zones)
		}
	}
}

func partitionItem(db *pgx.ConnPool, partitions int, zones []string) {
	const nItems = 100000
	iIDs := make([]int, partitions+1)
	for i := 0; i < partitions; i++ {
		iIDs[i] = i * (nItems / partitions)
	}
	iIDs[partitions] = nItems

	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE item PARTITION BY RANGE (i_id) (\n")
	for i := 0; i < partitions; i++ {
		fmt.Fprintf(&buf, "  PARTITION p%d VALUES FROM (%d) to (%d)", i, iIDs[i], iIDs[i+1])
		if i+1 < partitions {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}

	for i := 0; i < partitions; i++ {
		configureZone(db, "item", fmt.Sprintf("p%d", i), i, zones)
	}
}

func partitionTables(db *pgx.ConnPool, warehouses, partitions int, zones []string) {
	wIDs := make([]int, partitions+1)
	for i := 0; i < partitions; i++ {
		wIDs[i] = i * (warehouses / partitions)
	}
	wIDs[partitions] = warehouses

	partitionWarehouse(db, wIDs, partitions, zones)
	partitionDistrict(db, wIDs, partitions, zones)
	partitionNewOrder(db, wIDs, partitions, zones)
	partitionOrder(db, wIDs, partitions, zones)
	partitionOrderLine(db, wIDs, partitions, zones)
	partitionStock(db, wIDs, partitions, zones)
	partitionCustomer(db, wIDs, partitions, zones)
	partitionHistory(db, wIDs, partitions, zones)
	partitionItem(db, partitions, zones)
}

func isTableAlreadyPartitioned(db *pgx.ConnPool) (bool, error) {
	var count int
	if err := db.QueryRow(
		// Check for the existence of a partition named p0, which indicates that the
		// table has been paritioned already.
		`SELECT count(*) FROM crdb_internal.partitions where name = 'p0'`,
	).Scan(&count); err != nil {
		return false, err
	}

	return count > 0, nil
}
