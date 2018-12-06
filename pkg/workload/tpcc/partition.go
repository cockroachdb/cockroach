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
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
)

// partitioner encapsulates all logic related to partitioning discrete numbers
// of warehouses into disjoint sets of roughly equal sizes. Partitions are then
// evenly assigned "active" warehouses, which allows for an even split of live
// warehouses across partitions without the need to repartition when the active
// count is changed.
type partitioner struct {
	total  int // e.g. the total number of warehouses
	active int // e.g. the active number of warehouses
	parts  int // the number of partitions to break `total` into

	partBounds   []int       // the boundary points between partitions
	partElems    [][]int     // the elements active in each partition
	partElemsMap map[int]int // mapping from element to partition index
	totalElems   []int       // all active elements
}

func makePartitioner(total, active, parts int) (*partitioner, error) {
	if total <= 0 {
		return nil, errors.Errorf("total must be positive; %d", total)
	}
	if active <= 0 {
		return nil, errors.Errorf("active must be positive; %d", active)
	}
	if parts <= 0 {
		return nil, errors.Errorf("parts must be positive; %d", parts)
	}
	if active > total {
		return nil, errors.Errorf("active > total; %d > %d", active, total)
	}
	if parts > total {
		return nil, errors.Errorf("parts > total; %d > %d", parts, total)
	}

	// Partition boundary points.
	//
	// bounds contains the boundary points between partitions, where each point
	// in the slice corresponds to the exclusive end element of one partition
	// and and the inclusive start element of the next.
	//
	//  total  = 20
	//  parts  = 3
	//  bounds = [0, 6, 13, 20]
	//
	bounds := make([]int, parts+1)
	for i := range bounds {
		bounds[i] = (i * total) / parts
	}

	// Partition sizes.
	//
	// sizes contains the number of elements that are active in each partition.
	//
	//  active = 10
	//  parts  = 3
	//  sizes  = [3, 3, 4]
	//
	sizes := make([]int, parts)
	for i := range sizes {
		s := (i * active) / parts
		e := ((i + 1) * active) / parts
		sizes[i] = e - s
	}

	// Partitions.
	//
	// partElems enumerates the active elements in each partition.
	//
	//  total     = 20
	//  active    = 10
	//  parts     = 3
	//  partElems = [[0, 1, 2], [6, 7, 8], [13, 14, 15, 16]]
	//
	partElems := make([][]int, parts)
	for i := range partElems {
		partAct := make([]int, sizes[i])
		for j := range partAct {
			partAct[j] = bounds[i] + j
		}
		partElems[i] = partAct
	}

	// Partition reverse mapping.
	//
	// partElemsMap maps each active element to its partition index.
	//
	//  total        = 20
	//  active       = 10
	//  parts        = 3
	//  partElemsMap = {0:0, 1:0, 2:0, 6:1, 7:1, 8:1, 13:2, 14:2, 15:2, 16:2}
	//
	partElemsMap := make(map[int]int)
	for p, elems := range partElems {
		for _, elem := range elems {
			partElemsMap[elem] = p
		}
	}

	// Total elements.
	//
	// totalElems aggregates all active elements into a single slice.
	//
	//  total      = 20
	//  active     = 10
	//  parts      = 3
	//  totalElems = [0, 1, 2, 6, 7, 8, 13, 14, 15, 16]
	//
	var totalElems []int
	for _, elems := range partElems {
		totalElems = append(totalElems, elems...)
	}

	return &partitioner{
		total:  total,
		active: active,
		parts:  parts,

		partBounds:   bounds,
		partElems:    partElems,
		partElemsMap: partElemsMap,
		totalElems:   totalElems,
	}, nil
}

// randActive returns a random active element.
func (p *partitioner) randActive(rng *rand.Rand) int {
	return p.totalElems[rng.Intn(len(p.totalElems))]
}

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

// partitionObject partitions the specified object (TABLE or INDEX) with the
// provided name, given the partitioning. Callers of the function must specify
// the associated table and the partition's number.
func partitionObject(
	db *pgx.ConnPool, p *partitioner, zones []string, obj, name, col, table string, idx int,
) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "ALTER %s %s PARTITION BY RANGE (%s) (\n", obj, name, col)
	for i := 0; i < p.parts; i++ {
		fmt.Fprintf(&buf, "  PARTITION p%d_%d VALUES FROM (%d) to (%d)",
			idx, i, p.partBounds[i], p.partBounds[i+1])
		if i+1 < p.parts {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}

	for i := 0; i < p.parts; i++ {
		configureZone(db, table, fmt.Sprintf("p%d_%d", idx, i), i, zones)
	}
}

func partitionTable(db *pgx.ConnPool, p *partitioner, zones []string, table, col string, idx int) {
	partitionObject(db, p, zones, "TABLE", table, col, table, idx)
}

func partitionIndex(
	db *pgx.ConnPool, p *partitioner, zones []string, table, index, col string, idx int,
) {
	indexStr := fmt.Sprintf("%s@%s", table, index)
	partitionObject(db, p, zones, "INDEX", indexStr, col, table, idx)
}

func partitionWarehouse(db *pgx.ConnPool, wPart *partitioner, zones []string) {
	partitionTable(db, wPart, zones, "warehouse", "w_id", 0)
}

func partitionDistrict(db *pgx.ConnPool, wPart *partitioner, zones []string) {
	partitionTable(db, wPart, zones, "district", "d_w_id", 0)
}

func partitionNewOrder(db *pgx.ConnPool, wPart *partitioner, zones []string) {
	partitionTable(db, wPart, zones, "new_order", "no_w_id", 0)
}

func partitionOrder(db *pgx.ConnPool, wPart *partitioner, zones []string) {
	partitionTable(db, wPart, zones, `"order"`, "o_w_id", 0)
	partitionIndex(db, wPart, zones, `"order"`, "order_idx", "o_w_id", 1)
	partitionIndex(db, wPart, zones, `"order"`, "order_o_w_id_o_d_id_o_c_id_idx", "o_w_id", 2)
}

func partitionOrderLine(db *pgx.ConnPool, wPart *partitioner, zones []string) {
	partitionTable(db, wPart, zones, "order_line", "ol_w_id", 0)
	partitionIndex(db, wPart, zones, "order_line", "order_line_fk", "ol_supply_w_id", 1)
}

func partitionStock(db *pgx.ConnPool, wPart, iPart *partitioner, zones []string) {
	partitionTable(db, wPart, zones, "stock", "s_w_id", 0)
	partitionIndex(db, iPart, zones, "stock", "stock_s_i_id_idx", "s_i_id", 1)
}

func partitionCustomer(db *pgx.ConnPool, wPart *partitioner, zones []string) {
	partitionTable(db, wPart, zones, "customer", "c_w_id", 0)
	partitionIndex(db, wPart, zones, "customer", "customer_idx", "c_w_id", 1)
}

func partitionHistory(db *pgx.ConnPool, wPart *partitioner, zones []string) {
	const maxVal = math.MaxUint64
	temp := make([]byte, 16)
	rowids := make([]uuid.UUID, wPart.parts+1)
	for i := 0; i < wPart.parts; i++ {
		var err error

		// We're splitting the UUID rowid column evenly into N partitions. The
		// column is sorted lexicographically on the bytes of the UUID which means
		// we should put the partitioning values at the front of the UUID.
		binary.BigEndian.PutUint64(temp, uint64(i)*(maxVal/uint64(wPart.parts)))
		rowids[i], err = uuid.FromBytes(temp)
		if err != nil {
			panic(err)
		}
	}

	rowids[wPart.parts], _ = uuid.FromString("ffffffff-ffff-ffff-ffff-ffffffffffff")

	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE history PARTITION BY RANGE (rowid) (\n")
	for i := 0; i < wPart.parts; i++ {
		fmt.Fprintf(&buf, "  PARTITION p0_%d VALUES FROM ('%s') to ('%s')", i, rowids[i], rowids[i+1])
		if i+1 < wPart.parts {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}

	for i := 0; i < wPart.parts; i++ {
		configureZone(db, `history`, fmt.Sprintf("p0_%d", i), i, zones)
	}

	partitionIndex(db, wPart, zones, "history", "history_h_w_id_h_d_id_idx", "h_w_id", 1)
	partitionIndex(db, wPart, zones, "history", "history_h_c_w_id_h_c_d_id_h_c_id_idx", "h_c_w_id", 2)
}

func partitionItem(db *pgx.ConnPool, iPart *partitioner, zones []string) {
	partitionTable(db, iPart, zones, "item", "i_id", 0)
}

func partitionTables(db *pgx.ConnPool, wPart *partitioner, zones []string) {
	// Create a separate partitioning for the fixed-size items table and its
	// associated indexes.
	const nItems = 100000
	iPart, err := makePartitioner(nItems, nItems, wPart.parts)
	if err != nil {
		panic(fmt.Sprintf("Couldn't create item partitioner: %s\n", err))
	}

	partitionWarehouse(db, wPart, zones)
	partitionDistrict(db, wPart, zones)
	partitionNewOrder(db, wPart, zones)
	partitionOrder(db, wPart, zones)
	partitionOrderLine(db, wPart, zones)
	partitionStock(db, wPart, iPart, zones)
	partitionCustomer(db, wPart, zones)
	partitionHistory(db, wPart, zones)
	partitionItem(db, iPart, zones)
}

func isTableAlreadyPartitioned(db *pgx.ConnPool) (bool, error) {
	var count int
	if err := db.QueryRow(
		// Check for the existence of a partition named p0_0, which indicates that the
		// table has been partitioned already.
		`SELECT count(*) FROM crdb_internal.partitions where name = 'p0_0'`,
	).Scan(&count); err != nil {
		return false, err
	}

	return count > 0, nil
}
