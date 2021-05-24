// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"golang.org/x/exp/rand"
)

type partitionStrategy int

const (
	// The partitionedReplication strategy constrains replication for a given
	// partition to within a single zone. It does so by requiring that all
	// replicas of each range in a partition are stored in the same zone.
	//
	// Example of 9 warehouses partitioned over 3 zones:
	//  partitions = [0,1,2], [3,4,5], [6,7,8]
	//  w = warehouse #
	//  L = leaseholder
	//
	// us-east1-b:
	//  n1 = [w0(L), w1,    w2   ]
	//  n2 = [w0,    w1(L), w2   ]
	//  n3 = [w0,    w1,    w2(L)]
	//
	// us-west1-b:
	//  n4 = [w3(L), w4,    w5   ]
	//  n5 = [w3,    w4,    w5   ]
	//  n6 = [w3,    w4(L), w5(L)]
	//
	// europe-west2-b:
	//  n7 = [w6,    w7,    w8(L)]
	//  n8 = [w6,    w7(L), w8   ]
	//  n9 = [w6(L), w7,    w8   ]
	//
	// NOTE: the lease for a range is randomly scattered within the zone
	// that contains all replicas of the range.
	//
	partitionedReplication partitionStrategy = iota
	// The partitionedLeases strategy collocates read leases for a given
	// partition to within a single zone. It does so by configuring lease
	// preferences on each range in a partition to prefer the same zone.
	// Unlike the partitioned replication strategy, it does not prevent
	// cross-zone replication.
	//
	// Example of 9 warehouses partitioned over 3 zones:
	//  partitions = [0,1,2], [3,4,5], [6,7,8]
	//  w = warehouse #
	//  L = leaseholder
	//
	// us-east1-b:
	//  n1 = [w0(L), w3, w6]
	//  n2 = [w1(L), w4, w7]
	//  n3 = [w2(L), w5, w8]
	//
	// us-west1-b:
	//  n4 = [w0,    w1,    w2   ]
	//  n5 = [w3(L), w4(L), w5(L)]
	//  n6 = [w6,    w7,    w8   ]
	//
	// europe-west2-b:
	//  n7 = [w2, w5, w8(L)]
	//  n8 = [w1, w4, w7(L)]
	//  n9 = [w0, w3, w6(L)]
	//
	// NOTE: a copy of each range is randomly scattered within each zone.
	//
	partitionedLeases
)

// Part of pflag's Value interface.
func (ps partitionStrategy) String() string {
	switch ps {
	case partitionedReplication:
		return "replication"
	case partitionedLeases:
		return "leases"
	}
	panic("unexpected")
}

// Part of pflag's Value interface.
func (ps *partitionStrategy) Set(value string) error {
	switch value {
	case "replication":
		*ps = partitionedReplication
		return nil
	case "leases":
		*ps = partitionedLeases
		return nil
	}
	return errors.Errorf("unknown partition strategy %q", value)
}

// Part of pflag's Value interface.
func (ps partitionStrategy) Type() string {
	return "partitionStrategy"
}

type zoneConfig struct {
	zones    []string
	strategy partitionStrategy
}

type survivalGoal int

const (
	survivalGoalZone survivalGoal = iota
	survivalGoalRegion
)

// Part of pflag's Value interface.
func (s survivalGoal) String() string {
	switch s {
	case survivalGoalZone:
		return "zone"
	case survivalGoalRegion:
		return "region"
	}
	panic("unexpected")
}

// Part of pflag's Value interface.
func (s *survivalGoal) Set(value string) error {
	switch value {
	case "zone":
		*s = survivalGoalZone
	case "region":
		*s = survivalGoalRegion
	default:
		return errors.Errorf("unknown survival goal %q", value)
	}
	return nil
}

// Part of pflag's Value interface.
func (s survivalGoal) Type() string {
	return "survival_goal"
}

type multiRegionConfig struct {
	regions      []string
	survivalGoal survivalGoal
}

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

// configureZone sets up zone configs for previously created partitions. By
// default it adds constraints/preferences in terms of racks, but if the zones
// flag is passed into tpcc, it will set the constraints/preferences based on
// the geographic zones provided.
func configureZone(db *gosql.DB, cfg zoneConfig, table, partition string, partIdx int) error {
	var kv string
	if len(cfg.zones) > 0 {
		kv = fmt.Sprintf("zone=%s", cfg.zones[partIdx])
	} else {
		kv = fmt.Sprintf("rack=%d", partIdx)
	}

	var opts string
	switch cfg.strategy {
	case partitionedReplication:
		// Place all replicas in the zone.
		opts = fmt.Sprintf(`constraints = '[+%s]'`, kv)
	case partitionedLeases:
		// Place one replica in the zone and give that replica lease preference.
		opts = fmt.Sprintf(`num_replicas = COPY FROM PARENT, constraints = '{"+%s":1}', lease_preferences = '[[+%s]]'`, kv, kv)
	default:
		panic("unexpected")
	}

	sql := fmt.Sprintf(`ALTER PARTITION %s OF TABLE %s CONFIGURE ZONE USING %s`,
		partition, table, opts)
	if _, err := db.Exec(sql); err != nil {
		return errors.Wrapf(err, "Couldn't exec %q", sql)
	}
	return nil
}

// partitionObject partitions the specified object (TABLE or INDEX) with the
// provided name, given the partitioning. Callers of the function must specify
// the associated table and the partition's number.
func partitionObject(
	db *gosql.DB, cfg zoneConfig, p *partitioner, obj, name, col, table string, idx int,
) error {
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
		return errors.Wrapf(err, "Couldn't exec %q", buf.String())
	}

	for i := 0; i < p.parts; i++ {
		if err := configureZone(db, cfg, table, fmt.Sprintf("p%d_%d", idx, i), i); err != nil {
			return err
		}
	}
	return nil
}

func partitionTable(
	db *gosql.DB, cfg zoneConfig, p *partitioner, table, col string, idx int,
) error {
	return partitionObject(db, cfg, p, "TABLE", table, col, table, idx)
}

func partitionIndex(
	db *gosql.DB, cfg zoneConfig, p *partitioner, table, index, col string, idx int,
) error {
	indexStr := fmt.Sprintf("%s@%s", table, index)
	if exists, err := indexExists(db, table, index); err != nil {
		return err
	} else if !exists {
		return errors.Errorf("could not find index %q", indexStr)
	}
	return partitionObject(db, cfg, p, "INDEX", indexStr, col, table, idx)
}

func partitionWarehouse(db *gosql.DB, cfg zoneConfig, wPart *partitioner) error {
	return partitionTable(db, cfg, wPart, "warehouse", "w_id", 0)
}

func partitionDistrict(db *gosql.DB, cfg zoneConfig, wPart *partitioner) error {
	return partitionTable(db, cfg, wPart, "district", "d_w_id", 0)
}

func partitionNewOrder(db *gosql.DB, cfg zoneConfig, wPart *partitioner) error {
	return partitionTable(db, cfg, wPart, "new_order", "no_w_id", 0)
}

func partitionOrder(db *gosql.DB, cfg zoneConfig, wPart *partitioner) error {
	if err := partitionTable(db, cfg, wPart, `"order"`, "o_w_id", 0); err != nil {
		return err
	}
	return partitionIndex(db, cfg, wPart, `"order"`, "order_idx", "o_w_id", 1)
}

func partitionOrderLine(db *gosql.DB, cfg zoneConfig, wPart *partitioner) error {
	return partitionTable(db, cfg, wPart, "order_line", "ol_w_id", 0)
}

func partitionStock(db *gosql.DB, cfg zoneConfig, wPart *partitioner) error {
	return partitionTable(db, cfg, wPart, "stock", "s_w_id", 0)
}

func partitionCustomer(db *gosql.DB, cfg zoneConfig, wPart *partitioner) error {
	if err := partitionTable(db, cfg, wPart, "customer", "c_w_id", 0); err != nil {
		return err
	}
	return partitionIndex(db, cfg, wPart, "customer", "customer_idx", "c_w_id", 1)
}

func partitionHistory(db *gosql.DB, cfg zoneConfig, wPart *partitioner) error {
	return partitionTable(db, cfg, wPart, "history", "h_w_id", 0)
}

// replicateColumns creates covering replicated indexes for a given table
// for each of the zones provided.
//
// It is recommended to do this for columns that are immutable as it allows
// lookups on those columns to be local within the provided zone. If there are
// no zones, it assumes that each partition corresponds to a rack.
func replicateColumns(
	db *gosql.DB,
	cfg zoneConfig,
	wPart *partitioner,
	name string,
	pkColumns []string,
	storedColumns []string,
) error {
	constraints := synthesizeConstraints(cfg, wPart)
	for i, constraint := range constraints {
		if _, err := db.Exec(
			fmt.Sprintf(`CREATE UNIQUE INDEX %[1]s_idx_%[2]d ON %[1]s (%[3]s) STORING (%[4]s)`,
				name, i, strings.Join(pkColumns, ","), strings.Join(storedColumns, ",")),
		); err != nil {
			return err
		}
		if _, err := db.Exec(fmt.Sprintf(
			`ALTER INDEX %[1]s@%[1]s_idx_%[2]d
CONFIGURE ZONE USING num_replicas = COPY FROM PARENT, constraints='{"%[3]s": 1}', lease_preferences='[[%[3]s]]'`,
			name, i, constraint)); err != nil {
			return err
		}
	}
	return nil
}

func replicateWarehouse(db *gosql.DB, cfg zoneConfig, wPart *partitioner) error {
	return replicateColumns(db, cfg, wPart, "warehouse", []string{"w_id"}, []string{"w_tax"})
}

func replicateDistrict(db *gosql.DB, cfg zoneConfig, wPart *partitioner) error {
	return replicateColumns(db, cfg, wPart, "district", []string{"d_w_id", "d_id"},
		[]string{"d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip"})
}

func replicateItem(db *gosql.DB, cfg zoneConfig, wPart *partitioner) error {
	return replicateColumns(db, cfg, wPart, "item", []string{"i_id"},
		[]string{"i_im_id", "i_name", "i_price", "i_data"})
}

func synthesizeConstraints(cfg zoneConfig, wPart *partitioner) []string {
	var constraints []string
	if len(cfg.zones) > 0 {
		for _, zone := range cfg.zones {
			constraints = append(constraints, "+zone="+zone)
		}
	} else {
		// Assume we have parts number of racks which are zero indexed.
		for i := 0; i < wPart.parts; i++ {
			constraints = append(constraints, fmt.Sprintf("+rack=%d", i))
		}
	}
	return constraints
}

func partitionTables(
	db *gosql.DB, cfg zoneConfig, wPart *partitioner, replicateStaticColumns bool,
) error {
	if err := partitionWarehouse(db, cfg, wPart); err != nil {
		return err
	}
	if err := partitionDistrict(db, cfg, wPart); err != nil {
		return err
	}
	if err := partitionNewOrder(db, cfg, wPart); err != nil {
		return err
	}
	if err := partitionOrder(db, cfg, wPart); err != nil {
		return err
	}
	if err := partitionOrderLine(db, cfg, wPart); err != nil {
		return err
	}
	if err := partitionStock(db, cfg, wPart); err != nil {
		return err
	}
	if err := partitionCustomer(db, cfg, wPart); err != nil {
		return err
	}
	if err := partitionHistory(db, cfg, wPart); err != nil {
		return err
	}
	if replicateStaticColumns {
		if err := replicateDistrict(db, cfg, wPart); err != nil {
			return err
		}
		if err := replicateWarehouse(db, cfg, wPart); err != nil {
			return err
		}
	}
	return replicateItem(db, cfg, wPart)
}

func partitionCount(db *gosql.DB) (int, error) {
	var count int
	if err := db.QueryRow(`
		SELECT count(*)
		FROM crdb_internal.tables t
		JOIN crdb_internal.partitions p
		USING (table_id)
		WHERE t.name = 'warehouse'
		AND p.name ~ 'p0_\d+'
	`).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func indexExists(db *gosql.DB, table, index string) (bool, error) {
	// Strip any quotes around the table name.
	table = strings.ReplaceAll(table, `"`, ``)

	var exists bool
	if err := db.QueryRow(`
		SELECT count(*) > 0
		FROM information_schema.statistics
		WHERE table_name = $1
		AND   index_name = $2
	`, table, index).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}
