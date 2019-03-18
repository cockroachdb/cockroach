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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package tpcc

import (
	"math"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"golang.org/x/exp/rand"
)

// These constants are all set by the spec - they're not knobs. Don't change
// them.
const (
	numItems                 = 100000
	numDistrictsPerWarehouse = 10
	numStockPerWarehouse     = 100000
	numCustomersPerDistrict  = 3000
	numCustomersPerWarehouse = numCustomersPerDistrict * numDistrictsPerWarehouse
	numOrdersPerDistrict     = numCustomersPerDistrict
	numOrdersPerWarehouse    = numOrdersPerDistrict * numDistrictsPerWarehouse
	numNewOrdersPerDistrict  = 900
	numNewOrdersPerWarehouse = numNewOrdersPerDistrict * numDistrictsPerWarehouse
	minOrderLinesPerOrder    = 5
	maxOrderLinesPerOrder    = 15

	originalString = "ORIGINAL"
	wYtd           = 300000.00
	ytd            = 30000.00
	nextOrderID    = 3001
	creditLimit    = 50000.00
	balance        = -10.00
	ytdPayment     = 10.00
	middleName     = "OE"
	paymentCount   = 1
	deliveryCount  = 0
	goodCredit     = "GC"
	badCredit      = "BC"
)

// These constants configure how we split the tables when splitting is enabled.
const (
	numWarehousesPerRange = 10
	numItemsPerRange      = 100

	historyRanges                 = 1000
	numHistoryValsPerRange uint64 = math.MaxUint64 / historyRanges
)

type generateLocals struct {
	rng *rand.Rand
	a   bufalloc.ByteAllocator
}

func (w *tpcc) tpccItemInitialRow(rowIdx int) []interface{} {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	iID := rowIdx + 1

	return []interface{}{
		iID,
		randInt(l.rng, 1, 10000),         // im_id: "Image ID associated to Item"
		randAString(l.rng, &l.a, 14, 24), // name
		float64(randInt(l.rng, 100, 10000)) / float64(100), // price
		randOriginalString(l.rng, &l.a),
	}
}

func (w *tpcc) tpccItemStats() []stats.JSONStatistic {
	rowCount := uint64(numItems)
	// The random alphanumeric strings below have a huge number of possible
	// values, so we assume the number of distinct values equals the row count.
	return []stats.JSONStatistic{
		workload.MakeStat([]string{"i_id"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"i_im_id"}, rowCount, workload.DistinctCount(rowCount, 10000), 0),
		workload.MakeStat([]string{"i_name"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"i_price"}, rowCount, workload.DistinctCount(rowCount, 9901), 0),
		workload.MakeStat([]string{"i_data"}, rowCount, rowCount, 0),
	}
}

func (w *tpcc) tpccWarehouseInitialRow(rowIdx int) []interface{} {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	wID := rowIdx // warehouse ids are 0-indexed. every other table is 1-indexed

	return []interface{}{
		wID,
		strconv.Itoa(randInt(l.rng, 6, 10)),  // name
		strconv.Itoa(randInt(l.rng, 10, 20)), // street_1
		strconv.Itoa(randInt(l.rng, 10, 20)), // street_2
		strconv.Itoa(randInt(l.rng, 10, 20)), // city
		randState(l.rng, &l.a),
		randZip(l.rng, &l.a),
		randTax(l.rng),
		wYtd,
	}
}

func (w *tpcc) tpccWarehouseStats() []stats.JSONStatistic {
	rowCount := uint64(w.warehouses)
	return []stats.JSONStatistic{
		workload.MakeStat([]string{"w_id"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"w_name"}, rowCount, workload.DistinctCount(rowCount, 5), 0),
		workload.MakeStat([]string{"w_street_1"}, rowCount, workload.DistinctCount(rowCount, 11), 0),
		workload.MakeStat([]string{"w_street_2"}, rowCount, workload.DistinctCount(rowCount, 11), 0),
		workload.MakeStat([]string{"w_city"}, rowCount, workload.DistinctCount(rowCount, 11), 0),
		// States consist of two random letters.
		workload.MakeStat([]string{"w_state"}, rowCount, workload.DistinctCount(rowCount, 26*26), 0),
		// Zip codes consist of a random 4-digit number plus 11111.
		workload.MakeStat([]string{"w_zip"}, rowCount, workload.DistinctCount(rowCount, 9999), 0),
		workload.MakeStat([]string{"w_tax"}, rowCount, workload.DistinctCount(rowCount, 2001), 0),
		workload.MakeStat([]string{"w_ytd"}, rowCount, 1, 0),
	}
}

func (w *tpcc) tpccStockInitialRow(rowIdx int) []interface{} {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	sID := (rowIdx % numStockPerWarehouse) + 1
	wID := (rowIdx / numStockPerWarehouse)

	return []interface{}{
		sID, wID,
		randInt(l.rng, 10, 100),          // quantity
		randAString(l.rng, &l.a, 24, 24), // dist_01
		randAString(l.rng, &l.a, 24, 24), // dist_02
		randAString(l.rng, &l.a, 24, 24), // dist_03
		randAString(l.rng, &l.a, 24, 24), // dist_04
		randAString(l.rng, &l.a, 24, 24), // dist_05
		randAString(l.rng, &l.a, 24, 24), // dist_06
		randAString(l.rng, &l.a, 24, 24), // dist_07
		randAString(l.rng, &l.a, 24, 24), // dist_08
		randAString(l.rng, &l.a, 24, 24), // dist_09
		randAString(l.rng, &l.a, 24, 24), // dist_10
		0,                                // ytd
		0,                                // order_cnt
		0,                                // remote_cnt
		randOriginalString(l.rng, &l.a),  // data
	}
}

func (w *tpcc) tpccStockStats() []stats.JSONStatistic {
	rowCount := uint64(w.warehouses * numStockPerWarehouse)
	// For all the s_dist_XX fields below, the number of possible values is
	// math.Pow(26+26+10, 24), which is larger than MaxUint64. Therefore, we
	// assume the number of distinct values is equal to the row count.
	// s_data has a similarly huge number of possible values.
	return []stats.JSONStatistic{
		workload.MakeStat([]string{"s_i_id"}, rowCount, numStockPerWarehouse, 0),
		workload.MakeStat([]string{"s_w_id"}, rowCount, uint64(w.warehouses), 0),
		workload.MakeStat([]string{"s_quantity"}, rowCount, workload.DistinctCount(rowCount, 91), 0),
		workload.MakeStat([]string{"s_dist_01"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"s_dist_02"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"s_dist_03"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"s_dist_04"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"s_dist_05"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"s_dist_06"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"s_dist_07"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"s_dist_08"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"s_dist_09"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"s_dist_10"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"s_ytd"}, rowCount, 1, 0),
		workload.MakeStat([]string{"s_order_cnt"}, rowCount, 1, 0),
		workload.MakeStat([]string{"s_remote_cnt"}, rowCount, 1, 0),
		workload.MakeStat([]string{"s_data"}, rowCount, rowCount, 0),
	}
}

func (w *tpcc) tpccDistrictInitialRow(rowIdx int) []interface{} {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	dID := (rowIdx % numDistrictsPerWarehouse) + 1
	wID := (rowIdx / numDistrictsPerWarehouse)

	return []interface{}{
		dID,
		wID,
		randAString(l.rng, &l.a, 6, 10),  // name
		randAString(l.rng, &l.a, 10, 20), // street 1
		randAString(l.rng, &l.a, 10, 20), // street 2
		randAString(l.rng, &l.a, 10, 20), // city
		randState(l.rng, &l.a),
		randZip(l.rng, &l.a),
		randTax(l.rng),
		ytd,
		nextOrderID,
	}
}

func (w *tpcc) tpccDistrictStats() []stats.JSONStatistic {
	rowCount := uint64(w.warehouses * numDistrictsPerWarehouse)
	// Several of the random alphanumeric strings below have a huge number of
	// possible values, so we assume the number of distinct values equals
	// the row count.
	return []stats.JSONStatistic{
		workload.MakeStat([]string{"d_id"}, rowCount, numDistrictsPerWarehouse, 0),
		workload.MakeStat([]string{"d_w_id"}, rowCount, uint64(w.warehouses), 0),
		workload.MakeStat([]string{"d_name"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"d_street_1"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"d_street_2"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"d_city"}, rowCount, rowCount, 0),
		// States consist of two random letters.
		workload.MakeStat([]string{"d_state"}, rowCount, workload.DistinctCount(rowCount, 26*26), 0),
		// Zip codes consist of a random 4-digit number plus 11111.
		workload.MakeStat([]string{"d_zip"}, rowCount, workload.DistinctCount(rowCount, 9999), 0),
		workload.MakeStat([]string{"d_tax"}, rowCount, workload.DistinctCount(rowCount, 2001), 0),
		workload.MakeStat([]string{"d_ytd"}, rowCount, 1, 0),
		workload.MakeStat([]string{"d_next_o_id"}, rowCount, 1, 0),
	}
}

func (w *tpcc) tpccCustomerInitialRow(rowIdx int) []interface{} {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	cID := (rowIdx % numCustomersPerDistrict) + 1
	dID := ((rowIdx / numCustomersPerDistrict) % numDistrictsPerWarehouse) + 1
	wID := (rowIdx / numCustomersPerWarehouse)

	// 10% of the customer rows have bad credit.
	// See section 4.3, under the CUSTOMER table population section.
	credit := goodCredit
	if l.rng.Intn(9) == 0 {
		// Poor 10% :(
		credit = badCredit
	}
	var lastName string
	// The first 1000 customers get a last name generated according to their id;
	// the rest get an NURand generated last name.
	if cID <= 1000 {
		lastName = randCLastSyllables(cID - 1)
	} else {
		lastName = randCLast(l.rng)
	}

	return []interface{}{
		cID, dID, wID,
		randAString(l.rng, &l.a, 8, 16), // first name
		middleName,
		lastName,
		randAString(l.rng, &l.a, 10, 20), // street 1
		randAString(l.rng, &l.a, 10, 20), // street 2
		randAString(l.rng, &l.a, 10, 20), // city name
		randState(l.rng, &l.a),
		randZip(l.rng, &l.a),
		randNString(l.rng, &l.a, 16, 16), // phone number
		w.nowString,
		credit,
		creditLimit,
		float64(randInt(l.rng, 0, 5000)) / float64(10000.0), // discount
		balance,
		ytdPayment,
		paymentCount,
		deliveryCount,
		randAString(l.rng, &l.a, 300, 500), // data
	}
}

func (w *tpcc) tpccCustomerStats() []stats.JSONStatistic {
	rowCount := uint64(w.warehouses * numCustomersPerWarehouse)
	// Several of the random alphanumeric strings below have a huge number of
	// possible values, so we assume the number of distinct values equals
	// the row count.
	return []stats.JSONStatistic{
		workload.MakeStat([]string{"c_id"}, rowCount, numCustomersPerDistrict, 0),
		workload.MakeStat([]string{"c_d_id"}, rowCount, numDistrictsPerWarehouse, 0),
		workload.MakeStat([]string{"c_w_id"}, rowCount, uint64(w.warehouses), 0),
		workload.MakeStat([]string{"c_first"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"c_middle"}, rowCount, 1, 0),
		// Last names consist of 3 syllables, each with 10 options.
		workload.MakeStat([]string{"c_last"}, rowCount, workload.DistinctCount(rowCount, 10*10*10), 0),
		workload.MakeStat([]string{"c_street_1"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"c_street_2"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"c_city"}, rowCount, rowCount, 0),
		// States consist of two random letters.
		workload.MakeStat([]string{"c_state"}, rowCount, workload.DistinctCount(rowCount, 26*26), 0),
		// Zip codes consist of a random 4-digit number plus 11111.
		workload.MakeStat([]string{"c_zip"}, rowCount, workload.DistinctCount(rowCount, 9999), 0),
		workload.MakeStat([]string{"c_phone"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"c_since"}, rowCount, 1, 0),
		// Credit is either good or bad.
		workload.MakeStat([]string{"c_credit"}, rowCount, 2, 0),
		workload.MakeStat([]string{"c_credit_lim"}, rowCount, 1, 0),
		workload.MakeStat([]string{"c_discount"}, rowCount, workload.DistinctCount(rowCount, 5001), 0),
		workload.MakeStat([]string{"c_balance"}, rowCount, 1, 0),
		workload.MakeStat([]string{"c_ytd_payment"}, rowCount, 1, 0),
		workload.MakeStat([]string{"c_payment_cnt"}, rowCount, 1, 0),
		workload.MakeStat([]string{"c_delivery_cnt"}, rowCount, 1, 0),
		workload.MakeStat([]string{"c_data"}, rowCount, rowCount, 0),
	}
}

func (w *tpcc) tpccHistoryInitialRow(rowIdx int) []interface{} {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	rowID := uuid.MakeV4().String()
	cID := (rowIdx % numCustomersPerDistrict) + 1
	dID := ((rowIdx / numCustomersPerDistrict) % numDistrictsPerWarehouse) + 1
	wID := (rowIdx / numCustomersPerWarehouse)

	return []interface{}{
		rowID, cID, dID, wID, dID, wID, w.nowString, 10.00, randAString(l.rng, &l.a, 12, 24),
	}
}

func (w *tpcc) tpccHistoryStats() []stats.JSONStatistic {
	rowCount := uint64(w.warehouses * numCustomersPerWarehouse)
	return []stats.JSONStatistic{
		workload.MakeStat([]string{"rowid"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"h_c_id"}, rowCount, numCustomersPerDistrict, 0),
		workload.MakeStat([]string{"h_c_d_id"}, rowCount, numDistrictsPerWarehouse, 0),
		workload.MakeStat([]string{"h_c_w_id"}, rowCount, uint64(w.warehouses), 0),
		workload.MakeStat([]string{"h_d_id"}, rowCount, numDistrictsPerWarehouse, 0),
		workload.MakeStat([]string{"h_w_id"}, rowCount, uint64(w.warehouses), 0),
		workload.MakeStat([]string{"h_date"}, rowCount, 1, 0),
		workload.MakeStat([]string{"h_amount"}, rowCount, 1, 0),
		// h_data has a huge number of possible values, so we assume the number of
		// distinct values equals the row count.
		workload.MakeStat([]string{"h_data"}, rowCount, rowCount, 0),
	}
}

func (w *tpcc) tpccOrderInitialRow(rowIdx int) []interface{} {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	l.rng.Seed(w.seed + uint64(rowIdx))
	numOrderLines := randInt(l.rng, minOrderLinesPerOrder, maxOrderLinesPerOrder)

	oID := (rowIdx % numOrdersPerDistrict) + 1
	dID := ((rowIdx / numOrdersPerDistrict) % numDistrictsPerWarehouse) + 1
	wID := (rowIdx / numOrdersPerWarehouse)

	var cID int
	{
		// TODO(dan): We can get rid of this cache if we change workload.Table
		// initial data to be batches of rows instead of rows. This would also
		// let us fix the hackOrderLinesPerOrder TODO below.
		w.randomCIDsCache.Lock()
		if w.randomCIDsCache.values == nil {
			w.randomCIDsCache.values = make([][]int, numDistrictsPerWarehouse*w.warehouses+1)
		}
		if w.randomCIDsCache.values[dID] == nil {
			// We need a random permutation of customers that stable for all orders in a
			// district, so use the district ID to seed the random permutation.
			w.randomCIDsCache.values[dID] = make([]int, numCustomersPerDistrict)
			for i, cID := range rand.New(rand.NewSource(uint64(dID))).Perm(numCustomersPerDistrict) {
				w.randomCIDsCache.values[dID][i] = cID + 1
			}
		}
		cID = w.randomCIDsCache.values[dID][oID-1]
		w.randomCIDsCache.Unlock()
	}

	var carrierID interface{}
	if oID < 2101 {
		carrierID = strconv.Itoa(randInt(l.rng, 1, 10))
	}

	return []interface{}{
		oID, dID, wID, cID, w.nowString, carrierID, numOrderLines, 1,
	}
}

func (w *tpcc) tpccOrderStats() []stats.JSONStatistic {
	rowCount := uint64(w.warehouses * numOrdersPerWarehouse)
	return []stats.JSONStatistic{
		workload.MakeStat([]string{"o_id"}, rowCount, numOrdersPerDistrict, 0),
		workload.MakeStat([]string{"o_d_id"}, rowCount, numDistrictsPerWarehouse, 0),
		workload.MakeStat([]string{"o_w_id"}, rowCount, uint64(w.warehouses), 0),
		workload.MakeStat([]string{"o_c_id"}, rowCount, numCustomersPerDistrict, 0),
		workload.MakeStat([]string{"o_entry_d"}, rowCount, 1, 0),
		// The null count corresponds to the number of orders with incomplete
		// delivery.
		workload.MakeStat([]string{"o_carrier_id"}, rowCount, workload.DistinctCount(rowCount, 10),
			uint64(w.warehouses)*numDistrictsPerWarehouse*(numOrdersPerDistrict-2100),
		),
		workload.MakeStat([]string{"o_ol_cnt"}, rowCount, workload.DistinctCount(rowCount, 11), 0),
		workload.MakeStat([]string{"o_all_local"}, rowCount, 1, 0),
	}
}

func (w *tpcc) tpccNewOrderInitialRow(rowIdx int) []interface{} {
	// The last numNewOrdersPerDistrict orders have entries in new orders.
	const firstNewOrderOffset = numOrdersPerDistrict - numNewOrdersPerDistrict
	oID := (rowIdx % numNewOrdersPerDistrict) + firstNewOrderOffset + 1
	dID := ((rowIdx / numNewOrdersPerDistrict) % numDistrictsPerWarehouse) + 1
	wID := (rowIdx / numNewOrdersPerWarehouse)

	return []interface{}{
		oID, dID, wID,
	}
}

func (w *tpcc) tpccNewOrderStats() []stats.JSONStatistic {
	rowCount := uint64(w.warehouses * numNewOrdersPerWarehouse)
	return []stats.JSONStatistic{
		workload.MakeStat([]string{"no_o_id"}, rowCount, numNewOrdersPerDistrict, 0),
		workload.MakeStat([]string{"no_d_id"}, rowCount, numDistrictsPerWarehouse, 0),
		workload.MakeStat([]string{"no_w_id"}, rowCount, uint64(w.warehouses), 0),
	}
}

func (w *tpcc) tpccOrderLineInitialRowBatch(orderRowIdx int) [][]interface{} {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	l.rng.Seed(w.seed + uint64(orderRowIdx))
	numOrderLines := randInt(l.rng, minOrderLinesPerOrder, maxOrderLinesPerOrder)

	// NB: There is one batch of order_line rows per order
	oID := (orderRowIdx % numOrdersPerDistrict) + 1
	dID := ((orderRowIdx / numOrdersPerDistrict) % numDistrictsPerWarehouse) + 1
	wID := (orderRowIdx / numOrdersPerWarehouse)

	var rows [][]interface{}
	for i := 0; i < numOrderLines; i++ {
		olNumber := i + 1

		var amount float64
		var deliveryD interface{}
		if oID < 2101 {
			amount = 0
			deliveryD = w.nowString
		} else {
			amount = float64(randInt(l.rng, 1, 999999)) / 100.0
		}

		rows = append(rows, []interface{}{
			oID, dID, wID, olNumber,
			randInt(l.rng, 1, 100000), // ol_i_id
			wID,                       // supply_w_id
			deliveryD,
			5, // quantity
			amount,
			randAString(l.rng, &l.a, 24, 24),
		})
	}
	return rows
}

func (w *tpcc) tpccOrderLineStats() []stats.JSONStatistic {
	averageOrderLines := float64(maxOrderLinesPerOrder+minOrderLinesPerOrder) / 2
	rowCount := uint64(int64(float64(w.warehouses) * numOrdersPerWarehouse * averageOrderLines))
	deliveryIncomplete := uint64(int64(
		float64(w.warehouses) * numDistrictsPerWarehouse * (numOrdersPerDistrict - 2100) * averageOrderLines,
	))
	return []stats.JSONStatistic{
		workload.MakeStat([]string{"ol_o_id"}, rowCount, numOrdersPerDistrict, 0),
		workload.MakeStat([]string{"ol_d_id"}, rowCount, numDistrictsPerWarehouse, 0),
		workload.MakeStat([]string{"ol_w_id"}, rowCount, uint64(w.warehouses), 0),
		workload.MakeStat([]string{"ol_number"}, rowCount, maxOrderLinesPerOrder, 0),
		workload.MakeStat([]string{"ol_i_id"}, rowCount, workload.DistinctCount(rowCount, 100000), 0),
		workload.MakeStat([]string{"ol_supply_w_id"}, rowCount, uint64(w.warehouses), 0),
		workload.MakeStat([]string{"ol_delivery_d"}, rowCount, 1, deliveryIncomplete),
		workload.MakeStat([]string{"ol_quantity"}, rowCount, 1, 0),
		// When delivery is incomplete, there are at most 999999 different values
		// for amount. When delivery is complete, there is exactly one value
		// (amount=0).
		workload.MakeStat(
			[]string{"ol_amount"}, rowCount, workload.DistinctCount(deliveryIncomplete, 999999)+1, 0,
		),
		// ol_dist_info has a huge number of possible values, so we assume the
		// number of distinct values equals the row count.
		workload.MakeStat([]string{"ol_dist_info"}, rowCount, rowCount, 0),
	}
}
