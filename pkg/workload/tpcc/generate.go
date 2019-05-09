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

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
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
	numHistoryPerWarehouse   = numCustomersPerWarehouse
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
	paymentCount   = 1
	deliveryCount  = 0
	goodCredit     = "GC"
	badCredit      = "BC"
)

var middleName = []byte(`OE`)

// These constants configure how we split the tables when splitting is enabled.
const (
	numWarehousesPerRange = 10
	numItemsPerRange      = 100

	historyRanges                 = 1000
	numHistoryValsPerRange uint64 = math.MaxUint64 / historyRanges
)

type generateLocals struct {
	rng *rand.Rand
}

var itemColTypes = []types.T{
	types.Int64,
	types.Int64,
	types.Bytes,
	types.Float64,
	types.Bytes,
}

func (w *tpcc) tpccItemInitialRowBatch(rowIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	iID := rowIdx + 1

	cb.Reset(itemColTypes, 1)
	cb.ColVec(0).Int64()[0] = int64(iID)
	cb.ColVec(1).Int64()[0] = randInt(l.rng, 1, 10000)                             // im_id: "Image ID associated to Item"
	cb.ColVec(2).Bytes()[0] = randAString(l.rng, a, 14, 24)                        // name
	cb.ColVec(3).Float64()[0] = float64(randInt(l.rng, 100, 10000)) / float64(100) // price
	cb.ColVec(4).Bytes()[0] = randOriginalString(l.rng, a)
}

func (w *tpcc) tpccItemStats() []workload.JSONStatistic {
	rowCount := uint64(numItems)
	// The random alphanumeric strings below have a huge number of possible
	// values, so we assume the number of distinct values equals the row count.
	return []workload.JSONStatistic{
		workload.MakeStat([]string{"i_id"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"i_im_id"}, rowCount, workload.DistinctCount(rowCount, 10000), 0),
		workload.MakeStat([]string{"i_name"}, rowCount, rowCount, 0),
		workload.MakeStat([]string{"i_price"}, rowCount, workload.DistinctCount(rowCount, 9901), 0),
		workload.MakeStat([]string{"i_data"}, rowCount, rowCount, 0),
	}
}

var warehouseColTypes = []types.T{
	types.Int64,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Float64,
	types.Float64,
}

func (w *tpcc) tpccWarehouseInitialRowBatch(
	rowIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	wID := rowIdx // warehouse ids are 0-indexed. every other table is 1-indexed

	cb.Reset(warehouseColTypes, 1)
	cb.ColVec(0).Int64()[0] = int64(wID)
	cb.ColVec(1).Bytes()[0] = []byte(strconv.FormatInt(randInt(l.rng, 6, 10), 10))  // name
	cb.ColVec(2).Bytes()[0] = []byte(strconv.FormatInt(randInt(l.rng, 10, 20), 10)) // street_1
	cb.ColVec(3).Bytes()[0] = []byte(strconv.FormatInt(randInt(l.rng, 10, 20), 10)) // street_2
	cb.ColVec(4).Bytes()[0] = []byte(strconv.FormatInt(randInt(l.rng, 10, 20), 10)) // city
	cb.ColVec(5).Bytes()[0] = randState(l.rng, a)
	cb.ColVec(6).Bytes()[0] = randZip(l.rng, a)
	cb.ColVec(7).Float64()[0] = randTax(l.rng)
	cb.ColVec(8).Float64()[0] = wYtd
}

func (w *tpcc) tpccWarehouseStats() []workload.JSONStatistic {
	rowCount := uint64(w.warehouses)
	return []workload.JSONStatistic{
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

var stockColTypes = []types.T{
	types.Int64,
	types.Int64,
	types.Int64,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Int64,
	types.Int64,
	types.Int64,
	types.Bytes,
}

func (w *tpcc) tpccStockInitialRowBatch(rowIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	sID := (rowIdx % numStockPerWarehouse) + 1
	wID := (rowIdx / numStockPerWarehouse)

	cb.Reset(stockColTypes, 1)
	cb.ColVec(0).Int64()[0] = int64(sID)
	cb.ColVec(1).Int64()[0] = int64(wID)
	cb.ColVec(2).Int64()[0] = randInt(l.rng, 10, 100)        // quantity
	cb.ColVec(3).Bytes()[0] = randAString(l.rng, a, 24, 24)  // dist_01
	cb.ColVec(4).Bytes()[0] = randAString(l.rng, a, 24, 24)  // dist_02
	cb.ColVec(5).Bytes()[0] = randAString(l.rng, a, 24, 24)  // dist_03
	cb.ColVec(6).Bytes()[0] = randAString(l.rng, a, 24, 24)  // dist_04
	cb.ColVec(7).Bytes()[0] = randAString(l.rng, a, 24, 24)  // dist_05
	cb.ColVec(8).Bytes()[0] = randAString(l.rng, a, 24, 24)  // dist_06
	cb.ColVec(9).Bytes()[0] = randAString(l.rng, a, 24, 24)  // dist_07
	cb.ColVec(10).Bytes()[0] = randAString(l.rng, a, 24, 24) // dist_08
	cb.ColVec(11).Bytes()[0] = randAString(l.rng, a, 24, 24) // dist_09
	cb.ColVec(12).Bytes()[0] = randAString(l.rng, a, 24, 24) // dist_10
	cb.ColVec(13).Int64()[0] = 0                             // ytd
	cb.ColVec(14).Int64()[0] = 0                             // order_cnt
	cb.ColVec(15).Int64()[0] = 0                             // remote_cnt
	cb.ColVec(16).Bytes()[0] = randOriginalString(l.rng, a)  // data
}

func (w *tpcc) tpccStockStats() []workload.JSONStatistic {
	rowCount := uint64(w.warehouses * numStockPerWarehouse)
	// For all the s_dist_XX fields below, the number of possible values is
	// math.Pow(26+26+10, 24), which is larger than MaxUint64. Therefore, we
	// assume the number of distinct values is equal to the row count.
	// s_data has a similarly huge number of possible values.
	return []workload.JSONStatistic{
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

var districtColTypes = []types.T{
	types.Int64,
	types.Int64,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Float64,
	types.Float64,
	types.Int64,
}

func (w *tpcc) tpccDistrictInitialRowBatch(
	rowIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	dID := (rowIdx % numDistrictsPerWarehouse) + 1
	wID := (rowIdx / numDistrictsPerWarehouse)

	cb.Reset(districtColTypes, 1)
	cb.ColVec(0).Int64()[0] = int64(dID)
	cb.ColVec(1).Int64()[0] = int64(wID)
	cb.ColVec(2).Bytes()[0] = randAString(l.rng, a, 6, 10)  // name
	cb.ColVec(3).Bytes()[0] = randAString(l.rng, a, 10, 20) // street 1
	cb.ColVec(4).Bytes()[0] = randAString(l.rng, a, 10, 20) // street 2
	cb.ColVec(5).Bytes()[0] = randAString(l.rng, a, 10, 20) // city
	cb.ColVec(6).Bytes()[0] = randState(l.rng, a)
	cb.ColVec(7).Bytes()[0] = randZip(l.rng, a)
	cb.ColVec(8).Float64()[0] = randTax(l.rng)
	cb.ColVec(9).Float64()[0] = ytd
	cb.ColVec(10).Int64()[0] = nextOrderID
}

func (w *tpcc) tpccDistrictStats() []workload.JSONStatistic {
	rowCount := uint64(w.warehouses * numDistrictsPerWarehouse)
	// Several of the random alphanumeric strings below have a huge number of
	// possible values, so we assume the number of distinct values equals
	// the row count.
	return []workload.JSONStatistic{
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

var customerColTypes = []types.T{
	types.Int64,
	types.Int64,
	types.Int64,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Float64,
	types.Float64,
	types.Float64,
	types.Float64,
	types.Int64,
	types.Int64,
	types.Bytes,
}

func (w *tpcc) tpccCustomerInitialRowBatch(
	rowIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
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
	var lastName []byte
	// The first 1000 customers get a last name generated according to their id;
	// the rest get an NURand generated last name.
	if cID <= 1000 {
		lastName = randCLastSyllables(cID-1, a)
	} else {
		lastName = randCLast(l.rng, a)
	}

	cb.Reset(customerColTypes, 1)
	cb.ColVec(0).Int64()[0] = int64(cID)
	cb.ColVec(1).Int64()[0] = int64(dID)
	cb.ColVec(2).Int64()[0] = int64(wID)
	cb.ColVec(3).Bytes()[0] = randAString(l.rng, a, 8, 16) // first name
	cb.ColVec(4).Bytes()[0] = middleName
	cb.ColVec(5).Bytes()[0] = lastName
	cb.ColVec(6).Bytes()[0] = randAString(l.rng, a, 10, 20) // street 1
	cb.ColVec(7).Bytes()[0] = randAString(l.rng, a, 10, 20) // street 2
	cb.ColVec(8).Bytes()[0] = randAString(l.rng, a, 10, 20) // city name
	cb.ColVec(9).Bytes()[0] = randState(l.rng, a)
	cb.ColVec(10).Bytes()[0] = randZip(l.rng, a)
	cb.ColVec(11).Bytes()[0] = randNString(l.rng, a, 16, 16) // phone number
	cb.ColVec(12).Bytes()[0] = w.nowString
	cb.ColVec(13).Bytes()[0] = []byte(credit)
	cb.ColVec(14).Float64()[0] = creditLimit
	cb.ColVec(15).Float64()[0] = float64(randInt(l.rng, 0, 5000)) / float64(10000.0) // discount
	cb.ColVec(16).Float64()[0] = balance
	cb.ColVec(17).Float64()[0] = ytdPayment
	cb.ColVec(18).Int64()[0] = paymentCount
	cb.ColVec(19).Int64()[0] = deliveryCount
	cb.ColVec(20).Bytes()[0] = randAString(l.rng, a, 300, 500) // data
}

func (w *tpcc) tpccCustomerStats() []workload.JSONStatistic {
	rowCount := uint64(w.warehouses * numCustomersPerWarehouse)
	// Several of the random alphanumeric strings below have a huge number of
	// possible values, so we assume the number of distinct values equals
	// the row count.
	return []workload.JSONStatistic{
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

var historyColTypes = []types.T{
	types.Bytes,
	types.Int64,
	types.Int64,
	types.Int64,
	types.Int64,
	types.Int64,
	types.Bytes,
	types.Float64,
	types.Bytes,
}

func (w *tpcc) tpccHistoryInitialRowBatch(rowIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	var rowID []byte
	*a, rowID = a.Alloc(36, 0 /* extraCap */)
	uuid.MakeV4().StringBytes(rowID)

	cID := (rowIdx % numCustomersPerDistrict) + 1
	dID := ((rowIdx / numCustomersPerDistrict) % numDistrictsPerWarehouse) + 1
	wID := (rowIdx / numCustomersPerWarehouse)

	cb.Reset(historyColTypes, 1)
	cb.ColVec(0).Bytes()[0] = rowID
	cb.ColVec(1).Int64()[0] = int64(cID)
	cb.ColVec(2).Int64()[0] = int64(dID)
	cb.ColVec(3).Int64()[0] = int64(wID)
	cb.ColVec(4).Int64()[0] = int64(dID)
	cb.ColVec(5).Int64()[0] = int64(wID)
	cb.ColVec(6).Bytes()[0] = w.nowString
	cb.ColVec(7).Float64()[0] = 10.00
	cb.ColVec(8).Bytes()[0] = randAString(l.rng, a, 12, 24)
}

func (w *tpcc) tpccHistoryStats() []workload.JSONStatistic {
	rowCount := uint64(w.warehouses * numCustomersPerWarehouse)
	return []workload.JSONStatistic{
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

var orderColTypes = []types.T{
	types.Int64,
	types.Int64,
	types.Int64,
	types.Int64,
	types.Bytes,
	types.Int64,
	types.Int64,
	types.Int64,
}

func (w *tpcc) tpccOrderInitialRowBatch(rowIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
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

	var carrierSet bool
	var carrierID int64
	if oID < 2101 {
		carrierSet = true
		carrierID = randInt(l.rng, 1, 10)
	}

	cb.Reset(orderColTypes, 1)
	cb.ColVec(0).Int64()[0] = int64(oID)
	cb.ColVec(1).Int64()[0] = int64(dID)
	cb.ColVec(2).Int64()[0] = int64(wID)
	cb.ColVec(3).Int64()[0] = int64(cID)
	cb.ColVec(4).Bytes()[0] = w.nowString
	cb.ColVec(5).Nulls().UnsetNulls()
	if carrierSet {
		cb.ColVec(5).Int64()[0] = carrierID
	} else {
		cb.ColVec(5).Nulls().SetNull64(0)
		cb.ColVec(5).Int64()[0] = 0
	}
	cb.ColVec(6).Int64()[0] = numOrderLines
	cb.ColVec(7).Int64()[0] = 1
}

func (w *tpcc) tpccOrderStats() []workload.JSONStatistic {
	rowCount := uint64(w.warehouses * numOrdersPerWarehouse)
	return []workload.JSONStatistic{
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

var newOrderColTypes = []types.T{
	types.Int64,
	types.Int64,
	types.Int64,
}

func (w *tpcc) tpccNewOrderInitialRowBatch(
	rowIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	// The last numNewOrdersPerDistrict orders have entries in new orders.
	const firstNewOrderOffset = numOrdersPerDistrict - numNewOrdersPerDistrict
	oID := (rowIdx % numNewOrdersPerDistrict) + firstNewOrderOffset + 1
	dID := ((rowIdx / numNewOrdersPerDistrict) % numDistrictsPerWarehouse) + 1
	wID := (rowIdx / numNewOrdersPerWarehouse)

	cb.Reset(newOrderColTypes, 1)
	cb.ColVec(0).Int64()[0] = int64(oID)
	cb.ColVec(1).Int64()[0] = int64(dID)
	cb.ColVec(2).Int64()[0] = int64(wID)
}

func (w *tpcc) tpccNewOrderStats() []workload.JSONStatistic {
	rowCount := uint64(w.warehouses * numNewOrdersPerWarehouse)
	return []workload.JSONStatistic{
		workload.MakeStat([]string{"no_o_id"}, rowCount, numNewOrdersPerDistrict, 0),
		workload.MakeStat([]string{"no_d_id"}, rowCount, numDistrictsPerWarehouse, 0),
		workload.MakeStat([]string{"no_w_id"}, rowCount, uint64(w.warehouses), 0),
	}
}

var orderLineColTypes = []types.T{
	types.Int64,
	types.Int64,
	types.Int64,
	types.Int64,
	types.Int64,
	types.Int64,
	types.Bytes,
	types.Int64,
	types.Float64,
	types.Bytes,
}

func (w *tpcc) tpccOrderLineInitialRowBatch(
	orderRowIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)

	l.rng.Seed(w.seed + uint64(orderRowIdx))
	numOrderLines := int(randInt(l.rng, minOrderLinesPerOrder, maxOrderLinesPerOrder))

	// NB: There is one batch of order_line rows per order
	oID := (orderRowIdx % numOrdersPerDistrict) + 1
	dID := ((orderRowIdx / numOrdersPerDistrict) % numDistrictsPerWarehouse) + 1
	wID := (orderRowIdx / numOrdersPerWarehouse)

	cb.Reset(orderLineColTypes, numOrderLines)
	olOIDCol := cb.ColVec(0).Int64()
	olDIDCol := cb.ColVec(1).Int64()
	olWIDCol := cb.ColVec(2).Int64()
	olNumberCol := cb.ColVec(3).Int64()
	olIIDCol := cb.ColVec(4).Int64()
	olSupplyWIDCol := cb.ColVec(5).Int64()
	olDeliveryD := cb.ColVec(6)
	olDeliveryD.Nulls().UnsetNulls()
	olDeliveryDCol := olDeliveryD.Bytes()
	olQuantityCol := cb.ColVec(7).Int64()
	olAmountCol := cb.ColVec(8).Float64()
	olDistInfoCol := cb.ColVec(9).Bytes()

	for rowIdx := 0; rowIdx < numOrderLines; rowIdx++ {
		olNumber := rowIdx + 1

		var amount float64
		var deliveryDSet bool
		var deliveryD []byte
		if oID < 2101 {
			amount = 0
			deliveryDSet = true
			deliveryD = w.nowString
		} else {
			amount = float64(randInt(l.rng, 1, 999999)) / 100.0
		}

		olOIDCol[rowIdx] = int64(oID)
		olDIDCol[rowIdx] = int64(dID)
		olWIDCol[rowIdx] = int64(wID)
		olNumberCol[rowIdx] = int64(olNumber)
		olIIDCol[rowIdx] = randInt(l.rng, 1, 100000)
		olSupplyWIDCol[rowIdx] = int64(wID)
		if deliveryDSet {
			olDeliveryDCol[rowIdx] = deliveryD
		} else {
			olDeliveryD.Nulls().SetNull64(uint64(rowIdx))
			olDeliveryDCol[rowIdx] = nil
		}
		olQuantityCol[rowIdx] = 5
		olAmountCol[rowIdx] = amount
		olDistInfoCol[rowIdx] = randAString(l.rng, a, 24, 24)
	}
}

func (w *tpcc) tpccOrderLineStats() []workload.JSONStatistic {
	averageOrderLines := float64(maxOrderLinesPerOrder+minOrderLinesPerOrder) / 2
	rowCount := uint64(int64(float64(w.warehouses) * numOrdersPerWarehouse * averageOrderLines))
	deliveryIncomplete := uint64(int64(
		float64(w.warehouses) * numDistrictsPerWarehouse * (numOrdersPerDistrict - 2100) * averageOrderLines,
	))
	return []workload.JSONStatistic{
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
