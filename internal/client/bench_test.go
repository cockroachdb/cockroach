// Copyright 2016 The Cockroach Authors.
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

// KV API benchmarks over a three node cluster

package client_test

import (
	"fmt"
	"math/rand"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
)

func dbForBenchmarks(b *testing.B) (serverutils.TestClusterInterface, *client.DB) {
	c := serverutils.StartTestCluster(b, 3, base.TestClusterArgs{})
	db := createTestClient(b, c.Stopper(), c.Server(0).ServingAddr())

	// Load up some data.
	batch := &client.Batch{}
	for j := 0; j < 10000; j++ {
		key := roachpb.Key(fmt.Sprintf("/key %02d", j))
		batch.Put(key, int64(j))
	}
	if err := db.Run(context.TODO(), batch); err != nil {
		b.Error(err)
	}
	return c, db
}

func BenchmarkBatch10Get(b *testing.B) {
	benchmarkBatchGet(b, 10)
}

func BenchmarkBatch100Get(b *testing.B) {
	benchmarkBatchGet(b, 100)
}

func BenchmarkBatch1000Get(b *testing.B) {
	benchmarkBatchGet(b, 1000)
}

func BenchmarkBatch10000Get(b *testing.B) {
	benchmarkBatchGet(b, 10000)
}

func benchmarkBatchGet(b *testing.B, numKeys int) {
	c, db := dbForBenchmarks(b)
	defer c.Stopper().Stop()

	batch := &client.Batch{}
	perm := rand.Perm(numKeys)
	for j := 0; j < numKeys; j++ {
		key := roachpb.Key(fmt.Sprintf("/key %02d", perm[j]))
		batch.Get(key)
	}

	for i := 0; i < b.N; i++ {
		if err := db.Run(context.TODO(), batch); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkBatch10Put(b *testing.B) {
	benchmarkBatchPut(b, 10)
}

func BenchmarkBatch100Put(b *testing.B) {
	benchmarkBatchPut(b, 100)
}

func BenchmarkBatch1000Put(b *testing.B) {
	benchmarkBatchPut(b, 1000)
}

func BenchmarkBatch10000Put(b *testing.B) {
	benchmarkBatchPut(b, 10000)
}

func benchmarkBatchPut(b *testing.B, numKeys int) {
	c, db := dbForBenchmarks(b)
	defer c.Stopper().Stop()

	batch := &client.Batch{}
	perm := rand.Perm(numKeys)
	for j := 0; j < numKeys; j++ {
		key := roachpb.Key(fmt.Sprintf("/key %02d", perm[j]))
		batch.Put(key, int64(perm[j]))
	}

	for i := 0; i < b.N; i++ {
		if err := db.Run(context.TODO(), batch); err != nil {
			b.Error(err)
		}
	}
}
