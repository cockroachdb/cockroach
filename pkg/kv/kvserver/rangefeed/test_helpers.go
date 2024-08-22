// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func generateRandomizedTs(rand *rand.Rand) hlc.Timestamp {
	// Avoid generating zero timestamp which will equal to an empty event.
	return hlc.Timestamp{WallTime: int64(rand.Intn(100)) + 1}
}

func generateStartAndEndKey(rand *rand.Rand) (roachpb.Key, roachpb.Key) {
	start := rand.Intn(2 << 20)
	end := start + rand.Intn(2<<20)
	startDatum := tree.NewDInt(tree.DInt(start))
	endDatum := tree.NewDInt(tree.DInt(end))
	const tableID = 42

	startKey, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		startDatum,
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}

	endKey, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		endDatum,
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}
	return startKey, endKey
}
