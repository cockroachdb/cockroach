// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlstatstestutil

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
)

// GetRandomizedCollectedStatementStatisticsForTest returns a
// appstatspb.CollectedStatementStatistics with its fields randomly filled.
func GetRandomizedCollectedStatementStatisticsForTest(
	t *testing.T,
) (result appstatspb.CollectedStatementStatistics) {
	data := sqlstatsutil.GenRandomData()
	sqlstatsutil.FillObject(t, reflect.ValueOf(&result), &data)

	return result
}
