// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package covering

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func BenchmarkOverlapCoveringMerge(b *testing.B) {

	benchmarks := []struct {
		// benchmark test
		name string
		// rate we would like to "simulate" backups
		backupRate int
		// period which backups is going to cover during benchmark
		backupPeriod int
		// amount of coverings (ranges) we going to have per each period
		coveringsPerPeriod int
	}{
		{
			name:               "OneDaysHourlyBackup_10000Coverings",
			backupRate:         1,
			backupPeriod:       1,
			coveringsPerPeriod: 10000,
		},
		{
			name:               "TwoDaysHourlyBackup_10000Coverings",
			backupRate:         1,
			backupPeriod:       2,
			coveringsPerPeriod: 10000,
		},
		{
			name:               "WeekHourlyBackup_10000Coverings",
			backupRate:         1,
			backupPeriod:       7,
			coveringsPerPeriod: 10000,
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Prepare benchmark input
			b.StopTimer()
			var inputs []Covering
			rand.Seed(timeutil.Now().Unix())
			backupsRate := 1  // hourly
			backupPeriod := 2 // two days
			coveringsPerPeriod := 10000

			for i := 0; i < 24*backupsRate*backupPeriod; i++ {
				var payload int
				var c Covering
				step := 1 + rand.Intn(10)

				for j := 0; j < coveringsPerPeriod; j += step {
					start := make([]byte, 4)
					binary.LittleEndian.PutUint32(start, uint32(j))

					end := make([]byte, 4)
					binary.LittleEndian.PutUint32(end, uint32(j+step))

					c = append(c, Range{
						Start:   start,
						End:     end,
						Payload: payload,
					})
					payload++
				}

				inputs = append(inputs, c)
			}

			// Having input prepared next we run the benchmark itself
			b.StartTimer()

			for i := 0; i < b.N; i++ {
				OverlapCoveringMerge(inputs)
			}
		})
	}
}
