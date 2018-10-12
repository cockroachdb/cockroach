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

package main

import "fmt"

func dump(dirs []string) error {
	switch n := len(dirs); n {
	case 0:
		return fmt.Errorf("no test directory specified")
	case 1, 2:
		d1, err := loadTestData(dirs[0])
		if err != nil {
			return err
		}
		if n == 1 {
			return dump1(d1)
		}
		d2, err := loadTestData(dirs[1])
		if err != nil {
			return err
		}
		return dump2(d1, d2)
	default:
		return fmt.Errorf("too many test directories: %s", dirs)
	}
}

func dump1(d *testData) error {
	fmt.Println(d.Metadata.Test)
	fmt.Println("_____N_____ops/sec__avg(ms)__p50(ms)__p95(ms)__p99(ms)")
	for _, r := range d.Runs {
		fmt.Printf("%6d %11.1f %8.1f %8.1f %8.1f %8.1f\n", r.Concurrency,
			r.OpsSec, r.AvgLat, r.P50Lat, r.P95Lat, r.P99Lat)
	}
	return nil
}

func dump2(d1, d2 *testData) error {
	d1, d2 = alignTestData(d1, d2)
	fmt.Println(d1.Metadata.Test)
	fmt.Println("_____N__ops/sec(1)__ops/sec(2)_____delta")
	for i := range d1.Runs {
		r1 := d1.Runs[i]
		r2 := d2.Runs[i]
		fmt.Printf("%6d %11.1f %11.1f %8.2f%%\n",
			r1.Concurrency, r1.OpsSec, r2.OpsSec, 100*(r2.OpsSec-r1.OpsSec)/r1.OpsSec)
	}
	return nil
}
