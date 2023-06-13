// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build linux
// +build linux

package status

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestSumAndFilterDiskCountersLinux(t *testing.T) {
	defer leaktest.AfterTest(t)()

	counters := []DiskStats{
		{
			Name:           "nvme0",
			ReadBytes:      1,
			readCount:      1,
			iopsInProgress: 1,
			WriteBytes:     1,
			writeCount:     1,
		},
		{ // This must be excluded from the sum.
			Name:           "sda1",
			ReadBytes:      100,
			readCount:      100,
			iopsInProgress: 100,
			WriteBytes:     100,
			writeCount:     100,
		},
		{
			Name:           "nvme1n1",
			ReadBytes:      1,
			readCount:      1,
			iopsInProgress: 1,
			WriteBytes:     1,
			writeCount:     1,
		},
	}
	summed, err := sumAndFilterDiskCounters(counters)
	if err != nil {
		t.Fatalf("error: %s", err.Error())
	}
	expected := DiskStats{
		ReadBytes:      2,
		readCount:      2,
		WriteBytes:     2,
		writeCount:     2,
		iopsInProgress: 2,
	}
	if !reflect.DeepEqual(summed, expected) {
		t.Fatalf("expected %+v; got %+v", expected, summed)
	}
}
