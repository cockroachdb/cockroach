// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestNewStoreSpec verifies that the --store arguments are correctly parsed
// into StoreSpecs.
func TestNewStoreSpec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const examplePebbleOptions = `[Version]
pebble_version=0.1
[Options]
bytes_per_sync=524288
cleaner=delete
disable_wal=true
l0_compaction_threshold=4
l0_stop_writes_threshold=12
lbase_max_bytes=67108864
max_concurrent_compactions=1
max_manifest_file_size=134217728
max_open_files=1000
mem_table_size=4194304
mem_table_stop_writes_threshold=2
min_compaction_rate=4194304
min_flush_rate=1048576
table_property_collectors=[]
wal_dir=
[Level "0"]
block_restart_interval=16
block_size=4096
compression=Snappy
index_block_size=4096
target_file_size=2097152`

	testCases := []struct {
		value       string
		expectedErr string
		expected    base.StoreSpec
	}{
		// path
		{"path=/mnt/hda1", "", StoreSpec{Path: "/mnt/hda1"}},
		{",path=/mnt/hda1", "", StoreSpec{Path: "/mnt/hda1"}},
		{"path=/mnt/hda1,", "", StoreSpec{Path: "/mnt/hda1"}},
		{",,,path=/mnt/hda1,,,", "", StoreSpec{Path: "/mnt/hda1"}},
		{"/mnt/hda1", "", StoreSpec{Path: "/mnt/hda1"}},
		{"path=", "no value specified for path", StoreSpec{}},
		{"path=/mnt/hda1,path=/mnt/hda2", "path field was used twice in store definition", StoreSpec{}},
		{"/mnt/hda1,path=/mnt/hda2", "path field was used twice in store definition", StoreSpec{}},

		// attributes
		{"path=/mnt/hda1,attrs=ssd", "", StoreSpec{
			Path:       "/mnt/hda1",
			Attributes: roachpb.Attributes{Attrs: []string{"ssd"}},
		}},
		{"path=/mnt/hda1,attrs=ssd:hdd", "", StoreSpec{
			Path:       "/mnt/hda1",
			Attributes: roachpb.Attributes{Attrs: []string{"hdd", "ssd"}},
		}},
		{"path=/mnt/hda1,attrs=hdd:ssd", "", StoreSpec{
			Path:       "/mnt/hda1",
			Attributes: roachpb.Attributes{Attrs: []string{"hdd", "ssd"}},
		}},
		{"attrs=ssd:hdd,path=/mnt/hda1", "", StoreSpec{
			Path:       "/mnt/hda1",
			Attributes: roachpb.Attributes{Attrs: []string{"hdd", "ssd"}},
		}},
		{"attrs=hdd:ssd,path=/mnt/hda1,", "", StoreSpec{
			Path:       "/mnt/hda1",
			Attributes: roachpb.Attributes{Attrs: []string{"hdd", "ssd"}},
		}},
		{"attrs=hdd:ssd", "no path specified", StoreSpec{}},
		{"path=/mnt/hda1,attrs=", "no value specified for attrs", StoreSpec{}},
		{"path=/mnt/hda1,attrs=hdd:hdd", "duplicate attribute given for store: hdd", StoreSpec{}},
		{"path=/mnt/hda1,attrs=hdd,attrs=ssd", "attrs field was used twice in store definition", StoreSpec{}},

		// size
		{"path=/mnt/hda1,size=671088640", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 671088640}}},
		{"path=/mnt/hda1,size=20GB", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 20000000000}}},
		{"size=20GiB,path=/mnt/hda1", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 21474836480}}},
		{"size=0.1TiB,path=/mnt/hda1", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 109951162777}}},
		{"path=/mnt/hda1,size=.1TiB", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 109951162777}}},
		{"path=/mnt/hda1,size=123TB", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 123000000000000}}},
		{"path=/mnt/hda1,size=123TiB", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 135239930216448}}},
		// %
		{"path=/mnt/hda1,size=50.5%", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 50.5}}},
		{"path=/mnt/hda1,size=100%", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 100}}},
		{"path=/mnt/hda1,size=1%", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 1}}},
		{"path=/mnt/hda1,size=0.999999%", "store size (0.999999%) must be between 1.000000% and 100.000000%", StoreSpec{}},
		{"path=/mnt/hda1,size=100.0001%", "store size (100.0001%) must be between 1.000000% and 100.000000%", StoreSpec{}},
		// 0.xxx
		{"path=/mnt/hda1,size=0.99", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 99}}},
		{"path=/mnt/hda1,size=0.5000000", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 50}}},
		{"path=/mnt/hda1,size=0.01", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 1}}},
		{"path=/mnt/hda1,size=0.009999", "store size (0.009999) must be between 1.000000% and 100.000000%", StoreSpec{}},
		// .xxx
		{"path=/mnt/hda1,size=.999", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 99.9}}},
		{"path=/mnt/hda1,size=.5000000", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 50}}},
		{"path=/mnt/hda1,size=.01", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 1}}},
		{"path=/mnt/hda1,size=.009999", "store size (.009999) must be between 1.000000% and 100.000000%", StoreSpec{}},
		// errors
		{"path=/mnt/hda1,size=0", "store size (0) must be larger than 640 MiB", StoreSpec{}},
		{"path=/mnt/hda1,size=abc", "could not parse store size (abc) strconv.ParseFloat: parsing \"\": invalid syntax", StoreSpec{}},
		{"path=/mnt/hda1,size=", "no value specified for size", StoreSpec{}},
		{"size=20GiB,path=/mnt/hda1,size=20GiB", "size field was used twice in store definition", StoreSpec{}},
		{"size=123TB", "no path specified", StoreSpec{}},

		// type
		{"type=mem,size=20GiB", "", StoreSpec{Size: SizeSpec{InBytes: 21474836480}, InMemory: true}},
		{"size=20GiB,type=mem", "", StoreSpec{Size: SizeSpec{InBytes: 21474836480}, InMemory: true}},
		{"size=20.5GiB,type=mem", "", StoreSpec{Size: SizeSpec{InBytes: 22011707392}, InMemory: true}},
		{"size=20GiB,type=mem,attrs=mem", "", StoreSpec{
			Size:       SizeSpec{InBytes: 21474836480},
			InMemory:   true,
			Attributes: roachpb.Attributes{Attrs: []string{"mem"}},
		}},
		{"type=mem,size=20", "store size (20) must be larger than 640 MiB", StoreSpec{}},
		{"type=mem,size=", "no value specified for size", StoreSpec{}},
		{"type=mem,attrs=ssd", "size must be specified for an in memory store", StoreSpec{}},
		{"path=/mnt/hda1,type=mem", "path specified for in memory store", StoreSpec{}},
		{"path=/mnt/hda1,type=other", "other is not a valid store type", StoreSpec{}},
		{"path=/mnt/hda1,type=mem,size=20GiB", "path specified for in memory store", StoreSpec{}},

		// RocksDB
		{"path=/,rocksdb=key1=val1;key2=val2", "", StoreSpec{Path: "/", RocksDBOptions: "key1=val1;key2=val2"}},

		// Pebble
		{"path=/,pebble=[Options] l0_compaction_threshold=2 l0_stop_writes_threshold=10", "", StoreSpec{Path: "/",
			PebbleOptions: "[Options]\nl0_compaction_threshold=2\nl0_stop_writes_threshold=10"}},
		{fmt.Sprintf("path=/,pebble=%s", examplePebbleOptions), "", StoreSpec{Path: "/", PebbleOptions: examplePebbleOptions}},
		{"path=/mnt/hda1,pebble=[Options] not_a_real_option=10", "pebble: unknown option: Options.not_a_real_option", StoreSpec{}},

		// all together
		{"path=/mnt/hda1,attrs=hdd:ssd,size=20GiB", "", StoreSpec{
			Path:       "/mnt/hda1",
			Size:       SizeSpec{InBytes: 21474836480},
			Attributes: roachpb.Attributes{Attrs: []string{"hdd", "ssd"}},
		}},
		{"type=mem,attrs=hdd:ssd,size=20GiB", "", StoreSpec{
			Size:       SizeSpec{InBytes: 21474836480},
			InMemory:   true,
			Attributes: roachpb.Attributes{Attrs: []string{"hdd", "ssd"}},
		}},

		// other error cases
		{"", "no value specified", StoreSpec{}},
		{",", "no path specified", StoreSpec{}},
		{",,,", "no path specified", StoreSpec{}},
		{"path=/mnt/hda1,something=abc", "something is not a valid store field", StoreSpec{}},
		{"something=abc", "something is not a valid store field", StoreSpec{}},
		{"type=mem,other=abc", "other is not a valid store field", StoreSpec{}},
	}

	for i, testCase := range testCases {
		storeSpec, err := base.NewStoreSpec(testCase.value)
		if err != nil {
			if len(testCase.expectedErr) == 0 {
				t.Errorf("%d(%s): no expected error, got %s", i, testCase.value, err)
			}
			if testCase.expectedErr != fmt.Sprint(err) {
				t.Errorf("%d(%s): expected error \"%s\" does not match actual \"%s\"", i, testCase.value,
					testCase.expectedErr, err)
			}
			continue
		}
		if len(testCase.expectedErr) > 0 {
			t.Errorf("%d(%s): expected error %s but there was none", i, testCase.value, testCase.expectedErr)
			continue
		}
		if !reflect.DeepEqual(testCase.expected, storeSpec) {
			t.Errorf("%d(%s): actual doesn't match expected\nactual:   %+v\nexpected: %+v", i,
				testCase.value, storeSpec, testCase.expected)
		}

		// Now test String() to make sure the result can be parsed.
		storeSpecString := storeSpec.String()
		storeSpec2, err := base.NewStoreSpec(storeSpecString)
		if err != nil {
			t.Errorf("%d(%s): error parsing String() result: %s", i, testCase.value, err)
			continue
		}
		// Compare strings to deal with floats not matching exactly.
		if !reflect.DeepEqual(storeSpecString, storeSpec2.String()) {
			t.Errorf("%d(%s): actual doesn't match expected\nactual:   %#+v\nexpected: %#+v", i, testCase.value,
				storeSpec, storeSpec2)
		}
	}
}

// StoreSpec aliases base.StoreSpec for convenience.
type StoreSpec = base.StoreSpec

// SizeSpec aliases base.SizeSpec for convenience.
type SizeSpec = base.SizeSpec

func TestJoinListType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		join string
		exp  string
		err  string
	}{
		{"", "", "no address specified in --join"},
		{":", "--join=:" + base.DefaultPort, ""},
		{"a", "--join=a:" + base.DefaultPort, ""},
		{"a,b", "--join=a:" + base.DefaultPort + " --join=b:" + base.DefaultPort, ""},
		{"a,,b", "--join=a:" + base.DefaultPort + " --join=b:" + base.DefaultPort, ""},
		{",a", "--join=a:" + base.DefaultPort, ""},
		{"a,", "--join=a:" + base.DefaultPort, ""},
		{"a:123,b", "--join=a:123 --join=b:" + base.DefaultPort, ""},
		{"[::1]:123,b", "--join=[::1]:123 --join=b:" + base.DefaultPort, ""},
		{"[::1,b", "", `address \[::1: missing ']' in address`},
	}

	for _, test := range testData {
		t.Run(test.join, func(t *testing.T) {
			var jls base.JoinListType
			err := jls.Set(test.join)
			if !testutils.IsError(err, test.err) {
				t.Fatalf("error: expected %q, got: %+v", test.err, err)
			}
			if test.err != "" {
				return
			}
			actual := jls.String()
			if actual != test.exp {
				t.Errorf("expected: %q, got: %q", test.exp, actual)
			}
		})
	}
}

func TestStoreSpecListPreventedStartupMessage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	boomStoreDir := filepath.Join(dir, "boom")
	boomAuxDir := filepath.Join(boomStoreDir, base.AuxiliaryDir)
	okStoreDir := filepath.Join(dir, "ok")
	okAuxDir := filepath.Join(okStoreDir, base.AuxiliaryDir)

	for _, sd := range []string{boomAuxDir, okAuxDir} {
		require.NoError(t, os.MkdirAll(sd, 0755))
	}

	ssl := base.StoreSpecList{
		Specs: []base.StoreSpec{
			{Path: "foo", InMemory: true},
			{Path: okStoreDir},
			{Path: boomStoreDir},
		},
	}

	err := ssl.PriorCriticalAlertError()
	require.NoError(t, err)

	require.NoError(t, ioutil.WriteFile(ssl.Specs[2].PreventedStartupFile(), []byte("boom"), 0644))

	err = ssl.PriorCriticalAlertError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "startup forbidden by prior critical alert")
	require.Contains(t, errors.FlattenDetails(err), "boom")
}
