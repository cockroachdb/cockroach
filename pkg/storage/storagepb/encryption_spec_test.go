// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storagepb

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestNewStoreEncryptionSpec verifies that the --enterprise-encryption arguments are correctly parsed
// into StoreEncryptionSpecs.
func TestNewStoreEncryptionSpec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	absDataPath, err := filepath.Abs("data")
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		value       string
		expectedErr string
		expected    StoreEncryptionSpec
	}{
		// path
		{",", "no path specified", StoreEncryptionSpec{}},
		{"", "no path specified", StoreEncryptionSpec{}},
		{"/mnt/hda1", "field not in the form <key>=<value>: /mnt/hda1", StoreEncryptionSpec{}},
		{"path=", "no value specified for path", StoreEncryptionSpec{}},
		{"path=~/data", "path cannot start with '~': ~/data", StoreEncryptionSpec{}},
		{"path=data,path=data2", "path field was used twice in encryption definition", StoreEncryptionSpec{}},

		// The same logic applies to key and old-key, don't repeat everything.
		{"path=data", "no key specified", StoreEncryptionSpec{}},
		{"path=data,key=new.key", "no old-key specified", StoreEncryptionSpec{}},

		// Rotation period.
		{"path=data,key=new.key,old-key=old.key,rotation-period", "field not in the form <key>=<value>: rotation-period", StoreEncryptionSpec{}},
		{"path=data,key=new.key,old-key=old.key,rotation-period=", "no value specified for rotation-period", StoreEncryptionSpec{}},
		{"path=data,key=new.key,old-key=old.key,rotation-period=1", `could not parse rotation-duration value: 1: time: missing unit in duration "1"`, StoreEncryptionSpec{}},
		{"path=data,key=new.key,old-key=old.key,rotation-period=1d", `could not parse rotation-duration value: 1d: time: unknown unit "d" in duration "1d"`, StoreEncryptionSpec{}},

		// Good values. Note that paths get absolutized so we start most of them
		// with / so we can used fixed expected values.
		{"path=/data,key=/new.key,old-key=/old.key", "", StoreEncryptionSpec{Path: "/data", KeyPath: "/new.key", OldKeyPath: "/old.key", RotationPeriod: DefaultRotationPeriod}},
		{"path=/data,key=/new.key,old-key=/old.key,rotation-period=1h", "", StoreEncryptionSpec{Path: "/data", KeyPath: "/new.key", OldKeyPath: "/old.key", RotationPeriod: time.Hour}},
		{"path=/data,key=plain,old-key=/old.key,rotation-period=1h", "", StoreEncryptionSpec{Path: "/data", KeyPath: "plain", OldKeyPath: "/old.key", RotationPeriod: time.Hour}},
		{"path=/data,key=/new.key,old-key=plain,rotation-period=1h", "", StoreEncryptionSpec{Path: "/data", KeyPath: "/new.key", OldKeyPath: "plain", RotationPeriod: time.Hour}},

		// One relative path to test absolutization.
		{"path=data,key=/new.key,old-key=/old.key", "", StoreEncryptionSpec{Path: absDataPath, KeyPath: "/new.key", OldKeyPath: "/old.key", RotationPeriod: DefaultRotationPeriod}},

		// Special path * is not absolutized.
		{"path=*,key=/new.key,old-key=/old.key", "", StoreEncryptionSpec{Path: "*", KeyPath: "/new.key", OldKeyPath: "/old.key", RotationPeriod: DefaultRotationPeriod}},
	}

	for i, testCase := range testCases {
		storeEncryptionSpec, err := NewStoreEncryptionSpec(testCase.value)
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
		if !reflect.DeepEqual(testCase.expected, storeEncryptionSpec) {
			t.Errorf("%d(%s): actual doesn't match expected\nactual:   %+v\nexpected: %+v", i,
				testCase.value, storeEncryptionSpec, testCase.expected)
		}

		// Now test String() to make sure the result can be parsed.
		storeEncryptionSpecString := storeEncryptionSpec.String()
		storeEncryptionSpec2, err := NewStoreEncryptionSpec(storeEncryptionSpecString)
		if err != nil {
			t.Errorf("%d(%s): error parsing String() result: %s", i, testCase.value, err)
			continue
		}
		// Compare strings to deal with floats not matching exactly.
		if !reflect.DeepEqual(storeEncryptionSpecString, storeEncryptionSpec2.String()) {
			t.Errorf("%d(%s): actual doesn't match expected\nactual:   %#+v\nexpected: %#+v", i, testCase.value,
				storeEncryptionSpec, storeEncryptionSpec2)
		}
	}
}
