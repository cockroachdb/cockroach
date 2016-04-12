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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package protoutil_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/cockroachdb/cockroach/util/uuid"
)

func TestCloneProto(t *testing.T) {
	testCases := []struct {
		pb          proto.Message
		shouldPanic bool
	}{
		{&roachpb.StoreIdent{}, false},
		{&roachpb.StoreIdent{ClusterID: uuid.MakeV4()}, true},
		{&roachpb.TxnMeta{}, false},
		{&roachpb.TxnMeta{ID: uuid.NewV4()}, true},
		{&roachpb.Transaction{}, false},
		{&config.ZoneConfig{RangeMinBytes: 123, RangeMaxBytes: 456}, false},
	}
	for _, tc := range testCases {
		var clone proto.Message
		var panicObj interface{}
		func() {
			defer func() {
				panicObj = recover()
			}()
			clone = protoutil.Clone(tc.pb)
		}()

		if tc.shouldPanic {
			if panicObj == nil {
				t.Errorf("%T: expected panic but didn't get one", tc.pb)
			}
		} else {
			if panicObj != nil {
				if panicStr := fmt.Sprint(panicObj); !strings.Contains(panicStr, "attempt to clone") {
					t.Errorf("%T: got unexpected panic %s", tc.pb, panicStr)
				}
			}
		}

		if panicObj == nil {
			realClone := proto.Clone(tc.pb)
			if !reflect.DeepEqual(clone, realClone) {
				t.Errorf("%T: clone did not equal original. expected:\n%+v\ngot:\n%+v", tc.pb, realClone, clone)
			}
		}
	}
}
