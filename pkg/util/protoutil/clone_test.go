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

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestCloneProto(t *testing.T) {
	u := uuid.MakeV4()

	testCases := []struct {
		pb          proto.Message
		shouldPanic bool
	}{
		// StoreIdent contains a UUID by value, so it is always uncloneable.
		{&roachpb.StoreIdent{}, true},
		{&roachpb.StoreIdent{ClusterID: uuid.MakeV4()}, true},
		// TxnMeta contains a UUID by pointer, so it is cloneable if the id is nil.
		{&enginepb.TxnMeta{}, false},
		{&enginepb.TxnMeta{ID: &u}, true},
		{&roachpb.Transaction{}, false},
		{&roachpb.Error{}, false},
		{&roachpb.Error{UnexposedTxn: &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{ID: &u}}}, true},
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
			} else {
				if panicStr := fmt.Sprint(panicObj); !strings.Contains(panicStr, "attempt to clone") {
					t.Errorf("%T: got unexpected panic %s", tc.pb, panicStr)
				}
			}
		} else {
			if panicObj != nil {
				t.Errorf("%T: got unexpected panic %v", tc.pb, panicObj)
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
