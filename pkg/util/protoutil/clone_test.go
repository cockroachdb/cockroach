// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protoutil_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/gogo/protobuf/proto"
)

func TestCloneProto(t *testing.T) {
	testCases := []struct {
		pb          protoutil.Message
		shouldPanic bool
	}{
		// Uncloneable types (all contain UUID fields).
		{&roachpb.StoreIdent{}, true},
		{&enginepb.TxnMeta{}, true},
		{&roachpb.Transaction{}, true},
		{&roachpb.Error{}, true},
		{&protoutil.RecursiveAndUncloneable{}, true},

		// Cloneable types. This includes all types for which a
		// protoutil.Clone call exists in the codebase as of 2016-11-21.
		{&zonepb.ZoneConfig{}, false},
		{&gossip.Info{}, false},
		{&gossip.BootstrapInfo{}, false},
		{&descpb.IndexDescriptor{}, false},
		{&roachpb.SplitTrigger{}, false},
		{&roachpb.Value{}, false},
		{&kvserverpb.ReplicaState{}, false},
		{&roachpb.RangeDescriptor{}, false},
		{&descpb.PartitioningDescriptor{}, false},
	}
	for _, tc := range testCases {
		var clone protoutil.Message
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
