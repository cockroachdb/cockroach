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
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package storagebase

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func makeTS(walltime int64, logical int32) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime: walltime,
		Logical:  logical,
	}
}

func TestQueueState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	qs := QueueState{}
	if ts := qs.GetLastProcessed("foo"); ts != hlc.ZeroTimestamp {
		t.Errorf("expected %s; got %s", hlc.ZeroTimestamp, ts)
	}
	fooTS := makeTS(200, 0)
	qs.SetLastProcessed("foo", fooTS)
	if ts := qs.GetLastProcessed("foo"); ts != fooTS {
		t.Errorf("expected %s; got %s", fooTS, ts)
	}
}
