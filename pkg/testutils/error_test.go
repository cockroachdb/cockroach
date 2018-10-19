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

package testutils

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/petermattis/goid"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"time"
	"runtime/debug"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestIsSQLRetryableError(t *testing.T) {
	errAmbiguous := &roachpb.AmbiguousResultError{}
	if !IsSQLRetryableError(roachpb.NewError(errAmbiguous).GoError()) {
		t.Fatalf("%s should be a SQLRetryableError", errAmbiguous)
	}
}

func TestGoroutine(t *testing.T) {
	ctx := context.TODO()

	var a int
	f := func() {
		myid := goid.Get()
		tBegin := timeutil.Now()
		log.Infof(ctx, "my id is %d", myid)
		time.Sleep(time.Second)
		debug.PrintStack()
		for timeutil.Since(tBegin) < 3*time.Second {
			a = 5
		}
	}
	go f()
	f()
	t.Log(a)
}
