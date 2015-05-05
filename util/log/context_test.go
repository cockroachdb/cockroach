// Copyright 2015 The Cockroach Authors.
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
//
// Author: Tobias Schottdorf

package log

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"
)

func TestContextLog(t *testing.T) {
	ctx := Add(context.Background(),
		Method, "Put",
		NodeID, 5,
		Err, fmt.Errorf("everything broke"))
	expS := "NodeID=5 Method=Put Err=everything broke"
	if s := contextKV(ctx).String(); s != expS {
		t.Errorf("formatted output %s != %s", s, expS)
	}
	Warningc(ctx, "just testing")

	ctx = Add(context.Background(), "not_a_field", 5)
	if s := contextKV(ctx).String(); s != "" {
		t.Errorf("wanted empty string, not %s", s)
	}
}
