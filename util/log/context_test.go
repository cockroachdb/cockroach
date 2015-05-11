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

// TestContextKV tests adding log fields to a context out of order and
// retrieving them in their proper order.
func TestContextKV(t *testing.T) {
	kvs := []interface{}{Method, "Put", NodeID, 5, Err, fmt.Errorf("everything broke")}
	exp := append([]interface{}(nil), kvs[2], kvs[3], kvs[0], kvs[1], kvs[4], kvs[5])
	ctx := Add(context.Background(), kvs...)
	if ckv := contextKV(ctx); fmt.Sprintf("%+v", exp) != fmt.Sprintf("%+v", ckv) {
		t.Fatalf("expected %+v, got %+v", exp, ckv)
	}
}
