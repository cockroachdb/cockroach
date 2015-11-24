// Copyright 2014 The Cockroach Authors.
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
// Author: Veteran Lu (23907238@qq.com)

package roachpb

import "testing"

// TestBatchRequestGetArg tests that the method for BatchRequest.GetArg
func TestBatchRequestGetArg(t *testing.T) {
	testCases := []struct {
		bu  []RequestUnion
		exp bool
	}{
		{[]RequestUnion{}, false},
		{[]RequestUnion{{Get: &GetRequest{}}}, false},
		{[]RequestUnion{{EndTransaction: &EndTransactionRequest{}}, {Get: &GetRequest{}}}, false},
		{[]RequestUnion{{EndTransaction: &EndTransactionRequest{}}}, true},
		{[]RequestUnion{{Get: &GetRequest{}}, {EndTransaction: &EndTransactionRequest{}}}, true},
	}

	for i, c := range testCases {
		br := BatchRequest{Requests: c.bu}
		if _, r := br.GetArg(EndTransaction); r != c.exp {
			t.Errorf("%d: unexpected next bytes for %v: %v", i, c.bu, c.exp)
		}
	}
}
