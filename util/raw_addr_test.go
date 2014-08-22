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
// Author: jqmp (jaqueramaphan@gmail.com)

package util

import "testing"

func TestRawAddr(t *testing.T) {
	network := "tcp"
	str := "host:1234"
	addr := MakeRawAddr(network, str)

	if addr.Network() != network {
		t.Errorf("Expected addr.Network() to be %s; got %s", network, addr.Network())
	}
	if addr.String() != str {
		t.Errorf("Expected addr.String() to be %s; got %s", str, addr.String())
	}
}
