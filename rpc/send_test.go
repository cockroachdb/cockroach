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
// Author: joezxy (joe.zxy@foxmail.com)

package rpc

import (
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/util"
)

func TestInvalidAddrLength(t *testing.T) {
	testAddrs := []net.Addr{
		util.MakeRawAddr("tcp", "127.0.0.1:8080"),
		util.MakeRawAddr("tcp", "127.0.0.1:8081"),
		util.MakeRawAddr("tcp", "127.0.0.1:8082"),
	}

	testSendOption := Options{
		N: 5,
	}

	ret, _ := Send(testSendOption, "", testAddrs, nil, nil, nil)
	if ret != nil {
		t.Fatalf("Shorter addrs should have nil and SendError return")
	}
}
