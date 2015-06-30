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
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package driver

import (
	"log"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/sqlwire"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestSend(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	sender, err := newHTTPSender(s.ServingAddr(), testutils.NewRootTestBaseContext(), defaultRetryOptions)
	if err != nil {
		log.Fatalf("Couldn't create HTTPSender for server:(%s)", s.ServingAddr())
	}
	testCases := []struct {
		req   string
		reply string
	}{
		{"ping", "ping"},
		{"default", "default"},
	}
	for _, test := range testCases {
		request := &sqlwire.Request{Sql: test.req}
		call := sqlwire.Call{Args: request, Reply: &sqlwire.Response{}}
		sender.Send(context.TODO(), call)
		reply := *call.Reply.Results[0].Rows[0].Values[0].StringVal
		if reply != test.reply {
			log.Fatalf("Server sent back reply: %s", reply)
		}
	}
}
