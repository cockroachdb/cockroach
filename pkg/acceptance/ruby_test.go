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

package acceptance

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDockerRuby(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "ruby", []string{"ruby", "-e", strings.Replace(ruby, "%v", "3", 1)})
	testDockerFail(ctx, t, "ruby", []string{"ruby", "-e", strings.Replace(ruby, "%v", `"a"`, 1)})
}

const ruby = `
require 'pg'

conn = PG.connect()
res = conn.exec_params('SELECT 1, 2 > $1, $1', [%v])
raise 'Unexpected: ' + res.values.to_s unless res.values == [["1", "f", "3"]]

res = conn.exec('SELECT 1e1::decimal')
raise 'Unexpected: ' + res.values.to_s unless res.values == [["1E+1"]]
`
