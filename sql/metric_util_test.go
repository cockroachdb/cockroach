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
// Author: Andrei Matei (andreimatei1@gmail.com)

// Functions used for testing metrics.

package sql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/pkg/errors"
)

func checkCounterEQ(
	t *testing.T, s serverutils.TestServerInterface, key string, e int64,
) {
	if a := s.MustGetSQLCounter(key); a != e {
		t.Error(errors.Errorf("stat %s: actual %d != expected %d", key, a, e))
	}
}

func checkCounterGE(
	t *testing.T, s serverutils.TestServerInterface, key string, e int64,
) {
	if a := s.MustGetSQLCounter(key); a < e {
		t.Error(errors.Errorf("stat %s: expected: actual %d >= %d", key, a, e))
	}
}
