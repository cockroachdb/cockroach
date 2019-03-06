// Copyright 2019 The Cockroach Authors.
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

package sqlsmith

import (
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	_ "github.com/lib/pq"
)

func TestGenerate(t *testing.T) {
	t.Skip("used in local dev only")

	db, err := gosql.Open("postgres", "user=root port=26257 sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	rnd, _ := randutil.NewPseudoRand()
	smither, err := NewSmither(db, rnd)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		fmt.Println(smither.Generate())
	}
}
