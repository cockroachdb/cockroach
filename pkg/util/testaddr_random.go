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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

// +build linux

package util

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func init() {
	r, _ := randutil.NewPseudoRand()
	// 127.255.255.255 is special (broadcast), so choose values less
	// than 255.
	a := r.Intn(255)
	b := r.Intn(255)
	c := r.Intn(255)
	IsolatedTestAddr = NewUnresolvedAddr("tcp", fmt.Sprintf("127.%d.%d.%d:0", a, b, c))
}
