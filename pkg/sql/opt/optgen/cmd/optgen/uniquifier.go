// Copyright 2018 The Cockroach Authors.
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

package main

import (
	"fmt"
)

// uniquifier generates a unique variable name from an input string by
// appending an incrementing counter in case of collisions with previously
// added names.
type uniquifier struct {
	seen map[string]struct{}
}

func (u *uniquifier) init() {
	u.seen = make(map[string]struct{})
}

func (u *uniquifier) makeUnique(s string) string {
	try := s
	for i := 2; ; i++ {
		_, ok := u.seen[try]
		if !ok {
			u.seen[try] = struct{}{}
			return try
		}

		try = fmt.Sprintf("%s%d", s, i)
	}
}
