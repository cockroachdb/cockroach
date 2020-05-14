// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright 2017 Raphael 'kena' Poss
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

// +build !darwin,!freebsd,!linux,!openbsd,!netbsd,!dragonfly

package libedit

import (
	"os"

	edit "github.com/cockroachdb/cockroach/pkg/go-libedit/other"
)

type EditLine = edit.EditLine

func Init(x string, w bool) (EditLine, error) { return edit.Init(x, w) }
func InitFiles(a string, w bool, stdin, stdout, stderr *os.File) (EditLine, error) {
	return edit.InitFiles(a, w, stdin, stdout, stderr)
}
