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
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package security_test

import (
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	_ "github.com/cockroachdb/cockroach/util/log" // for flags
)

func init() {
	ResetTest()
}

// ResetTest sets up the test environment. In particular, it embeds the
// EmbeddedCertsDir folder and makes the tls package load from there.
func ResetTest() {
	security.SetReadFileFn(securitytest.Asset)
}

//go:generate ../util/leaktest/add-leaktest.sh *_test.go
