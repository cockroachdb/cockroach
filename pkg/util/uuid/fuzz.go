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

// Copyright (c) 2018 Andrei Tudor Călin <mail@acln.ro>
// Use of this source code is governed by a MIT-style
// license that can be found in licenses/MIT-gofrs.txt.

// This code originated in github.com/gofrs/uuid.

// +build gofuzz

package uuid

// Fuzz implements a simple fuzz test for FromString / UnmarshalText.
//
// To run:
//
//     $ go get github.com/dvyukov/go-fuzz/...
//     $ cd $GOPATH/src/github.com/gofrs/uuid
//     $ go-fuzz-build github.com/gofrs/uuid
//     $ go-fuzz -bin=uuid-fuzz.zip -workdir=./testdata
//
// If you make significant changes to FromString / UnmarshalText and add
// new cases to fromStringTests (in codec_test.go), please run
//
//    $ go test -seed_fuzz_corpus
//
// to seed the corpus with the new interesting inputs, then run the fuzzer.
func Fuzz(data []byte) int {
	_, err := FromString(string(data))
	if err != nil {
		return 0
	}
	return 1
}
