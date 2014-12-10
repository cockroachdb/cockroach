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
// Author: James Graves (james.c.graves.jr@gmail.com)

/*
Compute MD5 sum from standard input.
*/
package main

import (
	"crypto/md5"
	"fmt"
	"os"
)

func main() {
	maxlen := 5000000
	buf := make([]byte, maxlen)
	n, err := os.Stdin.Read(buf)
	if err != nil {
		os.Exit(1)
	}
	if n == maxlen {
		os.Exit(2)
	}

	sum := md5.Sum(buf[:n])
	for _, b := range sum {
		fmt.Printf("%02x", b)
	}
	fmt.Println("")
}
