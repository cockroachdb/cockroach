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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

var ballastFile = flag.String("ballast_file", "data/ballast_file_to_fill_store", "ballast file in cockroach data directory")
var fillRatio = flag.Float64("fill_ratio", 0.0, "fraction of cockroach data "+
	"directory to be filled")
var diskLeftInBytes = flag.Int64("disk_left_bytes", 0, "amount of cockroach data "+
	"directory to be left empty")

func main() {
	flag.Parse()
	if *fillRatio < 0 {
		fmt.Println("fill_ratio expected to be positive, got: ", *fillRatio)
		os.Exit(1)
	}
	if *diskLeftInBytes < 0 {
		fmt.Println("disk_left_bytes expected to be positive, got: ", *diskLeftInBytes)
		os.Exit(1)
	}
	if *fillRatio > 0 && *diskLeftInBytes > 0 {
		fmt.Printf(
			"exactly one of fill_ratio or disk_left_bytes expected to be positive, "+
				"got: fill_ratio: %v, disk_left_bytesv: %v\n",
			*fillRatio,
			*diskLeftInBytes,
		)
		os.Exit(1)
	}
	dataDirectory := filepath.Dir(*ballastFile)
	fs := syscall.Statfs_t{}

	err := syscall.Statfs(dataDirectory, &fs)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	total := int64(fs.Blocks) * int64(fs.Bsize)
	free := int64(fs.Bavail) * int64(fs.Bsize)
	used := total - free
	var toFill int64
	if *fillRatio > 0 {
		toFill = int64((*fillRatio) * float64(total))
	}
	if *diskLeftInBytes > 0 {
		toFill = total - *diskLeftInBytes
	}
	if used > toFill {
		fmt.Printf("Used space %v already more than needed to be filled %v\n", used, toFill)
		os.Exit(1)
	}
	if used == toFill {
		return
	}
	ballastSize := toFill - used
	createBallastCommand := exec.Command("fallocate", "-l", fmt.Sprint(ballastSize), *ballastFile)
	if err := createBallastCommand.Run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
