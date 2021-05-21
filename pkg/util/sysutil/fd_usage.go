// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sysutil

// ProcFDUsage is used to retrieve the file descriptor usage of the
// _current_ process.
//
// Its API follows the pattern of the gosigar library.
//
//```
// fdu := &ProcFDUsage{}
// if err := fdu.Get(); err != nil {
//    return err
// }
//```
type ProcFDUsage struct {
	Open      uint64
	SoftLimit uint64
	HardLimit uint64
}
