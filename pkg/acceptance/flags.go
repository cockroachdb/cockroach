// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package acceptance

import (
	"flag"
	"time"
)

var flagDuration = flag.Duration("d", 5*time.Second, "for duration-limited tests, how long to run them for")
var flagLogDir = flag.String("l", "", "the directory to store log files, relative to the test source")
