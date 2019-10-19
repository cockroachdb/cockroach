// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package acceptance

import (
	"flag"
	"time"
)

var flagDuration = flag.Duration("d", 5*time.Second, "for duration-limited tests, how long to run them for")
var flagLogDir = flag.String("l", "", "the directory to store log files, relative to the test source")
