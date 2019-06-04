// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package acceptance

import (
	"flag"
	"time"
)

func init() {
	flag.Parse()
}

var flagDuration = flag.Duration("d", 5*time.Second, "for duration-limited tests, how long to run them for")
var flagLogDir = flag.String("l", "", "the directory to store log files, relative to the test source")
