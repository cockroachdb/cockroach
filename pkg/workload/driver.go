// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workload

import (
	gosql "database/sql"
	"database/sql/driver"
	"strings"
	"sync/atomic"

	"github.com/lib/pq"
)

// cockroachDriver is a wrapper around lib/pq which provides for round-robin
// load balancing amongst a list of URLs. The name passed to Open() is a space
// separated list of "postgres" URLs to connect to.
//
// Note that the round-robin load balancing can lead to imbalances in
// connections across the cluster. This is currently only suitable for
// simplistic setups where nodes in the cluster are stable and do not go up and
// down.
type cockroachDriver struct {
	idx uint32
}

func (d *cockroachDriver) Open(name string) (driver.Conn, error) {
	urls := strings.Split(name, " ")
	i := atomic.AddUint32(&d.idx, 1) - 1
	return pq.Open(urls[i%uint32(len(urls))])
}

func init() {
	gosql.Register("cockroach", &cockroachDriver{})
}
