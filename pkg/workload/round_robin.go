// Copyright 2019 The Cockroach Authors.
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
	"strings"
	"sync/atomic"
)

// RoundRobinDB is a wrapper around *gosql.DB's that round robins individual
// queries among the different databases that it was created with.
type RoundRobinDB struct {
	handles []*gosql.DB
	current uint32
}

// NewRoundRobinDB creates a RoundRobinDB using the databases from a list
// of comma separated database connection url's.
func NewRoundRobinDB(urlStr string) (*RoundRobinDB, error) {
	urls := strings.Split(urlStr, " ")
	r := &RoundRobinDB{current: 0, handles: make([]*gosql.DB, 0, len(urls))}
	for _, url := range urls {
		db, err := gosql.Open(`cockroach`, url)
		if err != nil {
			return nil, err
		}
		r.handles = append(r.handles, db)
	}
	return r, nil
}

func (db *RoundRobinDB) next() *gosql.DB {
	return db.handles[(atomic.AddUint32(&db.current, 1)-1)%uint32(len(db.handles))]
}

func (db *RoundRobinDB) QueryRow(query string, args ...interface{}) *gosql.Row {
	return db.next().QueryRow(query, args...)
}
func (db *RoundRobinDB) Exec(query string, args ...interface{}) (gosql.Result, error) {
	return db.next().Exec(query, args...)
}
