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
	"sync/atomic"
)

// RoundRobinDB is a wrapper around *gosql.DB's that round robins individual
// queries among the different databases that it was created with.
type RoundRobinDB struct {
	handles []*gosql.DB
	current uint32
}

// NewRoundRobinDB creates a RoundRobinDB from the input list of
// database connection URLs.
func NewRoundRobinDB(urls []string) (*RoundRobinDB, error) {
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

// QueryRow executes (*gosql.DB).QueryRow on the next available DB.
func (db *RoundRobinDB) QueryRow(query string, args ...interface{}) *gosql.Row {
	return db.next().QueryRow(query, args...)
}

// Exec executes (*gosql.DB).Exec on the next available DB.
func (db *RoundRobinDB) Exec(query string, args ...interface{}) (gosql.Result, error) {
	return db.next().Exec(query, args...)
}
