// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

type SharedCache interface {
	FetchWithLock(ctx context.Context, key []byte, fn func() ([]byte, error)) ([]byte, error)
}

type jobInfoCache struct {
	job_id int
	db     isql.DB
}

func sharedCacheForJob(id catpb.JobID, db isql.DB) SharedCache {
	return jobInfoCache{job_id: int(id), db: db}
}

func newSharedCacheForCore() SharedCache {
	return &inMemoryCache{}
}

func (j jobInfoCache) FetchWithLock(ctx context.Context, key []byte, fn func() ([]byte, error)) (val []byte, err error) {
	err = j.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		datums, err := txn.QueryRowEx(ctx, `changefeed-cache-fetch`, txn.KV(), sessiondata.NoSessionDataOverride, `SELECT value FROM system.job_info WHERE job_id = $1 AND info_key = $2 FOR UPDATE`, j.job_id, key)
		if err != nil {
			return err
		}
		if len(datums) == 0 {
			val, err = fn()
			if err != nil {
				return err
			}
			txn.ExecEx(ctx, `changefeed-cache-fetch`, txn.KV(), sessiondata.NoSessionDataOverride, `INSERT INTO system.job_info(job_id, info_key, value) VALUES ($1,$2,$3)`, j.job_id, key, val)
			return nil
		}
		val = []byte(tree.MustBeDBytes(datums[0]))
		return nil
	})
	return
}

type inMemoryCache struct {
	sync.Mutex
	vals map[string][]byte
}

var _ SharedCache = &inMemoryCache{}

func (i *inMemoryCache) FetchWithLock(ctx context.Context, key []byte, fn func() ([]byte, error)) (val []byte, err error) {
	i.Lock()
	var ok bool
	val, ok = i.vals[string(key)]
	if !ok {
		val, err = fn()
		if err == nil {
			i.vals[string(key)] = val
		}
	}
	i.Unlock()
	return
}
