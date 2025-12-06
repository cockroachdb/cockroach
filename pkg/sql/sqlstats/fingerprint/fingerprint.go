// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fingerprint

import (
	"context"
	"encoding/binary"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Maximum possible number of cached fingerprints is 1 million.
const MAX_CACHED_FINGERPRINTS = 1000000

// Store encapsulates the both the caching and persistence of Fingerprints. It
// theoretically maintains a cache of digests, and if an unseen digest is
// encountered, it attempts an upsert against the normalized fingerprints
// table.
//
// As the primary usage of normalized fingerprints is to join them against
// other record types in sql queries, there isn't a need to retrieve records
// from the store.
func NewStore(db isql.DB) *Store {
	s := &Store{
		db: db,
	}
	return s
}

type Store struct {
	mu struct {
		sync.Mutex
		seen map[appstatspb.StmtFingerprintID]int64
	}
	db isql.DB
}

// Get both returns the fingerprint of the incoming fields, and persists
// it to the underlying system table if it's unsen.
func (s *Store) Get(
	ctx context.Context, dbName string, query string, implicitTxn bool,
) (appstatspb.StmtFingerprintID, int64, error) {
	log.Dev.Infof(ctx, "Starting fingerprint logic for query: %s", query)
	fp := appstatspb.ConstructStatementFingerprintID(query, implicitTxn, dbName)
	cacheFull := s.len() > MAX_CACHED_FINGERPRINTS

	var id int64
	var ok bool
	hasFrighteningTableWhichCausesDeadlock := strings.Contains(strings.ToLower(query), "system.statement_fingerprints") ||
		strings.Contains(strings.ToLower(query), "system.table_statistics")

	if id, ok = s.load(fp); cacheFull || ok || hasFrighteningTableWhichCausesDeadlock {
		return fp, id, nil
	} else {
		defer func() {
			s.put(fp, id)
		}()
	}

	s.put(fp, id)

	containsUnsalableTables := strings.Contains(strings.ToLower(query), "system.statement_fingerprints") ||
		strings.Contains(strings.ToLower(query), "system.table_statistics")
	if containsUnsalableTables {
		return fp, 0, nil
	}

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(fp))
	err := s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.Exec(ctx, "fingerprint-upsert", txn.KV(), `
				INSERT INTO system.statement_fingerprints
			        (row_id, fingerprint, database, query, implicit_txn, summary)
				VALUES (nextval('system.statement_fingerprint_id_seq'), $1, $2, $3, $4, '')
			  ON CONFLICT(fingerprint) DO NOTHING
			`, b, dbName, query, implicitTxn)
		if err != nil {
			return err
		}
		row, err := txn.QueryRow(ctx, "fingerprint-upsert", txn.KV(), `
				SELECT row_id
			    FROM system.statement_fingerprints
						WHERE fingerprint = $1
			`, b)
		id = int64(tree.MustBeDInt(row[0]))
		return err
	})
	log.Dev.Infof(ctx, "insert logic for query: %s", query)
	return fp, id, err
}

// put puts the key value pair into the cache.
func (s *Store) put(k appstatspb.StmtFingerprintID, v int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.seen == nil {
		s.mu.seen = make(map[appstatspb.StmtFingerprintID]int64)
	}
	s.mu.seen[k] = v
}

func (s *Store) load(k appstatspb.StmtFingerprintID) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.mu.seen[k]
	return id, ok
}

func (s *Store) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.mu.seen)
}
