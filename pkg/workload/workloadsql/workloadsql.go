// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workloadsql

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"
)

// Setup creates the given tables and fills them with initial data.
//
// The size of the loaded data is returned in bytes, suitable for use with
// SetBytes of benchmarks. The exact definition of this is deferred to the
// InitialDataLoader implementation.
func Setup(
	ctx context.Context, db *gosql.DB, gen workload.Generator, l workload.InitialDataLoader,
) (int64, error) {
	bytes, err := l.InitialDataLoad(ctx, db, gen)
	if err != nil {
		return 0, err
	}

	const splitConcurrency = 384 // TODO(dan): Don't hardcode this.
	for _, table := range gen.Tables() {
		if err := Split(ctx, db, table, splitConcurrency); err != nil {
			return 0, err
		}
	}

	var hooks workload.Hooks
	if h, ok := gen.(workload.Hookser); ok {
		hooks = h.Hooks()
	}
	if hooks.PostLoad != nil {
		if err := hooks.PostLoad(db); err != nil {
			return 0, errors.Wrapf(err, "Could not postload")
		}
	}

	return bytes, nil
}

// maybeDisableMergeQueue disables the merge queue for versions prior to
// 19.2.
func maybeDisableMergeQueue(db *gosql.DB) error {
	var ok bool
	if err := db.QueryRow(
		`SELECT count(*) > 0 FROM [ SHOW ALL CLUSTER SETTINGS ] AS _ (v) WHERE v = 'kv.range_merge.queue_enabled'`,
	).Scan(&ok); err != nil || !ok {
		return err
	}
	var versionStr string
	err := db.QueryRow(
		`SELECT value FROM crdb_internal.node_build_info WHERE field = 'Version'`,
	).Scan(&versionStr)
	if err != nil {
		return err
	}
	v, err := version.Parse(versionStr)
	// If we can't parse the error then we'll assume that we should disable the
	// queue. This happens in testing.
	if err == nil && (v.Major() > 19 || (v.Major() == 19 && v.Minor() >= 2)) {
		return nil
	}
	_, err = db.Exec("SET CLUSTER SETTING kv.range_merge.queue_enabled = false")
	return err
}

// Split creates the range splits defined by the given table.
func Split(ctx context.Context, db *gosql.DB, table workload.Table, concurrency int) error {
	// Prevent the merge queue from immediately discarding our splits.
	if err := maybeDisableMergeQueue(db); err != nil {
		return err
	}

	if table.Splits.NumBatches <= 0 {
		return nil
	}
	splitPoints := make([][]interface{}, 0, table.Splits.NumBatches)
	for splitIdx := 0; splitIdx < table.Splits.NumBatches; splitIdx++ {
		splitPoints = append(splitPoints, table.Splits.BatchRows(splitIdx)...)
	}
	sort.Sort(sliceSliceInterface(splitPoints))

	type pair struct {
		lo, hi int
	}
	splitCh := make(chan pair, len(splitPoints)/2+1)
	splitCh <- pair{0, len(splitPoints)}
	doneCh := make(chan struct{})

	log.Infof(ctx, `starting %d splits`, len(splitPoints))
	g := ctxgroup.WithContext(ctx)
	// Rate limit splitting to prevent replica imbalance.
	r := rate.NewLimiter(128, 1)
	for i := 0; i < concurrency; i++ {
		g.GoCtx(func(ctx context.Context) error {
			var buf bytes.Buffer
			for {
				select {
				case p, ok := <-splitCh:
					if !ok {
						return nil
					}
					if err := r.Wait(ctx); err != nil {
						return err
					}
					m := (p.lo + p.hi) / 2
					split := strings.Join(StringTuple(splitPoints[m]), `,`)

					buf.Reset()
					fmt.Fprintf(&buf, `ALTER TABLE %s SPLIT AT VALUES (%s)`, table.Name, split)
					// If you're investigating an error coming out of this Exec, see the
					// HACK comment in ColBatchToRows for some context that may (or may
					// not) help you.
					stmt := buf.String()
					if _, err := db.Exec(stmt); err != nil {
						return errors.Wrapf(err, "executing %s", stmt)
					}

					buf.Reset()
					fmt.Fprintf(&buf, `ALTER TABLE %s SCATTER FROM (%s) TO (%s)`,
						table.Name, split, split)
					stmt = buf.String()
					if _, err := db.Exec(stmt); err != nil {
						// SCATTER can collide with normal replicate queue
						// operations and fail spuriously, so only print the
						// error.
						log.Warningf(ctx, `%s: %v`, stmt, err)
					}

					select {
					case doneCh <- struct{}{}:
					case <-ctx.Done():
						return ctx.Err()
					}

					if p.lo < m {
						splitCh <- pair{p.lo, m}
					}
					if m+1 < p.hi {
						splitCh <- pair{m + 1, p.hi}
					}
				case <-ctx.Done():
					return ctx.Err()
				}

			}
		})
	}
	g.GoCtx(func(ctx context.Context) error {
		finished := 0
		for finished < len(splitPoints) {
			select {
			case <-doneCh:
				finished++
				if finished%1000 == 0 {
					log.Infof(ctx, "finished %d of %d splits", finished, len(splitPoints))
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		close(splitCh)
		return nil
	})
	return g.Wait()
}

// StringTuple returns the given datums as strings suitable for use in directly
// in SQL.
//
// TODO(dan): Remove this once SCATTER supports placeholders.
func StringTuple(datums []interface{}) []string {
	s := make([]string, len(datums))
	for i, datum := range datums {
		if datum == nil {
			s[i] = `NULL`
			continue
		}
		switch x := datum.(type) {
		case int:
			s[i] = strconv.Itoa(x)
		case int64:
			s[i] = strconv.FormatInt(x, 10)
		case uint64:
			s[i] = strconv.FormatUint(x, 10)
		case string:
			s[i] = lex.EscapeSQLString(x)
		case float64:
			s[i] = fmt.Sprintf(`%f`, x)
		case []byte:
			// See the HACK comment in ColBatchToRows.
			s[i] = lex.EscapeSQLString(string(x))
		default:
			panic(errors.AssertionFailedf("unsupported type %T: %v", x, x))
		}
	}
	return s
}

type sliceSliceInterface [][]interface{}

func (s sliceSliceInterface) Len() int      { return len(s) }
func (s sliceSliceInterface) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sliceSliceInterface) Less(i, j int) bool {
	for offset := 0; ; offset++ {
		iLen, jLen := len(s[i]), len(s[j])
		if iLen <= offset || jLen <= offset {
			return iLen < jLen
		}
		var cmp int
		switch x := s[i][offset].(type) {
		case int:
			if y := s[j][offset].(int); x < y {
				return true
			} else if x > y {
				return false
			}
			continue
		case int64:
			if y := s[j][offset].(int64); x < y {
				return true
			} else if x > y {
				return false
			}
			continue
		case float64:
			if y := s[j][offset].(float64); x < y {
				return true
			} else if x > y {
				return false
			}
			continue
		case uint64:
			if y := s[j][offset].(uint64); x < y {
				return true
			} else if x > y {
				return false
			}
			continue
		case string:
			cmp = strings.Compare(x, s[j][offset].(string))
		case []byte:
			cmp = bytes.Compare(x, s[j][offset].([]byte))
		default:
			panic(errors.AssertionFailedf("unsupported type %T: %v", x, x))
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
}
