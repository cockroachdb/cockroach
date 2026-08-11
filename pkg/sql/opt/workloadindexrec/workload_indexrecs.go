// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workloadindexrec

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parserutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/errors"
)

// FindWorkloadRecs finds index recommendations for the whole workload after the
// timestamp ts within the space budget represented by budgetBytes.
func FindWorkloadRecs(
	ctx context.Context, evalCtx *eval.Context, ts *tree.DTimestampTZ,
) ([]WorkloadIndexRec, error) {
	cis, dis, err := collectIndexRecs(ctx, evalCtx, ts)
	if err != nil {
		return nil, err
	}

	trieMap := buildTrieForIndexRecs(cis)
	newCis, err := extractIndexCovering(trieMap)
	if err != nil {
		return nil, err
	}

	// Since we collect all the indexes represented by the leaf nodes, all the
	// indexes with "DROP INDEX" has been covered, so we can directly drop all of
	// them without duplicates.
	var disMap = make(map[tree.TableIndexName][]uint64)
	for _, indx := range dis {
		di := indx.index
		for _, index := range di.IndexList {
			disMap[*index] = append(disMap[*index], indx.fingerprintId)
		}
	}

	for index := range disMap {
		dropCmd := tree.DropIndex{
			IndexList: []*tree.TableIndexName{&index},
		}
		newCis = append(newCis, WorkloadIndexRec{
			Index:          dropCmd.String() + ";",
			FingerprintIds: disMap[index],
		})
	}

	return newCis, nil
}

// collectIndexRecs collects all the index recommendations stored in the
// system.statement_statistics with the time later than ts.
func collectIndexRecs(
	ctx context.Context, evalCtx *eval.Context, ts *tree.DTimestampTZ,
) ([]createIndex, []dropIndex, error) {
	query := `SELECT index_recommendations, fingerprint_id FROM system.statement_statistics
						 WHERE (statistics -> 'statistics' ->> 'lastExecAt')::TIMESTAMPTZ > $1
						 AND array_length(index_recommendations, 1) > 0;`
	indexRecs, err := evalCtx.Planner.QueryIteratorEx(ctx, "get-candidates-for-workload-indexrecs",
		sessiondata.NoSessionDataOverride, query, ts.Time)
	if err != nil {
		return nil, nil, err
	}

	var cis []createIndex
	var dis []dropIndex
	var ok bool

	// The index recommendation starts with "creation", "replacement" or
	// "alteration".
	var r = regexp.MustCompile(`\s*(creation|replacement|alteration)\s*:\s*(.*)`)

	for ok, err = indexRecs.Next(ctx); ; ok, err = indexRecs.Next(ctx) {
		if err != nil {
			err = errors.CombineErrors(err, indexRecs.Close())
			indexRecs = nil
			return cis, dis, err
		}

		if !ok {
			break
		}

		indexes := tree.MustBeDArray(indexRecs.Cur()[0])
		fingerprintId, e := sqlstatsutil.DatumToUint64(indexRecs.Cur()[1])
		if e != nil {
			return cis, dis, err
		}
		for _, index := range indexes.Array {
			indexStr, ok := index.(*tree.DString)
			if !ok {
				err = errors.CombineErrors(errors.Newf("%s is not a string!", index.String()), indexRecs.Close())
				indexRecs = nil
				return cis, dis, err
			}

			indexStrArr := r.FindStringSubmatch(string(*indexStr))
			if indexStrArr == nil {
				err = errors.CombineErrors(errors.Newf("%s is not a valid index recommendation!", string(*indexStr)), indexRecs.Close())
				indexRecs = nil
				return cis, dis, err
			}

			// Since Alter index recommendation only makes invisible indexes visible,
			// so we skip it for now.
			if indexStrArr[1] == "alteration" {
				continue
			}

			stmts, err := parserutils.Parse(indexStrArr[2])
			if err != nil {
				err = errors.CombineErrors(errors.Newf("%s is not a valid index operation!", indexStrArr[2]), indexRecs.Close())
				indexRecs = nil
				return cis, dis, err
			}

			for _, stmt := range stmts {
				switch stmt := stmt.AST.(type) {
				case *tree.CreateIndex:
					// Ignore all the inverted, vector, partial, sharded, etc. indexes right now.
					if stmt.Type == idxtype.FORWARD && stmt.Predicate == nil && stmt.Sharded == nil {
						ci := createIndex{fingerprintId: fingerprintId, index: *stmt}
						// A "replacement" recommendation supersedes an existing
						// index; carry the superseded index's identity so the
						// workload-level output can emit the same guidance the
						// per-statement recommendation does.
						if indexStrArr[1] == "replacement" {
							ci.supersededIndexes = extractSupersededIndexes(indexStrArr[2])
						}
						cis = append(cis, ci)
					}
				case *tree.DropIndex:
					dis = append(dis, dropIndex{fingerprintId: fingerprintId, index: *stmt})
				}
			}
		}
	}

	return cis, dis, nil
}

// WorkloadIndexRec contains an index recommendation and the fingerprint ids
// that the index is recommended for.
type WorkloadIndexRec struct {
	Index          string
	FingerprintIds []uint64
}

// createIndex contains a recommended tree.CreateIndex and the fingerprint id
// that the index is recommended for.
type createIndex struct {
	fingerprintId     uint64
	index             tree.CreateIndex
	supersededIndexes []string
}

// dropIndex contains a recommended tree.DropIndex and the fingerprint id
// that the index is recommended for.
type dropIndex struct {
	fingerprintId uint64
	index         tree.DropIndex
}

// buildTrieForIndexRecs builds the relation among all the indexRecs by a trie tree.
func buildTrieForIndexRecs(cis []createIndex) map[tree.TableName]*indexTrie {
	trieMap := make(map[tree.TableName]*indexTrie)
	for _, idx := range cis {
		ci := idx.index
		if _, ok := trieMap[ci.Table]; !ok {
			trieMap[ci.Table] = NewTrie()
		}

		trieMap[ci.Table].Insert(ci.Columns, ci.Storing, idx.fingerprintId, idx.supersededIndexes)
	}
	return trieMap
}

// extractIndexCovering pushes down the storing part of the internal nodes: find
// whether it is covered by some leaf nodes. If yes, discard it; Otherwise,
// assign it to the shallowest leaf node. Then extractIndexCovering collects all
// the indexes represented by the leaf node.
func extractIndexCovering(tm map[tree.TableName]*indexTrie) ([]WorkloadIndexRec, error) {
	for _, t := range tm {
		t.RemoveStorings()
	}
	for _, t := range tm {
		t.AssignStoring()
	}
	var wcis []WorkloadIndexRec
	for table, trie := range tm {
		indexedColsArray, storingColsArray := collectAllLeavesForTable(trie)
		// The length of indexedCols and storingCols must be equal
		if len(indexedColsArray) != len(storingColsArray) {
			return nil, errors.Newf("The length of indexedColsArray and storingColsArray after collecting leaves from table %s is not equal!", table)
		}
		for i, indexedCols := range indexedColsArray {
			cisIndexedCols := make([]tree.IndexElem, len(indexedCols.indexedColumns))
			for j, col := range indexedCols.indexedColumns {
				cisIndexedCols[j] = tree.IndexElem{
					Column:    col.column,
					Direction: col.direction,
				}
				// Recover the ASC to Default direction.
				if col.direction == tree.Ascending {
					cisIndexedCols[j].Direction = tree.DefaultDirection
				}
			}
			index := tree.CreateIndex{
				Table:   table,
				Columns: cisIndexedCols,
				Storing: storingColsArray[i],
			}
			indexStr := index.String() + ";"
			// If the merged index supersedes existing indexes, attach the
			// replacement guidance comment so downstream consumers know which
			// indexes are now redundant.
			if len(indexedCols.supersededIndexes) > 0 {
				indexStr = appendReplacementGuidance(indexStr, indexedCols.supersededIndexes)
			}
			wcis = append(wcis, WorkloadIndexRec{
				Index:          indexStr,
				FingerprintIds: indexedCols.fingerprints,
			})
		}
	}
	return wcis, nil
}

// extractSupersededIndexes recovers the names of the existing indexes that a
// "replacement" recommendation supersedes. The per-statement recommendation
// embeds this guidance as a trailing SQL comment on the CREATE INDEX statement
// (generated by indexrec.FormatIndexRec), naming the superseded index via
// `ALTER INDEX <name> NOT VISIBLE` and `DROP INDEX <name>`. The SQL parser
// discards comments, so we recover the identity by scraping it back out of the
// stored recommendation string.
//
// We key on the `ALTER INDEX <name> NOT VISIBLE` command, which only appears in
// the replacement guidance comment (v25.3+ format). This deliberately does not
// match a standalone `DROP INDEX <name>;` statement — that is the pre-v25.3
// two-statement form, whose DROP is already captured separately by the caller.
func extractSupersededIndexes(sqlStr string) []string {
	// Matches `ALTER INDEX <name> NOT VISIBLE` inside the guidance comment, e.g.:
	//   /* ... After ... manually run: `ALTER INDEX t@t_i NOT VISIBLE` ... */
	re := regexp.MustCompile(`ALTER INDEX\s+([^\s` + "`" + `]+)\s+NOT VISIBLE`)
	var superseded []string
	seen := make(map[string]struct{})
	for _, m := range re.FindAllStringSubmatch(sqlStr, -1) {
		if len(m) < 2 {
			continue
		}
		name := m[1]
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		superseded = append(superseded, name)
	}
	return superseded
}

// appendReplacementGuidance appends a guidance comment to the workload-level
// CREATE INDEX statement, naming the existing indexes it supersedes. This
// mirrors the per-statement replacement guidance so that downstream consumers
// know which indexes are now redundant. The guidance is a comment, not a
// separate executable DROP INDEX row, because the workload output is a flat
// unordered list where a standalone DROP could be applied before the CREATE
// that justifies it.
func appendReplacementGuidance(createStr string, superseded []string) string {
	var sb strings.Builder
	sb.WriteString(createStr)
	sb.WriteString(" /* supersedes")
	for i, name := range superseded {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(" ")
		sb.WriteString(name)
	}
	sb.WriteString("; after creating, run")
	for _, name := range superseded {
		fmt.Fprintf(&sb, " `ALTER INDEX %s NOT VISIBLE`,", name)
	}
	sb.WriteString(" then")
	for _, name := range superseded {
		fmt.Fprintf(&sb, " `DROP INDEX %s`,", name)
	}
	s := sb.String()
	// Trim the trailing comma before the closing comment.
	s = strings.TrimSuffix(s, ",")
	return s + " once verified */;"
}
