// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TableComments stores the comment data for a table.
type TableComments struct {
	Comment *string
	Columns []Comment
	Indexes []Comment
}

// Comment stores sub_id and comment. For details, check system.comments.
type Comment struct {
	SubID   int
	Comment string
}

// SelectComment retrieves all the comments pertaining to a table (comments on the table
// itself but also column and index comments.)
func SelectComment(ctx context.Context, p PlanHookState, tableID sqlbase.ID) (tc *TableComments) {
	query := fmt.Sprintf("SELECT type, object_id, sub_id, comment FROM system.comments WHERE object_id = %d", tableID)

	commentRows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
		ctx, "show-tables-with-comment", p.Txn(), query)
	if err != nil {
		log.VEventf(ctx, 1, "%q", err)
	} else {
		for _, row := range commentRows {
			commentType := int(tree.MustBeDInt(row[0]))
			switch commentType {
			case keys.TableCommentType, keys.ColumnCommentType, keys.IndexCommentType:
				subID := int(tree.MustBeDInt(row[2]))
				cmt := string(tree.MustBeDString(row[3]))

				if tc == nil {
					tc = &TableComments{}
				}

				switch commentType {
				case keys.TableCommentType:
					tc.Comment = &cmt
				case keys.ColumnCommentType:
					tc.Columns = append(tc.Columns, Comment{subID, cmt})
				case keys.IndexCommentType:
					tc.Indexes = append(tc.Indexes, Comment{subID, cmt})
				}
			}
		}
	}

	return tc
}
