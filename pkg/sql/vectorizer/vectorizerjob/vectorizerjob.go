// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package vectorizerjob implements the resumer for the vectorizer background
// job. Each run processes a batch of rows from the source table that do not
// yet have embeddings in the companion table, generates embeddings using the
// embedding engine, and inserts them.
package vectorizerjob

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/embedding"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type vectorizerResumer struct {
	job *jobs.Job
	st  *cluster.Settings
}

var _ jobs.Resumer = (*vectorizerResumer)(nil)

// Resume implements the jobs.Resumer interface. It finds rows in the source
// table missing embeddings, generates them, and inserts into the companion
// table.
func (r *vectorizerResumer) Resume(ctx context.Context, execCtx interface{}) error {
	jobExecCtx := execCtx.(sql.JobExecContext)
	execCfg := jobExecCtx.ExecCfg()

	details := r.job.Details().(jobspb.VectorizerDetails)
	tableID := details.TableID

	// Get the embedding engine.
	engine, err := embedding.GetEngine()
	if err != nil {
		return jobs.MarkAsPermanentJobError(
			errors.Wrap(err, "vectorizer: embedding engine not available"))
	}

	// Read the vectorizer configuration from the table descriptor.
	var sourceColumns []string
	var tmpl string
	var batchSize int64
	var companionTableName tree.TableName
	var sourceTableName tree.TableName
	var pkColNames []string

	if err := execCfg.InternalDB.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		tableDesc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID)
		if err != nil {
			return errors.Wrapf(err, "vectorizer: resolving table %d", tableID)
		}

		vectorizerCfg := tableDesc.TableDesc().Vectorizer
		if vectorizerCfg == nil {
			return errors.Newf(
				"vectorizer: table %d has no vectorizer configured", tableID,
			)
		}

		sourceColumns = vectorizerCfg.SourceColumns
		tmpl = vectorizerCfg.Template
		batchSize = vectorizerCfg.BatchSize
		if batchSize <= 0 {
			batchSize = 64
		}

		// Look up the source table name.
		tn, err := descs.GetObjectName(
			ctx, txn.KV(), txn.Descriptors(), tableDesc,
		)
		if err != nil {
			return errors.Wrap(err, "vectorizer: resolving source table name")
		}
		sourceTableName = tree.MakeTableNameWithSchema(
			tree.Name(tn.Catalog()),
			tree.Name(tn.Schema()),
			tree.Name(tn.Object()),
		)

		// Look up the companion table name.
		companionDesc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, vectorizerCfg.EmbeddingTableID)
		if err != nil {
			return errors.Wrap(err, "vectorizer: resolving companion table")
		}
		cn, err := descs.GetObjectName(
			ctx, txn.KV(), txn.Descriptors(), companionDesc,
		)
		if err != nil {
			return errors.Wrap(
				err, "vectorizer: resolving companion table name",
			)
		}
		companionTableName = tree.MakeTableNameWithSchema(
			tree.Name(cn.Catalog()),
			tree.Name(cn.Schema()),
			tree.Name(cn.Object()),
		)

		// Get PK column names from source table.
		pkIdx := tableDesc.GetPrimaryIndex()
		pkColNames = make([]string, pkIdx.NumKeyColumns())
		for i := range pkColNames {
			pkColNames[i] = pkIdx.GetKeyColumnName(i)
		}
		return nil
	}); err != nil {
		return err
	}

	// Build the SELECT query to find rows missing embeddings.
	//
	// SELECT s.<pk_cols>, s.<source_cols>
	// FROM <source_table> AS s
	// LEFT JOIN <companion_table> AS e
	//   ON s.<pk> = e.source_<pk>
	// WHERE e.source_<pk[0]> IS NULL
	// LIMIT <batch_size>
	var selectCols []string
	for _, pk := range pkColNames {
		selectCols = append(selectCols,
			fmt.Sprintf("s.%s", tree.NameString(pk)))
	}
	for _, col := range sourceColumns {
		selectCols = append(selectCols,
			fmt.Sprintf("s.%s", tree.NameString(col)))
	}

	var joinConds []string
	for _, pk := range pkColNames {
		joinConds = append(joinConds, fmt.Sprintf(
			"s.%s = e.%s",
			tree.NameString(pk),
			tree.NameString("source_"+pk),
		))
	}

	selectSQL := fmt.Sprintf(
		"SELECT %s FROM %s AS s LEFT JOIN %s AS e ON %s "+
			"WHERE e.%s IS NULL LIMIT %d",
		strings.Join(selectCols, ", "),
		sourceTableName.String(),
		companionTableName.String(),
		strings.Join(joinConds, " AND "),
		tree.NameString("source_"+pkColNames[0]),
		batchSize,
	)

	// Execute the query.
	rows, err := execCfg.InternalDB.Executor().QueryBufferedEx(
		ctx, "vectorizer-select-pending", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		selectSQL,
	)
	if err != nil {
		return errors.Wrap(err, "vectorizer: querying pending rows")
	}

	if len(rows) == 0 {
		log.Ops.Infof(ctx,
			"vectorizer: no pending rows for table %d", tableID)
		return nil
	}

	log.Ops.Infof(ctx,
		"vectorizer: processing %d rows for table %d", len(rows), tableID)

	numPKCols := len(pkColNames)

	// Build text to embed for each row.
	texts := make([]string, len(rows))
	for i, row := range rows {
		texts[i] = buildTextFromRow(row, numPKCols, sourceColumns, tmpl)
	}

	embeddings, err := engine.EmbedBatch(texts)
	if err != nil {
		return errors.Wrap(err, "vectorizer: generating embeddings")
	}

	// Insert embeddings into the companion table.
	var insertedCount int64
	for i, row := range rows {
		var colNames []string
		var placeholders []string
		var args []interface{}
		argIdx := 1

		for j, pk := range pkColNames {
			colNames = append(colNames, tree.NameString("source_"+pk))
			placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
			args = append(args, row[j])
			argIdx++
		}

		colNames = append(colNames, "chunk_seq", "chunk", "embedding")
		placeholders = append(placeholders,
			fmt.Sprintf("$%d", argIdx),
			fmt.Sprintf("$%d", argIdx+1),
			fmt.Sprintf("$%d", argIdx+2),
		)
		args = append(args, 0, texts[i], vectorToSQL(embeddings[i]))

		insertSQL := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
			companionTableName.String(),
			strings.Join(colNames, ", "),
			strings.Join(placeholders, ", "),
		)

		if _, err := execCfg.InternalDB.Executor().ExecEx(
			ctx, "vectorizer-insert-embedding", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			insertSQL, args...,
		); err != nil {
			return errors.Wrapf(
				err, "vectorizer: inserting embedding for row %d", i,
			)
		}
		insertedCount++
	}

	log.Ops.Infof(ctx,
		"vectorizer: inserted %d embeddings for table %d",
		insertedCount, tableID)

	// Update job progress.
	return r.job.NoTxn().FractionProgressed(ctx, func(
		ctx context.Context, details jobspb.ProgressDetails,
	) float32 {
		prog := details.(*jobspb.Progress_Vectorizer).Vectorizer
		prog.RowsProcessed += insertedCount
		return 1.0 // Single-batch job, always complete.
	})
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *vectorizerResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (r *vectorizerResumer) CollectProfile(ctx context.Context, execCtx interface{}) error {
	return nil
}

// buildTextFromRow constructs the text to embed from a result row.
// The row contains [pk_cols..., source_cols...].
func buildTextFromRow(row tree.Datums, numPKCols int, sourceColumns []string, tmpl string) string {
	if tmpl != "" {
		text := tmpl
		for i, col := range sourceColumns {
			datum := row[numPKCols+i]
			text = strings.ReplaceAll(text, "$"+col, datum.String())
		}
		return text
	}
	// Default: join source column values with newlines.
	var parts []string
	for i := range sourceColumns {
		datum := row[numPKCols+i]
		parts = append(parts, datum.String())
	}
	return strings.Join(parts, "\n")
}

// vectorToSQL converts a float32 slice to a SQL VECTOR literal.
func vectorToSQL(v []float32) string {
	parts := make([]string, len(v))
	for i, f := range v {
		parts[i] = fmt.Sprintf("%g", f)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeVectorizer,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &vectorizerResumer{
				job: job,
				st:  settings,
			}
		},
		jobs.UsesTenantCostControl,
	)
}
