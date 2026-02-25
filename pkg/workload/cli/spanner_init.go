// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

func runSpannerInit(ctx context.Context, gen workload.Generator, dbPath string, drop bool) error {
	lc := strings.ToLower(*dataLoader)
	if lc == "auto" {
		lc = "insert"
	}
	if lc != "insert" {
		return errors.Errorf("data loader %q is not supported for dialect %q", *dataLoader, *dialect)
	}

	admin, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return errors.Wrap(err, "creating spanner database admin client")
	}
	defer func() {
		err := admin.Close()
		if err != nil {
			log.Dev.Errorf(ctx, "error closing spanner admin client: %v", err)
		}
	}()

	existingTables, existingIndexes, err := fetchSpannerSchema(ctx, admin, dbPath)
	if err != nil {
		return err
	}

	tables := gen.Tables()
	if drop {
		dropStmts := make([]string, 0, len(tables))
		for _, table := range tables {
			if existingTables[table.Name] {
				dropStmts = append(dropStmts, fmt.Sprintf("DROP TABLE %s", table.Name))
			}
		}
		if len(dropStmts) > 0 {
			if err := applySpannerDDL(ctx, admin, dbPath, dropStmts); err != nil {
				return errors.Wrap(err, "dropping Spanner tables")
			}
			for _, table := range tables {
				delete(existingTables, table.Name)
			}
			existingIndexes = map[string]bool{}
		}
	}

	createStmts := make([]string, 0, len(tables))
	indexStmts := make([]string, 0, len(tables))
	for _, table := range tables {
		if existingTables[table.Name] {
			continue
		}
		createStmts = append(createStmts, fmt.Sprintf("CREATE TABLE %s %s", table.Name, table.Schema))
	}
	for _, table := range tables {
		for _, stmt := range table.Indexes {
			name, ok := spannerDDLIndexName(stmt)
			if !ok {
				return errors.Errorf("invalid Spanner index statement for table %q: %s", table.Name, stmt)
			}
			if existingIndexes[name] {
				continue
			}
			indexStmts = append(indexStmts, stmt)
		}
	}
	if len(createStmts)+len(indexStmts) > 0 {
		statements := append(createStmts, indexStmts...)
		if err := applySpannerDDL(ctx, admin, dbPath, statements); err != nil {
			return errors.Wrap(err, "creating Spanner tables")
		}
	}

	if err := loadSpannerInitialData(ctx, dbPath, tables); err != nil {
		return err
	}
	return nil
}

const spannerInitMaxMutations = 200

func loadSpannerInitialData(ctx context.Context, dbPath string, tables []workload.Table) error {
	needsData := false
	for _, table := range tables {
		if table.InitialRows.NumBatches > 0 {
			needsData = true
			break
		}
	}
	if !needsData {
		return nil
	}

	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		return errors.Wrap(err, "creating spanner data client")
	}
	defer client.Close()

	for _, table := range tables {
		if table.InitialRows.NumBatches == 0 {
			continue
		}
		if table.InitialRows.FillBatch == nil {
			return errors.Errorf("initial data is not supported for workload %s", table.Name)
		}
		if table.ObjectPrefix != nil && table.ObjectPrefix.ExplicitCatalog {
			return errors.Errorf("multi-database table %q is not supported for Spanner init", table.Name)
		}
		columns, err := spannerColumnsFromSchema(table.Schema)
		if err != nil {
			return errors.Wrapf(err, "parsing columns for table %q", table.Name)
		}

		workers := *initConns
		if workers < 1 {
			workers = 1
		}
		batchesPerWorker := table.InitialRows.NumBatches / workers
		g, gCtx := errgroup.WithContext(ctx)
		for i := 0; i < workers; i++ {
			startIdx := i * batchesPerWorker
			endIdx := startIdx + batchesPerWorker
			if i == workers-1 {
				endIdx = table.InitialRows.NumBatches
			}
			table := table
			g.Go(func() error {
				mutations := make([]*spanner.Mutation, 0, spannerInitMaxMutations)
				flush := func() error {
					if len(mutations) == 0 {
						return nil
					}
					if _, err := client.Apply(gCtx, mutations); err != nil {
						return err
					}
					mutations = mutations[:0]
					return nil
				}
				for batchIdx := startIdx; batchIdx < endIdx; batchIdx++ {
					for _, row := range table.InitialRows.BatchRows(batchIdx) {
						if len(row) != len(columns) {
							return errors.Errorf("row/column mismatch for table %q: %d values, %d columns",
								table.Name, len(row), len(columns))
						}
						mutations = append(mutations, spanner.InsertOrUpdate(table.Name, columns, row))
						if len(mutations) >= spannerInitMaxMutations {
							if err := flush(); err != nil {
								return err
							}
						}
					}
				}
				return flush()
			})
		}
		if err := g.Wait(); err != nil {
			return errors.Wrapf(err, "loading initial data for table %q", table.Name)
		}
	}
	return nil
}

func spannerColumnsFromSchema(schema string) ([]string, error) {
	start := strings.Index(schema, "(")
	if start == -1 {
		return nil, errors.New("schema missing opening parenthesis")
	}
	depth := 0
	segmentStart := -1
	var segments []string
scan:
	for i := start; i < len(schema); i++ {
		switch schema[i] {
		case '(':
			if depth == 0 {
				segmentStart = i + 1
			}
			depth++
		case ')':
			depth--
			if depth == 0 {
				if segmentStart != -1 && segmentStart < i {
					segments = append(segments, schema[segmentStart:i])
				}
				break scan
			}
		case ',':
			if depth == 1 && segmentStart != -1 {
				segments = append(segments, schema[segmentStart:i])
				segmentStart = i + 1
			}
		}
		if depth < 0 {
			return nil, errors.New("schema has mismatched parentheses")
		}
	}
	if depth != 0 {
		return nil, errors.New("schema has mismatched parentheses")
	}
	if len(segments) == 0 {
		return nil, errors.New("schema has no columns")
	}
	cols := make([]string, 0, len(segments))
	for _, seg := range segments {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		parts := strings.Fields(seg)
		if len(parts) == 0 {
			continue
		}
		col := strings.Trim(parts[0], "`")
		switch strings.ToUpper(col) {
		case "PRIMARY", "UNIQUE", "INDEX", "CONSTRAINT", "FOREIGN":
			continue
		}
		cols = append(cols, col)
	}
	if len(cols) == 0 {
		return nil, errors.New("schema has no column definitions")
	}
	return cols, nil
}

func applySpannerDDL(
	ctx context.Context, admin *database.DatabaseAdminClient, dbPath string, statements []string,
) error {
	op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: statements,
	})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

func fetchSpannerSchema(
	ctx context.Context, admin *database.DatabaseAdminClient, dbPath string,
) (map[string]bool, map[string]bool, error) {
	resp, err := admin.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{
		Database: dbPath,
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "fetching Spanner database DDL")
	}
	tables := make(map[string]bool, len(resp.Statements))
	indexes := make(map[string]bool, len(resp.Statements))
	for _, stmt := range resp.Statements {
		if name, ok := spannerDDLTableName(stmt); ok {
			tables[name] = true
			continue
		}
		if name, ok := spannerDDLIndexName(stmt); ok {
			indexes[name] = true
		}
	}
	return tables, indexes, nil
}

func spannerDDLTableName(stmt string) (string, bool) {
	trimmed := strings.TrimSpace(stmt)
	const prefix = "CREATE TABLE "
	if !strings.HasPrefix(strings.ToUpper(trimmed), prefix) {
		return "", false
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	end := strings.IndexAny(rest, " (")
	if end == -1 {
		return "", false
	}
	name := strings.TrimSpace(rest[:end])
	name = strings.Trim(name, "`")
	if name == "" {
		return "", false
	}
	return name, true
}

func spannerDDLIndexName(stmt string) (string, bool) {
	trimmed := strings.TrimSpace(stmt)
	upper := strings.ToUpper(trimmed)
	var prefix string
	switch {
	case strings.HasPrefix(upper, "CREATE INDEX "):
		prefix = "CREATE INDEX "
	case strings.HasPrefix(upper, "CREATE UNIQUE INDEX "):
		prefix = "CREATE UNIQUE INDEX "
	default:
		return "", false
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	parts := strings.Fields(rest)
	if len(parts) == 0 {
		return "", false
	}
	name := strings.Trim(parts[0], "`")
	if name == "" {
		return "", false
	}
	return name, true
}
