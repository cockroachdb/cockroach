// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sampledataccl

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
)

// ToBackup creates an enterprise backup in `<externalIODir>/<path>`.
func ToBackup(t testing.TB, data workload.Table, externalIODir, path string) (*Backup, error) {
	return toBackup(t, data, externalIODir, path, 0)
}

func toBackup(
	t testing.TB, data workload.Table, externalIODir, path string, chunkBytes int64,
) (*Backup, error) {
	// TODO(dan): Get rid of the `t testing.TB` parameter and this `TestServer`.
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: externalIODir})
	defer s.Stopper().Stop(ctx)
	if _, err := db.Exec(`CREATE DATABASE data`); err != nil {
		return nil, err
	}

	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s %s;\n", data.Name, data.Schema)); err != nil {
		return nil, err
	}

	for rowIdx := 0; rowIdx < data.InitialRows.NumBatches; rowIdx++ {
		var stmts bytes.Buffer
		for _, row := range data.InitialRows.BatchRows(rowIdx) {
			rowBatch := strings.Join(workloadsql.StringTuple(row), `,`)
			fmt.Fprintf(&stmts, "INSERT INTO %s VALUES (%s);\n", data.Name, rowBatch)
		}
		if _, err := db.Exec(stmts.String()); err != nil {
			return nil, err
		}
	}

	if _, err := db.Exec("BACKUP DATABASE data TO $1", "nodelocal://0/"+path); err != nil {
		return nil, err
	}
	return &Backup{BaseDir: filepath.Join(externalIODir, path)}, nil
}

// Backup is a representation of an enterprise BACKUP.
type Backup struct {
	// BaseDir can be used for a RESTORE. All paths in the descriptor are
	// relative to this.
	BaseDir string
}
