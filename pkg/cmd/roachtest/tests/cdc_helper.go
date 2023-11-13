// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// createTargetTableStmt returns two statements - one to create a new table with
// name newTableName and with same schema as the targetTableName table and
// another to drop the table. For example, given target table name stock and new
// table stock_1, return CREATE TABLE stock_1 (LIKE tpcc.stock INCLUDING ALL).
func createTargetTableStmt(
	targetTableName string, newTableName string,
) (createStmt string, dropStmt string) {
	tpccTableName := fmt.Sprintf("tpcc.%s", targetTableName)
	createStmt = fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING ALL)",
		newTableName, tpccTableName)
	// Return drop stmt to defer cleanups to callers.
	dropStmt = fmt.Sprintf("DROP TABLE %s", newTableName)
	return createStmt, dropStmt
}

// extractTableNameFromFileName extracts the table name assocaited with the
// changefeed from the provided fileName. For example, given the file name given
// the file name
// /2023-11-07/202311071946288411402400000000000-c1a4f08eaf3f6ecd-1-5-000000b7-stock-7.parquet,
// the function would return "stock". It returns a non-nil error when unable to
// parse the table name.
func extractTableNameFromFileName(fileName string) (string, error) {
	splittedString := strings.Split(fileName, ".")
	if len(splittedString) <= 1 {
		return "", errors.New("unexpected file name: unable to split by .")
	}
	if splittedString[len(splittedString)-1] != "parquet" {
		return "", errors.New("unexpected file format")
	}
	// Split the first part of filename by -.
	parts := strings.Split(splittedString[0], "-")
	if len(parts) <= 2 {
		return "", errors.New("unexpected file name: unable to find table name")
	}
	return parts[len(parts)-2], nil
}

// upsertStmtForTable formats and returns a SQL string to upsert args into the
// table tableName "UPSERT INTO tableName VALUES (args)".
func upsertStmtForTable(tableName string, args []string) string {
	b := strings.Builder{}
	b.WriteString("UPSERT INTO ")
	b.WriteString(tableName)
	b.WriteString(" VALUES (")
	for i, arg := range args {
		if i != 0 {
			b.WriteByte(',')
		}
		b.WriteString(arg)
	}
	b.WriteByte(')')
	return b.String()
}

// getRandomIndex returns a random index ∈ [0, sizeOfSlice).
func getRandomIndex(sizeOfSlice int) int {
	return rand.Intn(sizeOfSlice)
}

// fetchFilesOfTargetTable returns the file names for changefeed output files on
// cloudstorage along with any error. Theoretically, we should never expect to
// see a file for a table other than selectedTargetTable. It is just passed in
// for a validation check.
func fetchFilesOfTargetTable(
	selectedTargetTable string, cs cloud.ExternalStorage,
) (csFileNames []string, _ error) {
	err := cs.List(context.Background(), "", "", func(str string) error {
		targetTableName, err := extractTableNameFromFileName(str)
		if err != nil {
			return err
		}
		if targetTableName != selectedTargetTable {
			return errors.New("unexpected mismatch between the target table and " +
				"table names inferred from file names")
		}
		csFileNames = append(csFileNames, str)
		return nil
	})
	return csFileNames, err
}

// downloadFileFromCloudStorage downloads the file from cloud storage and
// returns the name of the local file downloaded. Note that caller is
// responsible for cleaning up the local files after usage.
func downloadFileFromCloudStorage(
	ctx context.Context, es cloud.ExternalStorage, fileName string,
) (downloadedFileName string, _ error) {
	reader, _, err := es.ReadFile(context.Background(), fileName, cloud.ReadOptions{NoFileSize: true})
	if err != nil {
		return "", err
	}
	defer func() {
		err = reader.Close(ctx)
	}()

	f, err := os.CreateTemp(os.TempDir(), "")
	if err != nil {
		return "", err
	}

	bytes, err := ioctx.ReadAll(ctx, reader)
	if err != nil {
		return "", err
	}

	_, err = f.Write(bytes)
	if err != nil {
		return "", err
	}
	return f.Name(), nil
}

// cleanUpDownloadedFiles deletes the given local files.
func cleanUpDownloadedFiles(fileNames []string) error {
	for _, fileName := range fileNames {
		if err := os.Remove(fileName); err != nil {
			return err
		}
	}
	return nil
}

// processTable reads the local file specified by fileName, parse the data
// content, execute upsert statement for the file content into the provided
// targetTable. Note that only INSERT operations are permitted. If any steps
// fail, an error is returned.
func processTable(
	t test.Test, sqlRunner *sqlutils.SQLRunner, targetTable string, fileName string,
) error {
	meta, filesInDatums, err := parquet.ReadFile(fileName)
	if err != nil {
		return err
	}
	eventTypeColIdx, err := changefeedccl.GetEventTypeColIdx(meta)
	if err != nil {
		return err
	}

	for _, rowInDatums := range filesInDatums {
		var argsInDatumString []string
		for i, argInDatum := range rowInDatums {
			if i == eventTypeColIdx {
				// Note that we expect the only operations involved in the changefeeds
				// are INSERT operations.
				if tree.AsStringWithFlags(argInDatum, tree.FmtBareStrings) != "c" {
					return errors.New("unable to process operations other than INSERT")
				} else {
					continue
				}
			}
			argsInDatumString = append(argsInDatumString, argInDatum.String())
		}
		sqlRunner.Exec(t, upsertStmtForTable(targetTable, argsInDatumString))
	}
	return nil
}

// processTables read in fileNames and execute UPSERT stmts for the file content
// into the given targetTable to eliminate duplicates.
func processTables(
	t test.Test, sqlRunner *sqlutils.SQLRunner, targetTable string, fileNames []string,
) error {
	for _, fn := range fileNames {
		if err := processTable(t, sqlRunner, targetTable, fn); err != nil {
			return err
		}
	}
	return nil
}

// downloadFiles downloads the files from the cloud storage given the fileNames
// and returns a list of files downloaded to local.
func downloadFiles(
	ctx context.Context, es cloud.ExternalStorage, fileNames []string,
) (downloadedFileNames []string, _ error) {
	for _, fn := range fileNames {
		downloadedFn, err := downloadFileFromCloudStorage(ctx, es, fn)
		if err != nil {
			return []string{}, err
		}
		downloadedFileNames = append(downloadedFileNames, downloadedFn)
	}
	return
}

// checkTwoChangeFeedExportContent checks if the given two sinks have the same
// changefeed export output on the cloud storage. Theoretically, we should never
// expect to see an output file for a table other than selectedTargetTable. It
// is passed in just for a validation check.
func checkTwoChangeFeedExportContent(
	ctx context.Context,
	t test.Test,
	sqlRunner *sqlutils.SQLRunner,
	firstSinkURI string,
	secSinkURI string,
	selectedTargetTable string,
) {
	require.NotEqual(t, firstSinkURI, secSinkURI)
	// TODO(wenyihu6): Is it faster if I create one table and do all ops in the
	// table at once?
	// Grab a handler to the cloud storage for the given sinks.
	firstCloudStorage, err := cloud.ExternalStorageFromURI(ctx, strings.TrimPrefix(firstSinkURI, `experimental-`),
		base.ExternalIODirConfig{},
		cluster.MakeTestingClusterSettings(),
		blobs.TestEmptyBlobClientFactory,
		username.RootUserName(),
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)
	secCloudStorage, err := cloud.ExternalStorageFromURI(ctx, strings.TrimPrefix(secSinkURI, `experimental-`),
		base.ExternalIODirConfig{},
		cluster.MakeTestingClusterSettings(),
		blobs.TestEmptyBlobClientFactory,
		username.RootUserName(),
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)

	firstTableName := selectedTargetTable + "_1"
	secTableName := selectedTargetTable + "_2"

	// Create two empty tables with same schema as the selectedTargetTable. For
	// example, create two tables stock_1, stock_2 for tpcc.stock.
	firstCreateStmt, firstDropStmt := createTargetTableStmt(selectedTargetTable, firstTableName)
	secCreateStmt, secDropStmt := createTargetTableStmt(selectedTargetTable, secTableName)
	sqlRunner.Exec(t, firstCreateStmt)
	sqlRunner.Exec(t, secCreateStmt)
	defer func() {
		sqlRunner.Exec(t, firstDropStmt)
		sqlRunner.Exec(t, secDropStmt)
	}()

	// Fetch file names of the changefeed output files in cloud storage.
	firstCloudStorageFileNames, err := fetchFilesOfTargetTable(selectedTargetTable, firstCloudStorage)
	require.NoError(t, err)
	secCloudStorageFileNames, err := fetchFilesOfTargetTable(selectedTargetTable, secCloudStorage)
	require.NoError(t, err)
	require.NotEmpty(t, firstCloudStorageFileNames)
	require.NotEmpty(t, secCloudStorageFileNames)

	// Download files from cloud storage and return the local files names.
	firstDownloadedFileNames, err := downloadFiles(ctx, firstCloudStorage, firstCloudStorageFileNames)
	require.NoError(t, err)
	secDownloadedFileNames, err := downloadFiles(ctx, secCloudStorage, secCloudStorageFileNames)
	require.NoError(t, err)
	require.NotEmpty(t, firstDownloadedFileNames)
	require.NotEmpty(t, secDownloadedFileNames)

	// Parse the downloaded files given the local file names and execute UPSERT
	// stmts for the file content into the two tables.
	err = processTables(t, sqlRunner, firstTableName, firstDownloadedFileNames)
	require.NoError(t, err)
	err = processTables(t, sqlRunner, secTableName, secDownloadedFileNames)
	require.NoError(t, err)

	// Assert that two tables have the same content by checking their
	// fingerprints.
	firstFingerPrint := sqlRunner.QueryStr(t,
		fmt.Sprintf("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s", firstTableName))
	secFingerPrint := sqlRunner.QueryStr(t,
		fmt.Sprintf("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s", secTableName))
	require.Equal(t, firstFingerPrint, secFingerPrint)
	require.GreaterOrEqual(t, len(firstFingerPrint[0]), 2)
	require.GreaterOrEqual(t, len(secFingerPrint[0]), 2)
	require.NotEqual(t, "NULL", firstFingerPrint[0][1])
	require.NotEqual(t, "NULL", secFingerPrint[0][1])
	require.NotEmpty(t, firstFingerPrint)
	require.NotEmpty(t, secFingerPrint)

	// Clean up downloaded local files.
	err = cleanUpDownloadedFiles(firstDownloadedFileNames)
	require.NoError(t, err)
	err = cleanUpDownloadedFiles(secDownloadedFileNames)
	require.NoError(t, err)
}
