// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func init() {
	sql.AddPlanHook("alter backup", alterBackupPlanHook)
}

func alterBackupPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	alterBackupStmt, ok := stmt.(*tree.AlterBackup)

	if !ok {
		return nil, nil, nil, false, nil
	}

	if err := featureflag.CheckEnabled(
		ctx,
		p.ExecCfg(),
		featureBackupEnabled,
		"ALTER BACKUP",
	); err != nil {
		return nil, nil, nil, false, err
	}

	backupFns := make([]func() ([]string, error), len(alterBackupStmt.Backup))
	for i := range alterBackupStmt.Backup {
		fromFn, err := p.TypeAsStringArray(ctx, tree.Exprs(alterBackupStmt.Backup[i]), "ALTER BACKUP")
		if err != nil {
			return nil, nil, nil, false, err
		}
		backupFns[i] = fromFn
	}

	var err error

	subdirFn := func() (string, error) { return "", nil }
	if alterBackupStmt.Subdir != nil {
		subdirFn, err = p.TypeAsString(ctx, alterBackupStmt.Subdir, "ALTER BACKUP")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var newKmsFn func() ([]string, error)
	newKmsFn, err = p.TypeAsStringArray(ctx, tree.Exprs(alterBackupStmt.NewKMSURI), "ALTER BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}

	var oldKmsFn func() ([]string, error)
	oldKmsFn, err = p.TypeAsStringArray(ctx, tree.Exprs(alterBackupStmt.OldKMSURI), "ALTER BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		backup := make([][]string, len(backupFns))
		for i := range backupFns {
			backup[i], err = backupFns[i]()
			if err != nil {
				return err
			}
		}

		subdir, err := subdirFn()
		if err != nil {
			return err
		}

		if subdir != "" {
			if strings.EqualFold(subdir, "LATEST") {
				// set subdir to content of latest file
				latest, err := readLatestFile(ctx, backup[0][0], p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, p.User())
				if err != nil {
					return err
				}
				subdir = latest
			}
			if len(backup) != 1 {
				return errors.Errorf("ALTER BACKUP ... IN can only by used against a single collection path (per-locality)")
			}

			appendPaths := func(uris []string, tailDir string) error {
				for i, uri := range uris {
					parsed, err := url.Parse(uri)
					if err != nil {
						return err
					}
					parsed.Path = path.Join(parsed.Path, tailDir)
					uris[i] = parsed.String()
				}
				return nil
			}

			if err = appendPaths(backup[0][:], subdir); err != nil {
				return err
			}
		}

		var newKms []string
		newKms, err = newKmsFn()

		if err != nil {
			return err
		}

		var oldKms []string
		oldKms, err = oldKmsFn()

		if err != nil {
			return err
		}

		return doAlterBackupPlan(ctx, alterBackupStmt, p, backup, newKms, oldKms)
	}

	return fn, utilccl.BulkJobExecutionResultHeader, nil, false, nil
}

func doAlterBackupPlan(
	ctx context.Context,
	alterBackupStmt *tree.AlterBackup,
	p sql.PlanHookState,
	backup [][]string,
	newKms []string,
	oldKms []string,
) error {
	if len(backup) < 1 || len(backup[0]) < 1 {
		return errors.New("invalid base backup specified")
	}

	baseStores := make([]cloud.ExternalStorage, len(backup[0]))
	for i := range backup[0] {
		store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, backup[0][i], p.User())
		if err != nil {
			return errors.Wrapf(err, "failed to open backup storage location")
		}
		defer store.Close()
		baseStores[i] = store
	}

	opts, err := readEncryptionOptions(ctx, baseStores[0])
	if err != nil {
		return err
	}

	ioConf := baseStores[0].ExternalIOConf()

	// Check that at least one of the old keys works on the backup.
	// Use the first one that works to decrypt the ENCRYPTION-INFO file(s).
	var defaultKMSInfo *jobspb.BackupEncryptionOptions_KMSInfo
	oldKMSFound := false
	for i := 0; !oldKMSFound; i++ {
		if i >= len(oldKms) {
			return fmt.Errorf("no key in old_kms matches the provided backup")
		}
		defaultKMSInfo, err = validateKMSURIsAgainstFullBackup([]string{oldKms[i]},
			newEncryptedDataKeyMapFromProtoMap(opts.EncryptedDataKeyByKMSMasterKeyID), &backupKMSEnv{
				baseStores[0].Settings(),
				&ioConf,
			})

		if err == nil {
			oldKMSFound = true
			break
		}
	}
	encryption := &jobspb.BackupEncryptionOptions{
		Mode:    jobspb.EncryptionMode_KMS,
		KMSInfo: defaultKMSInfo}

	// Recover the encryption key using the old key, so we can encrypt it again with the new keys.
	var plaintextDataKey []byte
	plaintextDataKey, err = getEncryptionKey(ctx, encryption, baseStores[0].Settings(),
		baseStores[0].ExternalIOConf())

	if err != nil {
		return err
	}

	kmsEnv := &backupKMSEnv{settings: p.ExecCfg().Settings, conf: &p.ExecCfg().ExternalIODirConfig}

	encryptedDataKeyByKMSMasterKeyID := newEncryptedDataKeyMap()

	// Add each new key user wants to add to a new data key map.
	for _, kmsURI := range newKms {
		masterKeyID, encryptedDataKey, err := getEncryptedDataKeyFromURI(ctx,
			plaintextDataKey, kmsURI, kmsEnv)
		if err != nil {
			return err
		}

		encryptedDataKeyByKMSMasterKeyID.addEncryptedDataKey(plaintextMasterKeyID(masterKeyID),
			encryptedDataKey)
	}

	encryptedDataKeyMapForProto := make(map[string][]byte)
	encryptedDataKeyByKMSMasterKeyID.rangeOverMap(
		func(masterKeyID hashedMasterKeyID, dataKey []byte) {
			encryptedDataKeyMapForProto[string(masterKeyID)] = dataKey
		})

	encryptionInfo := &jobspb.EncryptionInfo{EncryptedDataKeyByKMSMasterKeyID: encryptedDataKeyMapForProto}

	// Write the new ENCRYPTION-INFO file.
	err = writeAlteredEncryptionInfo(ctx, encryptionInfo, baseStores[0])
	if err != nil {
		return err
	}
	return nil
}

func getNumEncryptionInfoFiles(ctx context.Context, dest cloud.ExternalStorage) (int, error) {
	var fileCount int
	// Look for all files in dest that start with "/ENCRYPTION-INFO" and return them.
	err := dest.List(ctx, "", "", func(p string) error {
		p = strings.TrimPrefix(p, "/")
		if match, err := regexp.MatchString(backupEncryptionInfoFile+".*", p); err != nil {
			return err
		} else if match {
			fileCount++
		}
		return nil
	})

	if err != nil {
		return fileCount, err
	}

	return fileCount, nil
}

func writeAlteredEncryptionInfo(
	ctx context.Context, opts *jobspb.EncryptionInfo, dest cloud.ExternalStorage,
) error {
	files, err := getNumEncryptionInfoFiles(ctx, dest)
	if err != nil {
		return err
	}

	// New encryption-info file name is in the format "ENCRYPTION-INFO-<version number>"
	newEncryptionInfoFile := fmt.Sprintf("%s-%d", backupEncryptionInfoFile, files+1)

	buf, err := protoutil.Marshal(opts)
	if err != nil {
		return err
	}
	if err := cloud.WriteFile(ctx, dest, newEncryptionInfoFile, bytes.NewReader(buf)); err != nil {
		return err
	}
	return nil
}
