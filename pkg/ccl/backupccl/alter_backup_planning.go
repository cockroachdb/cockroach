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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

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

	fromFn, err := p.TypeAsString(ctx, alterBackupStmt.Backup, "ALTER BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}

	subdirFn := func() (string, error) { return "", nil }
	if alterBackupStmt.Subdir != nil {
		subdirFn, err = p.TypeAsString(ctx, alterBackupStmt.Subdir, "ALTER BACKUP")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var newKmsFn func() ([]string, error)
	var oldKmsFn func() ([]string, error)

	for _, cmd := range alterBackupStmt.Cmds {
		switch v := cmd.(type) {
		case *tree.AlterBackupKMS:
			newKmsFn, err = p.TypeAsStringArray(ctx, tree.Exprs(v.KMSInfo.NewKMSURI), "ALTER BACKUP")
			if err != nil {
				return nil, nil, nil, false, err
			}
			oldKmsFn, err = p.TypeAsStringArray(ctx, tree.Exprs(v.KMSInfo.OldKMSURI), "ALTER BACKUP")
			if err != nil {
				return nil, nil, nil, false, err
			}
		}
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		backup, err := fromFn()
		if err != nil {
			return err
		}

		subdir, err := subdirFn()
		if err != nil {
			return err
		}

		if subdir != "" {
			if strings.EqualFold(subdir, "LATEST") {
				// set subdir to content of latest file
				latest, err := readLatestFile(ctx, backup, p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, p.User())
				if err != nil {
					return err
				}
				subdir = latest
			}

			appendPaths := func(uri string, tailDir string) (string, error) {
				parsed, err := url.Parse(uri)
				if err != nil {
					return uri, err
				}
				parsed.Path = path.Join(parsed.Path, tailDir)
				uri = parsed.String()
				return uri, nil
			}

			if backup, err = appendPaths(backup, subdir); err != nil {
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

	return fn, nil, nil, false, nil
}

func doAlterBackupPlan(
	ctx context.Context,
	alterBackupStmt *tree.AlterBackup,
	p sql.PlanHookState,
	backup string,
	newKms []string,
	oldKms []string,
) error {
	if len(backup) < 1 {
		return errors.New("invalid base backup specified")
	}

	baseStore, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, backup, p.User())
	if err != nil {
		return errors.Wrapf(err, "failed to open backup storage location")
	}
	defer baseStore.Close()

	opts, err := readEncryptionOptions(ctx, baseStore)
	if err != nil {
		return err
	}

	ioConf := baseStore.ExternalIOConf()

	// Check that at least one of the old keys has been used to encrypt the backup in the past.
	// Use the first one that works to decrypt the ENCRYPTION-INFO file(s).
	var defaultKMSInfo *jobspb.BackupEncryptionOptions_KMSInfo
	oldKMSFound := false
	for _, old := range oldKms {
		for _, encFile := range opts {
			defaultKMSInfo, err = validateKMSURIsAgainstFullBackup([]string{old},
				newEncryptedDataKeyMapFromProtoMap(encFile.EncryptedDataKeyByKMSMasterKeyID), &backupKMSEnv{
					baseStore.Settings(),
					&ioConf,
				})

			if err == nil {
				oldKMSFound = true
				break
			}
		}
		if oldKMSFound {
			break
		}
	}
	if !oldKMSFound {
		return errors.New("no key in OLD_KMS matches a key that was previously used to encrypt the backup")
	}

	encryption := &jobspb.BackupEncryptionOptions{
		Mode:    jobspb.EncryptionMode_KMS,
		KMSInfo: defaultKMSInfo}

	// Recover the encryption key using the old key, so we can encrypt it again with the new keys.
	var plaintextDataKey []byte
	plaintextDataKey, err = getEncryptionKey(ctx, encryption, baseStore.Settings(),
		baseStore.ExternalIOConf())
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
			return errors.Wrap(err, "failed to encrypt data key when adding new KMS")
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
	return writeNewEncryptionInfoToBackup(ctx, encryptionInfo, baseStore, len(opts))
}

func writeNewEncryptionInfoToBackup(
	ctx context.Context, opts *jobspb.EncryptionInfo, dest cloud.ExternalStorage, numFiles int,
) error {
	// New encryption-info file name is in the format "ENCRYPTION-INFO-<version number>"
	newEncryptionInfoFile := fmt.Sprintf("%s-%d", backupEncryptionInfoFile, numFiles+1)

	buf, err := protoutil.Marshal(opts)
	if err != nil {
		return err
	}
	return cloud.WriteFile(ctx, dest, newEncryptionInfoFile, bytes.NewReader(buf))
}

func init() {
	sql.AddPlanHook("alter backup", alterBackupPlanHook)
}
