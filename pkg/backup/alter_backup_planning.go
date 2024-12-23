// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"net/url"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/backup/backupdest"
	"github.com/cockroachdb/cockroach/pkg/backup/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func alterBackupTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (ok bool, _ colinfo.ResultColumns, _ error) {
	alterBackupStmt, ok := stmt.(*tree.AlterBackup)
	if !ok {
		return false, nil, nil
	}
	if err := exprutil.TypeCheck(
		ctx, "ALTER BACKUP", p.SemaCtx(),
		exprutil.Strings{
			alterBackupStmt.Backup,
			alterBackupStmt.Subdir,
		},
	); err != nil {
		return false, nil, err
	}
	return true, nil, nil
}

func alterBackupPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	alterBackupStmt, ok := stmt.(*tree.AlterBackup)
	if !ok {
		return nil, nil, false, nil
	}

	if err := featureflag.CheckEnabled(
		ctx,
		p.ExecCfg(),
		featureBackupEnabled,
		"ALTER BACKUP",
	); err != nil {
		return nil, nil, false, err
	}

	exprEval := p.ExprEvaluator("ALTER BACKUP")
	backup, err := exprEval.String(ctx, alterBackupStmt.Backup)
	if err != nil {
		return nil, nil, false, err
	}

	var subdir string
	if alterBackupStmt.Subdir != nil {
		subdir, err = exprEval.String(ctx, alterBackupStmt.Subdir)
		if err != nil {
			return nil, nil, false, err
		}
	}

	var newKms []string
	var oldKms []string

	for _, cmd := range alterBackupStmt.Cmds {
		switch v := cmd.(type) {
		case *tree.AlterBackupKMS:
			newKms, err = exprEval.StringArray(ctx, tree.Exprs(v.KMSInfo.NewKMSURI))
			if err != nil {
				return nil, nil, false, err
			}
			oldKms, err = exprEval.StringArray(ctx, tree.Exprs(v.KMSInfo.OldKMSURI))
			if err != nil {
				return nil, nil, false, err
			}
		}
	}

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {

		if subdir != "" {
			if strings.EqualFold(subdir, "LATEST") {
				// set subdir to content of latest file
				latest, err := backupdest.ReadLatestFile(ctx, backup, p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, p.User())
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

		return doAlterBackupPlan(ctx, alterBackupStmt, p, backup, newKms, oldKms)
	}

	return fn, nil, false, nil
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

	opts, err := backupencryption.ReadEncryptionOptions(ctx, baseStore)
	if err != nil {
		return err
	}

	ioConf := baseStore.ExternalIOConf()
	kmsEnv := backupencryption.MakeBackupKMSEnv(
		baseStore.Settings(), &ioConf, p.ExecCfg().InternalDB, p.User(),
	)

	// Check that at least one of the old keys has been used to encrypt the backup in the past.
	// Use the first one that works to decrypt the ENCRYPTION-INFO file(s).
	var defaultKMSInfo *jobspb.BackupEncryptionOptions_KMSInfo
	oldKMSFound := false
	for _, old := range oldKms {
		for _, encFile := range opts {
			defaultKMSInfo, err = backupencryption.ValidateKMSURIsAgainstFullBackup(ctx, []string{old},
				backupencryption.NewEncryptedDataKeyMapFromProtoMap(encFile.EncryptedDataKeyByKMSMasterKeyID),
				&kmsEnv)

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
	plaintextDataKey, err = backupencryption.GetEncryptionKey(ctx, encryption, &kmsEnv)
	if err != nil {
		return err
	}

	encryptedDataKeyByKMSMasterKeyID := backupencryption.NewEncryptedDataKeyMap()

	// Add each new key user wants to add to a new data key map.
	for _, kmsURI := range newKms {
		masterKeyID, encryptedDataKey, err := backupencryption.GetEncryptedDataKeyFromURI(ctx,
			plaintextDataKey, kmsURI, &kmsEnv)
		if err != nil {
			return errors.Wrap(err, "failed to encrypt data key when adding new KMS")
		}

		encryptedDataKeyByKMSMasterKeyID.AddEncryptedDataKey(backupencryption.PlaintextMasterKeyID(masterKeyID),
			encryptedDataKey)
	}

	encryptedDataKeyMapForProto := make(map[string][]byte)
	encryptedDataKeyByKMSMasterKeyID.RangeOverMap(
		func(masterKeyID backupencryption.HashedMasterKeyID, dataKey []byte) {
			encryptedDataKeyMapForProto[string(masterKeyID)] = dataKey
		})

	encryptionInfo := &jobspb.EncryptionInfo{EncryptedDataKeyByKMSMasterKeyID: encryptedDataKeyMapForProto}

	// Write the new ENCRYPTION-INFO file.
	return backupencryption.WriteNewEncryptionInfoToBackup(ctx, encryptionInfo, baseStore, len(opts))
}

func init() {
	sql.AddPlanHook(
		"alter backup",
		alterBackupPlanHook,
		alterBackupTypeCheck,
	)
}
