package backupccl

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"net/url"
	"path"
	"strings"
)

const (
	backupOldEncKMS = "OLD_KMS"
	backupNewEncKMS = "NEW_KMS"
)

func init() {
	sql.AddPlanHook(alterBackupPlanHook)
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

		var oldKms []string
		oldKms, err = oldKmsFn()

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
	// TODO(darrylwong): implement this function
	return errors.AssertionFailedf("ALTER BACKUP not yet implemented")
}
