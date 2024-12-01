// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupdest

import (
	"context"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// The default subdirectory for incremental backups.
const (
	incBackupSubdirGlob = "/[0-9]*/[0-9]*.[0-9][0-9]/"
)

// backupSubdirRE identifies the portion of a larger path that refers to the full backup subdirectory.
var backupSubdirRE = regexp.MustCompile(`(.*)/([0-9]{4}/[0-9]{2}/[0-9]{2}-[0-9]{6}.[0-9]{2}/?)$`)

// CollectionsAndSubdir breaks up the given paths into those
// components, if applicable. An error returned if the matched
// subdirectory is different between paths.
//
// "Specific" commands, like BACKUP INTO and RESTORE FROM, don't need
// this.  "Vague" commands, like SHOW BACKUP and debug backup,
// sometimes do.
func CollectionsAndSubdir(paths []string, subdir string) ([]string, string, error) {
	if subdir != "" {
		return paths, subdir, nil
	}

	// Split out the backup name from the base directory so we can search the
	// default "incrementals" subdirectory.
	//
	// NOTE(ssd): I am unaware of a way to get to this point
	// without an explicit subdir but with multiple paths.
	output := make([]string, len(paths))
	var matchedSubdirectory string
	for i, p := range paths {
		matchResult := backupSubdirRE.FindStringSubmatch(p)
		if matchResult == nil {
			return paths, matchedSubdirectory, nil
		}
		output[i] = matchResult[1]
		if matchedSubdirectory == "" {
			matchedSubdirectory = matchResult[2]
		}
		if matchedSubdirectory != matchResult[2] {
			return paths, matchedSubdirectory, errors.Newf("provided backup locations appear to reference different full backups: %s and %s",
				matchedSubdirectory,
				matchResult[2])
		}
	}
	return output, matchedSubdirectory, nil
}

// FindPriorBackups finds "appended" incremental backups by searching
// for the subdirectories matching the naming pattern (e.g. YYMMDD/HHmmss.ss).
// If includeManifest is true the returned paths are to the manifests for the
// prior backup, otherwise it is just to the backup path.
func FindPriorBackups(
	ctx context.Context, store cloud.ExternalStorage, includeManifest bool,
) ([]string, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupdest.FindPriorBackups")
	defer sp.Finish()

	var prev []string
	if err := store.List(ctx, "", backupbase.ListingDelimDataSlash, func(p string) error {
		if ok, err := path.Match(incBackupSubdirGlob+backupbase.BackupManifestName, p); err != nil {
			return err
		} else if ok {
			if !includeManifest {
				p = strings.TrimSuffix(p, "/"+backupbase.BackupManifestName)
			}
			prev = append(prev, p)
			return nil
		}
		if ok, err := path.Match(incBackupSubdirGlob+backupbase.BackupOldManifestName, p); err != nil {
			return err
		} else if ok {
			if !includeManifest {
				p = strings.TrimSuffix(p, "/"+backupbase.BackupOldManifestName)
			}
			prev = append(prev, p)
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "reading previous backup layers")
	}
	sort.Strings(prev)
	return prev, nil
}

// backupsFromLocation is a small helper function to retrieve all prior
// backups from the specified location.
func backupsFromLocation(
	ctx context.Context, user username.SQLUsername, execCfg *sql.ExecutorConfig, loc string,
) ([]string, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupdest.backupsFromLocation")
	defer sp.Finish()

	mkStore := execCfg.DistSQLSrv.ExternalStorageFromURI
	store, err := mkStore(ctx, loc, user)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open backup storage location")
	}
	defer store.Close()
	prev, err := FindPriorBackups(ctx, store, false)
	return prev, err
}

// MakeBackupDestinationStores makes ExternalStorage handles to the passed in
// destinationDirs, and returns a cleanup function that closes this stores. It
// is the callers responsibility to call the returned cleanup function.
func MakeBackupDestinationStores(
	ctx context.Context,
	user username.SQLUsername,
	mkStore cloud.ExternalStorageFromURIFactory,
	destinationDirs []string,
) ([]cloud.ExternalStorage, func() error, error) {
	incStores := make([]cloud.ExternalStorage, len(destinationDirs))
	for i := range destinationDirs {
		store, err := mkStore(ctx, destinationDirs[i], user)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to open backup storage location")
		}
		incStores[i] = store
	}

	return incStores, func() error {
		// Close all the incremental stores in the returned cleanup function.
		for _, store := range incStores {
			if err := store.Close(); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

// ResolveIncrementalsBackupLocation returns the resolved locations of
// incremental backups by looking into either the explicitly provided
// incremental backup collections, or the full backup collections if no explicit
// incremental collections are provided.
func ResolveIncrementalsBackupLocation(
	ctx context.Context,
	user username.SQLUsername,
	execCfg *sql.ExecutorConfig,
	explicitIncrementalCollections []string,
	fullBackupCollections []string,
	subdir string,
) ([]string, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupdest.ResolveIncrementalsBackupLocation")
	defer sp.Finish()

	if len(explicitIncrementalCollections) > 0 {
		incPaths, err := backuputils.AppendPaths(explicitIncrementalCollections, subdir)
		if err != nil {
			return nil, err
		}

		// Check we can read from this location, though we don't need the backups here.
		// If we can't read, we want to throw the appropriate error so the caller
		// knows this isn't a usable incrementals store.
		// Some callers will abort, e.g. BACKUP. Others will proceed with a
		// warning, e.g. SHOW and RESTORE.
		_, err = backupsFromLocation(ctx, user, execCfg, incPaths[0])
		if err != nil {
			return nil, err
		}
		return incPaths, nil
	}

	resolvedIncrementalsBackupLocationOld, err := backuputils.AppendPaths(fullBackupCollections, subdir)
	if err != nil {
		return nil, err
	}

	// We can have >1 full backup collection specified, but each will have an
	// incremental layer iff all of them do. So it suffices to check only the
	// first.
	// Check we can read from this location, though we don't need the backups here.
	prevOld, err := backupsFromLocation(ctx, user, execCfg, resolvedIncrementalsBackupLocationOld[0])
	if err != nil {
		return nil, err
	}

	resolvedIncrementalsBackupLocation, err := backuputils.AppendPaths(fullBackupCollections, backupbase.DefaultIncrementalsSubdir, subdir)
	if err != nil {
		return nil, err
	}

	prev, err := backupsFromLocation(ctx, user, execCfg, resolvedIncrementalsBackupLocation[0])
	if err != nil {
		return nil, err
	}

	// TODO(bardin): This algorithm divides "destination resolution" and "actual backup lookup" for historical reasons,
	// but this doesn't quite make sense now that destination resolution depends on backup lookup.
	// Try to figure out a clearer way to organize this.
	if len(prevOld) > 0 && len(prev) > 0 {
		return nil, errors.New(
			"Incremental layers found in both old and new default locations. " +
				"Please choose a location manually with the `incremental_location` parameter.")
	}

	// If we have backups in the old default location, continue to use the old location.
	if len(prevOld) > 0 {
		return resolvedIncrementalsBackupLocationOld, nil
	}

	// Otherwise, use the new location.
	return resolvedIncrementalsBackupLocation, nil
}
