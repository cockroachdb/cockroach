// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupdest

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// The default subdirectory for incremental backups.
const (
	incBackupSubdirGlob = "/[0-9]*/[0-9]*.[0-9][0-9]/"
	// incBackupSubdirGlobWithSuffix is used for all backups taken on or after v25.2.
	incBackupSubdirGlobWithSuffix = "/[0-9]*/[0-9]*.[0-9][0-9]-[0-9]*.[0-9][0-9]/"
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

// FindAllIncrementalPaths finds all complete incremental backups that are
// chained off of the provided full backup subdirectory. It returns the paths to
// all incrementals relative to the subdir in the incremental storage location.
// It expects that subdir has been resolved and is not LATEST. Backups paths are
// returned in sorted ascending end time order, with ties broken in ascending
// start time. All paths are returned with a leading slash.
//
// TODO (kev-cao): On 26.2, we can fully deprecate the legacy path and remove
// the `incStore` parameter.
func FindAllIncrementalPaths(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	incStore cloud.ExternalStorage,
	rootStore cloud.ExternalStorage,
	subdir string,
	includeManifest bool,
	customIncLocation bool,
) ([]string, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupdest.FindAllIncrementalPaths")
	defer sp.Finish()

	// Backup indexes do not support custom incremental locations.
	if customIncLocation || !ReadBackupIndexEnabled.Get(&execCfg.Settings.SV) {
		return LegacyFindPriorBackups(ctx, incStore, includeManifest)
	}

	indexes, err := ListIndexes(ctx, rootStore, subdir)
	if err != nil {
		return nil, err
	}
	// Due to our policy for writing indexes, if there exists an index file for a
	// backup chain, we can assume the index is complete. So either an index has
	// been written for every backup in the chain, or there are no indexes at all.
	if len(indexes) == 0 {
		return LegacyFindPriorBackups(ctx, incStore, includeManifest)
	}

	paths, err := util.MapE(
		indexes[1:], // We skip the full backup
		func(indexFilename string) (string, error) {
			return parseBackupFilePathFromIndexFileName(subdir, indexFilename)
		},
	)
	if err != nil {
		return nil, err
	}

	if includeManifest {
		paths = util.Map(paths, func(p string) string {
			return path.Join(p, backupbase.DeprecatedBackupManifestName)
		})
	}
	return paths, nil
}

// LegacyFindPriorBackups finds "appended" incremental backups via the legacy
// path prior to the backup index. It searches for subdirectories matchingl the
// naming pattern (e.g. YYMMDD/HHmmss.ss) by delimiting on the `data/` dir.
// Backup paths are returned in ascending end time order.
//
// Note: store should be rooted at the directory containing the incremental
// backups (i.e. gs://my-bucket/backup/incrementals/2025/07/29-123456.00/)
func LegacyFindPriorBackups(
	ctx context.Context, store cloud.ExternalStorage, includeManifest bool,
) ([]string, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupdest.FindPriorBackups")
	defer sp.Finish()

	var prev []string
	if err := store.List(ctx, "", backupbase.ListingDelimDataSlash, func(p string) error {
		matchesGlob, err := path.Match(incBackupSubdirGlob+backupbase.DeprecatedBackupManifestName, p)
		if err != nil {
			return err
		} else if !matchesGlob {
			matchesGlob, err = path.Match(incBackupSubdirGlobWithSuffix+backupbase.DeprecatedBackupManifestName, p)
			if err != nil {
				return err
			}
		}

		if matchesGlob {
			if !includeManifest {
				p = strings.TrimSuffix(p, "/"+backupbase.DeprecatedBackupManifestName)
			}
			prev = append(prev, p)
			return nil
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
	ctx context.Context,
	user username.SQLUsername,
	execCfg *sql.ExecutorConfig,
	collectionURI string,
	subdir string,
	incLoc string,
	customIncLocation bool,
) ([]string, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupdest.backupsFromLocation")
	defer sp.Finish()

	mkStore := execCfg.DistSQLSrv.ExternalStorageFromURI
	rootStore, err := mkStore(ctx, collectionURI, user)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open root storage location")
	}
	defer rootStore.Close()

	incStore, err := mkStore(ctx, incLoc, user)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open backup storage location")
	}
	defer incStore.Close()

	return FindAllIncrementalPaths(
		ctx, execCfg, incStore, rootStore, subdir, false /* includeManifest */, customIncLocation,
	)
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
	stores := make([]cloud.ExternalStorage, len(destinationDirs))
	for i := range destinationDirs {
		store, err := mkStore(ctx, destinationDirs[i], user)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to open backup storage location")
		}
		stores[i] = store
	}

	return stores, func() error {
		// Close all the stores in the returned cleanup function.
		var combinedErr error
		for _, store := range stores {
			if err := store.Close(); err != nil {
				combinedErr = errors.CombineErrors(combinedErr, err)
			}
		}
		return combinedErr
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
	defaultCollectionURI, _, err := GetURIsByLocalityKV(fullBackupCollections, "")
	if err != nil {
		return nil, errors.Wrapf(err, "get default full backup collection URI")
	}
	rootStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, defaultCollectionURI, user)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open root storage location")
	}
	defer rootStore.Close()

	customIncLocation := len(explicitIncrementalCollections) > 0
	if customIncLocation {
		incPaths, err := backuputils.AppendPaths(explicitIncrementalCollections, subdir)
		if err != nil {
			return nil, err
		}

		// Check we can read from this location, though we don't need the backups here.
		// If we can't read, we want to throw the appropriate error so the caller
		// knows this isn't a usable incrementals store.
		// Some callers will abort, e.g. BACKUP. Others will proceed with a
		// warning, e.g. SHOW and RESTORE.
		_, err = backupsFromLocation(
			ctx, user, execCfg, defaultCollectionURI, subdir, incPaths[0], customIncLocation,
		)
		if err != nil {
			return nil, err
		}
		return incPaths, nil
	}
	resolvedIncrementalsBackupLocation, err := backuputils.AppendPaths(fullBackupCollections, backupbase.DefaultIncrementalsSubdir, subdir)
	if err != nil {
		return nil, err
	}
	_, err = backupsFromLocation(
		ctx, user, execCfg, defaultCollectionURI, subdir,
		resolvedIncrementalsBackupLocation[0], customIncLocation,
	)
	if err != nil {
		return nil, err
	}
	return resolvedIncrementalsBackupLocation, nil
}

// ResolveDefaultBaseIncrementalStorageLocation returns the default incremental
// storage location that contains all incremental backups.
// It is passed an array of locality-aware full backup collections and an
// optional array of explicit incremental backup locations.
//
// e.g. details for:
//
// BACKUP INTO (
//
//	'nodelocal://1/backup?COCKROACH_LOCALITY=default',
//	'nodelocal://1/backup2?COCKROACH_LOCALITY=region%3Dus-west'
//
// )
//
// returns 'nodelocal://1/backup/incrementals'
func ResolveDefaultBaseIncrementalStorageLocation(
	fullBackupCollections []string, explicitIncrementalCollections []string,
) (string, error) {
	if len(explicitIncrementalCollections) > 0 {
		defaultURI, _, err := GetURIsByLocalityKV(explicitIncrementalCollections, "")
		if err != nil {
			return "", errors.Wrapf(err, "get default incremental backup collection URI")
		}
		return defaultURI, nil
	}

	if len(fullBackupCollections) == 0 {
		return "", errors.New(
			"no full backup collections provided to resolve default incremental storage location",
		)
	}

	defaultURI, _, err := GetURIsByLocalityKV(fullBackupCollections, backupbase.DefaultIncrementalsSubdir)
	if err != nil {
		return "", errors.Wrapf(err, "get default incremental backup collection URI")
	}

	return defaultURI, nil
}

// ConstructDateBasedIncrementalFolderName constructs the name of a date-based
// incremental backup folder relative to the full subdirectory it belongs to.
//
// /2025/07/30-120000.00/20250730/130000.00-20250730-120000.00
//
//	 	                 └─────────────────────────────────────┘
//										               returns this
func ConstructDateBasedIncrementalFolderName(start, end time.Time) string {
	return fmt.Sprintf(
		"%s-%s",
		end.Format(backupbase.DateBasedIncFolderName),
		start.Format(backupbase.DateBasedIncFolderNameSuffix),
	)
}
