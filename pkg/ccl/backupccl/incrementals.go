// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/errors"
)

// The default subdirectory for incremental backups.
const (
	DefaultIncrementalsSubdir = "incrementals"
	incBackupSubdirGlob       = "/[0-9]*/[0-9]*.[0-9][0-9]/"

	// listingDelimDataSlash is used when listing to find backups and groups all the
	// data sst files in each backup, which start with "data/", into a single result
	// that can be skipped over quickly.
	listingDelimDataSlash = "data/"

	URLSeparator = '/'
)

// backupSubdirRE identifies the portion of a larger path that refers to the full backup subdirectory.
var backupSubdirRE = regexp.MustCompile(`(.*)/([0-9]{4}/[0-9]{2}/[0-9]{2}-[0-9]{6}.[0-9]{2}/?)$`)

// CollectionAndSubdir breaks up a path into those components, if applicable.
// "Specific" commands, like BACKUP INTO and RESTORE FROM, don't need this.
// "Vague" commands, like SHOW BACKUP and debug backup, sometimes do.
func CollectionAndSubdir(path string, subdir string) (string, string) {
	if subdir != "" {
		return path, subdir
	}

	// Split out the backup name from the base directory so we can search the
	// default "incrementals" subdirectory.
	matchResult := backupSubdirRE.FindStringSubmatch(path)
	if matchResult == nil {
		return path, subdir
	}
	return matchResult[1], matchResult[2]
}

// FindPriorBackups finds "appended" incremental backups by searching
// for the subdirectories matching the naming pattern (e.g. YYMMDD/HHmmss.ss).
// If includeManifest is true the returned paths are to the manifests for the
// prior backup, otherwise it is just to the backup path.
func FindPriorBackups(
	ctx context.Context, store cloud.ExternalStorage, includeManifest bool,
) ([]string, error) {
	var prev []string
	if err := store.List(ctx, "", listingDelimDataSlash, func(p string) error {
		if ok, err := path.Match(incBackupSubdirGlob+backupManifestName, p); err != nil {
			return err
		} else if ok {
			if !includeManifest {
				p = strings.TrimSuffix(p, "/"+backupManifestName)
			}
			prev = append(prev, p)
			return nil
		}
		if ok, err := path.Match(incBackupSubdirGlob+backupOldManifestName, p); err != nil {
			return err
		} else if ok {
			if !includeManifest {
				p = strings.TrimSuffix(p, "/"+backupOldManifestName)
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

func appendPaths(uris []string, tailDir ...string) ([]string, error) {
	retval := make([]string, len(uris))
	for i, uri := range uris {
		parsed, err := url.Parse(uri)
		if err != nil {
			return nil, err
		}
		joinArgs := append([]string{parsed.Path}, tailDir...)
		parsed.Path = JoinURLPath(joinArgs...)
		retval[i] = parsed.String()
	}
	return retval, nil
}

// JoinURLPath forces a relative path join by removing any leading slash, then
// re-prepending it later.
//
// Stores are an odd combination of absolute and relative path.
// They present as absolute paths, since they contain a hostname. URL.Parse
// thus prepends each URL.Path with a leading slash.
// But some schemes, e.g. nodelocal, can legally travel _above_ the ostensible
// root (e.g. nodelocal://0/.../). This is not typically possible in file
// paths, and the standard path package doesn't like it. Specifically, it will
// clean up something like nodelocal://0/../ to nodelocal://0. This is normally
// correct behavior, but is wrong here.
//
// In point of fact we block this URLs resolved this way elsewhere. But we
// still want to make sure to resolve the paths correctly here. We don't want
// to accidentally correct an unauthorized file path to an authorized one, then
// write a backup to an unexpected place or print the wrong error message on
// a restore.
func JoinURLPath(args ...string) string {
	argsCopy := make([]string, 0)
	for _, arg := range args {
		if len(arg) == 0 {
			continue
		}
		// We only want non-empty tokens.
		argsCopy = append(argsCopy, arg)
	}
	if len(argsCopy) == 0 {
		return path.Join(argsCopy...)
	}

	// We have at least 1 arg, and each has at least length 1.
	isAbs := false
	if argsCopy[0][0] == URLSeparator {
		isAbs = true
		argsCopy[0] = argsCopy[0][1:]
	}
	joined := path.Join(argsCopy...)
	if isAbs {
		joined = string(URLSeparator) + joined
	}
	return joined
}

// backupsFromLocation is a small helper function to retrieve all prior
// backups from the specified location.
func backupsFromLocation(
	ctx context.Context, user username.SQLUsername, execCfg *sql.ExecutorConfig, loc string,
) ([]string, error) {
	mkStore := execCfg.DistSQLSrv.ExternalStorageFromURI
	store, err := mkStore(ctx, loc, user)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open backup storage location")
	}
	defer store.Close()
	prev, err := FindPriorBackups(ctx, store, false)
	return prev, err
}

func resolveIncrementalsBackupLocation(
	ctx context.Context,
	user username.SQLUsername,
	execCfg *sql.ExecutorConfig,
	explicitIncrementalCollections []string,
	fullBackupCollections []string,
	subdir string,
) ([]string, error) {
	if len(explicitIncrementalCollections) > 0 {
		incPaths, err := appendPaths(explicitIncrementalCollections, subdir)
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

	resolvedIncrementalsBackupLocationOld, err := appendPaths(fullBackupCollections, subdir)
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

	resolvedIncrementalsBackupLocation, err := appendPaths(fullBackupCollections, DefaultIncrementalsSubdir, subdir)
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

	// If the cluster isn't fully migrated, or we have backups in the old default
	// location, continue to use the old location.
	if len(prevOld) > 0 || !execCfg.Settings.Version.IsActive(ctx, clusterversion.IncrementalBackupSubdir) {
		return resolvedIncrementalsBackupLocationOld, nil
	}

	// Otherwise, use the new location.
	return resolvedIncrementalsBackupLocation, nil
}
