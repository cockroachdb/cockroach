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
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// IncrementalsBackupSource describes the location of a set of incremental backups.
type IncrementalsBackupSource int

// Describes the location of a set of incremental backups.
const (
	None IncrementalsBackupSource = iota // Unused.
	// The old default. Incremental backups go in the same collection as the
	// full backup they're based on.
	SameAsFull
	// The user has specified a custom collection for incremental backups.
	Custom
	// The new default. Incremental backups go in the subdirectory "incrementals"
	// in same collection as the full backup they're based on.
	Incrementals
)

// The default subdirectory for incremental backups.
const (
	DefaultIncrementalsSubdir = "incrementals"
	incBackupSubdirGlob       = "/[0-9]*/[0-9]*.[0-9][0-9]/"

	// listingDelimDataSlash is used when listing to find backups and groups all the
	// data sst files in each backup, which start with "data/", into a single result
	// that can be skipped over quickly.
	listingDelimDataSlash = "data/"
)

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
		parsed.Path = joinURLPath(joinArgs...)
		retval[i] = parsed.String()
	}
	return retval, nil
}

// Stores are an odd combination of absolute and relative path.
// They present as absolute paths, since they contain a hostname. URL.Parse
// thus prepends each URL.Path with a leading slash.
// But some schemes, e.g. nodelocal, can legally travel _above_ the ostensible
// root (e.g. nodelocal://0/.../). This is not typically possible in file
// paths, and the standard path package doesn't like it. Specifically, it will
// clean up something like nodelocal://0/../ to nodelocal://0. This is normally
// correct behavior, but is wrong here.
//
// In point of fact we block this behavior at other points. But we want to
// make sure to resolve the paths correctly here. We don't want to accidentally
// correct an unauthorized file path to an authorized one, then write a backup
// to an unexpected place or print the wrong error message on a restore.
func joinURLPath(args ...string) string {
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
	if argsCopy[0][0] == filepath.Separator {
		isAbs = true
		argsCopy[0] = argsCopy[0][1:]
	}
	joined := path.Join(argsCopy...)
	if isAbs {
		joined = string(filepath.Separator) + joined
	}
	return joined
}

func resolveIncrementalsBackupLocation(
	ctx context.Context,
	user security.SQLUsername,
	execCfg *sql.ExecutorConfig,
	explicitIncrementalCollections []string,
	fullBackupCollections []string,
	subdir string,
) ([]string, error) {
	if len(explicitIncrementalCollections) > 0 {
		return appendPaths(explicitIncrementalCollections, subdir)
	}
	var prev []string
	var err error
	mkStore := execCfg.DistSQLSrv.ExternalStorageFromURI

	resolvedIncrementalsBackupLocationOld, err := appendPaths(fullBackupCollections, subdir)
	if err != nil {
		return nil, err
	}

	// We can have >1 full backup collection specified, but each will have an
	// incremental layer iff all of them do. So it suffices to check only the
	// first.
	store, err := mkStore(ctx, resolvedIncrementalsBackupLocationOld[0], user)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open backup storage location")
	}
	defer store.Close()
	prev, err = FindPriorBackups(ctx, store, false)

	if errors.Is(err, cloud.ErrListingUnsupported) {
		log.Warningf(ctx, "storage sink %T does not support listing, only resolving the base backup", store)
		// Can't read incremental backups here, so don't write them either.
		return nil, nil
	}

	if len(prev) > 0 || !execCfg.Settings.Version.IsActive(ctx, clusterversion.IncrementalBackupSubdir) {
		return resolvedIncrementalsBackupLocationOld, nil
	}
	return appendPaths(fullBackupCollections, DefaultIncrementalsSubdir, subdir)
}
