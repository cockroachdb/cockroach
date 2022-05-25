// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupdestination

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// featureFullBackupUserSubdir, when true, will create a full backup at a user
// specified subdirectory if no backup already exists at that subdirectory. As
// of 22.1, this feature is default disabled, and will be totally disabled by 22.2.
var featureFullBackupUserSubdir = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"bulkio.backup.deprecated_full_backup_with_subdir.enabled",
	"when true, a backup command with a user specified subdirectory will create a full backup at"+
		" the subdirectory if no backup already exists at that subdirectory.",
	false,
).WithPublic()

// ErrLocalityDescriptor is a special error returned when the LocalityDescriptor
// is not found.
var ErrLocalityDescriptor = errors.New(`Locality Descriptor not found`)

const (
	// backupMetadataDirectory is the directory where metadata about a backup
	// collection is stored. In v22.1 it contains the latest directory.
	backupMetadataDirectory = "metadata"

	// LatestHistoryDirectory is the directory where all 22.1 and beyond
	// LATEST files will be stored as we no longer want to overwrite it.
	LatestHistoryDirectory = backupMetadataDirectory + "/" + "latest"

	// LatestFileName is the name of a file in the collection which contains the
	// path of the most recently taken full backup in the backup collection.
	LatestFileName = "LATEST"

	localityURLParam = "COCKROACH_LOCALITY"

	defaultLocalityValue = "default"
)

// On some cloud storage platforms (i.e. GS, S3), backups in a base bucket may
// omit a leading slash. However, backups in a subdirectory of a base bucket
// will contain one.
var backupPathRE = regexp.MustCompile("^/?[^\\/]+/[^\\/]+/[^\\/]+/" + backupinfo.BackupManifestName + "$")

// FetchPreviousBackups takes a list of URIs of previous backups and returns
// their manifest as well as the encryption options of the first backup in the
// chain.
func FetchPreviousBackups(
	ctx context.Context,
	mem *mon.BoundAccount,
	user username.SQLUsername,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	prevBackupURIs []string,
	encryptionParams jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) ([]backuppb.BackupManifest, *jobspb.BackupEncryptionOptions, int64, error) {
	if len(prevBackupURIs) == 0 {
		return nil, nil, 0, nil
	}

	baseBackup := prevBackupURIs[0]
	encryptionOptions, err := backupencryption.GetEncryptionFromBase(ctx, user, makeCloudStorage, baseBackup,
		encryptionParams, kmsEnv)
	if err != nil {
		return nil, nil, 0, err
	}
	prevBackups, size, err := getBackupManifests(ctx, mem, user, makeCloudStorage, prevBackupURIs,
		encryptionOptions)
	if err != nil {
		return nil, nil, 0, err
	}

	return prevBackups, encryptionOptions, size, nil
}

func getLocalityAndBaseURI(uri, appendPath string) (string, string, error) {
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}
	q := parsedURI.Query()
	localityKV := q.Get(localityURLParam)
	// Remove the backup locality parameter.
	q.Del(localityURLParam)
	parsedURI.RawQuery = q.Encode()

	parsedURI.Path = JoinURLPath(parsedURI.Path, appendPath)

	baseURI := parsedURI.String()
	return localityKV, baseURI, nil
}

// GetURIsByLocalityKV takes a slice of URIs for a single (possibly partitioned)
// backup, and returns the default backup destination URI and a map of all other
// URIs by locality KV, appending appendPath to the path component of both the
// default URI and all the locality URIs. The URIs in the result do not include
// the COCKROACH_LOCALITY parameter.
func GetURIsByLocalityKV(
	to []string, appendPath string,
) (defaultURI string, urisByLocalityKV map[string]string, err error) {
	urisByLocalityKV = make(map[string]string)
	if len(to) == 1 {
		localityKV, baseURI, err := getLocalityAndBaseURI(to[0], appendPath)
		if err != nil {
			return "", nil, err
		}
		if localityKV != "" && localityKV != defaultLocalityValue {
			return "", nil, errors.Errorf("%s %s is invalid for a single BACKUP location",
				localityURLParam, localityKV)
		}
		return baseURI, urisByLocalityKV, nil
	}

	for _, uri := range to {
		localityKV, baseURI, err := getLocalityAndBaseURI(uri, appendPath)
		if err != nil {
			return "", nil, err
		}
		if localityKV == "" {
			return "", nil, errors.Errorf(
				"multiple URLs are provided for partitioned BACKUP, but %s is not specified",
				localityURLParam,
			)
		}
		if localityKV == defaultLocalityValue {
			if defaultURI != "" {
				return "", nil, errors.Errorf("multiple default URLs provided for partition backup")
			}
			defaultURI = baseURI
		} else {
			kv := roachpb.Tier{}
			if err := kv.FromString(localityKV); err != nil {
				return "", nil, errors.Wrap(err, "failed to parse backup locality")
			}
			if _, ok := urisByLocalityKV[localityKV]; ok {
				return "", nil, errors.Errorf("duplicate URIs for locality %s", localityKV)
			}
			urisByLocalityKV[localityKV] = baseURI
		}
	}
	if defaultURI == "" {
		return "", nil, errors.Errorf("no default URL provided for partitioned backup")
	}
	return defaultURI, urisByLocalityKV, nil
}

// ResolveDest resolves the true destination of a backup. The backup command
// provided by the user may point to a backup collection, or a backup location
// which auto-appends incremental backups to it. This method checks for these
// cases and finds the actual directory where we'll write this new backup.
//
// In addition, in this case that this backup is an incremental backup (either
// explicitly, or due to the auto-append feature), it will resolve the
// encryption options based on the base backup, as well as find all previous
// backup manifests in the backup chain.
func ResolveDest(
	ctx context.Context,
	user username.SQLUsername,
	dest jobspb.BackupDetails_Destination,
	endTime hlc.Timestamp,
	incrementalFrom []string,
	execCfg *sql.ExecutorConfig,
) (
	collectionURI string,
	plannedBackupDefaultURI string, /* the full path for the planned backup */
	/* chosenSuffix is the automatically chosen suffix within the collection path
	   if we're backing up INTO a collection. */
	chosenSuffix string,
	urisByLocalityKV map[string]string,
	prevBackupURIs []string, /* list of full paths for previous backups in the chain */
	err error,
) {
	makeCloudStorage := execCfg.DistSQLSrv.ExternalStorageFromURI

	defaultURI, _, err := GetURIsByLocalityKV(dest.To, "")
	if err != nil {
		return "", "", "", nil, nil, err
	}

	chosenSuffix = dest.Subdir

	if chosenSuffix != "" {
		// The legacy backup syntax, BACKUP TO, leaves the dest.Subdir and collection parameters empty.
		collectionURI = defaultURI

		if chosenSuffix == LatestFileName {
			latest, err := ReadLatestFile(ctx, defaultURI, makeCloudStorage, user)
			if err != nil {
				return "", "", "", nil, nil, err
			}
			chosenSuffix = latest
		}
	}

	plannedBackupDefaultURI, urisByLocalityKV, err = GetURIsByLocalityKV(dest.To, chosenSuffix)
	if err != nil {
		return "", "", "", nil, nil, err
	}

	// At this point, the plannedBackupDefaultURI is the full path for the backup. For BACKUP
	// INTO, this path includes the chosenSuffix. Once this function returns, the
	// plannedBackupDefaultURI will be the full path for this backup in planning.
	if len(incrementalFrom) != 0 {
		// Legacy backup with deprecated BACKUP TO-syntax.
		prevBackupURIs = incrementalFrom
		return collectionURI, plannedBackupDefaultURI, chosenSuffix, urisByLocalityKV, prevBackupURIs, nil
	}

	defaultStore, err := makeCloudStorage(ctx, plannedBackupDefaultURI, user)
	if err != nil {
		return "", "", "", nil, nil, err
	}
	defer defaultStore.Close()
	exists, err := backupinfo.ContainsManifest(ctx, defaultStore)
	if err != nil {
		return "", "", "", nil, nil, err
	}
	if exists && !dest.Exists && chosenSuffix != "" && execCfg.Settings.Version.IsActive(ctx,
		clusterversion.Start22_1) {
		// We disallow a user from writing a full backup to a path in a collection containing an
		// existing backup iff we're 99.9% confident this backup was planned on a 22.1 node.
		return "",
			"",
			"",
			nil,
			nil,
			errors.Newf("A full backup already exists in %s. "+
				"Consider running an incremental backup to this full backup via `BACKUP INTO '%s' IN '%s'`",
				plannedBackupDefaultURI, chosenSuffix, dest.To[0])

	} else if !exists {
		if dest.Exists {
			// Implies the user passed a subdirectory in their backup command, either
			// explicitly or using LATEST; however, we could not find an existing
			// backup in that subdirectory.
			// - Pre 22.1: this was fine. we created a full backup in their specified subdirectory.
			// - 22.1: throw an error: full backups with an explicit subdirectory are deprecated.
			// User can use old behavior by switching the 'bulkio.backup.full_backup_with_subdir.
			// enabled' to true.
			// - 22.2+: the backup will fail unconditionally.
			// TODO (msbutler): throw error in 22.2
			if err := featureflag.CheckEnabled(
				ctx,
				execCfg,
				featureFullBackupUserSubdir,
				"'Full Backup with user defined subdirectory'",
			); err != nil {
				return "", "", "", nil, nil, errors.Wrapf(err,
					"The full backup cannot get written to '%s', a user defined subdirectory. "+
						"To take a full backup, remove the subdirectory from the backup command, "+
						"(i.e. run 'BACKUP ... INTO <collectionURI>'). "+
						"Or, to take a full backup at a specific subdirectory, "+
						"enable the deprecated syntax by switching the 'bulkio.backup."+
						"deprecated_full_backup_with_subdir.enable' cluster setting to true; "+
						"however, note this deprecated syntax will not be available in a future release.", chosenSuffix)
			}
		}
		// There's no full backup in the resolved subdirectory; therefore, we're conducting a full backup.
		return collectionURI, plannedBackupDefaultURI, chosenSuffix, urisByLocalityKV, prevBackupURIs, nil
	}

	// The defaultStore contains a full backup; consequently, we're conducting an incremental backup.
	fullyResolvedIncrementalsLocation, err := ResolveIncrementalsBackupLocation(
		ctx,
		user,
		execCfg,
		dest.IncrementalStorage,
		dest.To,
		chosenSuffix)
	if err != nil {
		return "", "", "", nil, nil, err
	}

	priorsDefaultURI, _, err := GetURIsByLocalityKV(fullyResolvedIncrementalsLocation, "")
	if err != nil {
		return "", "", "", nil, nil, err
	}
	incrementalStore, err := makeCloudStorage(ctx, priorsDefaultURI, user)
	if err != nil {
		return "", "", "", nil, nil, err
	}
	defer incrementalStore.Close()

	priors, err := FindPriorBackups(ctx, incrementalStore, OmitManifest)
	if err != nil {
		return "", "", "", nil, nil, errors.Wrap(err, "adjusting backup destination to append new layer to existing backup")
	}

	for _, prior := range priors {
		priorURI, err := url.Parse(priorsDefaultURI)
		if err != nil {
			return "", "", "", nil, nil, errors.Wrapf(err, "parsing default backup location %s",
				priorsDefaultURI)
		}
		priorURI.Path = JoinURLPath(priorURI.Path, prior)
		prevBackupURIs = append(prevBackupURIs, priorURI.String())
	}
	prevBackupURIs = append([]string{plannedBackupDefaultURI}, prevBackupURIs...)

	// Within the chosenSuffix dir, differentiate incremental backups with partName.
	partName := endTime.GoTime().Format(backupinfo.DateBasedIncFolderName)
	defaultIncrementalsURI, urisByLocalityKV, err := GetURIsByLocalityKV(fullyResolvedIncrementalsLocation, partName)
	if err != nil {
		return "", "", "", nil, nil, err
	}
	return collectionURI, defaultIncrementalsURI, chosenSuffix, urisByLocalityKV, prevBackupURIs, nil
}

// getBackupManifests fetches the backup manifest from a list of backup URIs.
func getBackupManifests(
	ctx context.Context,
	mem *mon.BoundAccount,
	user username.SQLUsername,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	backupURIs []string,
	encryption *jobspb.BackupEncryptionOptions,
) ([]backuppb.BackupManifest, int64, error) {
	manifests := make([]backuppb.BackupManifest, len(backupURIs))
	if len(backupURIs) == 0 {
		return manifests, 0, nil
	}

	memMu := struct {
		syncutil.Mutex
		total int64
		mem   *mon.BoundAccount
	}{}
	memMu.mem = mem

	g := ctxgroup.WithContext(ctx)
	for i := range backupURIs {
		i := i
		// boundAccount isn't threadsafe so we'll make a new one this goroutine to
		// pass while reading. When it is done, we'll lock an mu, reserve its size
		// from the main one tracking the total amount reserved.
		subMem := mem.Monitor().MakeBoundAccount()
		g.GoCtx(func(ctx context.Context) error {
			defer subMem.Close(ctx)
			// TODO(lucy): We may want to upgrade the table descs to the newer
			// foreign key representation here, in case there are backups from an
			// older cluster. Keeping the descriptors as they are works for now
			// since all we need to do is get the past backups' table/index spans,
			// but it will be safer for future code to avoid having older-style
			// descriptors around.
			uri := backupURIs[i]
			desc, size, err := backupinfo.ReadBackupManifestFromURI(
				ctx, &subMem, uri, user, makeCloudStorage, encryption,
			)
			if err != nil {
				return errors.Wrapf(err, "failed to read backup from %q",
					backupinfo.RedactURIForErrorMessage(uri))
			}

			memMu.Lock()
			err = memMu.mem.Grow(ctx, size)

			if err == nil {
				memMu.total += size
				manifests[i] = desc
			}
			subMem.Shrink(ctx, size)
			memMu.Unlock()

			return err
		})
	}

	if err := g.Wait(); err != nil {
		mem.Shrink(ctx, memMu.total)
		return nil, 0, err
	}

	return manifests, memMu.total, nil
}

// ReadLatestFile reads the LATEST file at the provided URI.
func ReadLatestFile(
	ctx context.Context,
	collectionURI string,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	user username.SQLUsername,
) (string, error) {
	collection, err := makeCloudStorage(ctx, collectionURI, user)
	if err != nil {
		return "", err
	}
	defer collection.Close()

	latestFile, err := FindLatestFile(ctx, collection)

	if err != nil {
		if errors.Is(err, cloud.ErrFileDoesNotExist) {
			return "", pgerror.Wrapf(err, pgcode.UndefinedFile, "path does not contain a completed latest backup")
		}
		return "", pgerror.WithCandidateCode(err, pgcode.Io)
	}
	latest, err := ioctx.ReadAll(ctx, latestFile)
	if err != nil {
		return "", err
	}
	if len(latest) == 0 {
		return "", errors.Errorf("malformed LATEST file")
	}
	return string(latest), nil
}

// FindLatestFile returns a ioctx.ReaderCloserCtx of the most recent LATEST
// file. First it tries reading from the latest directory. If
// the backup is from an older version, it may not exist there yet so
// it tries reading in the base directory if the first attempt fails.
func FindLatestFile(
	ctx context.Context, exportStore cloud.ExternalStorage,
) (ioctx.ReadCloserCtx, error) {
	var latestFile string
	var latestFileFound bool
	// First try reading from the metadata/latest directory. If the backup
	// is from an older version, it may not exist there yet so try reading
	// in the base directory if the first attempt fails.

	// We name files such that the most recent latest file will always
	// be at the top, so just grab the first filename.
	err := exportStore.List(ctx, LatestHistoryDirectory, "", func(p string) error {
		p = strings.TrimPrefix(p, "/")
		latestFile = p
		latestFileFound = true
		// We only want the first latest file so return an error that it is
		// done listing.
		return cloud.ErrListingDone
	})
	// If the list failed because the storage used does not support listing,
	// such as http, we can try reading the non-timestamped backup latest
	// file directly. This can still fail if it is a mixed cluster and the
	// latest file was written in the base directory.
	if errors.Is(err, cloud.ErrListingUnsupported) {
		r, err := exportStore.ReadFile(ctx, LatestHistoryDirectory+"/"+LatestFileName)
		if err == nil {
			return r, nil
		}
	} else if err != nil && !errors.Is(err, cloud.ErrListingDone) {
		return nil, err
	}

	if latestFileFound {
		return exportStore.ReadFile(ctx, LatestHistoryDirectory+"/"+latestFile)
	}

	// The latest file couldn't be found in the latest directory,
	// try the base directory instead.
	r, err := exportStore.ReadFile(ctx, LatestFileName)
	if err != nil {
		return nil, errors.Wrap(err, "LATEST file could not be read in base or metadata directory")
	}
	return r, nil
}

// WriteNewLatestFile writes a new LATEST file to both the base directory
// and latest-history directory, depending on cluster version.
func WriteNewLatestFile(
	ctx context.Context, settings *cluster.Settings, exportStore cloud.ExternalStorage, suffix string,
) error {
	// If the cluster is still running on a mixed version, we want to write
	// to the base directory instead of the metadata/latest directory. That
	// way an old node can still find the LATEST file.
	if !settings.Version.IsActive(ctx, clusterversion.BackupDoesNotOverwriteLatestAndCheckpoint) {
		return cloud.WriteFile(ctx, exportStore, LatestFileName, strings.NewReader(suffix))
	}

	// HTTP storage does not support listing and so we cannot rely on the
	// above-mentioned List method to return us the most recent latest file.
	// Instead, we disregard write once semantics and always read and write
	// a non-timestamped latest file for HTTP.
	if exportStore.Conf().Provider == roachpb.ExternalStorageProvider_http {
		return cloud.WriteFile(ctx, exportStore, LatestFileName, strings.NewReader(suffix))
	}

	// We timestamp the latest files in order to enforce write once backups.
	// When the job goes to read these timestamped files, it will List
	// the latest files and pick the file whose name is lexicographically
	// sorted to the top. This will be the last latest file we write. It
	// Takes the one's complement of the timestamp so that files are sorted
	// lexicographically such that the most recent is always the top.
	return cloud.WriteFile(ctx, exportStore, newTimestampedLatestFileName(), strings.NewReader(suffix))
}

// newTimestampedLatestFileName returns a string of a new latest filename
// with a suffixed version. It returns it in the format of LATEST-<version>
// where version is a hex encoded one's complement of the timestamp.
// This means that as long as the supplied timestamp is correct, the filenames
// will adhere to a lexicographical/utf-8 ordering such that the most
// recent file is at the top.
func newTimestampedLatestFileName() string {
	var buffer []byte
	buffer = encoding.EncodeStringDescending(buffer, timeutil.Now().String())
	return fmt.Sprintf("%s/%s-%s", LatestHistoryDirectory, LatestFileName, hex.EncodeToString(buffer))
}

// CheckForLatestFileInCollection checks whether the directory pointed by store contains the
// LatestFileName pointer directory.
func CheckForLatestFileInCollection(
	ctx context.Context, store cloud.ExternalStorage,
) (bool, error) {
	r, err := FindLatestFile(ctx, store)
	if err != nil {
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			return false, pgerror.WithCandidateCode(err, pgcode.Io)
		}

		r, err = store.ReadFile(ctx, LatestFileName)
	}
	if err != nil {
		if errors.Is(err, cloud.ErrFileDoesNotExist) {
			return false, nil
		}
		return false, pgerror.WithCandidateCode(err, pgcode.Io)
	}
	r.Close(ctx)
	return true, nil
}

// ListFullBackupsInCollection lists full backup paths in the collection
// of an export store
func ListFullBackupsInCollection(
	ctx context.Context, store cloud.ExternalStorage,
) ([]string, error) {
	var backupPaths []string
	if err := store.List(ctx, "", listingDelimDataSlash, func(f string) error {
		if backupPathRE.MatchString(f) {
			backupPaths = append(backupPaths, f)
		}
		return nil
	}); err != nil {
		// Can't happen, just required to handle the error for lint.
		return nil, err
	}
	for i, backupPath := range backupPaths {
		backupPaths[i] = strings.TrimSuffix(backupPath, "/"+backupinfo.BackupManifestName)
	}
	return backupPaths, nil
}

// getLocalityInfo takes a list of stores and their URIs, along with the main
// backup manifest searches each for the locality pieces listed in the the
// main manifest, returning the mapping.
func getLocalityInfo(
	ctx context.Context,
	stores []cloud.ExternalStorage,
	uris []string,
	mainBackupManifest backuppb.BackupManifest,
	encryption *jobspb.BackupEncryptionOptions,
	prefix string,
) (jobspb.RestoreDetails_BackupLocalityInfo, error) {
	var info jobspb.RestoreDetails_BackupLocalityInfo
	// Now get the list of expected partial per-store backup manifest filenames
	// and attempt to find them.
	urisByOrigLocality := make(map[string]string)
	for _, filename := range mainBackupManifest.PartitionDescriptorFilenames {
		if prefix != "" {
			filename = path.Join(prefix, filename)
		}
		found := false
		for i, store := range stores {
			// Iterate through the available stores in case the user moved a locality
			// partition, guarding against stale backup manifest info. In addition,
			// two locality aware URIs may end up writing to the same location (e.g.
			// in testing, 'nodelocal://0/foo?COCKROACH_LOCALITY=default' and
			// 'nodelocal://1/foo?COCKROACH_LOCALITY=dc=d1' will write to the same
			// tempdir), implying that it is possible for files that the manifest
			// claims are stored in two different localities, are actually stored in
			// the same place.
			if desc, _, err := backupinfo.ReadBackupPartitionDescriptor(ctx, nil /*mem*/, store, filename, encryption); err == nil {
				if desc.BackupID != mainBackupManifest.ID {
					return info, errors.Errorf(
						"expected backup part to have backup ID %s, found %s",
						mainBackupManifest.ID, desc.BackupID,
					)
				}
				origLocalityKV := desc.LocalityKV
				kv := roachpb.Tier{}
				if err := kv.FromString(origLocalityKV); err != nil {
					return info, errors.Wrapf(err, "reading backup manifest from %s",
						backupinfo.RedactURIForErrorMessage(uris[i]))
				}
				if _, ok := urisByOrigLocality[origLocalityKV]; ok {
					return info, errors.Errorf("duplicate locality %s found in backup", origLocalityKV)
				}
				urisByOrigLocality[origLocalityKV] = uris[i]
				found = true
				break
			}
		}
		if !found {
			return info, errors.Mark(errors.Newf("expected manifest %s not found in backup locations", filename), ErrLocalityDescriptor)
		}
	}
	info.URIsByOriginalLocalityKV = urisByOrigLocality
	return info, nil
}

const (
	// IncludeManifest is a named const that can be passed to FindPriorBackups.
	IncludeManifest = true
	// OmitManifest is a named const that can be passed to FindPriorBackups.
	OmitManifest = false
)

func validateEndTimeAndTruncate(
	defaultURIs []string,
	mainBackupManifests []backuppb.BackupManifest,
	localityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	endTime hlc.Timestamp,
) ([]string, []backuppb.BackupManifest, []jobspb.RestoreDetails_BackupLocalityInfo, error) {
	// Check that the requested target time, if specified, is valid for the list
	// of incremental backups resolved, truncating the results to the backup that
	// contains the target time.
	if endTime.IsEmpty() {
		return defaultURIs, mainBackupManifests, localityInfo, nil
	}
	for i, b := range mainBackupManifests {
		// Find the backup that covers the requested time.
		if !(b.StartTime.Less(endTime) && endTime.LessEq(b.EndTime)) {
			continue
		}

		// Ensure that the backup actually has revision history.
		if !endTime.Equal(b.EndTime) {
			if b.MVCCFilter != backuppb.MVCCFilter_All {
				const errPrefix = "invalid RESTORE timestamp: restoring to arbitrary time requires that BACKUP for requested time be created with 'revision_history' option."
				if i == 0 {
					return nil, nil, nil, errors.Errorf(
						errPrefix+" nearest backup time is %s",
						timeutil.Unix(0, b.EndTime.WallTime).UTC(),
					)
				}
				return nil, nil, nil, errors.Errorf(
					errPrefix+" nearest BACKUP times are %s or %s",
					timeutil.Unix(0, mainBackupManifests[i-1].EndTime.WallTime).UTC(),
					timeutil.Unix(0, b.EndTime.WallTime).UTC(),
				)
			}
			// Ensure that the revision history actually covers the requested time -
			// while the BACKUP's start and end might contain the requested time for
			// example if start time is 0 (full backup), the revision history was
			// only captured since the GC window. Note that the RevisionStartTime is
			// the latest for ranges backed up.
			if endTime.LessEq(b.RevisionStartTime) {
				return nil, nil, nil, errors.Errorf(
					"invalid RESTORE timestamp: BACKUP for requested time only has revision history"+
						" from %v", timeutil.Unix(0, b.RevisionStartTime.WallTime).UTC(),
				)
			}
		}
		return defaultURIs[:i+1], mainBackupManifests[:i+1], localityInfo[:i+1], nil

	}

	return nil, nil, nil, errors.Errorf(
		"invalid RESTORE timestamp: supplied backups do not cover requested time",
	)
}

// ResolveBackupManifests resolves the URIs that point to the incremental layers
// (each of which can be partitioned) of backups into the actual backup
// manifests and metadata required to RESTORE. If only one layer is explicitly
// provided, it is inspected to see if it contains "appended" layers internally
// that are then expanded into the result layers returned, similar to if those
// layers had been specified in `from` explicitly.
func ResolveBackupManifests(
	ctx context.Context,
	mem *mon.BoundAccount,
	baseStores []cloud.ExternalStorage,
	mkStore cloud.ExternalStorageFromURIFactory,
	fullyResolvedBaseDirectory []string,
	fullyResolvedIncrementalsDirectory []string,
	endTime hlc.Timestamp,
	encryption *jobspb.BackupEncryptionOptions,
	user username.SQLUsername,
) (
	defaultURIs []string,
	// mainBackupManifests contains the manifest located at each defaultURI in the backup chain.
	mainBackupManifests []backuppb.BackupManifest,
	localityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	reservedMemSize int64,
	_ error,
) {
	var ownedMemSize int64
	defer func() {
		if ownedMemSize != 0 {
			mem.Shrink(ctx, ownedMemSize)
		}
	}()
	baseManifest, memSize, err := backupinfo.ReadBackupManifestFromStore(ctx, mem, baseStores[0], encryption)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	ownedMemSize += memSize

	incStores := make([]cloud.ExternalStorage, len(fullyResolvedIncrementalsDirectory))
	for i := range fullyResolvedIncrementalsDirectory {
		store, err := mkStore(ctx, fullyResolvedIncrementalsDirectory[i], user)
		if err != nil {
			return nil, nil, nil, 0, errors.Wrapf(err, "failed to open backup storage location")
		}
		defer store.Close()
		incStores[i] = store
	}

	var prev []string
	if len(incStores) > 0 {
		prev, err = FindPriorBackups(ctx, incStores[0], IncludeManifest)
		if err != nil {
			return nil, nil, nil, 0, err
		}
	}
	numLayers := len(prev) + 1

	defaultURIs = make([]string, numLayers)
	mainBackupManifests = make([]backuppb.BackupManifest, numLayers)
	localityInfo = make([]jobspb.RestoreDetails_BackupLocalityInfo, numLayers)

	// Setup the full backup layer explicitly.
	defaultURIs[0] = fullyResolvedBaseDirectory[0]
	mainBackupManifests[0] = baseManifest
	localityInfo[0], err = getLocalityInfo(
		ctx, baseStores, fullyResolvedBaseDirectory, baseManifest, encryption, "", /* prefix */
	)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	// If we discovered additional layers, handle them too.
	if numLayers > 1 {
		numPartitions := len(fullyResolvedIncrementalsDirectory)
		// We need the parsed base URI (<prefix>/<subdir>) for each partition to calculate the
		// URI to each layer in that partition below.
		baseURIs := make([]*url.URL, numPartitions)
		for i := range fullyResolvedIncrementalsDirectory {
			baseURIs[i], err = url.Parse(fullyResolvedIncrementalsDirectory[i])
			if err != nil {
				return nil, nil, nil, 0, err
			}
		}

		// For each layer, we need to load the default manifest then calculate the URI and the
		// locality info for each partition.
		for i := range prev {
			defaultManifestForLayer, memSize, err := backupinfo.ReadBackupManifest(ctx, mem, incStores[0], prev[i], encryption)
			if err != nil {
				return nil, nil, nil, 0, err
			}
			ownedMemSize += memSize
			mainBackupManifests[i+1] = defaultManifestForLayer

			// prev[i] is the path to the manifest file itself for layer i -- the
			// dirname piece of that path is the subdirectory in each of the
			// partitions in which we'll also expect to find a partition manifest.
			// Recall full inc URI is <prefix>/<subdir>/<incSubDir>
			incSubDir := path.Dir(prev[i])
			partitionURIs := make([]string, numPartitions)
			for j := range baseURIs {
				u := *baseURIs[j] // NB: makes a copy to avoid mutating the baseURI.
				u.Path = JoinURLPath(u.Path, incSubDir)
				partitionURIs[j] = u.String()
			}
			defaultURIs[i+1] = partitionURIs[0]
			localityInfo[i+1], err = getLocalityInfo(ctx, incStores, partitionURIs, defaultManifestForLayer, encryption, incSubDir)
			if err != nil {
				return nil, nil, nil, 0, err
			}
		}
	}

	totalMemSize := ownedMemSize
	ownedMemSize = 0

	validatedDefaultURIs, validatedMainBackupManifests, validatedLocalityInfo, err := validateEndTimeAndTruncate(
		defaultURIs, mainBackupManifests, localityInfo, endTime)

	if err != nil {
		return nil, nil, nil, 0, err
	}
	return validatedDefaultURIs, validatedMainBackupManifests, validatedLocalityInfo, totalMemSize, nil
}
