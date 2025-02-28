// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupdest

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

const (
	// DefaultLocalityValue is the default locality tag used in a locality aware
	// backup/restore when an explicit COCKROACH_LOCALITY is not specified.
	DefaultLocalityValue = "default"
	// includeManifest is a named const that can be passed to FindPriorBackups.
	includeManifest = true
	// OmitManifest is a named const that can be passed to FindPriorBackups.
	OmitManifest = false
)

// On some cloud storage platforms (i.e. GS, S3), backups in a base bucket may
// omit a leading slash. However, backups in a subdirectory of a base bucket
// will contain one.
var backupPathRE = regexp.MustCompile("^/?[^\\/]+/[^\\/]+/[^\\/]+/" + backupbase.BackupManifestName + "$")

// featureFullBackupUserSubdir, when true, will create a full backup at a user
// specified subdirectory if no backup already exists at that subdirectory. As
// of 22.1, this feature is default disabled, and will be totally disabled by 22.2.
var featureFullBackupUserSubdir = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"bulkio.backup.deprecated_full_backup_with_subdir.enabled",
	"when true, a backup command with a user specified subdirectory will create a full backup at"+
		" the subdirectory if no backup already exists at that subdirectory",
	false,
	settings.WithPublic)

// TODO(adityamaru): Move this to the soon to be `backupinfo` package.
func containsManifest(ctx context.Context, exportStore cloud.ExternalStorage) (bool, error) {
	r, _, err := exportStore.ReadFile(ctx, backupbase.BackupManifestName, cloud.ReadOptions{NoFileSize: true})
	if err != nil {
		if errors.Is(err, cloud.ErrFileDoesNotExist) {
			return false, nil
		}
		return false, err
	}
	r.Close(ctx)
	return true, nil
}

// ResolvedDestination encapsulates information that is populated while
// resolving the destination of a backup.
type ResolvedDestination struct {
	// collectionURI is the URI pointing to the backup collection.
	CollectionURI string

	// defaultURI is the full path of the defaultURI of the backup.
	DefaultURI string

	// ChosenSubdir is the automatically chosen suffix within the collection path
	// if we're backing up INTO a collection.
	ChosenSubdir string

	// URIsByLocalityKV is a mapping from the locality tag to the corresponding
	// locality aware backup URI.
	URIsByLocalityKV map[string]string

	// PrevBackupURIs is the list of full paths for previous backups in the chain.
	// This includes the base backup at index 0, and any subsequent incremental
	// backups. This field will not be populated when running a full backup.
	PrevBackupURIs []string
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
	execCfg *sql.ExecutorConfig,
) (ResolvedDestination, error) {
	makeCloudStorage := execCfg.DistSQLSrv.ExternalStorageFromURI

	defaultURI, _, err := GetURIsByLocalityKV(dest.To, "")
	if err != nil {
		return ResolvedDestination{}, err
	}

	var collectionURI string
	chosenSuffix := dest.Subdir
	collectionURI = defaultURI

	if chosenSuffix == backupbase.LatestFileName {
		latest, err := ReadLatestFile(ctx, defaultURI, makeCloudStorage, user)
		if err != nil {
			return ResolvedDestination{}, err
		}
		chosenSuffix = latest
	}

	plannedBackupDefaultURI, urisByLocalityKV, err := GetURIsByLocalityKV(dest.To, chosenSuffix)
	if err != nil {
		return ResolvedDestination{}, err
	}

	defaultStore, err := makeCloudStorage(ctx, plannedBackupDefaultURI, user)
	if err != nil {
		return ResolvedDestination{}, err
	}
	defer defaultStore.Close()
	exists, err := containsManifest(ctx, defaultStore)
	if err != nil {
		return ResolvedDestination{}, err
	}
	if exists && !dest.Exists {
		// We disallow a user from writing a full backup to a path in a collection containing an
		// existing backup iff we're 99.9% confident this backup was planned on a 22.1 node.
		return ResolvedDestination{},
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
			if !featureFullBackupUserSubdir.Get(execCfg.SV()) {
				return ResolvedDestination{},
					errors.Errorf("No full backup exists in %q to append an incremental backup to. "+
						"To take a full backup, remove the subdirectory from the backup command "+
						"(i.e. run 'BACKUP ... INTO <collectionURI>'). ", chosenSuffix)
			}
		}
		// There's no full backup in the resolved subdirectory; therefore, we're conducting a full backup.
		return ResolvedDestination{
			CollectionURI:    collectionURI,
			DefaultURI:       plannedBackupDefaultURI,
			ChosenSubdir:     chosenSuffix,
			URIsByLocalityKV: urisByLocalityKV,
			PrevBackupURIs:   nil,
		}, nil
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
		return ResolvedDestination{}, err
	}

	priorsDefaultURI, _, err := GetURIsByLocalityKV(fullyResolvedIncrementalsLocation, "")
	if err != nil {
		return ResolvedDestination{}, err
	}
	incrementalStore, err := makeCloudStorage(ctx, priorsDefaultURI, user)
	if err != nil {
		return ResolvedDestination{}, err
	}
	defer incrementalStore.Close()

	priors, err := FindPriorBackups(ctx, incrementalStore, OmitManifest)
	if err != nil {
		return ResolvedDestination{}, errors.Wrap(err, "adjusting backup destination to append new layer to existing backup")
	}

	prevBackupURIs := make([]string, 0, len(priors))
	for _, prior := range priors {
		priorURI, err := url.Parse(priorsDefaultURI)
		if err != nil {
			return ResolvedDestination{}, errors.Wrapf(err, "parsing default backup location %s",
				priorsDefaultURI)
		}
		priorURI.Path = backuputils.JoinURLPath(priorURI.Path, prior)
		prevBackupURIs = append(prevBackupURIs, priorURI.String())
	}
	prevBackupURIs = append([]string{plannedBackupDefaultURI}, prevBackupURIs...)

	// Within the chosenSuffix dir, differentiate incremental backups with partName.
	partName := endTime.GoTime().Format(backupbase.DateBasedIncFolderName)
	defaultIncrementalsURI, urisByLocalityKV, err := GetURIsByLocalityKV(fullyResolvedIncrementalsLocation, partName)
	if err != nil {
		return ResolvedDestination{}, err
	}

	return ResolvedDestination{
		CollectionURI:    collectionURI,
		DefaultURI:       defaultIncrementalsURI,
		ChosenSubdir:     chosenSuffix,
		URIsByLocalityKV: urisByLocalityKV,
		PrevBackupURIs:   prevBackupURIs,
	}, nil
}

// ResolveDestForCompaction resolves the destination for a compacted backup.
// While the end time of this compacted backup matches the end time of
// the last backup in the chain to compact, when resolving the
// destination we need to adjust the end time to ensure that the backup
// location doesn't clobber the last backup in the chain. We do this by
// adding a small duration (large enough to change the backup path)
// to the end time.
func ResolveDestForCompaction(
	ctx context.Context, execCtx sql.JobExecContext, details jobspb.BackupDetails,
) (ResolvedDestination, error) {
	return ResolveDest(
		ctx,
		execCtx.User(),
		details.Destination,
		details.EndTime.AddDuration(10*time.Millisecond),
		execCtx.ExecCfg(),
	)
}

// ReadLatestFile reads the LATEST file from collectionURI and returns the path
// stored in the file.
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
	defer latestFile.Close(ctx)

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
	// be at the top, so just grab the first filename _unless_ it's the
	// empty object with a trailing '/'. The latter is never created by our code
	// but can be created by other tools, e.g., AWS DataSync to transfer an existing backup to
	// another bucket. (See https://github.com/cockroachdb/cockroach/issues/106070.)
	err := exportStore.List(ctx, backupbase.LatestHistoryDirectory, "", func(p string) error {
		p = strings.TrimPrefix(p, "/")
		if p == "" {
			// N.B. skip the empty object with a trailing '/', created by a third-party tool.
			return nil
		}
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
		r, _, err := exportStore.ReadFile(
			ctx, backupbase.LatestHistoryDirectory+"/"+backupbase.LatestFileName, cloud.ReadOptions{NoFileSize: true},
		)
		if err == nil {
			return r, nil
		}
	} else if err != nil && !errors.Is(err, cloud.ErrListingDone) {
		return nil, err
	}

	if latestFileFound {
		r, _, err := exportStore.ReadFile(ctx, backupbase.LatestHistoryDirectory+"/"+latestFile, cloud.ReadOptions{NoFileSize: true})
		return r, err
	}

	// The latest file couldn't be found in the latest directory,
	// try the base directory instead.
	r, _, err := exportStore.ReadFile(ctx, backupbase.LatestFileName, cloud.ReadOptions{NoFileSize: true})
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
	// HTTP storage does not support listing and so we cannot rely on the
	// above-mentioned List method to return us the most recent latest file.
	// Instead, we disregard write once semantics and always read and write
	// a non-timestamped latest file for HTTP.
	if exportStore.Conf().Provider == cloudpb.ExternalStorageProvider_http {
		return cloud.WriteFile(ctx, exportStore, backupbase.LatestFileName, strings.NewReader(suffix))
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
	return fmt.Sprintf("%s/%s-%s", backupbase.LatestHistoryDirectory, backupbase.LatestFileName, hex.EncodeToString(buffer))
}

// CheckForLatestFileInCollection checks whether the directory pointed by store contains the
// latestFileName pointer directory.
func CheckForLatestFileInCollection(
	ctx context.Context, store cloud.ExternalStorage,
) (bool, error) {
	r, err := FindLatestFile(ctx, store)
	if err != nil {
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			return false, pgerror.WithCandidateCode(err, pgcode.Io)
		}

		r, _, err = store.ReadFile(ctx, backupbase.LatestFileName, cloud.ReadOptions{NoFileSize: true})
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

func getLocalityAndBaseURI(uri, appendPath string) (string, string, error) {
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}
	q := parsedURI.Query()
	localityKV := q.Get(cloud.LocalityURLParam)
	// Remove the backup locality parameter.
	q.Del(cloud.LocalityURLParam)
	parsedURI.RawQuery = q.Encode()

	parsedURI.Path = backuputils.JoinURLPath(parsedURI.Path, appendPath)

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
		if localityKV != "" && localityKV != DefaultLocalityValue {
			return "", nil, errors.Errorf("%s %s is invalid for a single BACKUP location",
				cloud.LocalityURLParam, localityKV)
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
				cloud.LocalityURLParam,
			)
		}
		if localityKV == DefaultLocalityValue {
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

// ListFullBackupsInCollection lists full backup paths in the collection
// of an export store
func ListFullBackupsInCollection(
	ctx context.Context, store cloud.ExternalStorage,
) ([]string, error) {
	var backupPaths []string
	if err := store.List(ctx, "", backupbase.ListingDelimDataSlash, func(f string) error {
		if backupPathRE.MatchString(f) {
			backupPaths = append(backupPaths, f)
		}
		return nil
	}); err != nil {
		// Can't happen, just required to handle the error for lint.
		return nil, err
	}
	for i, backupPath := range backupPaths {
		backupPaths[i] = strings.TrimSuffix(backupPath, "/"+backupbase.BackupManifestName)
	}
	return backupPaths, nil
}

// ResolveBackupManifests resolves the URIs that point to the incremental layers
// (each of which can be partitioned) of backups into the actual backup
// manifests and metadata required to RESTORE. If only one layer is explicitly
// provided, it is inspected to see if it contains "appended" layers internally
// that are then expanded into the result layers returned, similar to if those
// layers had been specified in `from` explicitly. If `includeSkipped` is true,
// layers that do not actually contribute to the path from the base to the end
// timestamp are included in the result, otherwise they are elided.
func ResolveBackupManifests(
	ctx context.Context,
	mem *mon.BoundAccount,
	baseStores []cloud.ExternalStorage,
	incStores []cloud.ExternalStorage,
	mkStore cloud.ExternalStorageFromURIFactory,
	fullyResolvedBaseDirectory []string,
	fullyResolvedIncrementalsDirectory []string,
	endTime hlc.Timestamp,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	user username.SQLUsername,
	includeSkipped bool,
) (
	defaultURIs []string,
	// mainBackupManifests contains the manifest located at each defaultURI in the backup chain.
	mainBackupManifests []backuppb.BackupManifest,
	localityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	reservedMemSize int64,
	_ error,
) {
	ctx, sp := tracing.ChildSpan(ctx, "backupdest.ResolveBackupManifests")
	defer sp.Finish()

	var ownedMemSize int64
	defer func() {
		if ownedMemSize != 0 {
			mem.Shrink(ctx, ownedMemSize)
		}
	}()
	baseManifest, memSize, err := backupinfo.ReadBackupManifestFromStore(ctx, mem, baseStores[0], fullyResolvedBaseDirectory[0],
		encryption, kmsEnv)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	ownedMemSize += memSize

	var incrementalBackups []string
	if len(incStores) > 0 {
		incrementalBackups, err = FindPriorBackups(ctx, incStores[0], includeManifest)
		if err != nil {
			return nil, nil, nil, 0, err
		}
	}
	numLayers := len(incrementalBackups) + 1

	defaultURIs = make([]string, numLayers)
	mainBackupManifests = make([]backuppb.BackupManifest, numLayers)
	localityInfo = make([]jobspb.RestoreDetails_BackupLocalityInfo, numLayers)

	// Setup the full backup layer explicitly.
	defaultURIs[0] = fullyResolvedBaseDirectory[0]
	mainBackupManifests[0] = baseManifest
	localityInfo[0], err = backupinfo.GetLocalityInfo(
		ctx, baseStores, fullyResolvedBaseDirectory, baseManifest, encryption, kmsEnv, "", /* prefix */
	)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	// If we discovered additional layers, handle them too.
	if numLayers > 1 {
		numPartitions := len(fullyResolvedIncrementalsDirectory)
		// We need the parsed base URI (<prefix>/<subdir>) for each partition to
		// calculate the URI to each layer in that partition below.
		baseURIs := make([]*url.URL, numPartitions)
		for i := range fullyResolvedIncrementalsDirectory {
			baseURIs[i], err = url.Parse(fullyResolvedIncrementalsDirectory[i])
			if err != nil {
				return nil, nil, nil, 0, err
			}
		}

		// For each incremental backup layer we construct the default URI. We don't
		// load the manifests in this loop since we want to do that concurrently.
		for i := range incrementalBackups {
			// incrementalBackups[i] is the path to the manifest file itself for layer i -- the
			// dirname piece of that path is the subdirectory in each of the
			// partitions in which we'll also expect to find a partition manifest.
			// Recall full inc URI is <prefix>/<subdir>/<incSubDir>
			incSubDir := path.Dir(incrementalBackups[i])
			u := *baseURIs[0] // NB: makes a copy to avoid mutating the baseURI.
			u.Path = backuputils.JoinURLPath(u.Path, incSubDir)
			defaultURIs[i+1] = u.String()
		}

		// Load the default backup manifests for each backup layer, this is done
		// concurrently.
		defaultManifestsForEachLayer, memSize, err := backupinfo.GetBackupManifests(ctx, mem, user,
			mkStore, defaultURIs, encryption, kmsEnv)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		ownedMemSize += memSize

		// Iterate over the incremental backups one last time to memoize the loaded
		// manifests and read the locality info.
		//
		// TODO(adityamaru): Parallelize the loading of the locality descriptors.
		for i := range incrementalBackups {
			// The manifest for incremental layer i slots in at i+1 since the full
			// backup manifest occupies index 0 in `mainBackupManifests`.
			mainBackupManifests[i+1] = defaultManifestsForEachLayer[i+1]
			incSubDir := path.Dir(incrementalBackups[i])
			partitionURIs := make([]string, numPartitions)
			for j := range baseURIs {
				u := *baseURIs[j] // NB: makes a copy to avoid mutating the baseURI.
				u.Path = backuputils.JoinURLPath(u.Path, incSubDir)
				partitionURIs[j] = u.String()
			}

			localityInfo[i+1], err = backupinfo.GetLocalityInfo(ctx, incStores, partitionURIs,
				defaultManifestsForEachLayer[i+1], encryption, kmsEnv, incSubDir)
			if err != nil {
				return nil, nil, nil, 0, err
			}
		}
	}

	totalMemSize := ownedMemSize
	ownedMemSize = 0

	validatedDefaultURIs, validatedMainBackupManifests, validatedLocalityInfo, err := backupinfo.ValidateEndTimeAndTruncate(
		defaultURIs, mainBackupManifests, localityInfo, endTime, includeSkipped)

	if err != nil {
		return nil, nil, nil, 0, err
	}
	return validatedDefaultURIs, validatedMainBackupManifests, validatedLocalityInfo, totalMemSize, nil
}
