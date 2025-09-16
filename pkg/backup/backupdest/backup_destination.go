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

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
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
var backupPathRE = regexp.MustCompile("^/?[^\\/]+/[^\\/]+/[^\\/]+/" + backupbase.DeprecatedBackupManifestName + "$")

// TODO(adityamaru): Move this to the soon to be `backupinfo` package.
func containsManifest(ctx context.Context, exportStore cloud.ExternalStorage) (bool, error) {
	r, _, err := exportStore.ReadFile(ctx, backupbase.DeprecatedBackupManifestName, cloud.ReadOptions{NoFileSize: true})
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
// backup manifests in the backup chain. Additionally, if a startTime is
// provided, it will be used to determine its destination. If one is not
// provided, we assume that the incremental is chained off of the most recent
// backup in the chain and that backup's manifest will be fetched to determine
// the start time.
//
// The encryptions passed to the encryption options should include the raw
// encryption options, not the resolved key.
//
// TODO (kev-cao): Once we have completed the backup directory index work, we
// can remove the need for encryption and KMS.
func ResolveDest(
	ctx context.Context,
	user username.SQLUsername,
	dest jobspb.BackupDetails_Destination,
	startTime hlc.Timestamp,
	endTime hlc.Timestamp,
	execCfg *sql.ExecutorConfig,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
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
		return ResolvedDestination{},
			errors.Newf("a full backup already exists in %s", plannedBackupDefaultURI)

	} else if !exists {
		if dest.Exists {
			return ResolvedDestination{},
				errors.Errorf("No full backup exists in %q to append an incremental backup to", chosenSuffix)
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

	rootStore, err := makeCloudStorage(ctx, collectionURI, user)
	if err != nil {
		return ResolvedDestination{}, err
	}
	defer rootStore.Close()
	priors, err := FindAllIncrementalPaths(
		ctx, execCfg, incrementalStore, rootStore,
		chosenSuffix, OmitManifest, len(dest.IncrementalStorage) > 0,
	)
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

	// If startTime is not already set, we will find it via the previous backup
	// manifest.
	if startTime.IsEmpty() {
		baseEncryptionOptions, err := backupencryption.GetEncryptionFromBase(
			ctx, user, execCfg.DistSQLSrv.ExternalStorageFromURI, prevBackupURIs[0],
			encryption, kmsEnv,
		)
		if err != nil {
			return ResolvedDestination{}, err
		}

		// TODO (kev-cao): Once we have completed the backup directory index work, we
		// can remove the need to read an entire backup manifest just to fetch the
		// start time. We can instead read the metadata protobuf.
		mem := execCfg.RootMemoryMonitor.MakeBoundAccount()
		defer mem.Close(ctx)
		precedingBackupManifest, size, err := backupinfo.ReadBackupManifestFromURI(
			ctx, &mem, prevBackupURIs[len(prevBackupURIs)-1], user,
			execCfg.DistSQLSrv.ExternalStorageFromURI, baseEncryptionOptions, kmsEnv,
		)
		if err != nil {
			return ResolvedDestination{}, err
		}
		if err := mem.Grow(ctx, size); err != nil {
			return ResolvedDestination{}, err
		}
		defer mem.Shrink(ctx, size)
		startTime = precedingBackupManifest.EndTime
		if startTime.IsEmpty() {
			return ResolvedDestination{}, errors.Errorf("empty end time in prior backup manifest")
		}
	}

	partName := ConstructDateBasedIncrementalFolderName(startTime.GoTime(), endTime.GoTime())
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
	ctx context.Context, store cloud.ExternalStorage, useIndex bool,
) ([]string, error) {
	if useIndex {
		return ListSubdirsFromIndex(ctx, store)
	}

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
		backupPaths[i] = strings.TrimSuffix(backupPath, "/"+backupbase.DeprecatedBackupManifestName)
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
// timestamp are included in the result, otherwise they are elided. If
// `includedCompacted` is true, then backups created from compaction will be
// included in the result, otherwise they are filtered out.
// TODO (kev-cao): The sheer amount of parameters is absolutely horrifying.
// Must clean up.
func ResolveBackupManifests(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	mem *mon.BoundAccount,
	defaultCollectionURI string,
	collectionURIs []string,
	mkStore cloud.ExternalStorageFromURIFactory,
	resolvedSubdir string,
	fullyResolvedBaseDirectory []string,
	fullyResolvedIncrementalsDirectory []string,
	endTime hlc.Timestamp,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	user username.SQLUsername,
	includeSkipped bool,
	includeCompacted bool,
	isCustomIncLocation bool,
) (
	defaultURIs []string,
	mainBackupManifests []backuppb.BackupManifest,
	localityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	reservedMemSize int64,
	_ error,
) {
	rootStore, err := mkStore(ctx, defaultCollectionURI, user)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	defer rootStore.Close()

	exists, err := IndexExists(ctx, rootStore, resolvedSubdir)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	if !ReadBackupIndexEnabled.Get(&execCfg.Settings.SV) || !exists || isCustomIncLocation {
		return legacyResolveBackupManifests(
			ctx, execCfg, mem, defaultCollectionURI, mkStore,
			resolvedSubdir, fullyResolvedBaseDirectory, fullyResolvedIncrementalsDirectory,
			endTime, encryption, kmsEnv, user, includeSkipped, includeCompacted,
		)
	}

	return indexedResolveBackupManifests(
		ctx, mem, collectionURIs, mkStore, resolvedSubdir, endTime, encryption, kmsEnv,
		user, includeSkipped, includeCompacted,
	)
}

// TODO (kev-cao): Remove in 26.2 when all restorable backups are expected to
// have an index.
func legacyResolveBackupManifests(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	mem *mon.BoundAccount,
	defaultCollectionURI string,
	mkStore cloud.ExternalStorageFromURIFactory,
	resolvedSubdir string,
	fullyResolvedBaseDirectory []string,
	fullyResolvedIncrementalsDirectory []string,
	endTime hlc.Timestamp,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	user username.SQLUsername,
	includeSkipped bool,
	includeCompacted bool,
) (
	defaultURIs []string,
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

	baseStores, baseCleanupFn, err := MakeBackupDestinationStores(ctx, user, mkStore, fullyResolvedBaseDirectory)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	defer func() {
		if err := baseCleanupFn(); err != nil {
			log.Dev.Warningf(ctx, "failed to close base store: %+v", err)
		}
	}()

	incStores, incCleanupFn, err := MakeBackupDestinationStores(ctx, user, mkStore, fullyResolvedIncrementalsDirectory)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	defer func() {
		if err := incCleanupFn(); err != nil {
			log.Dev.Warningf(ctx, "failed to close inc store: %+v", err)
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
		incrementalBackups, err = LegacyFindPriorBackups(ctx, incStores[0], includeManifest)
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
	manifestEntries, err := backupinfo.ZipBackupTreeEntries(
		defaultURIs, mainBackupManifests, localityInfo,
	)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	validatedEntries, err := backupinfo.ValidateEndTimeAndTruncate(
		manifestEntries, endTime, includeSkipped, includeCompacted,
	)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	uris, manifests, localityInfo := backupinfo.UnzipBackupTreeEntries(validatedEntries)
	return uris, manifests, localityInfo, totalMemSize, nil
}

func indexedResolveBackupManifests(
	ctx context.Context,
	mem *mon.BoundAccount,
	collectionURIs []string,
	mkStore cloud.ExternalStorageFromURIFactory,
	resolvedSubdir string,
	endTime hlc.Timestamp,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	user username.SQLUsername,
	includeSkipped bool,
	includeCompacted bool,
) (
	defaultURIs []string,
	mainManifests []backuppb.BackupManifest,
	localityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	reservedMemSize int64,
	_ error,
) {
	ctx, sp := tracing.ChildSpan(ctx, "backupdest.ResolveBackupManifestsWithIndexes")
	defer sp.Finish()

	rootStores, rootCleanupFn, err := MakeBackupDestinationStores(ctx, user, mkStore, collectionURIs)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	defer func() {
		if err := rootCleanupFn(); err != nil {
			log.Dev.Warningf(ctx, "failed to close collection store: %s", err)
		}
	}()

	indexes, err := GetBackupTreeIndexMetadata(
		ctx, rootStores[0], resolvedSubdir,
	)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	indexes, err = backupinfo.ValidateEndTimeAndTruncate(
		indexes, endTime, includeSkipped, includeCompacted,
	)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	defaultURIs = make([]string, len(indexes))
	partitionURIs := make([][]string, len(indexes))
	localityInfo = make([]jobspb.RestoreDetails_BackupLocalityInfo, len(indexes))

	for idx, index := range indexes {
		indexDests, err := backuputils.AppendPaths(collectionURIs, index.Path)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		partitionURIs[idx] = indexDests
		defaultURIs[idx] = indexDests[0]
	}

	mainManifests, manifestsMem, err := backupinfo.GetBackupManifests(
		ctx, mem, user, mkStore, defaultURIs, encryption, kmsEnv,
	)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	group, gCtx := errgroup.WithContext(ctx)
	for i, indexes := range indexes {
		group.Go(func() error {
			locality, err := backupinfo.GetLocalityInfo(
				gCtx, rootStores, partitionURIs[i], mainManifests[i], encryption, kmsEnv, indexes.Path,
			)
			if err != nil {
				return err
			}
			localityInfo[i] = locality
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, nil, nil, 0, err
	}

	return defaultURIs, mainManifests, localityInfo, manifestsMem, nil
}
