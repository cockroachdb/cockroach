// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	descpb "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Files that may appear in a backup directory.
const (
	// backupManifestName is the file name used for serialized BackupManifest
	// protos.
	backupManifestName = "BACKUP_MANIFEST"
	// backupOldManifestName is an old name for the serialized BackupManifest
	// proto. It is used by 20.1 nodes and earlier.
	backupOldManifestName = "BACKUP"

	// backupManifestChecksumSuffix indicates where the checksum for the manifest
	// is stored if present. It can be found in the name of the backup manifest +
	// this suffix.
	backupManifestChecksumSuffix = "-CHECKSUM"

	// backupPartitionDescriptorPrefix is the file name prefix for serialized
	// BackupPartitionDescriptor protos.
	backupPartitionDescriptorPrefix = "BACKUP_PART"
	// backupManifestCheckpointName is the file name used to store the serialized
	// BackupManifest proto while the backup is in progress.
	backupManifestCheckpointName = "BACKUP-CHECKPOINT"
	// backupStatisticsFileName is the file name used to store the serialized
	// table statistics for the tables being backed up.
	backupStatisticsFileName = "BACKUP-STATISTICS"
	// backupEncryptionInfoFile is the file name used to store the serialized
	// EncryptionInfo proto while the backup is in progress.
	backupEncryptionInfoFile = "ENCRYPTION-INFO"
	// backupLockFile is the prefix of the file name used by the backup job to
	// lock the bucket from running concurrent backups to the same destination.
	backupLockFilePrefix = "BACKUP-LOCK-"
)

const (
	// BackupFormatDescriptorTrackingVersion added tracking of complete DBs.
	BackupFormatDescriptorTrackingVersion uint32 = 1

	// backupProgressDirectory is the directory where all 22.1 and beyond
	// CHECKPOINT files will be stored as we no longer want to overwrite
	// them.
	backupProgressDirectory = "progress"

	// DateBasedIncFolderName is the date format used when creating sub-directories
	// storing incremental backups for auto-appendable backups.
	// It is exported for testing backup inspection tooling.
	DateBasedIncFolderName = "/20060102/150405.00"

	// DateBasedIntoFolderName is the date format used when creating sub-directories
	// for storing backups in a collection.
	// Also exported for testing backup inspection tooling.
	DateBasedIntoFolderName = "/2006/01/02-150405.00"

	// latestFileName is the name of a file in the collection which contains the
	// path of the most recently taken full backup in the backup collection.
	latestFileName = "LATEST"

	// latestHistoryDirectory is the directory where all 22.1 and beyond
	// LATEST files will be stored as we no longer want to overwrite it.
	latestHistoryDirectory = backupMetadataDirectory + "/" + "latest"

	// backupMetadataDirectory is the directory where metadata about a backup
	// collection is stored. In v22.1 it contains the latest directory.
	backupMetadataDirectory = "metadata"
)

// On some cloud storage platforms (i.e. GS, S3), backups in a base bucket may
// omit a leading slash. However, backups in a subdirectory of a base bucket
// will contain one.
var backupPathRE = regexp.MustCompile("^/?[^\\/]+/[^\\/]+/[^\\/]+/" + backupManifestName + "$")

var writeMetadataSST = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"kv.bulkio.write_metadata_sst.enabled",
	"write experimental new format BACKUP metadata file",
	false,
)
var errEncryptionInfoRead = errors.New(`ENCRYPTION-INFO not found`)

// isGZipped detects whether the given bytes represent GZipped data. This check
// is used rather than a standard implementation such as http.DetectContentType
// since some zipped data may be mis-identified by that method. We've seen
// gzipped data incorrectly identified as "application/vnd.ms-fontobject". The
// magic bytes are from the MIME sniffing algorithm http.DetectContentType is
// based which can be found at https://mimesniff.spec.whatwg.org/.
//
// This method is only used to detect if protobufs are GZipped, and there are no
// conflicts between the starting bytes of a protobuf and these magic bytes.
func isGZipped(dat []byte) bool {
	gzipPrefix := []byte("\x1F\x8B\x08")
	return bytes.HasPrefix(dat, gzipPrefix)
}

// BackupFileDescriptors is an alias on which to implement sort's interface.
type BackupFileDescriptors []BackupManifest_File

func (r BackupFileDescriptors) Len() int      { return len(r) }
func (r BackupFileDescriptors) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r BackupFileDescriptors) Less(i, j int) bool {
	if cmp := bytes.Compare(r[i].Span.Key, r[j].Span.Key); cmp != 0 {
		return cmp < 0
	}
	return bytes.Compare(r[i].Span.EndKey, r[j].Span.EndKey) < 0
}

func (m *BackupManifest) isIncremental() bool {
	return !m.StartTime.IsEmpty()
}

// ReadBackupManifestFromURI creates an export store from the given URI, then
// reads and unmarshalls a BackupManifest at the standard location in the
// export storage.
func ReadBackupManifestFromURI(
	ctx context.Context,
	mem *mon.BoundAccount,
	uri string,
	user security.SQLUsername,
	makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
	encryption *jobspb.BackupEncryptionOptions,
) (BackupManifest, int64, error) {
	exportStore, err := makeExternalStorageFromURI(ctx, uri, user)

	if err != nil {
		return BackupManifest{}, 0, err
	}
	defer exportStore.Close()
	return ReadBackupManifestFromStore(ctx, mem, exportStore, encryption)
}

// ReadBackupManifestFromStore reads and unmarshalls a BackupManifest from the
// store and returns it with the size it reserved for it from the boundAccount.
func ReadBackupManifestFromStore(
	ctx context.Context,
	mem *mon.BoundAccount,
	exportStore cloud.ExternalStorage,
	encryption *jobspb.BackupEncryptionOptions,
) (BackupManifest, int64, error) {
	backupManifest, memSize, err := readBackupManifest(ctx, mem, exportStore, backupManifestName,
		encryption)
	if err != nil {
		oldManifest, newMemSize, newErr := readBackupManifest(ctx, mem, exportStore, backupOldManifestName,
			encryption)
		if newErr != nil {
			return BackupManifest{}, 0, err
		}
		backupManifest = oldManifest
		memSize = newMemSize
	}
	backupManifest.Dir = exportStore.Conf()
	// TODO(dan): Sanity check this BackupManifest: non-empty EndTime,
	// non-empty Paths, and non-overlapping Spans and keyranges in Files.
	return backupManifest, memSize, nil
}

func containsManifest(ctx context.Context, exportStore cloud.ExternalStorage) (bool, error) {
	r, err := exportStore.ReadFile(ctx, backupManifestName)
	if err != nil {
		if errors.Is(err, cloud.ErrFileDoesNotExist) {
			return false, nil
		}
		return false, err
	}
	r.Close(ctx)
	return true, nil
}

// compressData compresses data buffer and returns compressed
// bytes (i.e. gzip format).
func compressData(descBuf []byte) ([]byte, error) {
	gzipBuf := bytes.NewBuffer([]byte{})
	gz := gzip.NewWriter(gzipBuf)
	if _, err := gz.Write(descBuf); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return gzipBuf.Bytes(), nil
}

// decompressData decompresses gzip data buffer and
// returns decompressed bytes.
func decompressData(ctx context.Context, mem *mon.BoundAccount, descBytes []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewBuffer(descBytes))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return mon.ReadAll(ctx, ioctx.ReaderAdapter(r), mem)
}

// readBackupCheckpointManifest reads and unmarshals a BACKUP-CHECKPOINT
// manifest from filename in the provided export store.
func readBackupCheckpointManifest(
	ctx context.Context,
	mem *mon.BoundAccount,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
) (BackupManifest, int64, error) {
	checkpointFile, err := readLatestCheckpointFile(ctx, exportStore, filename)
	if err != nil {
		return BackupManifest{}, 0, err
	}
	defer checkpointFile.Close(ctx)

	// Look for a checksum, if one is not found it could be an older backup,
	// but we want to continue anyway.
	checksumFile, err := readLatestCheckpointFile(ctx, exportStore, filename+backupManifestChecksumSuffix)
	if err != nil {
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			return BackupManifest{}, 0, err
		}
		// Pass checksumFile as nil to indicate it was not found.
		return readManifest(ctx, mem, exportStore, encryption, checkpointFile, nil)
	}
	defer checksumFile.Close(ctx)
	return readManifest(ctx, mem, exportStore, encryption, checkpointFile, checksumFile)
}

// readBackupManifest reads and unmarshals a BackupManifest from filename in
// the provided export store.
func readBackupManifest(
	ctx context.Context,
	mem *mon.BoundAccount,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
) (BackupManifest, int64, error) {
	manifestFile, err := exportStore.ReadFile(ctx, filename)
	if err != nil {
		return BackupManifest{}, 0, err
	}
	defer manifestFile.Close(ctx)

	// Look for a checksum, if one is not found it could be an older backup,
	// but we want to continue anyway.
	checksumFile, err := exportStore.ReadFile(ctx, filename+backupManifestChecksumSuffix)
	if err != nil {
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			return BackupManifest{}, 0, err
		}
		// Pass checksumFile as nil to indicate it was not found.
		return readManifest(ctx, mem, exportStore, encryption, manifestFile, nil)
	}
	defer checksumFile.Close(ctx)
	return readManifest(ctx, mem, exportStore, encryption, manifestFile, checksumFile)
}

// readManifest reads and unmarshals a BackupManifest from filename in
// the provided export store. If the passed bound account is not nil, the bytes
// read are reserved from it as it is read and then the approximate in-memory
// size (the total decompressed serialized byte size) is reserved as well before
// deserialization and returned so that callers can then shrink the bound acct
// by that amount when they release the returned manifest.
func readManifest(
	ctx context.Context,
	mem *mon.BoundAccount,
	exportStore cloud.ExternalStorage,
	encryption *jobspb.BackupEncryptionOptions,
	manifestReader ioctx.ReadCloserCtx,
	checksumReader ioctx.ReadCloserCtx,
) (BackupManifest, int64, error) {
	descBytes, err := mon.ReadAll(ctx, manifestReader, mem)
	if err != nil {
		return BackupManifest{}, 0, err
	}
	defer func() {
		mem.Shrink(ctx, int64(cap(descBytes)))
	}()
	if checksumReader != nil {
		// If there is a checksum file present, check that it matches.
		checksumFileData, err := ioctx.ReadAll(ctx, checksumReader)
		if err != nil {
			return BackupManifest{}, 0, errors.Wrap(err, "reading checksum file")
		}
		checksum, err := getChecksum(descBytes)
		if err != nil {
			return BackupManifest{}, 0, errors.Wrap(err, "calculating checksum of manifest")
		}
		if !bytes.Equal(checksumFileData, checksum) {
			return BackupManifest{}, 0, errors.Newf("checksum mismatch; expected %s, got %s",
				hex.EncodeToString(checksumFileData), hex.EncodeToString(checksum))
		}
	}

	var encryptionKey []byte
	if encryption != nil {
		encryptionKey, err = getEncryptionKey(ctx, encryption, exportStore.Settings(),
			exportStore.ExternalIOConf())
		if err != nil {
			return BackupManifest{}, 0, err
		}
		plaintextBytes, err := storageccl.DecryptFile(ctx, descBytes, encryptionKey, mem)
		if err != nil {
			return BackupManifest{}, 0, err
		}
		mem.Shrink(ctx, int64(cap(descBytes)))
		descBytes = plaintextBytes
	}

	if isGZipped(descBytes) {
		decompressedBytes, err := decompressData(ctx, mem, descBytes)
		if err != nil {
			return BackupManifest{}, 0, errors.Wrap(
				err, "decompressing backup manifest")
		}
		// Release the compressed bytes from the monitor before we switch descBytes
		// to point at the decompressed bytes, since the deferred release will later
		// release the latter.
		mem.Shrink(ctx, int64(cap(descBytes)))
		descBytes = decompressedBytes
	}

	approxMemSize := int64(len(descBytes))
	if err := mem.Grow(ctx, approxMemSize); err != nil {
		return BackupManifest{}, 0, err
	}

	var backupManifest BackupManifest
	if err := protoutil.Unmarshal(descBytes, &backupManifest); err != nil {
		mem.Shrink(ctx, approxMemSize)
		if encryption == nil && storageccl.AppearsEncrypted(descBytes) {
			return BackupManifest{}, 0, errors.Wrapf(
				err, "file appears encrypted -- try specifying one of \"%s\" or \"%s\"",
				backupOptEncPassphrase, backupOptEncKMS)
		}
		return BackupManifest{}, 0, err
	}
	for _, d := range backupManifest.Descriptors {
		// Calls to GetTable are generally frowned upon.
		// This specific call exists to provide backwards compatibility with
		// backups created prior to version 19.1. Starting in v19.1 the
		// ModificationTime is always written in backups for all versions
		// of table descriptors. In earlier cockroach versions only later
		// table descriptor versions contain a non-empty ModificationTime.
		// Later versions of CockroachDB use the MVCC timestamp to fill in
		// the ModificationTime for table descriptors. When performing a restore
		// we no longer have access to that MVCC timestamp but we can set it
		// to a value we know will be safe.
		//
		// nolint:descriptormarshal
		if t := d.GetTable(); t == nil {
			continue
		} else if t.Version == 1 && t.ModificationTime.IsEmpty() {
			t.ModificationTime = hlc.Timestamp{WallTime: 1}
		}
	}
	return backupManifest, approxMemSize, nil
}

func readBackupPartitionDescriptor(
	ctx context.Context,
	mem *mon.BoundAccount,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
) (BackupPartitionDescriptor, int64, error) {
	r, err := exportStore.ReadFile(ctx, filename)
	if err != nil {
		return BackupPartitionDescriptor{}, 0, err
	}
	defer r.Close(ctx)
	descBytes, err := mon.ReadAll(ctx, r, mem)
	if err != nil {
		return BackupPartitionDescriptor{}, 0, err
	}
	defer func() {
		mem.Shrink(ctx, int64(cap(descBytes)))
	}()

	if encryption != nil {
		encryptionKey, err := getEncryptionKey(ctx, encryption, exportStore.Settings(),
			exportStore.ExternalIOConf())
		if err != nil {
			return BackupPartitionDescriptor{}, 0, err
		}
		plaintextData, err := storageccl.DecryptFile(ctx, descBytes, encryptionKey, mem)
		if err != nil {
			return BackupPartitionDescriptor{}, 0, err
		}
		mem.Shrink(ctx, int64(cap(descBytes)))
		descBytes = plaintextData
	}

	if isGZipped(descBytes) {
		decompressedData, err := decompressData(ctx, mem, descBytes)
		if err != nil {
			return BackupPartitionDescriptor{}, 0, errors.Wrap(
				err, "decompressing backup partition descriptor")
		}
		mem.Shrink(ctx, int64(cap(descBytes)))
		descBytes = decompressedData
	}

	memSize := int64(len(descBytes))

	if err := mem.Grow(ctx, memSize); err != nil {
		return BackupPartitionDescriptor{}, 0, err
	}

	var backupManifest BackupPartitionDescriptor
	if err := protoutil.Unmarshal(descBytes, &backupManifest); err != nil {
		mem.Shrink(ctx, memSize)
		return BackupPartitionDescriptor{}, 0, err
	}

	return backupManifest, memSize, err
}

// readTableStatistics reads and unmarshals a StatsTable from filename in
// the provided export store, and returns its pointer.
func readTableStatistics(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
) (*StatsTable, error) {
	r, err := exportStore.ReadFile(ctx, filename)
	if err != nil {
		return nil, err
	}
	defer r.Close(ctx)
	statsBytes, err := ioctx.ReadAll(ctx, r)
	if err != nil {
		return nil, err
	}
	if encryption != nil {
		encryptionKey, err := getEncryptionKey(ctx, encryption, exportStore.Settings(),
			exportStore.ExternalIOConf())
		if err != nil {
			return nil, err
		}
		statsBytes, err = storageccl.DecryptFile(ctx, statsBytes, encryptionKey, nil /* mm */)
		if err != nil {
			return nil, err
		}
	}
	var tableStats StatsTable
	if err := protoutil.Unmarshal(statsBytes, &tableStats); err != nil {
		return nil, err
	}
	return &tableStats, err
}

func writeBackupManifest(
	ctx context.Context,
	settings *cluster.Settings,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
	desc *BackupManifest,
) error {
	sort.Sort(BackupFileDescriptors(desc.Files))

	descBuf, err := protoutil.Marshal(desc)
	if err != nil {
		return err
	}

	descBuf, err = compressData(descBuf)
	if err != nil {
		return errors.Wrap(err, "compressing backup manifest")
	}

	if encryption != nil {
		encryptionKey, err := getEncryptionKey(ctx, encryption, settings, exportStore.ExternalIOConf())
		if err != nil {
			return err
		}
		descBuf, err = storageccl.EncryptFile(descBuf, encryptionKey)
		if err != nil {
			return err
		}
	}

	if err := cloud.WriteFile(ctx, exportStore, filename, bytes.NewReader(descBuf)); err != nil {
		return err
	}

	// Write the checksum file after we've successfully wrote the manifest.
	checksum, err := getChecksum(descBuf)
	if err != nil {
		return errors.Wrap(err, "calculating checksum")
	}

	if err := cloud.WriteFile(ctx, exportStore, filename+backupManifestChecksumSuffix, bytes.NewReader(checksum)); err != nil {
		return errors.Wrap(err, "writing manifest checksum")
	}

	return nil
}

// getChecksum returns a 32 bit keyed-checksum for the given data.
func getChecksum(data []byte) ([]byte, error) {
	const checksumSizeBytes = 4
	hash := sha256.New()
	if _, err := hash.Write(data); err != nil {
		return nil, errors.Wrap(err,
			`"It never returns an error." -- https://golang.org/pkg/hash`)
	}
	return hash.Sum(nil)[:checksumSizeBytes], nil
}

func getEncryptionKey(
	ctx context.Context,
	encryption *jobspb.BackupEncryptionOptions,
	settings *cluster.Settings,
	ioConf base.ExternalIODirConfig,
) ([]byte, error) {
	if encryption == nil {
		return nil, errors.New("FileEncryptionOptions is nil when retrieving encryption key")
	}
	switch encryption.Mode {
	case jobspb.EncryptionMode_Passphrase:
		return encryption.Key, nil
	case jobspb.EncryptionMode_KMS:
		// Contact the selected KMS to derive the decrypted data key.
		// TODO(pbardea): Add a check here if encryption.KMSInfo is unexpectedly nil
		// here to avoid a panic, and return an error instead.
		kms, err := cloud.KMSFromURI(ctx, encryption.KMSInfo.Uri, &backupKMSEnv{
			settings: settings,
			conf:     &ioConf,
		})
		if err != nil {
			return nil, err
		}

		defer func() {
			_ = kms.Close()
		}()

		plaintextDataKey, err := kms.Decrypt(ctx, encryption.KMSInfo.EncryptedDataKey)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decrypt data key")
		}

		return plaintextDataKey, nil
	}

	return nil, errors.New("invalid encryption mode")
}

// writeBackupPartitionDescriptor writes metadata (containing a locality KV and
// partial file listing) for a partitioned BACKUP to one of the stores in the
// backup.
func writeBackupPartitionDescriptor(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
	desc *BackupPartitionDescriptor,
) error {
	descBuf, err := protoutil.Marshal(desc)
	if err != nil {
		return err
	}
	descBuf, err = compressData(descBuf)
	if err != nil {
		return errors.Wrap(err, "compressing backup partition descriptor")
	}
	if encryption != nil {
		encryptionKey, err := getEncryptionKey(ctx, encryption, exportStore.Settings(),
			exportStore.ExternalIOConf())
		if err != nil {
			return err
		}
		descBuf, err = storageccl.EncryptFile(descBuf, encryptionKey)
		if err != nil {
			return err
		}
	}

	return cloud.WriteFile(ctx, exportStore, filename, bytes.NewReader(descBuf))
}

// writeTableStatistics writes a StatsTable object to a file of the filename
// to the specified exportStore. It will be encrypted according to the encryption
// option given.
func writeTableStatistics(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
	stats *StatsTable,
) error {
	statsBuf, err := protoutil.Marshal(stats)
	if err != nil {
		return err
	}
	if encryption != nil {
		encryptionKey, err := getEncryptionKey(ctx, encryption, exportStore.Settings(),
			exportStore.ExternalIOConf())
		if err != nil {
			return err
		}
		statsBuf, err = storageccl.EncryptFile(statsBuf, encryptionKey)
		if err != nil {
			return err
		}
	}
	return cloud.WriteFile(ctx, exportStore, filename, bytes.NewReader(statsBuf))
}

func loadBackupManifests(
	ctx context.Context,
	mem *mon.BoundAccount,
	uris []string,
	user security.SQLUsername,
	makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
	encryption *jobspb.BackupEncryptionOptions,
) ([]BackupManifest, int64, error) {
	backupManifests := make([]BackupManifest, len(uris))
	var reserved int64
	defer func() {
		if reserved != 0 {
			mem.Shrink(ctx, reserved)
		}
	}()
	for i, uri := range uris {
		desc, memSize, err := ReadBackupManifestFromURI(ctx, mem, uri, user, makeExternalStorageFromURI,
			encryption)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "failed to read backup descriptor")
		}
		reserved += memSize
		backupManifests[i] = desc
	}
	if len(backupManifests) == 0 {
		return nil, 0, errors.Newf("no backups found")
	}
	memSize := reserved
	reserved = 0

	return backupManifests, memSize, nil
}

// getLocalityInfo takes a list of stores and their URIs, along with the main
// backup manifest searches each for the locality pieces listed in the the
// main manifest, returning the mapping.
func getLocalityInfo(
	ctx context.Context,
	stores []cloud.ExternalStorage,
	uris []string,
	mainBackupManifest BackupManifest,
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
			if desc, _, err := readBackupPartitionDescriptor(ctx, nil /*mem*/, store, filename, encryption); err == nil {
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
						RedactURIForErrorMessage(uris[i]))
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
			return info, errors.Errorf("expected manifest %s not found in backup locations", filename)
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

// checkForLatestFileInCollection checks whether the directory pointed by store contains the
// latestFileName pointer directory.
func checkForLatestFileInCollection(
	ctx context.Context, store cloud.ExternalStorage,
) (bool, error) {
	r, err := findLatestFile(ctx, store)
	if err != nil {
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			return false, pgerror.WithCandidateCode(err, pgcode.Io)
		}

		r, err = store.ReadFile(ctx, latestFileName)
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

func resolveBackupManifestsExplicitIncrementals(
	ctx context.Context,
	mem *mon.BoundAccount,
	mkStore cloud.ExternalStorageFromURIFactory,
	from [][]string,
	endTime hlc.Timestamp,
	encryption *jobspb.BackupEncryptionOptions,
	user security.SQLUsername,
) (
	defaultURIs []string,
	// mainBackupManifests contains the manifest located at each defaultURI in the backup chain.
	mainBackupManifests []BackupManifest,
	localityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	reservedMemSize int64,
	_ error,
) {
	// If explicit incremental backups were are passed, we simply load them one
	// by one as specified and return the results.
	var ownedMemSize int64
	defer func() {
		if ownedMemSize != 0 {
			mem.Shrink(ctx, ownedMemSize)
		}
	}()

	defaultURIs = make([]string, len(from))
	localityInfo = make([]jobspb.RestoreDetails_BackupLocalityInfo, len(from))
	mainBackupManifests = make([]BackupManifest, len(from))

	var err error
	for i, uris := range from {
		// The first URI in the list must contain the main BACKUP manifest.
		defaultURIs[i] = uris[0]

		stores := make([]cloud.ExternalStorage, len(uris))
		for j := range uris {
			stores[j], err = mkStore(ctx, uris[j], user)
			if err != nil {
				return nil, nil, nil, 0, errors.Wrapf(err, "export configuration")
			}
			defer stores[j].Close()
		}

		var memSize int64
		mainBackupManifests[i], memSize, err = ReadBackupManifestFromStore(ctx, mem, stores[0], encryption)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		ownedMemSize += memSize

		if len(uris) > 1 {
			localityInfo[i], err = getLocalityInfo(
				ctx, stores, uris, mainBackupManifests[i], encryption, "", /* prefix */
			)
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

// resolveBackupManifests resolves the URIs that point to the incremental layers
// (each of which can be partitioned) of backups into the actual backup
// manifests and metadata required to RESTORE. If only one layer is explicitly
// provided, it is inspected to see if it contains "appended" layers internally
// that are then expanded into the result layers returned, similar to if those
// layers had been specified in `from` explicitly.
func resolveBackupManifests(
	ctx context.Context,
	mem *mon.BoundAccount,
	baseStores []cloud.ExternalStorage,
	mkStore cloud.ExternalStorageFromURIFactory,
	fullyResolvedBaseDirectory []string,
	fullyResolvedIncrementalsDirectory []string,
	endTime hlc.Timestamp,
	encryption *jobspb.BackupEncryptionOptions,
	user security.SQLUsername,
) (
	defaultURIs []string,
	// mainBackupManifests contains the manifest located at each defaultURI in the backup chain.
	mainBackupManifests []BackupManifest,
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
	baseManifest, memSize, err := ReadBackupManifestFromStore(ctx, mem, baseStores[0], encryption)
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
	mainBackupManifests = make([]BackupManifest, numLayers)
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
			defaultManifestForLayer, memSize, err := readBackupManifest(ctx, mem, incStores[0], prev[i], encryption)
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

func validateEndTimeAndTruncate(
	defaultURIs []string,
	mainBackupManifests []BackupManifest,
	localityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	endTime hlc.Timestamp,
) ([]string, []BackupManifest, []jobspb.RestoreDetails_BackupLocalityInfo, error) {
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
			if b.MVCCFilter != MVCCFilter_All {
				const errPrefix = "invalid RESTORE timestamp: restoring to arbitrary time requires that BACKUP for requested time be created with '%s' option."
				if i == 0 {
					return nil, nil, nil, errors.Errorf(
						errPrefix+" nearest backup time is %s", backupOptRevisionHistory,
						timeutil.Unix(0, b.EndTime.WallTime).UTC(),
					)
				}
				return nil, nil, nil, errors.Errorf(
					errPrefix+" nearest BACKUP times are %s or %s",
					backupOptRevisionHistory,
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

// TODO(anzoteh96): benchmark the performance of different search algorithms,
// e.g.  linear search, binary search, reverse linear search.
func getBackupIndexAtTime(backupManifests []BackupManifest, asOf hlc.Timestamp) (int, error) {
	if len(backupManifests) == 0 {
		return -1, errors.New("expected a nonempty backup manifest list, got an empty list")
	}
	backupManifestIndex := len(backupManifests) - 1
	if asOf.IsEmpty() {
		return backupManifestIndex, nil
	}
	for ind, b := range backupManifests {
		if asOf.Less(b.StartTime) {
			break
		}
		backupManifestIndex = ind
	}
	return backupManifestIndex, nil
}

func loadSQLDescsFromBackupsAtTime(
	backupManifests []BackupManifest, asOf hlc.Timestamp,
) ([]catalog.Descriptor, BackupManifest) {
	lastBackupManifest := backupManifests[len(backupManifests)-1]

	unwrapDescriptors := func(raw []descpb.Descriptor) []catalog.Descriptor {
		ret := make([]catalog.Descriptor, 0, len(raw))
		for i := range raw {
			ret = append(ret, descbuilder.NewBuilder(&raw[i]).BuildExistingMutable())
		}
		return ret
	}
	if asOf.IsEmpty() {
		if lastBackupManifest.DescriptorCoverage != tree.AllDescriptors {
			return unwrapDescriptors(lastBackupManifest.Descriptors), lastBackupManifest
		}

		// Cluster backups with revision history may have included previous
		// database versions of database descriptors in
		// lastBackupManifest.Descriptors. Find the correct set of descriptors by
		// going through their revisions. See #68541.
		asOf = lastBackupManifest.EndTime
	}

	for _, b := range backupManifests {
		if asOf.Less(b.StartTime) {
			break
		}
		lastBackupManifest = b
	}
	if len(lastBackupManifest.DescriptorChanges) == 0 {
		return unwrapDescriptors(lastBackupManifest.Descriptors), lastBackupManifest
	}

	byID := make(map[descpb.ID]*descpb.Descriptor, len(lastBackupManifest.Descriptors))
	for _, rev := range lastBackupManifest.DescriptorChanges {
		if asOf.Less(rev.Time) {
			break
		}
		if rev.Desc == nil {
			delete(byID, rev.ID)
		} else {
			byID[rev.ID] = rev.Desc
		}
	}

	allDescs := make([]catalog.Descriptor, 0, len(byID))
	for _, raw := range byID {
		// A revision may have been captured before it was in a DB that is
		// backed up -- if the DB is missing, filter the object.
		desc := descbuilder.NewBuilder(raw).BuildExistingMutable()
		var isObject bool
		switch d := desc.(type) {
		case catalog.TableDescriptor:
			// Filter out revisions in the dropped state.
			if d.GetState() == descpb.DescriptorState_DROP {
				continue
			}
			isObject = true
		case catalog.TypeDescriptor, catalog.SchemaDescriptor:
			isObject = true
		}
		if isObject && byID[desc.GetParentID()] == nil {
			continue
		}
		allDescs = append(allDescs, desc)
	}
	return allDescs, lastBackupManifest
}

// sanitizeLocalityKV returns a sanitized version of the input string where all
// characters that are not alphanumeric or -, =, or _ are replaced with _.
func sanitizeLocalityKV(kv string) string {
	sanitizedKV := make([]byte, len(kv))
	for i := 0; i < len(kv); i++ {
		if (kv[i] >= 'a' && kv[i] <= 'z') ||
			(kv[i] >= 'A' && kv[i] <= 'Z') ||
			(kv[i] >= '0' && kv[i] <= '9') || kv[i] == '-' || kv[i] == '=' {
			sanitizedKV[i] = kv[i]
		} else {
			sanitizedKV[i] = '_'
		}
	}
	return string(sanitizedKV)
}

// readEncryptionOptions takes in a backup location and tries to find
// and return all encryption option files in the backup. A backup
// normally only creates one encryption option file, but if the user
// uses ALTER BACKUP to add new keys, a new encryption option file
// will be placed side by side with the old one. Since the old file
// is still valid, as we never want to modify or delete an existing
// backup, we return both new and old files.
func readEncryptionOptions(
	ctx context.Context, src cloud.ExternalStorage,
) ([]jobspb.EncryptionInfo, error) {
	const encryptionReadErrorMsg = `could not find or read encryption information`

	files, err := getEncryptionInfoFiles(ctx, src)
	if err != nil {
		return nil, errors.Mark(errors.Wrap(err, encryptionReadErrorMsg), errEncryptionInfoRead)
	}
	var encInfo []jobspb.EncryptionInfo
	// The user is more likely to pass in a KMS URI that was used to
	// encrypt the backup recently, so we iterate the ENCRYPTION-INFO
	// files from latest to oldest.
	for i := len(files) - 1; i >= 0; i-- {
		r, err := src.ReadFile(ctx, files[i])
		if err != nil {
			return nil, errors.Wrap(err, encryptionReadErrorMsg)
		}
		defer r.Close(ctx)

		encInfoBytes, err := ioctx.ReadAll(ctx, r)
		if err != nil {
			return nil, errors.Wrap(err, encryptionReadErrorMsg)
		}
		var currentEncInfo jobspb.EncryptionInfo
		if err := protoutil.Unmarshal(encInfoBytes, &currentEncInfo); err != nil {
			return nil, err
		}
		encInfo = append(encInfo, currentEncInfo)
	}
	return encInfo, nil
}

func getEncryptionInfoFiles(ctx context.Context, dest cloud.ExternalStorage) ([]string, error) {
	var files []string
	// Look for all files in dest that start with "/ENCRYPTION-INFO"
	// and return them.
	err := dest.List(ctx, "", listingDelimDataSlash, func(p string) error {
		paths := strings.Split(p, "/")
		p = paths[len(paths)-1]
		if match := strings.HasPrefix(p, backupEncryptionInfoFile); match {
			files = append(files, p)
		}

		return nil
	})
	if len(files) < 1 {
		return nil, errors.New("no ENCRYPTION-INFO files found")
	}

	return files, err
}

func writeEncryptionInfoIfNotExists(
	ctx context.Context, opts *jobspb.EncryptionInfo, dest cloud.ExternalStorage,
) error {
	r, err := dest.ReadFile(ctx, backupEncryptionInfoFile)
	if err == nil {
		r.Close(ctx)
		// If the file already exists, then we don't need to create a new one.
		return nil
	}

	if !errors.Is(err, cloud.ErrFileDoesNotExist) {
		return errors.Wrapf(err,
			"returned an unexpected error when checking for the existence of %s file",
			backupEncryptionInfoFile)
	}

	buf, err := protoutil.Marshal(opts)
	if err != nil {
		return err
	}
	if err := cloud.WriteFile(ctx, dest, backupEncryptionInfoFile, bytes.NewReader(buf)); err != nil {
		return err
	}
	return nil
}

// RedactURIForErrorMessage redacts any storage secrets before returning a URI which is safe to
// return to the client in an error message.
func RedactURIForErrorMessage(uri string) string {
	redactedURI, err := cloud.SanitizeExternalStorageURI(uri, []string{})
	if err != nil {
		return "<uri_failed_to_redact>"
	}
	return redactedURI
}

// writeLockOnBackupLocation is responsible for writing a job ID suffixed
// `BACKUP-LOCK` file that will prevent concurrent backups from writing to the
// same location.
func writeLockOnBackupLocation(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	defaultURI string,
	jobID jobspb.JobID,
	user security.SQLUsername,
) error {
	defaultStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, defaultURI, user)
	if err != nil {
		return err
	}
	defer defaultStore.Close()

	// The lock file name consists of two parts `BACKUP-LOCK-<jobID>.
	//
	// The jobID is used in `checkForPreviousBackups` to ensure that we do not
	// read our own lock file on job resumption.
	lockFileName := fmt.Sprintf("%s%s", backupLockFilePrefix, strconv.FormatInt(int64(jobID), 10))

	return cloud.WriteFile(ctx, defaultStore, lockFileName, bytes.NewReader([]byte("lock")))
}

// checkForBackupLockFile returns true if it finds a `BACKUP-LOCK-<jobID>` file
// at `defaultURI`.
func checkForBackupLockFile(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	defaultURI string,
	jobID jobspb.JobID,
	user security.SQLUsername,
) (bool, error) {
	defaultStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, defaultURI, user)
	if err != nil {
		return false, err
	}
	defer defaultStore.Close()

	// Check for the existence of a BACKUP-LOCK file written by our job
	// corresponding to `jobID`. If present, we have already laid claim on the
	// location and do not need to check further.
	lockFileName := fmt.Sprintf("%s%s", backupLockFilePrefix, strconv.FormatInt(int64(jobID), 10))
	r, err := defaultStore.ReadFile(ctx, lockFileName)
	if err == nil {
		r.Close(ctx)
		return true, nil
	} else if errors.Is(err, cloud.ErrFileDoesNotExist) {
		return false, nil
	}

	return false, err
}

// checkForPreviousBackup ensures that the target location does not already
// contain a previous or concurrently running backup. It does this by checking
// for the existence of one of:
//
// 1) BACKUP_MANIFEST: Written on completion of a backup.
//
// 2) BACKUP-LOCK: Written by the coordinator node to lay claim on a backup
// location. This file is suffixed with the ID of the backup job to prevent a
// node from reading its own lock file on job resumption.
//
// 3) BACKUP-CHECKPOINT: Prior to 22.1.1, nodes would use the BACKUP-CHECKPOINT
// to lay claim on a backup location. To account for a mixed-version cluster
// where an older coordinator node may be running a concurrent backup to the
// same location, we must continue to check for a BACKUP-CHECKPOINT file.
//
// NB: The node will continue to write a BACKUP-CHECKPOINT file later in its
// execution, but we do not have to worry about reading our own
// BACKUP-CHECKPOINT file (and locking ourselves out) since
// `checkForPreviousBackup` is invoked as the first step on job resumption, and
// is not called again.
func checkForPreviousBackup(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	defaultURI string,
	jobID jobspb.JobID,
	user security.SQLUsername,
) error {
	defaultStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, defaultURI, user)
	if err != nil {
		return err
	}
	defer defaultStore.Close()

	redactedURI := RedactURIForErrorMessage(defaultURI)

	// Check for the presence of a BACKUP_MANIFEST.
	r, err := defaultStore.ReadFile(ctx, backupManifestName)
	if err == nil {
		r.Close(ctx)
		return pgerror.Newf(pgcode.FileAlreadyExists,
			"%s already contains a %s file",
			redactedURI, backupManifestName)
	}

	if !errors.Is(err, cloud.ErrFileDoesNotExist) {
		return errors.Wrapf(err,
			"%s returned an unexpected error when checking for the existence of %s file",
			redactedURI, backupManifestName)
	}

	// Check for the presence of a BACKUP-LOCK file with a job ID different from
	// that of our job.
	if err := defaultStore.List(ctx, "", listingDelimDataSlash, func(s string) error {
		s = strings.TrimPrefix(s, "/")
		if strings.HasPrefix(s, backupLockFilePrefix) {
			jobIDSuffix := strings.TrimPrefix(s, backupLockFilePrefix)
			if len(jobIDSuffix) == 0 {
				return errors.AssertionFailedf("malformed BACKUP-LOCK file %s, expected a job ID suffix", s)
			}
			if jobIDSuffix != strconv.FormatInt(int64(jobID), 10) {
				return pgerror.Newf(pgcode.FileAlreadyExists,
					"%s already contains a `BACKUP-LOCK` file written by job %s",
					redactedURI, jobIDSuffix)
			}
		}
		return nil
	}); err != nil {
		// HTTP external storage does not support listing, and so we skip checking
		// for a BACKUP-LOCK file.
		if !errors.Is(err, cloud.ErrListingUnsupported) {
			return errors.Wrap(err, "checking for BACKUP-LOCK file")
		}
		log.Warningf(ctx, "external storage %s does not support listing: skip checking for BACKUP_LOCK", redactedURI)
	}

	// Check for a BACKUP-CHECKPOINT that might have been written by a node
	// running a pre-22.1.1 binary.
	//
	// TODO(adityamaru): Delete in 23.1 since we will no longer need to check for
	// BACKUP-CHECKPOINT files as all backups will rely on BACKUP-LOCK to lock a
	// location.
	r, err = readLatestCheckpointFile(ctx, defaultStore, backupManifestCheckpointName)
	if err == nil {
		r.Close(ctx)
		return pgerror.Newf(pgcode.FileAlreadyExists,
			"%s already contains a %s file (is another operation already in progress?)",
			redactedURI, backupManifestCheckpointName)
	}

	if !errors.Is(err, cloud.ErrFileDoesNotExist) {
		return errors.Wrapf(err,
			"%s returned an unexpected error when checking for the existence of %s file",
			redactedURI, backupManifestCheckpointName)
	}

	return nil
}

// tempCheckpointFileNameForJob returns temporary filename for backup manifest checkpoint.
func tempCheckpointFileNameForJob(jobID jobspb.JobID) string {
	return fmt.Sprintf("%s-%d", backupManifestCheckpointName, jobID)
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
		backupPaths[i] = strings.TrimSuffix(backupPath, "/"+backupManifestName)
	}
	return backupPaths, nil
}

// readLatestCheckpointFile returns an ioctx.ReaderCloserCtx of the latest
// checkpoint file.
func readLatestCheckpointFile(
	ctx context.Context, exportStore cloud.ExternalStorage, filename string,
) (ioctx.ReadCloserCtx, error) {
	filename = strings.TrimPrefix(filename, "/")
	// First try reading from the progress directory. If the backup is from
	// an older version, it may not exist there yet so try reading
	// in the base directory if the first attempt fails.
	var checkpoint string
	var checkpointFound bool
	var r ioctx.ReadCloserCtx
	var err error

	// We name files such that the most recent checkpoint will always
	// be at the top, so just grab the first filename.
	err = exportStore.List(ctx, backupProgressDirectory, "", func(p string) error {
		// The first file returned by List could be either the checkpoint or
		// checksum file, but we are only concerned with the timestamped prefix.
		// We resolve if it is a checkpoint or checksum file separately below.
		p = strings.TrimPrefix(p, "/")
		checkpoint = strings.TrimSuffix(p, backupManifestChecksumSuffix)
		checkpointFound = true
		// We only want the first checkpoint so return an error that it is
		// done listing.
		return cloud.ErrListingDone
	})
	// If the list failed because the storage used does not support listing,
	// such as http we can try reading the non timestamped backup checkpoint
	// directly. This can still fail if it is a mixed cluster and the
	// checkpoint was written in the base directory.
	if errors.Is(err, cloud.ErrListingUnsupported) {
		r, err = exportStore.ReadFile(ctx, backupProgressDirectory+"/"+filename)
		// If we found the checkpoint in progress, then don't bother reading
		// from base, just return the reader.
		if err == nil {
			return r, nil
		}
	} else if err != nil && !errors.Is(err, cloud.ErrListingDone) {
		return nil, err
	}

	if checkpointFound {
		if strings.HasSuffix(filename, backupManifestChecksumSuffix) {
			return exportStore.ReadFile(ctx, backupProgressDirectory+"/"+checkpoint+backupManifestChecksumSuffix)
		}
		return exportStore.ReadFile(ctx, backupProgressDirectory+"/"+checkpoint)
	}

	// If the checkpoint wasn't found in the progress directory, then try
	// reading from the base directory instead.
	r, err = exportStore.ReadFile(ctx, filename)

	if err != nil {
		return nil, errors.Wrapf(err, "%s could not be read in the base or progress directory", filename)
	}
	return r, nil
}

// newTimestampedFilename returns a filename with a suffixed version. This new
// filename is in the format of filename-<version> where version is a hex
// encoded one's complement of the timestamp. This means that as long as the
// supplied timestamp is correct, the filenames will adhere to a
// lexicographical/utf-8 ordering such that the most recent file is at the top.
func newTimestampedFilename(filename string) string {
	var buffer []byte
	buffer = encoding.EncodeStringDescending(buffer, timeutil.Now().String())
	return fmt.Sprintf("%s-%s", filename, hex.EncodeToString(buffer))
}

// MakeBackupCodec returns the codec and tenantID that was used to encode the keys in the backup.
func makeBackupCodec(manifest BackupManifest) (keys.SQLCodec, roachpb.TenantID, error) {
	backupCodec := keys.SystemSQLCodec
	backupTenantID := roachpb.SystemTenantID
	if len(manifest.Spans) != 0 && !manifest.HasTenants() {
		// If there are no tenant targets, then the entire keyspace covered by
		// Spans must lie in 1 tenant.
		var err error
		_, backupTenantID, err = keys.DecodeTenantPrefix(manifest.Spans[0].Key)
		if err != nil {
			return backupCodec, backupTenantID, err
		}
		backupCodec = keys.MakeSQLCodec(backupTenantID)
	}
	return backupCodec, backupTenantID, nil
}
