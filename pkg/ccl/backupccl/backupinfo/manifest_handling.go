// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupinfo

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Files that may appear in a backup directory.
const (
	// BackupManifestName is the file name used for serialized BackupManifest
	// protos.
	BackupManifestName = "BACKUP_MANIFEST"
	// BackupOldManifestName is an old name for the serialized BackupManifest
	// proto. It is used by 20.1 nodes and earlier.
	BackupOldManifestName = "BACKUP"

	// BackupManifestChecksumSuffix indicates where the checksum for the manifest
	// is stored if present. It can be found in the name of the backup manifest +
	// this suffix.
	BackupManifestChecksumSuffix = "-CHECKSUM"

	// BackupManifestCheckpointName is the file name used to store the serialized
	// BackupManifest proto while the backup is in progress.
	BackupManifestCheckpointName = "BACKUP-CHECKPOINT"
)

const (
	// BackupFormatDescriptorTrackingVersion added tracking of complete DBs.
	BackupFormatDescriptorTrackingVersion uint32 = 1

	// BackupProgressDirectory is the directory where all 22.1 and beyond
	// CHECKPOINT files will be stored as we no longer want to overwrite
	// them.
	BackupProgressDirectory = "progress"

	// DateBasedIncFolderName is the date format used when creating sub-directories
	// storing incremental backups for auto-appendable backups.
	// It is exported for testing backup inspection tooling.
	DateBasedIncFolderName = "/20060102/150405.00"

	// DateBasedIntoFolderName is the date format used when creating sub-directories
	// for storing backups in a collection.
	// Also exported for testing backup inspection tooling.
	DateBasedIntoFolderName = "/2006/01/02-150405.00"
)

var WriteMetadataSST = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"kv.bulkio.write_metadata_sst.enabled",
	"write experimental new format BACKUP metadata file",
	true,
)

// IsGZipped detects whether the given bytes represent GZipped data. This check
// is used rather than a standard implementation such as http.DetectContentType
// since some zipped data may be mis-identified by that method. We've seen
// gzipped data incorrectly identified as "application/vnd.ms-fontobject". The
// magic bytes are from the MIME sniffing algorithm http.DetectContentType is
// based which can be found at https://mimesniff.spec.whatwg.org/.
//
// This method is only used to detect if protobufs are GZipped, and there are no
// conflicts between the starting bytes of a protobuf and these magic bytes.
func IsGZipped(dat []byte) bool {
	gzipPrefix := []byte("\x1F\x8B\x08")
	return bytes.HasPrefix(dat, gzipPrefix)
}

// BackupFileDescriptors is an alias on which to implement sort's interface.
type BackupFileDescriptors []backuppb.BackupManifest_File

func (r BackupFileDescriptors) Len() int      { return len(r) }
func (r BackupFileDescriptors) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r BackupFileDescriptors) Less(i, j int) bool {
	if cmp := bytes.Compare(r[i].Span.Key, r[j].Span.Key); cmp != 0 {
		return cmp < 0
	}
	return bytes.Compare(r[i].Span.EndKey, r[j].Span.EndKey) < 0
}

// ReadBackupManifestFromURI creates an export store from the given URI, then
// reads and unmarshalls a BackupManifest at the standard location in the
// export storage.
func ReadBackupManifestFromURI(
	ctx context.Context,
	mem *mon.BoundAccount,
	uri string,
	user username.SQLUsername,
	makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
	encryption *jobspb.BackupEncryptionOptions,
) (backuppb.BackupManifest, int64, error) {
	exportStore, err := makeExternalStorageFromURI(ctx, uri, user)

	if err != nil {
		return backuppb.BackupManifest{}, 0, err
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
) (backuppb.BackupManifest, int64, error) {
	backupManifest, memSize, err := ReadBackupManifest(ctx, mem, exportStore, BackupManifestName,
		encryption)
	if err != nil {
		oldManifest, newMemSize, newErr := ReadBackupManifest(ctx, mem, exportStore, BackupOldManifestName,
			encryption)
		if newErr != nil {
			return backuppb.BackupManifest{}, 0, err
		}
		backupManifest = oldManifest
		memSize = newMemSize
	}
	backupManifest.Dir = exportStore.Conf()
	// TODO(dan): Sanity check this BackupManifest: non-empty EndTime, non-empty
	// Paths, and non-overlapping Spans and keyranges in Files.
	return backupManifest, memSize, nil
}

// ContainsManifest checks for the existence of a BACKUP_MANIFEST in
// exportStore.
func ContainsManifest(ctx context.Context, exportStore cloud.ExternalStorage) (bool, error) {
	r, err := exportStore.ReadFile(ctx, BackupManifestName)
	if err != nil {
		if errors.Is(err, cloud.ErrFileDoesNotExist) {
			return false, nil
		}
		return false, err
	}
	r.Close(ctx)
	return true, nil
}

// CompressData compresses data buffer and returns compressed
// bytes (i.e. gzip format).
func CompressData(descBuf []byte) ([]byte, error) {
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

// DecompressData decompresses gzip data buffer and
// returns decompressed bytes.
func DecompressData(ctx context.Context, mem *mon.BoundAccount, descBytes []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewBuffer(descBytes))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return mon.ReadAll(ctx, ioctx.ReaderAdapter(r), mem)
}

// ReadBackupCheckpointManifest reads and unmarshals a BACKUP-CHECKPOINT
// manifest from filename in the provided export store.
func ReadBackupCheckpointManifest(
	ctx context.Context,
	mem *mon.BoundAccount,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
) (backuppb.BackupManifest, int64, error) {
	checkpointFile, err := readLatestCheckpointFile(ctx, exportStore, filename)
	if err != nil {
		return backuppb.BackupManifest{}, 0, err
	}
	defer checkpointFile.Close(ctx)

	// Look for a checksum, if one is not found it could be an older backup,
	// but we want to continue anyway.
	checksumFile, err := readLatestCheckpointFile(ctx, exportStore, filename+BackupManifestChecksumSuffix)
	if err != nil {
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			return backuppb.BackupManifest{}, 0, err
		}
		// Pass checksumFile as nil to indicate it was not found.
		return readManifest(ctx, mem, exportStore, encryption, checkpointFile, nil)
	}
	defer checksumFile.Close(ctx)
	return readManifest(ctx, mem, exportStore, encryption, checkpointFile, checksumFile)
}

// ReadBackupManifest reads and unmarshals a BackupManifest from filename in the
// provided export store.
func ReadBackupManifest(
	ctx context.Context,
	mem *mon.BoundAccount,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
) (backuppb.BackupManifest, int64, error) {
	manifestFile, err := exportStore.ReadFile(ctx, filename)
	if err != nil {
		return backuppb.BackupManifest{}, 0, err
	}
	defer manifestFile.Close(ctx)

	// Look for a checksum, if one is not found it could be an older backup,
	// but we want to continue anyway.
	checksumFile, err := exportStore.ReadFile(ctx, filename+BackupManifestChecksumSuffix)
	if err != nil {
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			return backuppb.BackupManifest{}, 0, err
		}
		// Pass checksumFile as nil to indicate it was not found.
		return readManifest(ctx, mem, exportStore, encryption, manifestFile, nil)
	}
	defer checksumFile.Close(ctx)
	return readManifest(ctx, mem, exportStore, encryption, manifestFile, checksumFile)
}

// readManifest reads and unmarshals a BackupManifest from filename in the
// provided export store. If the passed bound account is not nil, the bytes read
// are reserved from it as it is read and then the approximate in-memory size
// (the total decompressed serialized byte size) is reserved as well before
// deserialization and returned so that callers can then shrink the bound acct
// by that amount when they release the returned manifest.
func readManifest(
	ctx context.Context,
	mem *mon.BoundAccount,
	exportStore cloud.ExternalStorage,
	encryption *jobspb.BackupEncryptionOptions,
	manifestReader ioctx.ReadCloserCtx,
	checksumReader ioctx.ReadCloserCtx,
) (backuppb.BackupManifest, int64, error) {
	descBytes, err := mon.ReadAll(ctx, manifestReader, mem)
	if err != nil {
		return backuppb.BackupManifest{}, 0, err
	}
	defer func() {
		mem.Shrink(ctx, int64(cap(descBytes)))
	}()
	if checksumReader != nil {
		// If there is a checksum file present, check that it matches.
		checksumFileData, err := ioctx.ReadAll(ctx, checksumReader)
		if err != nil {
			return backuppb.BackupManifest{}, 0, errors.Wrap(err, "reading checksum file")
		}
		checksum, err := GetChecksum(descBytes)
		if err != nil {
			return backuppb.BackupManifest{}, 0, errors.Wrap(err, "calculating checksum of manifest")
		}
		if !bytes.Equal(checksumFileData, checksum) {
			return backuppb.BackupManifest{}, 0, errors.Newf("checksum mismatch; expected %s, got %s",
				hex.EncodeToString(checksumFileData), hex.EncodeToString(checksum))
		}
	}

	var encryptionKey []byte
	if encryption != nil {
		encryptionKey, err = backupencryption.GetEncryptionKey(ctx, encryption, exportStore.Settings(),
			exportStore.ExternalIOConf())
		if err != nil {
			return backuppb.BackupManifest{}, 0, err
		}
		plaintextBytes, err := storageccl.DecryptFile(ctx, descBytes, encryptionKey, mem)
		if err != nil {
			return backuppb.BackupManifest{}, 0, err
		}
		mem.Shrink(ctx, int64(cap(descBytes)))
		descBytes = plaintextBytes
	}

	if IsGZipped(descBytes) {
		decompressedBytes, err := DecompressData(ctx, mem, descBytes)
		if err != nil {
			return backuppb.BackupManifest{}, 0, errors.Wrap(
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
		return backuppb.BackupManifest{}, 0, err
	}

	var backupManifest backuppb.BackupManifest
	if err := protoutil.Unmarshal(descBytes, &backupManifest); err != nil {
		mem.Shrink(ctx, approxMemSize)
		if encryption == nil && storageccl.AppearsEncrypted(descBytes) {
			return backuppb.BackupManifest{}, 0, errors.Wrap(
				err, "file appears encrypted -- try specifying one of \"encryption_passphrase\" or \"kms\"")
		}
		return backuppb.BackupManifest{}, 0, err
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

// ReadBackupPartitionDescriptor reads the BackupPartitionDescriptor from the
// provided external storage URI. This method also returns the byte size
// reserved in mem to account for the returned descriptor.
func ReadBackupPartitionDescriptor(
	ctx context.Context,
	mem *mon.BoundAccount,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
) (backuppb.BackupPartitionDescriptor, int64, error) {
	r, err := exportStore.ReadFile(ctx, filename)
	if err != nil {
		return backuppb.BackupPartitionDescriptor{}, 0, err
	}
	defer r.Close(ctx)
	descBytes, err := mon.ReadAll(ctx, r, mem)
	if err != nil {
		return backuppb.BackupPartitionDescriptor{}, 0, err
	}
	defer func() {
		mem.Shrink(ctx, int64(cap(descBytes)))
	}()

	if encryption != nil {
		encryptionKey, err := backupencryption.GetEncryptionKey(ctx, encryption, exportStore.Settings(),
			exportStore.ExternalIOConf())
		if err != nil {
			return backuppb.BackupPartitionDescriptor{}, 0, err
		}
		plaintextData, err := storageccl.DecryptFile(ctx, descBytes, encryptionKey, mem)
		if err != nil {
			return backuppb.BackupPartitionDescriptor{}, 0, err
		}
		mem.Shrink(ctx, int64(cap(descBytes)))
		descBytes = plaintextData
	}

	if IsGZipped(descBytes) {
		decompressedData, err := DecompressData(ctx, mem, descBytes)
		if err != nil {
			return backuppb.BackupPartitionDescriptor{}, 0, errors.Wrap(
				err, "decompressing backup partition descriptor")
		}
		mem.Shrink(ctx, int64(cap(descBytes)))
		descBytes = decompressedData
	}

	memSize := int64(len(descBytes))

	if err := mem.Grow(ctx, memSize); err != nil {
		return backuppb.BackupPartitionDescriptor{}, 0, err
	}

	var backupManifest backuppb.BackupPartitionDescriptor
	if err := protoutil.Unmarshal(descBytes, &backupManifest); err != nil {
		mem.Shrink(ctx, memSize)
		return backuppb.BackupPartitionDescriptor{}, 0, err
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
) (*backuppb.StatsTable, error) {
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
		encryptionKey, err := backupencryption.GetEncryptionKey(ctx, encryption, exportStore.Settings(),
			exportStore.ExternalIOConf())
		if err != nil {
			return nil, err
		}
		statsBytes, err = storageccl.DecryptFile(ctx, statsBytes, encryptionKey, nil /* mm */)
		if err != nil {
			return nil, err
		}
	}
	var tableStats backuppb.StatsTable
	if err := protoutil.Unmarshal(statsBytes, &tableStats); err != nil {
		return nil, err
	}
	return &tableStats, err
}

// GetStatisticsFromBackup retrieves Statistics from backup manifest,
// either through the Statistics field or from the files.
func GetStatisticsFromBackup(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	encryption *jobspb.BackupEncryptionOptions,
	backup backuppb.BackupManifest,
) ([]*stats.TableStatisticProto, error) {
	// This part deals with pre-20.2 stats format where backup statistics
	// are stored as a field in backup manifests instead of in their
	// individual files.
	if backup.DeprecatedStatistics != nil {
		return backup.DeprecatedStatistics, nil
	}
	tableStatistics := make([]*stats.TableStatisticProto, 0, len(backup.StatisticsFilenames))
	uniqueFileNames := make(map[string]struct{})
	for _, fname := range backup.StatisticsFilenames {
		if _, exists := uniqueFileNames[fname]; !exists {
			uniqueFileNames[fname] = struct{}{}
			myStatsTable, err := readTableStatistics(ctx, exportStore, fname, encryption)
			if err != nil {
				return tableStatistics, err
			}
			tableStatistics = append(tableStatistics, myStatsTable.Statistics...)
		}
	}

	return tableStatistics, nil
}


// WriteBackupManifest writes the backup manifest `desc` to external storage.
func WriteBackupManifest(
	ctx context.Context,
	settings *cluster.Settings,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
	desc *backuppb.BackupManifest,
) error {
	sort.Sort(BackupFileDescriptors(desc.Files))

	descBuf, err := protoutil.Marshal(desc)
	if err != nil {
		return err
	}

	descBuf, err = CompressData(descBuf)
	if err != nil {
		return errors.Wrap(err, "compressing backup manifest")
	}

	if encryption != nil {
		encryptionKey, err := backupencryption.GetEncryptionKey(ctx, encryption, settings, exportStore.ExternalIOConf())
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
	checksum, err := GetChecksum(descBuf)
	if err != nil {
		return errors.Wrap(err, "calculating checksum")
	}

	if err := cloud.WriteFile(ctx, exportStore, filename+BackupManifestChecksumSuffix, bytes.NewReader(checksum)); err != nil {
		return errors.Wrap(err, "writing manifest checksum")
	}

	return nil
}

// GetChecksum returns a 32 bit keyed-checksum for the given data.
func GetChecksum(data []byte) ([]byte, error) {
	const checksumSizeBytes = 4
	hash := sha256.New()
	if _, err := hash.Write(data); err != nil {
		return nil, errors.Wrap(err,
			`"It never returns an error." -- https://golang.org/pkg/hash`)
	}
	return hash.Sum(nil)[:checksumSizeBytes], nil
}

// WriteBackupPartitionDescriptor writes metadata (containing a locality KV and
// partial file listing) for a partitioned BACKUP to one of the stores in the
// backup.
func WriteBackupPartitionDescriptor(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
	desc *backuppb.BackupPartitionDescriptor,
) error {
	descBuf, err := protoutil.Marshal(desc)
	if err != nil {
		return err
	}
	descBuf, err = CompressData(descBuf)
	if err != nil {
		return errors.Wrap(err, "compressing backup partition descriptor")
	}
	if encryption != nil {
		encryptionKey, err := backupencryption.GetEncryptionKey(ctx, encryption, exportStore.Settings(),
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

// WriteTableStatistics writes a StatsTable object to a file of the filename
// to the specified exportStore. It will be encrypted according to the encryption
// option given.
func WriteTableStatistics(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
	stats *backuppb.StatsTable,
) error {
	statsBuf, err := protoutil.Marshal(stats)
	if err != nil {
		return err
	}
	if encryption != nil {
		encryptionKey, err := backupencryption.GetEncryptionKey(ctx, encryption, exportStore.Settings(),
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

// LoadBackupManifests loads the BACKUP_MANIFESTs
func LoadBackupManifests(
	ctx context.Context,
	mem *mon.BoundAccount,
	uris []string,
	user username.SQLUsername,
	makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
	encryption *jobspb.BackupEncryptionOptions,
) ([]backuppb.BackupManifest, int64, error) {
	backupManifests := make([]backuppb.BackupManifest, len(uris))
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

// GetBackupIndexAtTime returns the index in backupManifests corresponding to
// the BACKUP_MANIFEST that applies to the passed in AOST.
func GetBackupIndexAtTime(backupManifests []backuppb.BackupManifest, asOf hlc.Timestamp) (int, error) {
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

// RedactURIForErrorMessage redacts any storage secrets before returning a URI which is safe to
// return to the client in an error message.
func RedactURIForErrorMessage(uri string) string {
	redactedURI, err := cloud.SanitizeExternalStorageURI(uri, []string{})
	if err != nil {
		return "<uri_failed_to_redact>"
	}
	return redactedURI
}

// CheckForPreviousBackup ensures that the target location does not already
// contain a BACKUP or checkpoint, locking out accidental concurrent operations
// on that location. Note that the checkpoint file should be written as soon as
// the job actually starts.
func CheckForPreviousBackup(
	ctx context.Context, exportStore cloud.ExternalStorage, defaultURI string,
) error {
	redactedURI := RedactURIForErrorMessage(defaultURI)
	r, err := exportStore.ReadFile(ctx, BackupManifestName)
	if err == nil {
		r.Close(ctx)
		return pgerror.Newf(pgcode.FileAlreadyExists,
			"%s already contains a %s file",
			redactedURI, BackupManifestName)
	}

	if !errors.Is(err, cloud.ErrFileDoesNotExist) {
		return errors.Wrapf(err,
			"%s returned an unexpected error when checking for the existence of %s file",
			redactedURI, BackupManifestName)
	}

	r, err = readLatestCheckpointFile(ctx, exportStore, BackupManifestCheckpointName)
	if err == nil {
		r.Close(ctx)
		return pgerror.Newf(pgcode.FileAlreadyExists,
			"%s already contains a %s file (is another operation already in progress?)",
			redactedURI, BackupManifestCheckpointName)
	}

	if !errors.Is(err, cloud.ErrFileDoesNotExist) {
		return errors.Wrapf(err,
			"%s returned an unexpected error when checking for the existence of %s file",
			redactedURI, BackupManifestCheckpointName)
	}

	return nil
}

// TempCheckpointFileNameForJob returns temporary filename for backup manifest checkpoint.
func TempCheckpointFileNameForJob(jobID jobspb.JobID) string {
	return fmt.Sprintf("%s-%d", BackupManifestCheckpointName, jobID)
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
	err = exportStore.List(ctx, BackupProgressDirectory, "", func(p string) error {
		// The first file returned by List could be either the checkpoint or
		// checksum file, but we are only concerned with the timestamped prefix.
		// We resolve if it is a checkpoint or checksum file separately below.
		p = strings.TrimPrefix(p, "/")
		checkpoint = strings.TrimSuffix(p, BackupManifestChecksumSuffix)
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
		r, err = exportStore.ReadFile(ctx, BackupProgressDirectory+"/"+filename)
		// If we found the checkpoint in progress, then don't bother reading
		// from base, just return the reader.
		if err == nil {
			return r, nil
		}
	} else if err != nil && !errors.Is(err, cloud.ErrListingDone) {
		return nil, err
	}

	if checkpointFound {
		if strings.HasSuffix(filename, BackupManifestChecksumSuffix) {
			return exportStore.ReadFile(ctx, BackupProgressDirectory+"/"+checkpoint+BackupManifestChecksumSuffix)
		}
		return exportStore.ReadFile(ctx, BackupProgressDirectory+"/"+checkpoint)
	}

	// If the checkpoint wasn't found in the progress directory, then try
	// reading from the base directory instead.
	r, err = exportStore.ReadFile(ctx, filename)

	if err != nil {
		return nil, errors.Wrapf(err, "%s could not be read in the base or progress directory", filename)
	}
	return r, nil

}

// NewTimestampedCheckpointFileName returns a string of a new checkpoint filename
// with a suffixed version. It returns it in the format of BACKUP-CHECKPOINT-<version>
// where version is a hex encoded one's complement of the timestamp.
// This means that as long as the supplied timestamp is correct, the filenames
// will adhere to a lexicographical/utf-8 ordering such that the most
// recent file is at the top.
func NewTimestampedCheckpointFileName() string {
	var buffer []byte
	buffer = encoding.EncodeStringDescending(buffer, timeutil.Now().String())
	return fmt.Sprintf("%s-%s", BackupManifestCheckpointName, hex.EncodeToString(buffer))
}

// WriteBackupManifestCheckpoint writes a new BACKUP-CHECKPOINT MANIFEST and
// CHECKSUM file. If it is a pure v22.1 cluster or later, it will write a
// timestamped BACKUP-CHECKPOINT to the /progress directory. If it is a mixed
// cluster version, it will write a non timestamped BACKUP-CHECKPOINT to the
// base directory in order to not break backup jobs that resume on a v21.2 node.
func WriteBackupManifestCheckpoint(
	ctx context.Context,
	storageURI string,
	encryption *jobspb.BackupEncryptionOptions,
	desc *backuppb.BackupManifest,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
) error {
	defaultStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, storageURI, user)
	if err != nil {
		return err
	}
	defer defaultStore.Close()

	sort.Sort(BackupFileDescriptors(desc.Files))

	descBuf, err := protoutil.Marshal(desc)
	if err != nil {
		return err
	}

	descBuf, err = CompressData(descBuf)
	if err != nil {
		return errors.Wrap(err, "compressing backup manifest")
	}

	if encryption != nil {
		encryptionKey, err := backupencryption.GetEncryptionKey(ctx, encryption, execCfg.Settings, defaultStore.ExternalIOConf())
		if err != nil {
			return err
		}
		descBuf, err = storageccl.EncryptFile(descBuf, encryptionKey)
		if err != nil {
			return err
		}
	}

	// If the cluster is still running on a mixed version, we want to write
	// to the base directory instead of the progress directory. That way if
	// an old node resumes a backup, it doesn't have to start over.
	if !execCfg.Settings.Version.IsActive(ctx, clusterversion.BackupDoesNotOverwriteLatestAndCheckpoint) {
		// We want to overwrite the latest checkpoint in the base directory,
		// just write to the non versioned BACKUP-CHECKPOINT file.
		err = cloud.WriteFile(ctx, defaultStore, BackupManifestCheckpointName, bytes.NewReader(descBuf))
		if err != nil {
			return err
		}

		checksum, err := GetChecksum(descBuf)
		if err != nil {
			return err
		}

		return cloud.WriteFile(ctx, defaultStore, BackupManifestCheckpointName+BackupManifestChecksumSuffix, bytes.NewReader(checksum))
	}

	// We timestamp the checkpoint files in order to enforce write once backups.
	// When the job goes to read these timestamped files, it will List
	// the checkpoints and pick the file whose name is lexicographically
	// sorted to the top. This will be the last checkpoint we write, for
	// details refer to newTimestampedCheckpointFileName.
	filename := NewTimestampedCheckpointFileName()

	// HTTP storage does not support listing and so we cannot rely on the
	// above-mentioned List method to return us the latest checkpoint file.
	// Instead, we will write a checkpoint once with a well-known filename,
	// and teach the job to always reach for that filename in the face of
	// a resume. We may lose progress, but this is a cost we are willing
	// to pay to uphold write-once semantics.
	if defaultStore.Conf().Provider == roachpb.ExternalStorageProvider_http {
		// TODO (darryl): We should do this only for file not found or directory
		// does not exist errors. As of right now we only specifically wrap
		// ReadFile errors for file not found so this is not possible yet.
		if r, err := defaultStore.ReadFile(ctx, BackupProgressDirectory+"/"+BackupManifestCheckpointName); err != nil {
			// Since we did not find the checkpoint file this is the first time
			// we are going to write a checkpoint, so write it with the well
			// known filename.
			filename = BackupManifestCheckpointName
		} else {
			err = r.Close(ctx)
			if err != nil {
				return err
			}
		}
	}

	err = cloud.WriteFile(ctx, defaultStore, BackupProgressDirectory+"/"+filename, bytes.NewReader(descBuf))
	if err != nil {
		return errors.Wrap(err, "calculating checksum")
	}

	// Write the checksum file after we've successfully wrote the checkpoint.
	checksum, err := GetChecksum(descBuf)
	if err != nil {
		return errors.Wrap(err, "calculating checksum")
	}

	err = cloud.WriteFile(ctx, defaultStore, BackupProgressDirectory+"/"+filename+BackupManifestChecksumSuffix, bytes.NewReader(checksum))
	if err != nil {
		return err
	}

	return nil
}

