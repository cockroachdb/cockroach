// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupinfo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	descpb "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	gzip "github.com/klauspost/compress/gzip"
)

// Files that may appear in a backup directory.
const (
	// BackupManifestChecksumSuffix indicates where the checksum for the manifest
	// is stored if present. It can be found in the name of the backup manifest +
	// this suffix.
	BackupManifestChecksumSuffix = "-CHECKSUM"

	// BackupManifestCheckpointName is the file name used to store the serialized
	// BackupManifest proto while the backup is in progress.
	BackupManifestCheckpointName = "BACKUP-CHECKPOINT"

	// BackupStatisticsFileName is the file name used to store the serialized
	// table statistics for the tables being backed up.
	BackupStatisticsFileName = "BACKUP-STATISTICS"

	// BackupLockFile is the prefix of the file name used by the backup job to
	// lock the bucket from running concurrent backups to the same destination.
	BackupLockFilePrefix = "BACKUP-LOCK-"

	// BackupFormatDescriptorTrackingVersion added tracking of complete DBs.
	BackupFormatDescriptorTrackingVersion uint32 = 1

	// BackupProgressDirectory is the directory where all 22.1 and beyond
	// CHECKPOINT files will be stored as we no longer want to overwrite
	// them.
	BackupProgressDirectory = "progress"
)

// WriteMetadataWithExternalSSTsEnabled controls if we write a `BACKUP_METADATA`
// file along with external SSTs containing lists of `BackupManifest_Files` and
// descriptors. This new format of metadata is written in addition to the
// `BACKUP_MANIFEST` file, and is expected to be its replacement in the future.
var WriteMetadataWithExternalSSTsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"backup.write_metadata_with_external_ssts.enabled",
	"write BACKUP metadata along with supporting SST files",
	metamorphic.ConstantWithTestBool("backup.write_metadata_with_external_ssts.enabled", true),
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
	kmsEnv cloud.KMSEnv,
) (backuppb.BackupManifest, int64, error) {
	exportStore, err := makeExternalStorageFromURI(ctx, uri, user)

	if err != nil {
		return backuppb.BackupManifest{}, 0, err
	}
	defer exportStore.Close()
	return ReadBackupManifestFromStore(ctx, mem, exportStore, uri, encryption, kmsEnv)
}

// ReadBackupManifestFromStore reads and unmarshalls a BackupManifest from the
// store and returns it with the size it reserved for it from the boundAccount.
func ReadBackupManifestFromStore(
	ctx context.Context,
	mem *mon.BoundAccount,
	exportStore cloud.ExternalStorage,
	storeURI string,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (backuppb.BackupManifest, int64, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.ReadBackupManifestFromStore")
	defer sp.Finish()

	manifest, memSize, err := ReadBackupManifest(ctx, mem, exportStore, backupbase.BackupMetadataName,
		encryption, kmsEnv)
	if err != nil {
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			return backuppb.BackupManifest{}, 0, err
		}

		// If we did not find `BACKUP_METADATA` we look for the
		// `BACKUP_MANIFEST` file as it is possible the backup was created by a
		// pre-23.1 node.
		log.VInfof(ctx, 2, "could not find BACKUP_METADATA, falling back to BACKUP_MANIFEST")
		backupManifest, backupManifestMemSize, backupManifestErr := ReadBackupManifest(ctx, mem, exportStore,
			backupbase.BackupManifestName, encryption, kmsEnv)
		if backupManifestErr != nil {
			if !errors.Is(backupManifestErr, cloud.ErrFileDoesNotExist) {
				return backuppb.BackupManifest{}, 0, backupManifestErr
			}

			// If we did not find a `BACKUP_MANIFEST` we look for a `BACKUP` file as
			// it is possible the backup was created by a pre-20.1 node.
			//
			// TODO(adityamaru): Remove this logic once we disallow restores beyond
			// the binary upgrade compatibility window.
			log.VInfof(ctx, 2, "could not find BACKUP_MANIFEST, falling back to BACKUP")
			oldBackupManifest, oldBackupManifestMemSize, oldBackupManifestErr := ReadBackupManifest(ctx, mem, exportStore,
				backupbase.BackupOldManifestName, encryption, kmsEnv)
			if oldBackupManifestErr != nil {
				if errors.Is(oldBackupManifestErr, cloud.ErrFileDoesNotExist) {
					log.VInfof(ctx, 2, "could not find any of the supported backup metadata files")
					return backuppb.BackupManifest{}, 0,
						errors.Wrapf(oldBackupManifestErr, "could not find BACKUP manifest file in any of the known locations: %s, %s, %s",
							backupbase.BackupMetadataName, backupbase.BackupManifestName, backupbase.BackupOldManifestName)
				}
				return backuppb.BackupManifest{}, 0, oldBackupManifestErr
			} else {
				// We found a `BACKUP` manifest file.
				manifest = oldBackupManifest
				memSize = oldBackupManifestMemSize
			}
		} else {
			// We found a `BACKUP_MANIFEST` file.
			manifest = backupManifest
			memSize = backupManifestMemSize
		}
	}
	manifest.Dir = exportStore.Conf()
	manifest.Dir.URI = storeURI
	return manifest, memSize, nil
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

// DecompressData decompresses gzip data buffer and returns decompressed bytes.
func DecompressData(ctx context.Context, mem *mon.BoundAccount, descBytes []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewBuffer(descBytes))
	if err != nil {
		return nil, err
	}
	defer func() {
		// Swallow any errors, this is only a read operation.
		_ = r.Close()
	}()
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
	kmsEnv cloud.KMSEnv,
) (backuppb.BackupManifest, int64, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.ReadBackupCheckpointManifest")
	defer sp.Finish()

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
		return readManifest(ctx, mem, encryption, kmsEnv, checkpointFile, nil)
	}
	defer checksumFile.Close(ctx)
	return readManifest(ctx, mem, encryption, kmsEnv, checkpointFile, checksumFile)
}

// ReadBackupManifest reads and unmarshals a BackupManifest from filename in the
// provided export store.
func ReadBackupManifest(
	ctx context.Context,
	mem *mon.BoundAccount,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (backuppb.BackupManifest, int64, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.ReadBackupManifest")
	defer sp.Finish()

	manifestFile, _, err := exportStore.ReadFile(ctx, filename, cloud.ReadOptions{NoFileSize: true})
	if err != nil {
		return backuppb.BackupManifest{}, 0, err
	}
	defer manifestFile.Close(ctx)

	// Look for a checksum, if one is not found it could be an older backup,
	// but we want to continue anyway.
	checksumFile, _, err := exportStore.ReadFile(ctx, filename+BackupManifestChecksumSuffix, cloud.ReadOptions{NoFileSize: true})
	if err != nil {
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			return backuppb.BackupManifest{}, 0, err
		}
		// Pass checksumFile as nil to indicate it was not found.
		return readManifest(ctx, mem, encryption, kmsEnv, manifestFile, nil)
	}
	defer checksumFile.Close(ctx)
	return readManifest(ctx, mem, encryption, kmsEnv, manifestFile, checksumFile)
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
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	manifestReader ioctx.ReadCloserCtx,
	checksumReader ioctx.ReadCloserCtx,
) (backuppb.BackupManifest, int64, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.readManifest")
	defer sp.Finish()

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
		encryptionKey, err = backupencryption.GetEncryptionKey(ctx, encryption, kmsEnv)
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
			return backuppb.BackupManifest{}, 0, errors.Wrapf(
				err, "file appears encrypted -- try specifying one of \"%s\" or \"%s\"",
				backupencryption.BackupOptEncPassphrase, backupencryption.BackupOptEncKMS)
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
	if err := backupManifest.UpgradeTenantDescriptors(); err != nil {
		return backuppb.BackupManifest{}, 0, err
	}

	return backupManifest, approxMemSize, nil
}

func readBackupPartitionDescriptor(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (backuppb.BackupPartitionDescriptor, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.readBackupPartitionDescriptor")
	defer sp.Finish()

	r, _, err := exportStore.ReadFile(ctx, filename, cloud.ReadOptions{NoFileSize: true})
	if err != nil {
		return backuppb.BackupPartitionDescriptor{}, err
	}
	defer r.Close(ctx)
	descBytes, err := ioctx.ReadAll(ctx, r)
	if err != nil {
		return backuppb.BackupPartitionDescriptor{}, err
	}

	memAcc := mon.NewStandaloneUnlimitedAccount()

	if encryption != nil {
		encryptionKey, err := backupencryption.GetEncryptionKey(ctx, encryption, kmsEnv)
		if err != nil {
			return backuppb.BackupPartitionDescriptor{}, err
		}
		plaintextData, err := storageccl.DecryptFile(ctx, descBytes, encryptionKey, memAcc)
		if err != nil {
			return backuppb.BackupPartitionDescriptor{}, err
		}
		descBytes = plaintextData
	}

	if IsGZipped(descBytes) {
		decompressedData, err := DecompressData(ctx, memAcc, descBytes)
		if err != nil {
			return backuppb.BackupPartitionDescriptor{}, errors.Wrap(
				err, "decompressing backup partition descriptor")
		}
		descBytes = decompressedData
	}

	var backupManifest backuppb.BackupPartitionDescriptor
	if err := protoutil.Unmarshal(descBytes, &backupManifest); err != nil {
		return backuppb.BackupPartitionDescriptor{}, err
	}
	return backupManifest, err
}

// readTableStatistics reads and unmarshals a StatsTable from filename in
// the provided export store, and returns its pointer.
func readTableStatistics(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (*backuppb.StatsTable, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.readTableStatistics")
	defer sp.Finish()

	r, _, err := exportStore.ReadFile(ctx, filename, cloud.ReadOptions{NoFileSize: true})
	if err != nil {
		return nil, err
	}
	defer r.Close(ctx)
	statsBytes, err := ioctx.ReadAll(ctx, r)
	if err != nil {
		return nil, err
	}
	if encryption != nil {
		encryptionKey, err := backupencryption.GetEncryptionKey(ctx, encryption, kmsEnv)
		if err != nil {
			return nil, err
		}
		statsBytes, err = storageccl.DecryptFile(ctx, statsBytes, encryptionKey, mon.NewStandaloneUnlimitedAccount())
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
	kmsEnv cloud.KMSEnv,
	backup backuppb.BackupManifest,
) ([]*stats.TableStatisticProto, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.GetStatisticsFromBackup")
	defer sp.Finish()

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
			myStatsTable, err := readTableStatistics(ctx, exportStore, fname, encryption, kmsEnv)
			if err != nil {
				return tableStatistics, err
			}
			tableStatistics = append(tableStatistics, myStatsTable.Statistics...)
		}
	}

	return tableStatistics, nil
}

// WriteBackupLock is responsible for writing a job ID suffixed
// `BACKUP-LOCK` file that will prevent concurrent backups from writing to the
// same location.
func WriteBackupLock(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	defaultURI string,
	jobID jobspb.JobID,
	user username.SQLUsername,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.WriteBackupLock")
	defer sp.Finish()

	defaultStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, defaultURI, user)
	if err != nil {
		return err
	}
	defer defaultStore.Close()

	// The lock file name consists of two parts `BACKUP-LOCK-<jobID>.
	//
	// The jobID is used in `checkForPreviousBackups` to ensure that we do not
	// read our own lock file on job resumption.
	lockFileName := fmt.Sprintf("%s%s", BackupLockFilePrefix, strconv.FormatInt(int64(jobID), 10))

	return cloud.WriteFile(ctx, defaultStore, lockFileName, bytes.NewReader([]byte("lock")))
}

// WriteMetadataWithExternalSSTs writes a "slim" version of manifest to
// `exportStore`. This version has the alloc heavy `Files`, `Descriptors`, and
// `DescriptorChanges` repeated fields nil'ed out, and written to an
// accompanying SST instead.
func WriteMetadataWithExternalSSTs(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	manifest *backuppb.BackupManifest,
) error {
	if err := WriteFilesListSST(ctx, exportStore, encryption, kmsEnv, manifest,
		BackupMetadataFilesListPath); err != nil {
		return errors.Wrap(err, "failed to write backup metadata Files SST")
	}

	if err := WriteDescsSST(ctx, manifest, exportStore, encryption, kmsEnv, BackupMetadataDescriptorsListPath); err != nil {
		return errors.Wrap(err, "failed to write backup metadata descriptors SST")
	}

	return errors.Wrap(writeExternalSSTsMetadata(ctx, exportStore, backupbase.BackupMetadataName, encryption,
		kmsEnv, manifest), "failed to write the backup metadata with external Files list")
}

// writeExternalSSTsMetadata compresses and writes a slimmer version of the
// BackupManifest `desc` to `exportStore` with the `Files`, `Descriptors`, and
// `DescriptorChanges` fields of the proto set to bogus values that will error
// out on incorrect use.
func writeExternalSSTsMetadata(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	manifest *backuppb.BackupManifest,
) error {
	slimManifest := *manifest
	// We write a bogus file entry to ensure that no call site incorrectly uses
	// the `Files` field from the FilesListMetadata proto.
	bogusFile := backuppb.BackupManifest_File{
		Span: roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
		Path: "assertion: this placeholder legacy Files entry should never be opened",
	}
	slimManifest.Files = []backuppb.BackupManifest_File{bogusFile}

	// We write a bogus descriptor to Descriptors and DescriptorChanges with max
	// timestamp as the modification time so RunPostDeserializationChanges()
	// always fails on restore.
	bogusTableID := descpb.ID(1)
	bogusTableDesc := descpb.Descriptor{
		Union: &descpb.Descriptor_Table{
			Table: &descpb.TableDescriptor{
				ID:               bogusTableID,
				Name:             "assertion: this placeholder legacy Descriptor entry should never be used",
				Version:          1,
				ModificationTime: hlc.MaxTimestamp,
			},
		},
	}
	slimManifest.Descriptors = []descpb.Descriptor{bogusTableDesc}

	bogusDescRev := backuppb.BackupManifest_DescriptorRevision{
		ID:   bogusTableID,
		Time: hlc.MaxTimestamp,
		Desc: &bogusTableDesc,
	}
	slimManifest.DescriptorChanges = []backuppb.BackupManifest_DescriptorRevision{bogusDescRev}

	slimManifest.HasExternalManifestSSTs = true
	return WriteBackupManifest(ctx, exportStore, filename, encryption, kmsEnv, &slimManifest)
}

// WriteBackupManifest compresses and writes the passed in BackupManifest `desc`
// to `exportStore`.
func WriteBackupManifest(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	filename string,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	desc *backuppb.BackupManifest,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.WriteBackupManifest")
	defer sp.Finish()

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
		encryptionKey, err := backupencryption.GetEncryptionKey(ctx, encryption, kmsEnv)
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

	if err := cloud.WriteFile(ctx, exportStore,
		filename+BackupManifestChecksumSuffix, bytes.NewReader(checksum)); err != nil {
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
	kmsEnv cloud.KMSEnv,
	desc *backuppb.BackupPartitionDescriptor,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.WriteBackupPartitionDescriptor")
	defer sp.Finish()

	descBuf, err := protoutil.Marshal(desc)
	if err != nil {
		return err
	}
	descBuf, err = compressData(descBuf)
	if err != nil {
		return errors.Wrap(err, "compressing backup partition descriptor")
	}
	if encryption != nil {
		encryptionKey, err := backupencryption.GetEncryptionKey(ctx, encryption, kmsEnv)
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
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	stats *backuppb.StatsTable,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.WriteTableStatistics")
	defer sp.Finish()

	statsBuf, err := protoutil.Marshal(stats)
	if err != nil {
		return err
	}
	if encryption != nil {
		encryptionKey, err := backupencryption.GetEncryptionKey(ctx, encryption, kmsEnv)
		if err != nil {
			return err
		}
		statsBuf, err = storageccl.EncryptFile(statsBuf, encryptionKey)
		if err != nil {
			return err
		}
	}
	return cloud.WriteFile(ctx, exportStore, BackupStatisticsFileName, bytes.NewReader(statsBuf))
}

// LoadBackupManifestsAtTime reads and returns the BackupManifests at the
// ExternalStorage locations in `uris`. Only manifests with a startTime < AsOf are returned.
//
// The caller is responsible for shrinking `mem` by the returned size once they
// are done with the returned manifests.
func LoadBackupManifestsAtTime(
	ctx context.Context,
	mem *mon.BoundAccount,
	uris []string,
	user username.SQLUsername,
	makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	asOf hlc.Timestamp,
) ([]backuppb.BackupManifest, int64, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.LoadBackupManifests")
	defer sp.Finish()

	backupManifests := make([]backuppb.BackupManifest, len(uris))
	var reserved int64
	defer func() {
		if reserved != 0 {
			mem.Shrink(ctx, reserved)
		}
	}()
	for i, uri := range uris {
		desc, memSize, err := ReadBackupManifestFromURI(ctx, mem, uri, user, makeExternalStorageFromURI,
			encryption, kmsEnv)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "failed to read backup descriptor")
		}
		if !asOf.IsEmpty() && asOf.Less(desc.StartTime) {
			break
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

// ErrLocalityDescriptor is the sentinel error that is thrown when a locality
// descriptor is not found.
var ErrLocalityDescriptor = errors.New(`Locality Descriptor not found`)

// GetLocalityInfo takes a list of stores and their URIs, along with the main
// backup manifest searches each for the locality pieces listed in the main
// manifest, returning the mapping.
func GetLocalityInfo(
	ctx context.Context,
	stores []cloud.ExternalStorage,
	uris []string,
	mainBackupManifest backuppb.BackupManifest,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	prefix string,
) (jobspb.RestoreDetails_BackupLocalityInfo, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.GetLocalityInfo")
	defer sp.Finish()

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
			if desc, err := readBackupPartitionDescriptor(
				ctx, store, filename, encryption, kmsEnv,
			); err == nil {
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
						backuputils.RedactURIForErrorMessage(uris[i]))
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

// ValidateEndTimeAndTruncate checks that the requested target time, if
// specified, is valid for the list of incremental backups resolved, truncating
// the results to the backup that contains the target time.
// The method also performs additional sanity checks to ensure the backups cover
// the requested time.
func ValidateEndTimeAndTruncate(
	defaultURIs []string,
	mainBackupManifests []backuppb.BackupManifest,
	localityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	endTime hlc.Timestamp,
	includeSkipped bool,
) ([]string, []backuppb.BackupManifest, []jobspb.RestoreDetails_BackupLocalityInfo, error) {
	if endTime.IsEmpty() {
		if includeSkipped {
			return defaultURIs, mainBackupManifests, localityInfo, nil
		}
		return ElideSkippedLayers(defaultURIs, mainBackupManifests, localityInfo)
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
		if includeSkipped {
			return defaultURIs[:i+1], mainBackupManifests[:i+1], localityInfo[:i+1], nil
		}
		return ElideSkippedLayers(defaultURIs[:i+1], mainBackupManifests[:i+1], localityInfo[:i+1])

	}

	return nil, nil, nil, errors.Errorf(
		"invalid RESTORE timestamp: supplied backups do not cover requested time",
	)
}

// ElideSkippedLayers removes backups that are skipped in the backup chain.
func ElideSkippedLayers(
	uris []string, backups []backuppb.BackupManifest, loc []jobspb.RestoreDetails_BackupLocalityInfo,
) ([]string, []backuppb.BackupManifest, []jobspb.RestoreDetails_BackupLocalityInfo, error) {
	i := len(backups) - 1
	for i > 0 {
		// Find j such that backups[j] is parent of backups[i].
		j := i - 1
		for j >= 0 && !backups[i].StartTime.Equal(backups[j].EndTime) {
			j--
		}
		// If there are backups between i and j, remove them.
		if j != i-1 {
			uris = slices.Delete(uris, j+1, i)
			backups = slices.Delete(backups, j+1, i)
			loc = slices.Delete(loc, j+1, i)
		}
		// Move up to check the chain from j now.
		i = j
	}

	return uris, backups, loc, nil
}

// GetBackupIndexAtTime returns the index of the latest backup in
// `backupManifests` with a StartTime >= asOf.
func GetBackupIndexAtTime(
	backupManifests []backuppb.BackupManifest, asOf hlc.Timestamp,
) (int, error) {
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

// LoadSQLDescsFromBackupsAtTime returns the Descriptors found in the last
// (latest) backup with a StartTime >= asOf.
//
// TODO(rui): note that materializing all descriptors doesn't scale with cluster
// size. We temporarily materialize all descriptors here to limit the scope of
// changes required to use BackupManifest with iterating repeated fields in
// restore.
func LoadSQLDescsFromBackupsAtTime(
	ctx context.Context,
	backupManifests []backuppb.BackupManifest,
	layerToBackupManifestFileIterFactory LayerToBackupManifestFileIterFactory,
	asOf hlc.Timestamp,
) ([]catalog.Descriptor, backuppb.BackupManifest, error) {
	lastBackupManifest := backupManifests[len(backupManifests)-1]
	lastIterFactory := layerToBackupManifestFileIterFactory[len(backupManifests)-1]

	if asOf.IsEmpty() {
		if lastBackupManifest.DescriptorCoverage != tree.AllDescriptors {
			descs, err := BackupManifestDescriptors(ctx, lastIterFactory, lastBackupManifest.EndTime)
			return descs, lastBackupManifest, err
		}

		// Cluster backups with revision history may have included previous database
		// versions of database descriptors in lastBackupManifest.Descriptors. Find
		// the correct set of descriptors by going through their revisions. See
		// #68541.
		asOf = lastBackupManifest.EndTime
	}

	for _, b := range backupManifests {
		if asOf.Less(b.StartTime) {
			break
		}
		lastBackupManifest = b
	}

	// From this point on we try to load descriptors based on descriptor
	// revisions. The algorithm below assumes that descriptor revisions are sorted
	// by DescChangesLess, which is a sort by descriptor ID, then descending by
	// revision time for revisions with the same ID. The external SST for
	// descriptors already have entries sorted in this order, we just have to make
	// sure the in-memory descriptors in the manifest are ordered as well.
	sort.Slice(lastBackupManifest.DescriptorChanges, func(i, j int) bool {
		return DescChangesLess(&lastBackupManifest.DescriptorChanges[i], &lastBackupManifest.DescriptorChanges[j])
	})

	descRevIt := lastIterFactory.NewDescriptorChangesIter(ctx)
	defer descRevIt.Close()
	if ok, err := descRevIt.Valid(); err != nil {
		return nil, backuppb.BackupManifest{}, err
	} else if !ok {
		descs, err := BackupManifestDescriptors(ctx, lastIterFactory, lastBackupManifest.EndTime)
		if err != nil {
			return nil, backuppb.BackupManifest{}, err
		}
		return descs, lastBackupManifest, nil
	}

	byID := make(map[descpb.ID]catalog.DescriptorBuilder, 0)
	prevRevID := descpb.InvalidID
	for ; ; descRevIt.Next() {
		if ok, err := descRevIt.Valid(); err != nil {
			return nil, backuppb.BackupManifest{}, err
		} else if !ok {
			break
		}

		rev := descRevIt.Value()
		if asOf.Less(rev.Time) {
			continue
		}

		// At this point descriptor revisions are sorted by DescChangesLess, which
		// is a sort by descriptor ID, then descending by revision time for
		// revisions with the same ID. If we've already seen a revision for this
		// descriptor ID that's not greater than asOf, then we can skip the rest of
		// the revisions for the ID.
		if rev.ID == prevRevID {
			continue
		}

		if rev.Desc != nil {
			byID[rev.ID] = newDescriptorBuilder(rev.Desc, rev.Time)
		}
		prevRevID = rev.ID
	}

	allDescs := make([]catalog.Descriptor, 0, len(byID))
	for _, b := range byID {
		if b == nil {
			continue
		}
		// A revision may have been captured before it was in a DB that is
		// backed up -- if the DB is missing, filter the object.
		if err := b.RunPostDeserializationChanges(); err != nil {
			return nil, backuppb.BackupManifest{}, err
		}
		desc := b.BuildCreatedMutable()
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
	return allDescs, lastBackupManifest, nil
}

// SanitizeLocalityKV returns a sanitized version of the input string where all
// characters that are not alphanumeric or -, =, or _ are replaced with _.
func SanitizeLocalityKV(kv string) string {
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

// CheckForBackupLock returns true if a lock file for this job already exists.
func CheckForBackupLock(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	defaultURI string,
	jobID jobspb.JobID,
	user username.SQLUsername,
) (bool, error) {
	defaultStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, defaultURI, user)
	if err != nil {
		return false, err
	}
	defer defaultStore.Close()

	// Check for the existence of a BACKUP-LOCK file written by our job
	// corresponding to `jobID`. If present, we have already laid claim on the
	// location and do not need to check further.
	lockFileName := fmt.Sprintf("%s%s", BackupLockFilePrefix, strconv.FormatInt(int64(jobID), 10))
	r, _, err := defaultStore.ReadFile(ctx, lockFileName, cloud.ReadOptions{NoFileSize: true})
	if err == nil {
		r.Close(ctx)
		return true, nil
	} else if errors.Is(err, cloud.ErrFileDoesNotExist) {
		return false, nil
	}

	return false, err
}

// CheckForPreviousBackup ensures that the target location does not already
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
func CheckForPreviousBackup(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	defaultURI string,
	jobID jobspb.JobID,
	user username.SQLUsername,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.CheckForPreviousBackup")
	defer sp.Finish()
	defaultStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, defaultURI, user)
	if err != nil {
		return err
	}
	defer defaultStore.Close()

	redactedURI := backuputils.RedactURIForErrorMessage(defaultURI)
	r, _, err := defaultStore.ReadFile(ctx, backupbase.BackupManifestName, cloud.ReadOptions{NoFileSize: true})
	if err == nil {
		r.Close(ctx)
		return pgerror.Newf(pgcode.FileAlreadyExists,
			"%s already contains a %s file",
			redactedURI, backupbase.BackupManifestName)
	}

	if !errors.Is(err, cloud.ErrFileDoesNotExist) {
		return errors.Wrapf(err,
			"%s returned an unexpected error when checking for the existence of %s file",
			redactedURI, backupbase.BackupManifestName)
	}

	// Check for the presence of a BACKUP-LOCK file with a job ID different from
	// that of our job.
	if err := defaultStore.List(ctx, "", backupbase.ListingDelimDataSlash, func(s string) error {
		s = strings.TrimPrefix(s, "/")
		if strings.HasPrefix(s, BackupLockFilePrefix) {
			jobIDSuffix := strings.TrimPrefix(s, BackupLockFilePrefix)
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
	r, err = readLatestCheckpointFile(ctx, defaultStore, BackupManifestCheckpointName)
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

// BackupManifestDescriptors returns the descriptors encoded in the manifest as
// a slice of mutable descriptors.
func BackupManifestDescriptors(
	ctx context.Context, iterFactory *IterFactory, endTime hlc.Timestamp,
) ([]catalog.Descriptor, error) {
	descIt := iterFactory.NewDescIter(ctx)
	defer descIt.Close()

	ret := make([]catalog.Descriptor, 0)
	for ; ; descIt.Next() {
		if ok, err := descIt.Valid(); err != nil {
			return nil, err
		} else if !ok {
			break
		}

		b := newDescriptorBuilder(descIt.Value(), endTime)
		if b == nil {
			continue
		}
		if err := b.RunPostDeserializationChanges(); err != nil {
			return nil, err
		}
		ret = append(ret, b.BuildCreatedMutable())
	}
	return ret, nil
}

// NewDescriptorForManifest returns a descriptor instance for a protobuf
// to be added to a backup manifest or in a backup job.
// In these cases, we know that the ModificationTime field has already correctly
// been set in the descriptor protobuf, because this descriptor has been read
// from storage and therefore has been updated using the MVCC timestamp.
func NewDescriptorForManifest(descProto *descpb.Descriptor) catalog.Descriptor {
	b := newDescriptorBuilder(descProto, hlc.Timestamp{})
	if b == nil {
		return nil
	}
	// No need to call RunPostDeserializationChanges, because the descriptor has
	// been read from storage and therefore this has been called already.
	//
	// Return a mutable descriptor because that's what the call sites assume.
	// TODO(postamar): revisit that assumption.
	return b.BuildCreatedMutable()
}

// newDescriptorBuilder constructs a catalog.DescriptorBuilder instance
// initialized using the descriptor protobuf message and a fake MVCC timestamp
// for the purpose of setting the descriptor's ModificationTime field to a
// valid value if it's still unset.
func newDescriptorBuilder(
	descProto *descpb.Descriptor, fakeMVCCTimestamp hlc.Timestamp,
) catalog.DescriptorBuilder {
	tbl, db, typ, sc, f := descpb.GetDescriptors(descProto)
	if tbl != nil {
		return tabledesc.NewBuilderWithMVCCTimestamp(tbl, fakeMVCCTimestamp)
	} else if db != nil {
		return dbdesc.NewBuilderWithMVCCTimestamp(db, fakeMVCCTimestamp)
	} else if typ != nil {
		return typedesc.NewBuilderWithMVCCTimestamp(typ, fakeMVCCTimestamp)
	} else if sc != nil {
		return schemadesc.NewBuilderWithMVCCTimestamp(sc, fakeMVCCTimestamp)
	} else if f != nil {
		return funcdesc.NewBuilderWithMVCCTimestamp(f, fakeMVCCTimestamp)
	}
	return nil
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
	kmsEnv cloud.KMSEnv,
	desc *backuppb.BackupManifest,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.WriteBackupManifestCheckpoint")
	defer sp.Finish()

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

	descBuf, err = compressData(descBuf)
	if err != nil {
		return errors.Wrap(err, "compressing backup manifest")
	}

	if encryption != nil {
		encryptionKey, err := backupencryption.GetEncryptionKey(ctx, encryption, kmsEnv)
		if err != nil {
			return err
		}
		descBuf, err = storageccl.EncryptFile(descBuf, encryptionKey)
		if err != nil {
			return err
		}
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
	if defaultStore.Conf().Provider == cloudpb.ExternalStorageProvider_http {
		// TODO (darryl): We should do this only for file not found or directory
		// does not exist errors. As of right now we only specifically wrap
		// ReadFile errors for file not found so this is not possible yet.
		if r, _, err := defaultStore.ReadFile(
			ctx,
			BackupProgressDirectory+"/"+BackupManifestCheckpointName,
			cloud.ReadOptions{NoFileSize: true},
		); err != nil {
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
		r, _, err = exportStore.ReadFile(ctx, BackupProgressDirectory+"/"+filename, cloud.ReadOptions{NoFileSize: true})
		// If we found the checkpoint in progress, then don't bother reading
		// from base, just return the reader.
		if err == nil {
			return r, nil
		}
	} else if err != nil && !errors.Is(err, cloud.ErrListingDone) {
		return nil, err
	}

	if checkpointFound {
		var name string
		if strings.HasSuffix(filename, BackupManifestChecksumSuffix) {
			name = BackupProgressDirectory + "/" + checkpoint + BackupManifestChecksumSuffix
		} else {
			name = BackupProgressDirectory + "/" + checkpoint
		}
		r, _, err = exportStore.ReadFile(ctx, name, cloud.ReadOptions{NoFileSize: true})
		return r, err
	}

	// If the checkpoint wasn't found in the progress directory, then try
	// reading from the base directory instead.
	r, _, err = exportStore.ReadFile(ctx, filename, cloud.ReadOptions{NoFileSize: true})

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

// GetBackupManifests fetches the backup manifest from a list of backup URIs.
// The caller is expected to pass in the fully hydrated encryptionParams
// required to read the manifests. The manifests are loaded from External
// Storage in parallel.
func GetBackupManifests(
	ctx context.Context,
	mem *mon.BoundAccount,
	user username.SQLUsername,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	backupURIs []string,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) ([]backuppb.BackupManifest, int64, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.GetBackupManifests")
	defer sp.Finish()

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
			desc, size, err := ReadBackupManifestFromURI(
				ctx, &subMem, uri, user, makeCloudStorage, encryption, kmsEnv,
			)
			if err != nil {
				return errors.Wrapf(err, "failed to read backup from %q",
					backuputils.RedactURIForErrorMessage(uri))
			}

			memMu.Lock()
			defer memMu.Unlock()
			err = memMu.mem.Grow(ctx, size)

			if err == nil {
				memMu.total += size
				manifests[i] = desc
			}
			subMem.Shrink(ctx, size)

			return err
		})
	}

	if err := g.Wait(); err != nil {
		mem.Shrink(ctx, memMu.total)
		return nil, 0, err
	}

	return manifests, memMu.total, nil
}

// MakeBackupCodec returns the codec that was used to encode the keys in the
// backup. We iterate over all the provided manifests and use the first
// non-empty manifest to determine the codec. If all manifests are empty we
// default to the system codec.
func MakeBackupCodec(manifests []backuppb.BackupManifest) (keys.SQLCodec, error) {
	backupCodec := keys.SystemSQLCodec
	for _, manifest := range manifests {
		if len(manifest.Spans) == 0 {
			continue
		}

		if !manifest.HasTenants() {
			// If there are no tenant targets, then the entire keyspace covered by
			// Spans must lie in 1 tenant.
			_, backupTenantID, err := keys.DecodeTenantPrefix(manifest.Spans[0].Key)
			if err != nil {
				return backupCodec, err
			}
			backupCodec = keys.MakeSQLCodec(backupTenantID)
		}
	}
	return backupCodec, nil
}

// IterFactory has provides factory methods to construct iterators that iterate
// over the `BackupManifest_Files`, `Descriptors`, and
// `BackupManifest_DescriptorRevision` in a `BackupManifest`. It is the callers
// responsibility to close the returned iterators.
type IterFactory struct {
	m                 *backuppb.BackupManifest
	store             cloud.ExternalStorage
	fileSSTPath       string
	descriptorSSTPath string
	encryption        *jobspb.BackupEncryptionOptions
	kmsEnv            cloud.KMSEnv
}

// NewIterFactory constructs a new IterFactory for a BackupManifest.
func NewIterFactory(
	m *backuppb.BackupManifest,
	store cloud.ExternalStorage,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) *IterFactory {
	return &IterFactory{
		m:                 m,
		store:             store,
		fileSSTPath:       BackupMetadataFilesListPath,
		descriptorSSTPath: BackupMetadataDescriptorsListPath,
		encryption:        encryption,
		kmsEnv:            kmsEnv,
	}
}

// LayerToBackupManifestFileIterFactory is the mapping from the idx of the
// backup layer to an IterFactory.
type LayerToBackupManifestFileIterFactory map[int]*IterFactory

// NewFileIter creates a new Iterator over BackupManifest_Files. It is assumed
// that the BackupManifest_File are sorted by FileCmp.
func (f *IterFactory) NewFileIter(
	ctx context.Context,
) (bulk.Iterator[*backuppb.BackupManifest_File], error) {
	if f.m.HasExternalManifestSSTs {
		storeFile := storageccl.StoreFile{
			Store:    f.store,
			FilePath: f.fileSSTPath,
		}
		var encOpts *kvpb.FileEncryptionOptions
		if f.encryption != nil {
			key, err := backupencryption.GetEncryptionKey(ctx, f.encryption, f.kmsEnv)
			if err != nil {
				return nil, err
			}
			encOpts = &kvpb.FileEncryptionOptions{Key: key}
		}
		return NewFileSSTIter(ctx, storeFile, encOpts)
	}

	return newSlicePointerIterator(f.m.Files), nil
}

// GetBackupManifestIterFactories constructs a mapping from the idx of the
// backup layer to an IterFactory.
func GetBackupManifestIterFactories(
	ctx context.Context,
	storeFactory cloud.ExternalStorageFactory,
	backupManifests []backuppb.BackupManifest,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (map[int]*IterFactory, error) {
	layerToFileIterFactory := make(map[int]*IterFactory)
	for layer := range backupManifests {
		es, err := storeFactory(ctx, backupManifests[layer].Dir)
		if err != nil {
			return nil, err
		}

		f := NewIterFactory(&backupManifests[layer], es, encryption, kmsEnv)
		layerToFileIterFactory[layer] = f
	}

	return layerToFileIterFactory, nil
}
