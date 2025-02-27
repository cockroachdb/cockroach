// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupencryption

import (
	"bytes"
	"context"
	"crypto"
	cryptorand "crypto/rand"
	"fmt"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	// backupEncryptionInfoFile is the file name used to store the serialized
	// EncryptionInfo proto while the backup is in progress.
	backupEncryptionInfoFile = "ENCRYPTION-INFO"

	// BackupOptEncKMS is the option name in a BACKUP statement to specify a KMS
	// URI for encryption.
	BackupOptEncKMS = "kms"
	// BackupOptEncPassphrase is the option name in a BACKUP statement to specify
	// a passphrase for encryption.
	BackupOptEncPassphrase = "encryption_passphrase"
)

// ErrEncryptionInfoRead is a special error returned when the ENCRYPTION-INFO
// file is not found.
var ErrEncryptionInfoRead = errors.New(`ENCRYPTION-INFO not found`)

// BackupKMSEnv is the environment in which a backup with KMS is configured and
// used.
type BackupKMSEnv struct {
	// Settings refers to the cluster settings that apply to the BackupKMSEnv.
	Settings *cluster.Settings
	// Conf represents the ExternalIODirConfig that applies to the BackupKMSEnv.
	Conf *base.ExternalIODirConfig
	// DB is the database handle that applies to the BackupKMSEnv.
	DB isql.DB
	// Username is the use that applies to the BackupKMSEnv.
	Username username.SQLUsername
}

// MakeBackupKMSEnv returns an instance of `BackupKMSEnv` that defines the
// environment in which KMS is configured and used.
func MakeBackupKMSEnv(
	settings *cluster.Settings, conf *base.ExternalIODirConfig, db isql.DB, user username.SQLUsername,
) BackupKMSEnv {
	return BackupKMSEnv{
		Settings: settings,
		Conf:     conf,
		DB:       db,
		Username: user,
	}
}

var _ cloud.KMSEnv = &BackupKMSEnv{}

// ClusterSettings implements the cloud.KMSEnv interface.
func (p *BackupKMSEnv) ClusterSettings() *cluster.Settings {
	return p.Settings
}

// KMSConfig implements the cloud.KMSEnv interface.
func (p *BackupKMSEnv) KMSConfig() *base.ExternalIODirConfig {
	return p.Conf
}

// DBHandle implements the cloud.KMSEnv interface.
func (p *BackupKMSEnv) DBHandle() isql.DB {
	return p.DB
}

// User returns the user associated with the KMSEnv.
func (p *BackupKMSEnv) User() username.SQLUsername {
	return p.Username
}

type (
	// PlaintextMasterKeyID is the plain text version of the master key ID.
	PlaintextMasterKeyID string
	// HashedMasterKeyID is the hashed version of the master key ID.
	HashedMasterKeyID string
	// EncryptedDataKeyMap is a mapping from the hashed version of the master key
	// ID to the encrypted data key.
	EncryptedDataKeyMap struct {
		m map[HashedMasterKeyID][]byte
	}
)

// NewEncryptedDataKeyMap returns a new EncryptedDataKeyMap.
func NewEncryptedDataKeyMap() *EncryptedDataKeyMap {
	return &EncryptedDataKeyMap{make(map[HashedMasterKeyID][]byte)}
}

// NewEncryptedDataKeyMapFromProtoMap constructs an EncryptedDataKeyMap from the
// passed in protoDataKeyMap.
func NewEncryptedDataKeyMapFromProtoMap(protoDataKeyMap map[string][]byte) *EncryptedDataKeyMap {
	encMap := &EncryptedDataKeyMap{make(map[HashedMasterKeyID][]byte)}
	for k, v := range protoDataKeyMap {
		encMap.m[HashedMasterKeyID(k)] = v
	}

	return encMap
}

// AddEncryptedDataKey adds an entry to the EncryptedDataKeyMap.
func (e *EncryptedDataKeyMap) AddEncryptedDataKey(
	masterKeyID PlaintextMasterKeyID, encryptedDataKey []byte,
) {
	// Hash the master key ID before writing to the map.
	hasher := crypto.SHA256.New()
	hasher.Write([]byte(masterKeyID))
	hash := hasher.Sum(nil)
	e.m[HashedMasterKeyID(hash)] = encryptedDataKey
}

func (e *EncryptedDataKeyMap) getEncryptedDataKey(
	masterKeyID PlaintextMasterKeyID,
) ([]byte, error) {
	// Hash the master key ID before reading from the map.
	hasher := crypto.SHA256.New()
	hasher.Write([]byte(masterKeyID))
	hash := hasher.Sum(nil)
	var encDataKey []byte
	var ok bool
	if encDataKey, ok = e.m[HashedMasterKeyID(hash)]; !ok {
		return nil, errors.New("could not find an entry in the encryptedDataKeyMap")
	}

	return encDataKey, nil
}

// RangeOverMap iterates over the map and executes fn on every key-value pair.
func (e *EncryptedDataKeyMap) RangeOverMap(fn func(masterKeyID HashedMasterKeyID, dataKey []byte)) {
	for k, v := range e.m {
		fn(k, v)
	}
}

// ValidateKMSURIsAgainstFullBackup ensures that the KMS URIs provided to an
// incremental BACKUP are a subset of those used during the full BACKUP. It does
// this by ensuring that the KMS master key ID of each KMS URI specified during
// the incremental BACKUP can be found in the map written to `encryption-info`
// during a base BACKUP.
//
// The method also returns the KMSInfo to be used for all subsequent
// encryption/decryption operations during this BACKUP. By default it is the
// first KMS URI passed during the incremental BACKUP.
func ValidateKMSURIsAgainstFullBackup(
	ctx context.Context,
	kmsURIs []string,
	kmsMasterKeyIDToDataKey *EncryptedDataKeyMap,
	kmsEnv cloud.KMSEnv,
) (*jobspb.BackupEncryptionOptions_KMSInfo, error) {
	var defaultKMSInfo *jobspb.BackupEncryptionOptions_KMSInfo
	for _, kmsURI := range kmsURIs {
		kms, err := cloud.KMSFromURI(ctx, kmsURI, kmsEnv)
		if err != nil {
			return nil, err
		}

		//nolint:deferloop TODO(#137605)
		defer func() {
			_ = kms.Close()
		}()

		// Depending on the KMS specific implementation, this may or may not contact
		// the remote KMS.
		id := kms.MasterKeyID()

		encryptedDataKey, err := kmsMasterKeyIDToDataKey.getEncryptedDataKey(PlaintextMasterKeyID(id))
		if err != nil {
			return nil,
				errors.Wrap(err,
					"one of the provided URIs was not used when encrypting the base BACKUP")
		}

		if defaultKMSInfo == nil {
			defaultKMSInfo = &jobspb.BackupEncryptionOptions_KMSInfo{
				Uri:              kmsURI,
				EncryptedDataKey: encryptedDataKey,
			}
		}
	}

	return defaultKMSInfo, nil
}

// MakeNewEncryptionOptions returns a new jobspb.BackupEncryptionOptions based
// on the passed in encryption parameters.
func MakeNewEncryptionOptions(
	ctx context.Context, encryptionParams *jobspb.BackupEncryptionOptions, kmsEnv cloud.KMSEnv,
) (*jobspb.BackupEncryptionOptions, *jobspb.EncryptionInfo, error) {
	if encryptionParams == nil || encryptionParams.Mode == jobspb.EncryptionMode_None {
		return nil, nil, nil
	}
	var encryptionOptions *jobspb.BackupEncryptionOptions
	var encryptionInfo *jobspb.EncryptionInfo
	switch encryptionParams.Mode {
	case jobspb.EncryptionMode_Passphrase:
		salt, err := storageccl.GenerateSalt()
		if err != nil {
			return nil, nil, err
		}

		encryptionInfo = &jobspb.EncryptionInfo{Salt: salt}
		encryptionOptions = &jobspb.BackupEncryptionOptions{
			Mode: jobspb.EncryptionMode_Passphrase,
			Key:  storageccl.GenerateKey([]byte(encryptionParams.RawPassphrase), salt),
		}
	case jobspb.EncryptionMode_KMS:
		// Generate a 32 byte/256-bit crypto-random number which will serve as
		// the data key for encrypting the BACKUP data and manifest files.
		plaintextDataKey := make([]byte, 32)
		_, err := cryptorand.Read(plaintextDataKey)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to generate DataKey")
		}

		encryptedDataKeyByKMSMasterKeyID, defaultKMSInfo, err :=
			GetEncryptedDataKeyByKMSMasterKeyID(ctx, encryptionParams.RawKmsUris, plaintextDataKey, kmsEnv)
		if err != nil {
			return nil, nil, err
		}

		encryptedDataKeyMapForProto := make(map[string][]byte)
		encryptedDataKeyByKMSMasterKeyID.RangeOverMap(
			func(masterKeyID HashedMasterKeyID, dataKey []byte) {
				encryptedDataKeyMapForProto[string(masterKeyID)] = dataKey
			})

		encryptionInfo = &jobspb.EncryptionInfo{EncryptedDataKeyByKMSMasterKeyID: encryptedDataKeyMapForProto}
		encryptionOptions = &jobspb.BackupEncryptionOptions{
			Mode:    jobspb.EncryptionMode_KMS,
			KMSInfo: defaultKMSInfo,
		}
	}
	return encryptionOptions, encryptionInfo, nil
}

// GetEncryptedDataKeyFromURI returns the encrypted data key from the KMS
// specified by kmsURI.
func GetEncryptedDataKeyFromURI(
	ctx context.Context, plaintextDataKey []byte, kmsURI string, kmsEnv cloud.KMSEnv,
) (string, []byte, error) {
	kms, err := cloud.KMSFromURI(ctx, kmsURI, kmsEnv)
	if err != nil {
		return "", nil, err
	}

	defer func() {
		_ = kms.Close()
	}()

	kmsURL, err := url.ParseRequestURI(kmsURI)
	if err != nil {
		return "", nil, errors.Wrap(err, "cannot parse KMSURI")
	}
	encryptedDataKey, err := kms.Encrypt(ctx, plaintextDataKey)
	if err != nil {
		return "", nil, cloud.KMSInaccessible(errors.Wrapf(err, "failed to encrypt data key for KMS scheme %s",
			kmsURL.Scheme))
	}

	return kms.MasterKeyID(), encryptedDataKey, nil
}

// GetEncryptedDataKeyByKMSMasterKeyID constructs a mapping {MasterKeyID :
// EncryptedDataKey} for each KMS URI provided during a full BACKUP. The
// MasterKeyID is hashed before writing it to the map.
//
// The method also returns the KMSInfo to be used for all subsequent
// encryption/decryption operations during this BACKUP. By default it is the
// first KMS URI.
func GetEncryptedDataKeyByKMSMasterKeyID(
	ctx context.Context, kmsURIs []string, plaintextDataKey []byte, kmsEnv cloud.KMSEnv,
) (*EncryptedDataKeyMap, *jobspb.BackupEncryptionOptions_KMSInfo, error) {
	encryptedDataKeyByKMSMasterKeyID := NewEncryptedDataKeyMap()
	// The coordinator node contacts every KMS and records the encrypted data
	// key for each one.
	var kmsInfo *jobspb.BackupEncryptionOptions_KMSInfo
	for _, kmsURI := range kmsURIs {
		masterKeyID, encryptedDataKey, err := GetEncryptedDataKeyFromURI(ctx,
			plaintextDataKey, kmsURI, kmsEnv)
		if err != nil {
			return nil, nil, err
		}

		// By default we use the first KMS URI and encrypted data key for subsequent
		// encryption/decryption operation during a BACKUP.
		if kmsInfo == nil {
			kmsInfo = &jobspb.BackupEncryptionOptions_KMSInfo{
				Uri:              kmsURI,
				EncryptedDataKey: encryptedDataKey,
			}
		}

		encryptedDataKeyByKMSMasterKeyID.AddEncryptedDataKey(PlaintextMasterKeyID(masterKeyID),
			encryptedDataKey)
	}

	return encryptedDataKeyByKMSMasterKeyID, kmsInfo, nil
}

// WriteNewEncryptionInfoToBackup writes a versioned ENCRYPTION-INFO file to
// external storage.
func WriteNewEncryptionInfoToBackup(
	ctx context.Context, opts *jobspb.EncryptionInfo, dest cloud.ExternalStorage, numFiles int,
) error {
	// New encryption-info file name is in the format "ENCRYPTION-INFO-<version number>"
	newEncryptionInfoFile := fmt.Sprintf("%s-%d", backupEncryptionInfoFile, numFiles+1)

	buf, err := protoutil.Marshal(opts)
	if err != nil {
		return err
	}
	return cloud.WriteFile(ctx, dest, newEncryptionInfoFile, bytes.NewReader(buf))
}

// GetEncryptionFromBase retrieves the encryption options of the base backup. It
// is expected that incremental backups use the same encryption options as the
// base backups.
func GetEncryptionFromBase(
	ctx context.Context,
	user username.SQLUsername,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	baseBackupURI string,
	encryptionParams *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (*jobspb.BackupEncryptionOptions, error) {
	if encryptionParams == nil || encryptionParams.Mode == jobspb.EncryptionMode_None {
		return nil, nil
	}
	exportStore, err := makeCloudStorage(ctx, baseBackupURI, user)
	if err != nil {
		return nil, err
	}
	defer exportStore.Close()
	return GetEncryptionFromBaseStore(ctx, exportStore, encryptionParams, kmsEnv)
}

// GetEncryptionFromBaseStore retrieves the encryption options of a base backup store.
func GetEncryptionFromBaseStore(
	ctx context.Context,
	baseStore cloud.ExternalStorage,
	encryptionParams *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (*jobspb.BackupEncryptionOptions, error) {
	if encryptionParams == nil || encryptionParams.Mode == jobspb.EncryptionMode_None {
		return nil, nil
	}
	opts, err := ReadEncryptionOptions(ctx, baseStore)
	if err != nil {
		return nil, err
	}
	var encryptionOptions *jobspb.BackupEncryptionOptions
	switch encryptionParams.Mode {
	case jobspb.EncryptionMode_Passphrase:
		encryptionOptions = &jobspb.BackupEncryptionOptions{
			Mode: jobspb.EncryptionMode_Passphrase,
			Key:  storageccl.GenerateKey([]byte(encryptionParams.RawPassphrase), opts[0].Salt),
		}
	case jobspb.EncryptionMode_KMS:
		var defaultKMSInfo *jobspb.BackupEncryptionOptions_KMSInfo
		for _, encFile := range opts {
			defaultKMSInfo, err = ValidateKMSURIsAgainstFullBackup(ctx, encryptionParams.RawKmsUris,
				NewEncryptedDataKeyMapFromProtoMap(encFile.EncryptedDataKeyByKMSMasterKeyID), kmsEnv)
			if err == nil {
				break
			}
		}
		if err != nil {
			return nil, err
		}
		encryptionOptions = &jobspb.BackupEncryptionOptions{
			Mode:    jobspb.EncryptionMode_KMS,
			KMSInfo: defaultKMSInfo,
		}
	}
	return encryptionOptions, nil
}

// GetEncryptionKey returns the decrypted plaintext data key to be used for
// encryption.
func GetEncryptionKey(
	ctx context.Context, encryption *jobspb.BackupEncryptionOptions, kmsEnv cloud.KMSEnv,
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
		kms, err := cloud.KMSFromURI(ctx, encryption.KMSInfo.Uri, kmsEnv)
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

// ReadEncryptionOptions takes in a backup location and tries to find
// and return all encryption option files in the backup. A backup
// normally only creates one encryption option file, but if the user
// uses ALTER BACKUP to add new keys, a new encryption option file
// will be placed side by side with the old one. Since the old file
// is still valid, as we never want to modify or delete an existing
// backup, we return both new and old files.
func ReadEncryptionOptions(
	ctx context.Context, src cloud.ExternalStorage,
) ([]jobspb.EncryptionInfo, error) {
	const encryptionReadErrorMsg = `could not find or read encryption information`

	files, err := GetEncryptionInfoFiles(ctx, src)
	if err != nil {
		return nil, errors.Mark(errors.Wrap(err, encryptionReadErrorMsg), ErrEncryptionInfoRead)
	}
	var encInfo []jobspb.EncryptionInfo
	// The user is more likely to pass in a KMS URI that was used to
	// encrypt the backup recently, so we iterate the ENCRYPTION-INFO
	// files from latest to oldest.
	for i := len(files) - 1; i >= 0; i-- {
		r, _, err := src.ReadFile(ctx, files[i], cloud.ReadOptions{NoFileSize: true})
		if err != nil {
			return nil, errors.Wrap(err, encryptionReadErrorMsg)
		}
		//nolint:deferloop TODO(#137605)
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

// GetEncryptionInfoFiles reads the ENCRYPTION-INFO files from external storage.
func GetEncryptionInfoFiles(ctx context.Context, dest cloud.ExternalStorage) ([]string, error) {
	var files []string
	// Look for all files in dest that start with "/ENCRYPTION-INFO"
	// and return them.
	err := dest.List(ctx, "", backupbase.ListingDelimDataSlash, func(p string) error {
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

// WriteEncryptionInfoIfNotExists writes EncryptionInfo to external storage.
func WriteEncryptionInfoIfNotExists(
	ctx context.Context, opts *jobspb.EncryptionInfo, dest cloud.ExternalStorage,
) error {
	r, _, err := dest.ReadFile(ctx, backupEncryptionInfoFile, cloud.ReadOptions{NoFileSize: true})
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
