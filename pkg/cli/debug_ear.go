// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflagcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/spf13/cobra"
)

const (
	// These constants are defined in libroach. They should NOT be changed.
	plaintextKeyID       = "plain"
	keyRegistryFilename  = "COCKROACHDB_DATA_KEYS"
	fileRegistryFilename = "COCKROACHDB_REGISTRY"
)

type keyInfoByAge []*enginepb.KeyInfo

func (ki keyInfoByAge) Len() int           { return len(ki) }
func (ki keyInfoByAge) Swap(i, j int)      { ki[i], ki[j] = ki[j], ki[i] }
func (ki keyInfoByAge) Less(i, j int) bool { return ki[i].CreationTime < ki[j].CreationTime }

// JSONTime is a json-marshalable time.Time.
type JSONTime time.Time

// MarshalJSON marshals time.Time into json.
func (t JSONTime) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", time.Time(t).String())), nil
}

// PrettyDataKey is the final json-exportable struct for a data key.
type PrettyDataKey struct {
	ID      string
	Active  bool `json:",omitempty"`
	Exposed bool `json:",omitempty"`
	Created JSONTime
	Files   []string `json:",omitempty"`
}

// PrettyStoreKey is the final json-exportable struct for a store key.
type PrettyStoreKey struct {
	ID       string
	Active   bool `json:",omitempty"`
	Type     string
	Created  JSONTime
	Source   string
	Files    []string        `json:",omitempty"`
	DataKeys []PrettyDataKey `json:",omitempty"`
}

var encryptionStatusOpts struct {
	activeStoreIDOnly bool
}

func init() {
	encryptionStatusCmd := &cobra.Command{
		Use:   "encryption-status <directory>",
		Short: "show encryption status of a store",
		Long: `
Shows encryption status of the store located in 'directory'.
Encryption keys must be specified in the '--enterprise-encryption' flag.

Displays all store and data keys as well as files encrypted with each.
Specifying --active-store-key-id-only prints the key ID of the active store key
and exits.
`,
		Args: cobra.ExactArgs(1),
		RunE: clierrorplus.MaybeDecorateError(runEncryptionStatus),
	}

	encryptionActiveKeyCmd := &cobra.Command{
		Use:   "encryption-active-key <directory>",
		Short: "return ID of the active store key",
		Long: `
Display the algorithm and key ID of the active store key for existing data directory 'directory'.
Does not require knowing the key.

Some sample outputs:
Plaintext:            # encryption not enabled
AES128_CTR:be235...   # AES-128 encryption with store key ID
`,
		Args: cobra.ExactArgs(1),
		RunE: clierrorplus.MaybeDecorateError(runEncryptionActiveKey),
	}

	encryptionDecryptCmd := &cobra.Command{
		Use:   "encryption-decrypt <directory> <in-file> [out-file]",
		Short: "decrypt a file from an encrypted store",
		Long: `Decrypts a file from an encrypted store, and outputs it to the
specified path.

If out-file is not specified, the command will output the decrypted contents to
stdout.
`,
		Args: cobra.MinimumNArgs(2),
		RunE: clierrorplus.MaybeDecorateError(runDecrypt),
	}

	encryptionRegistryList := &cobra.Command{
		Use:   "encryption-registry-list <directory>",
		Short: "list files in the encryption-at-rest file registry",
		Long: `Prints a list of files in an Encryption At Rest file registry, along
with their env type and encryption settings (if applicable).
`,
		Args: cobra.MinimumNArgs(1),
		RunE: clierrorplus.MaybeDecorateError(runList),
	}

	// Add commands to the root debug command.
	// We can't add them to the lists of commands (eg: DebugCmdsForPebble) as cli init() is called before us.
	DebugCmd.AddCommand(encryptionStatusCmd)
	DebugCmd.AddCommand(encryptionActiveKeyCmd)
	DebugCmd.AddCommand(encryptionDecryptCmd)
	DebugCmd.AddCommand(encryptionRegistryList)

	// Add the encryption flag to commands that need it.
	// For the encryption-status command.
	f := encryptionStatusCmd.Flags()
	cliflagcfg.VarFlag(f, &encryptionSpecs, cliflags.EnterpriseEncryption)
	// And other flags.
	f.BoolVar(&encryptionStatusOpts.activeStoreIDOnly, "active-store-key-id-only", false,
		"print active store key ID and exit")
	// For the encryption-decrypt command.
	f = encryptionDecryptCmd.Flags()
	cliflagcfg.VarFlag(f, &encryptionSpecs, cliflags.EnterpriseEncryption)
	// For the encryption-registry-list command.
	f = encryptionRegistryList.Flags()
	cliflagcfg.VarFlag(f, &encryptionSpecs, cliflags.EnterpriseEncryption)

	// Add encryption flag to all OSS debug commands that want it.
	for _, cmd := range DebugCommandsRequiringEncryption {
		// encryptionSpecs is in start.go.
		cliflagcfg.VarFlag(cmd.Flags(), &encryptionSpecs, cliflags.EnterpriseEncryption)
	}

	// TODO(baptist): We don't need to do this anymore since it is in the same
	// package. This should be done the same as the other commands.
	//
	// init has already run in cli/debug.go since this package imports it, so
	// debugPebbleCmd already has all its subcommands. We could traverse those
	// here. But we don't need to by using PersistentFlags.
	cliflagcfg.VarFlag(debugPebbleCmd.PersistentFlags(),
		&encryptionSpecs, cliflags.EnterpriseEncryption)

}

// fillEncryptionOptionsForStore fills the EnvConfig fields
// based on the --enterprise-encryption flag value.
func fillEncryptionOptionsForStore(dir string, cfg *fs.EnvConfig) error {
	opts, err := storagepb.EncryptionOptionsForStore(dir, encryptionSpecs)
	if err != nil {
		return err
	}
	cfg.EncryptionOptions = opts
	return nil
}

// getActiveEncryptionkey opens the file registry directly, bypassing Pebble.
// This allows looking up the active encryption key ID without knowing it.
func getActiveEncryptionkey(dir string) (string, string, error) {
	registryFile := filepath.Join(dir, fileRegistryFilename)

	// If the data directory does not exist, we return an error.
	if _, err := os.Stat(dir); err != nil {
		return "", "", errors.Wrapf(err, "data directory %s does not exist", dir)
	}

	// Open the file registry. Return plaintext if it does not exist.
	contents, err := os.ReadFile(registryFile)
	if err != nil {
		if oserror.IsNotExist(err) {
			return enginepb.EncryptionType_Plaintext.String(), "", nil
		}
		return "", "", errors.Wrapf(err, "could not open registry file %s", registryFile)
	}

	var fileRegistry enginepb.FileRegistry
	if err := protoutil.Unmarshal(contents, &fileRegistry); err != nil {
		return "", "", err
	}

	// Find the entry for the key registry file.
	entry, ok := fileRegistry.Files[keyRegistryFilename]
	if !ok {
		return "", "", fmt.Errorf("key registry file %s was not found in the file registry", keyRegistryFilename)
	}

	if entry.EnvType == enginepb.EnvType_Plain || len(entry.EncryptionSettings) == 0 {
		// Plaintext: no encryption settings to unmarshal.
		return enginepb.EncryptionType_Plaintext.String(), "", nil
	}

	var setting enginepb.EncryptionSettings
	if err := protoutil.Unmarshal(entry.EncryptionSettings, &setting); err != nil {
		return "", "", errors.Wrapf(err, "could not unmarshal encryption settings for %s", keyRegistryFilename)
	}

	return setting.EncryptionType.String(), setting.KeyId, nil
}

func runEncryptionStatus(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	dir := args[0]

	db, err := OpenEngine(dir, stopper, fs.ReadOnly, storage.MustExist)
	if err != nil {
		return err
	}

	registries, err := db.GetEncryptionRegistries()
	if err != nil {
		return err
	}

	if len(registries.KeyRegistry) == 0 {
		return nil
	}

	var fileRegistry enginepb.FileRegistry
	if err := protoutil.Unmarshal(registries.FileRegistry, &fileRegistry); err != nil {
		return err
	}

	var keyRegistry enginepb.DataKeysRegistry
	if err := protoutil.Unmarshal(registries.KeyRegistry, &keyRegistry); err != nil {
		return err
	}

	if encryptionStatusOpts.activeStoreIDOnly {
		fmt.Println(keyRegistry.ActiveStoreKeyId)
		return nil
	}

	// Build a map of 'key ID' -> list of files
	fileKeyMap := make(map[string][]string)

	for name, entry := range fileRegistry.Files {
		keyID := plaintextKeyID

		if entry.EnvType != enginepb.EnvType_Plain && len(entry.EncryptionSettings) > 0 {
			var setting enginepb.EncryptionSettings
			if err := protoutil.Unmarshal(entry.EncryptionSettings, &setting); err != nil {
				fmt.Fprintf(os.Stderr, "could not unmarshal encryption settings for file %s: %v", name, err)
				continue
			}
			keyID = setting.KeyId
		}

		fileKeyMap[keyID] = append(fileKeyMap[keyID], name)
	}

	// Build a map of 'store key ID' -> list of child data key info
	childKeyMap := make(map[string]keyInfoByAge)

	for _, dataKey := range keyRegistry.DataKeys {
		info := dataKey.Info
		parentKey := plaintextKeyID
		if len(info.ParentKeyId) > 0 {
			parentKey = info.ParentKeyId
		}
		childKeyMap[parentKey] = append(childKeyMap[parentKey], info)
	}

	// Make a sortable slice of store key infos.
	storeKeyList := make(keyInfoByAge, 0)
	for _, ki := range keyRegistry.StoreKeys {
		storeKeyList = append(storeKeyList, ki)
	}

	storeKeys := make([]PrettyStoreKey, 0, len(storeKeyList))
	sort.Sort(storeKeyList)
	for _, storeKey := range storeKeyList {
		storeNode := PrettyStoreKey{
			ID:      storeKey.KeyId,
			Active:  (storeKey.KeyId == keyRegistry.ActiveStoreKeyId),
			Type:    storeKey.EncryptionType.String(),
			Created: JSONTime(timeutil.Unix(storeKey.CreationTime, 0)),
			Source:  storeKey.Source,
		}

		// Files encrypted by the store key. This should only be the data key registry.
		if files, ok := fileKeyMap[storeKey.KeyId]; ok {
			sort.Strings(files)
			storeNode.Files = files
			delete(fileKeyMap, storeKey.KeyId)
		}

		// Child keys.
		if children, ok := childKeyMap[storeKey.KeyId]; ok {
			storeNode.DataKeys = make([]PrettyDataKey, 0, len(children))

			sort.Sort(children)
			for _, c := range children {
				dataNode := PrettyDataKey{
					ID:      c.KeyId,
					Active:  (c.KeyId == keyRegistry.ActiveDataKeyId),
					Exposed: c.WasExposed,
					Created: JSONTime(timeutil.Unix(c.CreationTime, 0)),
				}
				files, ok := fileKeyMap[c.KeyId]
				if ok {
					sort.Strings(files)
					dataNode.Files = files
					delete(fileKeyMap, c.KeyId)
				}
				storeNode.DataKeys = append(storeNode.DataKeys, dataNode)
			}
			delete(childKeyMap, storeKey.KeyId)
		}
		storeKeys = append(storeKeys, storeNode)
	}

	j, err := json.MarshalIndent(storeKeys, "", "  ")
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", j)

	if len(fileKeyMap) > 0 {
		fmt.Fprintf(os.Stderr, "WARNING: could not find key info for some files: %+v\n", fileKeyMap)
	}
	if len(childKeyMap) > 0 {
		fmt.Fprintf(os.Stderr, "WARNING: could not find parent key info for some data keys: %+v\n", childKeyMap)
	}

	return nil
}

func runEncryptionActiveKey(cmd *cobra.Command, args []string) error {
	keyType, keyID, err := getActiveEncryptionkey(args[0])
	if err != nil {
		return err
	}

	fmt.Printf("%s:%s\n", keyType, keyID)
	return nil
}
