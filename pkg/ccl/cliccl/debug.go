// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/baseccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/cliccl/cliflagsccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/cobra"
)

// Defines CCL-specific debug commands, adds the encryption flag to debug commands in
// `pkg/cli/debug.go`, and registers a callback to generate encryption options.

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
		RunE: cli.MaybeDecorateGRPCError(runEncryptionStatus),
	}

	// Add it to the root debug command. We can't add it to the lists of commands (eg: DebugCmdsForRocksDB)
	// as cli init() is called before us.
	cli.DebugCmd.AddCommand(encryptionStatusCmd)

	// Add the encryption flag.
	f := encryptionStatusCmd.Flags()
	cli.VarFlag(f, &storeEncryptionSpecs, cliflagsccl.EnterpriseEncryption)
	// And other flags.
	f.BoolVar(&encryptionStatusOpts.activeStoreIDOnly, "active-store-key-id-only", false,
		"print active store key ID and exit")

	// Add encryption flag to all OSS debug commands that want it.
	for _, cmd := range cli.DebugCmdsForRocksDB {
		// storeEncryptionSpecs is in start.go.
		cli.VarFlag(cmd.Flags(), &storeEncryptionSpecs, cliflagsccl.EnterpriseEncryption)
	}

	cli.PopulateRocksDBConfigHook = fillEncryptionOptionsForStore
}

// fillEncryptionOptionsForStore fills the RocksDBConfig fields
// based on the --enterprise-encryption flag value.
func fillEncryptionOptionsForStore(cfg *engine.RocksDBConfig) error {
	opts, err := baseccl.EncryptionOptionsForStore(cfg.Dir, storeEncryptionSpecs)
	if err != nil {
		return err
	}

	if opts != nil {
		cfg.ExtraOptions = opts
		cfg.UseFileRegistry = true
	}
	return nil
}

type keyInfoByAge []*enginepbccl.KeyInfo

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

func runEncryptionStatus(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	dir := args[0]

	db, err := cli.OpenExistingStore(dir, stopper, true /* readOnly */)
	if err != nil {
		return err
	}

	registries, err := db.GetEncryptionRegistries()
	if err != nil {
		return err
	}

	if len(registries.FileRegistry) == 0 || len(registries.KeyRegistry) == 0 {
		return nil
	}

	var fileRegistry enginepb.FileRegistry
	if err := protoutil.Unmarshal(registries.FileRegistry, &fileRegistry); err != nil {
		return err
	}

	var keyRegistry enginepbccl.DataKeysRegistry
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
		keyID := "plain"

		if entry.EnvType != enginepb.EnvType_Plaintext && len(entry.EncryptionSettings) > 0 {
			var setting enginepbccl.EncryptionSettings
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
		parentKey := "plain"
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
