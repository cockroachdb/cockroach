// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package engineccl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func writeToFile(t *testing.T, vfs vfs.FS, filename string, b []byte) {
	f, err := vfs.Create(filename, fs.UnspecifiedWriteCategory)
	require.NoError(t, err)
	breader := bytes.NewReader(b)
	_, err = io.Copy(f, breader)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
}

const (
	keyFile128 = "111111111111111111111111111111111234567890123456"
	keyFile192 = "22222222222222222222222222222222123456789012345678901234"
	keyFile256 = "3333333333333333333333333333333312345678901234567890123456789012"
	key128     = "1234567890123456"
	key192     = "123456789012345678901234"
	key256     = "12345678901234567890123456789012"

	// Hex of the binary value of the first KeyIDLength of key files
	keyID128 = "3131313131313131313131313131313131313131313131313131313131313131"
	keyID192 = "3232323232323232323232323232323232323232323232323232323232323232"
	keyID256 = "3333333333333333333333333333333333333333333333333333333333333333"
)

func TestStoreKeyManagerLoadErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	memFS := vfs.NewMem()

	type KeyFiles struct {
		filename string
		contents string
	}
	keys := []KeyFiles{
		{"empty.key", ""},
		{"noid_8.key", "12345678"},
		{"noid_16.key", "1234567890123456"},
		{"noid_24.key", "123456789012345678901234"},
		{"noid_32.key", "12345678901234567890123456789012"},
		{"16.key", keyFile128},
		{"24.key", keyFile192},
		{"32.key", keyFile256},
	}
	for _, k := range keys {
		writeToFile(t, memFS, k.filename, []byte(k.contents))
	}

	type Result int
	const (
		Ok Result = iota
		Err
	)
	type TestCase struct {
		activeFile string
		oldFile    string
		result     Result
	}
	testCases := []TestCase{
		{"", "", Err},
		{"missing_new.key", "missing_old.key", Err},
		{"plain", "missing_old.key", Err},
		{"plain", "plain", Ok},
		{"empty.key", "plain", Err},
		{"noid_8.key", "plain", Err},
		{"noid_16.key", "plain", Err},
		{"noid_24.key", "plain", Err},
		{"noid_32.key", "plain", Err},
		{"16.key", "plain", Ok},
		{"24.key", "plain", Ok},
		{"32.key", "plain", Ok},
		{"16.key", "noid_8.key", Err},
		{"16.key", "32.key", Ok},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			skm := &StoreKeyManager{fs: memFS, activeKeyFilename: tc.activeFile, oldKeyFilename: tc.oldFile}
			actual := Ok
			if err := skm.Load(context.Background()); err != nil {
				actual = Err
			}
			require.Equal(t, tc.result, actual)
		})
	}
}

func TestStoreKeyManager(t *testing.T) {
	defer leaktest.AfterTest(t)()

	memFS := vfs.NewMem()

	type KeyFiles struct {
		filename string
		contents string
	}
	keys := []KeyFiles{
		{"16.key", keyFile128},
		{"24.key", keyFile192},
		{"32.key", keyFile256},
	}

	kmTimeNow = func() time.Time { return timeutil.Unix(5, 0) }

	keyPlain := &enginepbccl.SecretKey{}
	require.NoError(t, proto.UnmarshalText(
		"info {encryption_type: Plaintext, key_id: \"plain\" creation_time: 5 source: \"plain\"}",
		keyPlain))
	key16 := &enginepbccl.SecretKey{}
	require.NoError(t, proto.UnmarshalText(fmt.Sprintf(
		"info {encryption_type: AES128_CTR, key_id: \"%s\" creation_time: 5 source: \"16.key\"} key: \"%s\"",
		keyID128, key128), key16))
	key24 := &enginepbccl.SecretKey{}
	require.NoError(t, proto.UnmarshalText(fmt.Sprintf(
		"info {encryption_type: AES192_CTR, key_id: \"%s\" creation_time: 5 source: \"24.key\"} key: \"%s\"",
		keyID192, key192), key24))
	key32 := &enginepbccl.SecretKey{}
	require.NoError(t, proto.UnmarshalText(fmt.Sprintf(
		"info {encryption_type: AES256_CTR, key_id: \"%s\" creation_time: 5 source: \"32.key\"} key: \"%s\"",
		keyID256, key256), key32))

	for _, k := range keys {
		writeToFile(t, memFS, k.filename, []byte(k.contents))
	}

	{
		skm := &StoreKeyManager{fs: memFS, activeKeyFilename: "plain", oldKeyFilename: "plain"}
		require.NoError(t, skm.Load(context.Background()))
		key, err := skm.ActiveKey(context.Background())
		require.NoError(t, err)
		require.Equal(t, keyPlain.String(), key.String())
		key, err = skm.GetKey("plain")
		require.NoError(t, err)
		require.Equal(t, keyPlain.String(), key.String())
		_, err = skm.GetKey("x")
		require.Error(t, err)
	}
	{
		skm := &StoreKeyManager{fs: memFS, activeKeyFilename: "16.key", oldKeyFilename: "24.key"}
		require.NoError(t, skm.Load(context.Background()))
		key, err := skm.ActiveKey(context.Background())
		require.NoError(t, err)
		require.Equal(t, key16.String(), key.String())
		key, err = skm.GetKey(keyID128)
		require.NoError(t, err)
		require.Equal(t, key16.String(), key.String())
		key, err = skm.GetKey(keyID192)
		require.NoError(t, err)
		require.Equal(t, key24.String(), key.String())
		_, err = skm.GetKey("plain")
		require.Error(t, err)
	}
	{
		skm := &StoreKeyManager{fs: memFS, activeKeyFilename: "32.key", oldKeyFilename: "plain"}
		require.NoError(t, skm.Load(context.Background()))
		key, err := skm.ActiveKey(context.Background())
		require.NoError(t, err)
		require.Equal(t, key32.String(), key.String())
		key, err = skm.GetKey(keyID256)
		require.NoError(t, err)
		require.Equal(t, key32.String(), key.String())
		key, err = skm.GetKey("plain")
		require.NoError(t, err)
		require.Equal(t, keyPlain.String(), key.String())
	}
}

func setActiveStoreKeyInProto(dkr *enginepbccl.DataKeysRegistry, id string) {
	dkr.StoreKeys[id] = &enginepbccl.KeyInfo{
		EncryptionType: enginepbccl.EncryptionType_AES128_CTR,
		KeyId:          id,
	}
	dkr.ActiveStoreKeyId = id
}

func setActiveDataKeyInProto(dkr *enginepbccl.DataKeysRegistry, id string) {
	dkr.DataKeys[id] = &enginepbccl.SecretKey{
		Info: &enginepbccl.KeyInfo{
			EncryptionType: enginepbccl.EncryptionType_AES192_CTR,
			KeyId:          id,
			CreationTime:   kmTimeNow().Unix(),
		},
		Key: []byte("some key"),
	}
	dkr.ActiveDataKeyId = id
}

func setActiveStoreKey(dkm *DataKeyManager, id string, kind enginepbccl.EncryptionType) string {
	err := dkm.SetActiveStoreKeyInfo(context.Background(), &enginepbccl.KeyInfo{
		EncryptionType: kind,
		KeyId:          id,
	})
	if err != nil {
		return err.Error()
	}
	return ""
}

func CompareKeys(last, curr *enginepbccl.SecretKey) string {
	if (last == nil && curr == nil) || (last != nil && curr == nil) || (last == nil && curr != nil) ||
		(last.Info.KeyId == curr.Info.KeyId) {
		return "same\n"
	}
	return "different\n"
}

func TestDataKeyManager(t *testing.T) {
	defer leaktest.AfterTest(t)()

	memFS := vfs.NewMem()

	var dkm *DataKeyManager
	var keyMap map[string]*enginepbccl.SecretKey
	var lastActiveDataKey *enginepbccl.SecretKey

	var unixTime int64
	kmTimeNow = func() time.Time {
		return timeutil.Unix(unixTime, 0)
	}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "data_key_manager"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				data := strings.Split(d.Input, "\n")
				if len(data) < 2 {
					return "insufficient arguments to init"
				}
				data[0] = strings.TrimSpace(data[0])
				data[1] = strings.TrimSpace(data[1])
				period, err := strconv.Atoi(data[1])
				if err != nil {
					return err.Error()
				}
				keyMap = make(map[string]*enginepbccl.SecretKey)
				lastActiveDataKey = nil
				require.NoError(t, memFS.MkdirAll(data[0], 0755))
				dkm = &DataKeyManager{fs: memFS, dbDir: data[0], rotationPeriod: int64(period)}
				dkr := &enginepbccl.DataKeysRegistry{
					StoreKeys: make(map[string]*enginepbccl.KeyInfo),
					DataKeys:  make(map[string]*enginepbccl.SecretKey),
				}
				for i := 2; i < len(data); i++ {
					keyInfo := strings.Split(data[i], " ")
					if len(keyInfo) != 2 {
						return "insufficient parameters: " + data[i]
					}
					keyInfo[0] = strings.TrimSpace(keyInfo[0])
					keyInfo[1] = strings.TrimSpace(keyInfo[1])
					switch keyInfo[0] {
					case "active-store-key":
						setActiveStoreKeyInProto(dkr, keyInfo[1])
					case "active-data-key":
						setActiveDataKeyInProto(dkr, keyInfo[1])
					default:
						return fmt.Sprintf("unknown command: %s", keyInfo[1])
					}
				}
				if len(data) > 2 {
					b, err := protoutil.Marshal(dkr)
					if err != nil {
						return err.Error()
					}
					writeToFile(t, memFS, memFS.PathJoin(data[0], keyRegistryFilename), b)
					marker, _, err := atomicfs.LocateMarker(memFS, data[0], keysRegistryMarkerName)
					require.NoError(t, err)
					require.NoError(t, marker.Move(keyRegistryFilename))
					require.NoError(t, marker.Close())
				}
				return ""
			case "load":
				if err := dkm.Load(context.Background()); err != nil {
					return err.Error()
				}
				return ""
			case "close":
				if err := dkm.Close(); err != nil {
					return err.Error()
				}
				return ""
			case "set-active-store-key":
				var id string
				d.ScanArgs(t, "id", &id)
				return setActiveStoreKey(dkm, id, enginepbccl.EncryptionType_AES128_CTR)
			case "set-active-store-key-plain":
				var id string
				d.ScanArgs(t, "id", &id)
				return setActiveStoreKey(dkm, d.CmdArgs[0].Vals[0], enginepbccl.EncryptionType_Plaintext)
			case "check-exposed":
				var val bool
				d.ScanArgs(t, "val", &val)
				for _, key := range dkm.mu.keyRegistry.DataKeys {
					if key.Info.WasExposed != val {
						return fmt.Sprintf(
							"WasExposed: actual: %t, expected: %t\n", key.Info.WasExposed, val)
					}
				}
				return ""
			case "get-active-data-key":
				key, err := dkm.ActiveKey(context.Background())
				if err != nil {
					return err.Error()
				}
				lastActiveDataKey = key
				if key == nil {
					return "none\n"
				}
				keyInfo := &enginepbccl.KeyInfo{}
				proto.Merge(keyInfo, key.Info)
				keyInfo.KeyId = ""
				return strings.TrimSpace(keyInfo.String()) + "\n"
			case "get-active-store-key":
				id := dkm.mu.keyRegistry.ActiveStoreKeyId
				if id == "" {
					return "none\n"
				}
				return id + "\n"
			case "get-store-key":
				var id string
				d.ScanArgs(t, "id", &id)
				if dkm.mu.keyRegistry.StoreKeys != nil && dkm.mu.keyRegistry.StoreKeys[id] != nil {
					return strings.TrimSpace(dkm.mu.keyRegistry.StoreKeys[id].String()) + "\n"
				}
				return "none\n"
			case "record-active-data-key":
				key, err := dkm.ActiveKey(context.Background())
				if err != nil {
					return err.Error()
				}
				if key != nil {
					keyMap[key.Info.KeyId] = key
				}
				return ""
			case "compare-active-data-key":
				key, err := dkm.ActiveKey(context.Background())
				if err != nil {
					return err.Error()
				}
				rv := CompareKeys(lastActiveDataKey, key)
				lastActiveDataKey = key
				return rv
			case "check-all-recorded-data-keys":
				actual := fmt.Sprint(dkm.mu.keyRegistry.DataKeys)
				expected := fmt.Sprint(keyMap)
				require.Equal(t, expected, actual)
				return ""
			case "wait":
				data := strings.Split(d.Input, "\n")
				if len(data) != 1 {
					return "incorrect arguments to wait"
				}
				interval, err := strconv.Atoi(strings.TrimSpace(data[0]))
				if err != nil {
					return err.Error()
				}
				unixTime += int64(interval)
				return ""
			default:
				return fmt.Sprintf("unknown command: %s\n", d.Cmd)
			}
		})
}

func TestDataKeyManagerIO(t *testing.T) {
	defer leaktest.AfterTest(t)()

	prev := kmTimeNow
	kmTimeNow = func() time.Time { return time.Time{} }
	defer func() { kmTimeNow = prev }()

	var buf bytes.Buffer
	fs := loggingFS{FS: vfs.NewMem(), w: &buf}

	appendError := func(err error) {
		if err != nil {
			fmt.Fprintf(&buf, "error: %s\n", err)
			return
		}
		fmt.Fprintf(&buf, "OK\n")
	}

	var dkm *DataKeyManager

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "data_key_manager_io"),
		func(t *testing.T, d *datadriven.TestData) string {
			fmt.Println(d.Pos)
			buf.Reset()

			switch d.Cmd {
			case "close":
				appendError(dkm.Close())
				dkm = nil
				return buf.String()
			case "list":
				ls, err := fs.List(d.CmdArgs[0].String())
				require.NoError(t, err)
				sort.Strings(ls)
				for _, filename := range ls {
					fmt.Fprintln(&buf, filename)
				}
				return buf.String()
			case "load":
				var dir string
				d.ScanArgs(t, "dir", &dir)

				require.Nil(t, dkm)
				dkm = &DataKeyManager{fs: fs, dbDir: dir, rotationPeriod: 10}
				err := dkm.Load(context.Background())
				appendError(err)
				if err != nil {
					dkm = nil
				}
				return buf.String()
			case "mkdir-all":
				appendError(fs.MkdirAll(d.CmdArgs[0].String(), os.ModePerm))
				return buf.String()
			case "rm-all":
				appendError(fs.RemoveAll(d.CmdArgs[0].String()))
				return buf.String()
			case "set-active-store-key":
				var id string
				d.ScanArgs(t, "id", &id)
				fmt.Fprintf(&buf, "%s", setActiveStoreKey(dkm, id, enginepbccl.EncryptionType_AES128_CTR))
				return buf.String()
			default:
				return fmt.Sprintf("unknown command: %s\n", d.Cmd)
			}
		})
}

type loggingFS struct {
	vfs.FS
	w io.Writer
}

func (fs loggingFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	fmt.Fprintf(fs.w, "create(%q)\n", name)
	f, err := fs.FS.Create(name, category)
	if err != nil {
		return nil, err
	}
	return loggingFile{f, name, fs.w}, nil
}

func (fs loggingFS) Link(oldname, newname string) error {
	fmt.Fprintf(fs.w, "link(%q, %q)\n", oldname, newname)
	return fs.FS.Link(oldname, newname)
}

func (fs loggingFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	fmt.Fprintf(fs.w, "open(%q)\n", name)
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return loggingFile{f, name, fs.w}, nil
}

func (fs loggingFS) OpenDir(name string) (vfs.File, error) {
	fmt.Fprintf(fs.w, "open-dir(%q)\n", name)
	f, err := fs.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return loggingFile{f, name, fs.w}, nil
}

func (fs loggingFS) Remove(name string) error {
	fmt.Fprintf(fs.w, "remove(%q)\n", name)
	return fs.FS.Remove(name)
}

func (fs loggingFS) Rename(oldname, newname string) error {
	fmt.Fprintf(fs.w, "rename(%q, %q)\n", oldname, newname)
	return fs.FS.Rename(oldname, newname)
}

func (fs loggingFS) ReuseForWrite(
	oldname, newname string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	fmt.Fprintf(fs.w, "reuseForWrite(%q, %q)\n", oldname, newname)
	f, err := fs.FS.ReuseForWrite(oldname, newname, category)
	if err == nil {
		f = loggingFile{f, newname, fs.w}
	}
	return f, err
}

func (fs loggingFS) Stat(path string) (os.FileInfo, error) {
	fmt.Fprintf(fs.w, "stat(%q)\n", path)
	return fs.FS.Stat(path)
}

func (fs loggingFS) MkdirAll(dir string, perm os.FileMode) error {
	fmt.Fprintf(fs.w, "mkdir-all(%q, %#o)\n", dir, perm)
	return fs.FS.MkdirAll(dir, perm)
}

func (fs loggingFS) Lock(name string) (io.Closer, error) {
	fmt.Fprintf(fs.w, "lock: %q\n", name)
	return fs.FS.Lock(name)
}

type loggingFile struct {
	vfs.File
	name string
	w    io.Writer
}

func (f loggingFile) Write(p []byte) (n int, err error) {
	fmt.Fprintf(f.w, "write(%q, <...%d bytes...>)\n", f.name, len(p))
	return f.File.Write(p)
}

func (f loggingFile) Close() error {
	fmt.Fprintf(f.w, "close(%q)\n", f.name)
	return f.File.Close()
}

func (f loggingFile) Sync() error {
	fmt.Fprintf(f.w, "sync(%q)\n", f.name)
	return f.File.Sync()
}
