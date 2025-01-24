// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package engineccl

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/securityccl/fipsccl"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

var testData = []byte("Call me Ishmael. Some years ago—never mind how long precisely—" +
	"having little or no money in my purse, and nothing particular to interest me " +
	"on shore, I thought I would sail about a little and see the watery part of the world.")

func generateKey(encType enginepb.EncryptionType) (*enginepb.SecretKey, error) {
	key := &enginepb.SecretKey{}
	key.Info = &enginepb.KeyInfo{}
	key.Info.EncryptionType = encType
	var keyLength int
	switch encType {
	case enginepb.EncryptionType_AES128_CTR:
		keyLength = 16
	case enginepb.EncryptionType_AES192_CTR:
		keyLength = 24
	case enginepb.EncryptionType_AES256_CTR:
		keyLength = 32
	}
	key.Key = make([]byte, keyLength)
	_, err := rand.Read(key.Key)
	return key, err
}

func readHex(s string) ([]byte, error) {
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "\n", "")
	return hex.DecodeString(s)
}

func writeHex(b []byte) string {
	var buf strings.Builder
	for i, c := range b {
		fmt.Fprintf(&buf, "%02x", c)
		if i%16 == 15 {
			buf.WriteString("\n")
		} else {
			buf.WriteString(" ")
		}
	}
	return buf.String()
}

func encryptManySubBlocks(
	t *testing.T, fcs FileStream, baseOffset int64, plaintext, ciphertext []byte,
) {
	// Split the text into many different left/right pairs, encrypt each one
	// separately, and make sure it matches the corresponding ciphertext.
	// This covers various cases such as full and partial blocks, aligned and
	// unaligned, etc.
	// Since we're only dealing with fairly small data sizes, we can iterate
	// through every possible split point and just try them all.
	for i := range plaintext {
		leftData := append([]byte{}, plaintext[0:i]...)
		fcs.Encrypt(baseOffset, leftData)
		if !bytes.Equal(leftData, ciphertext[0:i]) {
			t.Errorf("encrypting bytes 0:%d did not match full ciphertext", i)
		}
		rightData := append([]byte{}, plaintext[i:]...)
		fcs.Encrypt(baseOffset+int64(i), rightData)
		if !bytes.Equal(rightData, ciphertext[i:]) {
			t.Errorf("encrypting bytes %d:end did not match full ciphertext", i)
		}
	}
}

// Running non-fips mode:
// ./dev test pkg/ccl/storageccl/engineccl -f CTRStreamDataDriven  --rewrite --stream-output
// Running fips mode:
// ./dev test-binaries --cross=crosslinuxfips pkg/ccl/storageccl/engineccl && mkdir -p fipsbin && tar xf bin/test_binaries.tar.gz -C fipsbin && docker run -v
// $PWD/fipsbin:/fipsbin -it redhat/ubi9 bash -c 'cd /fipsbin/pkg/ccl/storageccl/engineccl/bin && ./run.sh -test.run CTRStreamDataDriven'
func TestCTRStreamDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, impl := range []string{"v1", "v2"} {
		var data []byte
		keys := map[string]*enginepb.SecretKey{}
		ivs := map[string][]byte{}
		seenCiphertexts := map[string]struct{}{}
		t.Run(impl, func(t *testing.T) {
			datadriven.RunTest(t, datapathutils.TestDataPath(t, "ctr_stream"),
				func(t *testing.T, d *datadriven.TestData) string {
					fmt.Println(d.Pos)

					switch d.Cmd {
					case "set-data":
						var err error
						data, err = readHex(d.Input)
						require.NoError(t, err)
						return "ok"

					case "create-key":
						var name string
						d.ScanArgs(t, "name", &name)
						decoded, err := readHex(d.Input)
						require.NoError(t, err)
						key := &enginepb.SecretKey{
							Info: &enginepb.KeyInfo{},
							Key:  decoded,
						}
						switch len(decoded) {
						case 16:
							key.Info.EncryptionType = enginepb.EncryptionType_AES128_CTR
						case 24:
							key.Info.EncryptionType = enginepb.EncryptionType_AES192_CTR
						case 32:
							key.Info.EncryptionType = enginepb.EncryptionType_AES256_CTR
						default:
							return fmt.Sprintf("invalid key size %d", len(decoded))
						}
						keys[name] = key
						return "ok"

					case "create-iv":
						var name string
						d.ScanArgs(t, "name", &name)
						decoded, err := readHex(d.Input)
						require.NoError(t, err)
						if len(decoded) != 16 {
							return "iv must be 16 bytes"
						}
						ivs[name] = decoded
						return "ok"

					case "encrypt":
						var offset int64
						d.ScanArgs(t, "offset", &offset)
						keyName := "default"
						d.MaybeScanArgs(t, "key", &keyName)
						ivName := "default"
						d.MaybeScanArgs(t, "iv", &ivName)
						var onlyVersion string
						d.MaybeScanArgs(t, "only-version", &onlyVersion)
						iv := ivs[ivName]
						var fcs FileStream
						if impl == "v1" {
							bcs, err := newCTRBlockCipherStream(keys[keyName], iv[:12], binary.BigEndian.Uint32(iv[12:16]))
							require.NoError(t, err)
							fcs = &fileCipherStream{bcs: bcs}
						} else {
							var err error
							fcs, err = newFileCipherStreamV2(keys[keyName].Key, iv)
							require.NoError(t, err)
						}
						// Encrypt() mutates its argument so make a copy of data.
						output := append([]byte{}, data...)
						fcs.Encrypt(offset, output)
						reencrypted := append([]byte{}, output...)
						fcs.Decrypt(offset, reencrypted)
						if !bytes.Equal(data, reencrypted) {
							t.Fatalf("decrypted data didn't match input")
						}

						outputString := string(output)
						if onlyVersion != "" && impl != onlyVersion {
							return d.Expected
						}
						_, isDuplicate := seenCiphertexts[outputString]
						if isDuplicate {
							// Assume that each test is using different parameters; if we see the same
							// ciphertext twice something's gone wrong.
							t.Fatalf("same ciphertext produced more than once")
						}
						seenCiphertexts[outputString] = struct{}{}
						encryptManySubBlocks(t, fcs, offset, data, output)
						return writeHex(output)

					default:
						return fmt.Sprintf("unknown command: %s\n", d.Cmd)
					}
				})
		})
	}
}

func TestFileCipherStream(t *testing.T) {
	defer leaktest.AfterTest(t)()

	encTypes := []enginepb.EncryptionType{enginepb.EncryptionType_AES128_CTR,
		enginepb.EncryptionType_AES192_CTR, enginepb.EncryptionType_AES256_CTR}
	for _, encType := range encTypes {
		key, err := generateKey(encType)
		require.NoError(t, err)
		var counter uint32 = 5
		nonce := make([]byte, ctrNonceSize)
		_, err = rand.Read(nonce)
		require.NoError(t, err)
		bcs, err := newCTRBlockCipherStream(key, nonce, counter)
		require.NoError(t, err)
		fcs := fileCipherStream{bcs: bcs}

		var data []byte
		data = append(data, testData...)

		// Using some arbitrary file offsets, and for each of these offsets cycle through the
		// full block size so that we have tested all partial blocks at the beginning and end
		// of a sequence.
		for _, fOffset := range []int64{5, 23, 435, 2000} {
			for i := 0; i < ctrBlockSize; i++ {
				offset := fOffset + int64(i)
				fcs.Encrypt(offset, data)
				if diff := pretty.Diff(data, testData); diff == nil {
					t.Fatal("encryption was a noop")
				}
				fcs.Decrypt(offset, data)
				if diff := pretty.Diff(data, testData); diff != nil {
					t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
				}
			}
		}
	}
}

type testKeyManager struct {
	keys     map[string]*enginepb.SecretKey
	activeID string
}

var _ PebbleKeyManager = &testKeyManager{}

func (m *testKeyManager) ActiveKeyForWriter(ctx context.Context) (*enginepb.SecretKey, error) {
	key, _ := m.GetKey(m.activeID)
	return key, nil
}

func (m *testKeyManager) ActiveKeyInfoForStats() *enginepb.KeyInfo {
	key, _ := m.GetKey(m.activeID)
	if key != nil {
		return key.Info
	}
	return nil
}

func (m *testKeyManager) GetKey(id string) (*enginepb.SecretKey, error) {
	key, found := m.keys[id]
	if !found {
		return nil, fmt.Errorf("")
	}
	return key, nil
}

func TestFileCipherStreamCreator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Key manager with a "foo" active key.
	km := testKeyManager{}
	km.activeID = "foo"
	key, err := generateKey(enginepb.EncryptionType_AES192_CTR)
	key.Info.KeyId = "foo"
	require.NoError(t, err)
	km.keys = make(map[string]*enginepb.SecretKey)
	km.keys["foo"] = key
	fcs := &FileCipherStreamCreator{envType: enginepb.EnvType_Data, keyManager: &km}

	// Existing stream that uses "foo" key.
	nonce := make([]byte, 12)
	encSettings := &enginepb.EncryptionSettings{
		EncryptionType: enginepb.EncryptionType_AES192_CTR, KeyId: "foo", Nonce: nonce}
	fs1, err := fcs.CreateExisting(encSettings)
	require.NoError(t, err)
	data := append([]byte{}, testData...)
	fs1.Encrypt(5, data)
	encData := append([]byte{}, data...) // remember the encrypted data.

	// Create another stream that uses "foo" key with the same nonce and counter (i.e., same file)
	// and decrypt and compare.
	fs2, err := fcs.CreateExisting(encSettings)
	require.NoError(t, err)
	fs2.Decrypt(5, data)
	if diff := pretty.Diff(data, testData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}

	// Encryption/decryption is noop.
	encSettings.EncryptionType = enginepb.EncryptionType_Plaintext
	fs3, err := fcs.CreateExisting(encSettings)
	require.NoError(t, err)
	fs3.Encrypt(5, data)
	if diff := pretty.Diff(data, testData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}
	fs3.Decrypt(5, data)
	if diff := pretty.Diff(data, testData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}

	// Create a new stream that uses the "foo" key. A different IV and nonce should be chosen so the
	// encrypted state will not be the same as the previous stream.
	encSettings, fs4, err := fcs.CreateNew(context.Background())
	require.Equal(t, "foo", encSettings.KeyId)
	require.Equal(t, enginepb.EncryptionType_AES192_CTR, encSettings.EncryptionType)
	require.NoError(t, err)
	fs4.Encrypt(5, data)
	if diff := pretty.Diff(data, testData); diff == nil {
		t.Fatalf("encryption was a noop")
	}
	if diff := pretty.Diff(data, encData); diff == nil {
		t.Fatalf("unexpected equality")
	}
	fs4.Decrypt(5, data)
	if diff := pretty.Diff(data, testData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}

	// Make the active key = nil, so encryption/decryption is a noop.
	km.activeID = "bar"
	encSettings, fs5, err := fcs.CreateNew(context.Background())
	require.NoError(t, err)
	require.Equal(t, "", encSettings.KeyId)
	require.Equal(t, enginepb.EncryptionType_Plaintext, encSettings.EncryptionType)
	fs5.Encrypt(5, data)
	if diff := pretty.Diff(data, testData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}
}

// Running non-fips mode:
// ./dev bench pkg/ccl/storageccl/engineccl -f FileCipherStream --stream-output --ignore-cache
// Running fips mode (be sure to look for fips=true in the output):
// ./dev test-binaries --cross=crosslinuxfips pkg/ccl/storageccl/engineccl && mkdir -p fipsbin && tar xf bin/test_binaries.tar.gz -C fipsbin && docker run -v
// $PWD/fipsbin:/fipsbin -it redhat/ubi9 /fipsbin/pkg/ccl/storageccl/engineccl/bin/engineccl_test -test.run '^$' -test.bench FileCipherStream
func BenchmarkFileCipherStream(b *testing.B) {
	isFips := fipsccl.IsFIPSReady()
	for _, impl := range []string{"v1", "v2"} {
		for _, seq := range []bool{false, true} {
			for _, keySize := range []int{128, 192, 256} {
				for _, blockSize := range []int{16, 256, 1024, 16 * 1024} {
					b.Run(fmt.Sprintf("fips=%t/impl=%s/seq=%t/key=%d/block=%d/", isFips, impl, seq, keySize, blockSize), func(b *testing.B) {
						keyBytes := make([]byte, keySize/8)
						if _, err := rand.Read(keyBytes); err != nil {
							panic(err)
						}
						var encType enginepb.EncryptionType
						switch keySize {
						case 128:
							encType = enginepb.EncryptionType_AES128_CTR
						case 192:
							encType = enginepb.EncryptionType_AES192_CTR
						case 256:
							encType = enginepb.EncryptionType_AES256_CTR
						default:
							panic("unknown key size")
						}
						key := &enginepb.SecretKey{
							Info: &enginepb.KeyInfo{
								EncryptionType: encType,
							},
							Key: keyBytes,
						}
						nonce := make([]byte, ctrNonceSize)
						if _, err := rand.Read(nonce); err != nil {
							panic(err)
						}
						initCounterBytes := make([]byte, 4)
						if _, err := rand.Read(initCounterBytes); err != nil {
							panic(err)
						}
						var stream FileStream
						if impl == "v1" {
							// Endianness doesn't matter for converting this random number to an int.
							initCounter := binary.LittleEndian.Uint32(initCounterBytes)
							blockStream, err := newCTRBlockCipherStream(key, nonce, initCounter)
							if err != nil {
								panic(err)
							}

							stream = &fileCipherStream{blockStream}
						} else {

							fullIv := append([]byte{}, nonce...)
							fullIv = append(fullIv, initCounterBytes...)
							var err error
							stream, err = newFileCipherStreamV2(key.Key, fullIv)
							require.NoError(b, err)
						}

						// Benchmarks are fun! We're just going to encrypt a bunch of zeros
						// and re-encrypt over the previous output because that doesn't matter
						// to the speed :)
						data := make([]byte, 32*1024)
						b.SetBytes(int64(len(data)))
						b.ResetTimer()

						for i := 0; i < b.N; i++ {
							for j := 0; j < len(data); j += blockSize {
								var offset int
								if seq {
									offset = j
								} else {
									offset = len(data) - j
								}
								// Add 1 to all offsets so they're not
								// block-aligned. This gives us more
								// conservative/pessimistic results compared to
								// the always-aligned case (makes a bigger
								// difference for small ops than larger ones).
								stream.Encrypt(int64(offset+1), data[0:blockSize])
							}
						}
					})
				}
			}
		}
	}
}
