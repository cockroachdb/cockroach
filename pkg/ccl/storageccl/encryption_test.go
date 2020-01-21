// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestEncryptDecrypt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	plaintext := bytes.Repeat([]byte("hello world\n"), 3)
	passphrase := []byte("this is a a key")
	key, salt, err := GenerateKey(passphrase)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("IV-only file", func(t *testing.T) {
		ciphertext, err := EncryptFile(plaintext, key)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(string(ciphertext), ciphertext)
		// check that it actually did _something_
		if bytes.Contains(ciphertext, plaintext) {
			t.Fatal("expected ciphertext != plaintext")
		}
		if !AppearsEncrypted(ciphertext) {
			t.Fatal("cipher text should appear encrypted")
		}
		decrypted, err := DecryptFile(ciphertext, key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(plaintext, decrypted) {
			t.Fatal("did not roundtrip")
		}
	})

	t.Run("salt-prefixed file", func(t *testing.T) {
		ciphertext, err := EncryptSaltedFile(plaintext, key, salt)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(ciphertext)
		// check that it actually did _something_
		if bytes.Contains(ciphertext, plaintext) {
			t.Fatal("expected ciphertext != plaintext")
		}
		if !AppearsEncrypted(ciphertext) {
			t.Fatal("file doesn't appear encrypted")
		}

		decrypted, keyDerived, saltFound, err := DecryptSaltedFile(ciphertext, passphrase)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(salt, saltFound) {
			t.Fatal("salt was not preserved")
		}
		if !bytes.Equal(key, keyDerived) {
			t.Fatal("key was not preserved")
		}
		if !bytes.Equal(plaintext, decrypted) {
			t.Fatal("did not roundtrip")
		}
	})

	t.Run("salt-prefixed file using key", func(t *testing.T) {
		ciphertext, err := EncryptSaltedFile(plaintext, key, salt)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(ciphertext)
		// check that it actually did _something_
		if bytes.Contains(ciphertext, plaintext) {
			t.Fatal("expected ciphertext != plaintext")
		}
		if !AppearsEncrypted(ciphertext) {
			t.Fatal("file doesn't appear encrypted")
		}

		decrypted, err := DecryptSaltedFileUsingKey(ciphertext, key, salt)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(plaintext, decrypted) {
			t.Fatal("did not roundtrip")
		}
		t.Run("with wrong salt", func(t *testing.T) {
			notSalt := make([]byte, len(salt))
			if _, err := DecryptSaltedFileUsingKey(ciphertext, key, notSalt); !testutils.IsError(err,
				"salt found in file does not match expected salt",
			) {
				t.Fatal(err)
			}
		})
	})

	t.Run("helpful error on bad input", func(t *testing.T) {
		if _, err := DecryptFile([]byte("a"), key); !testutils.IsError(
			err, "file does not appear to be encrypted",
		) {
			t.Fatalf("expected non-ciphertext to be rejected with clear error %q", err)
		}

		if _, _, _, err := DecryptSaltedFile([]byte("a"), passphrase); !testutils.IsError(
			err, "file does not appear to be encrypted",
		) {
			t.Fatalf("expected non-ciphertext to be rejected with clear error %q", err)
		}
	})

	t.Run("helpful error when using salt-prefix decode on IV-prefix file", func(t *testing.T) {
		encrypted, err := EncryptFile(plaintext, key)
		if err != nil {
			t.Fatal(err)
		}

		if _, _, _, err := DecryptSaltedFile(encrypted, passphrase); !testutils.IsError(
			err, "unexpected encryption",
		) {
			t.Fatalf("expected non-ciphertext to be rejected with clear error %q", err)
		}
	})
}
