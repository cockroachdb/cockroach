// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// JoinTokenVersion is used to version the encoding of join tokens, and is the
// first byte in the marshaled token.
type JoinTokenVersion byte

const (
	// V0 is the initial version of join token encoding.
	// The format of the text is:
	// <version:1>base64(<TokenID:uuid.Size><SharedSecret:joinTokenSecretLen><fingerprint:variable><crc:4>)
	V0 JoinTokenVersion = '0'
)

// joinTokenVersionError is used when unsupported version is found while
// (un)marshaling join tokens.
type joinTokenVersionError struct {
	v byte
}

func (e *joinTokenVersionError) Error() string {
	return fmt.Sprintf("unsupported join token version: %v", e.v)
}

const (
	// Length of the join token shared secret.
	joinTokenSecretLen = 16

	// JoinTokenExpiration is the default expiration time of newly created join
	// tokens.
	JoinTokenExpiration = 30 * time.Minute
)

// JoinToken is a container for a TokenID and associated SharedSecret for use
// in certificate-free add/join operations.
type JoinToken struct {
	TokenID      uuid.UUID
	SharedSecret []byte
	fingerprint  []byte
	version      JoinTokenVersion
}

// GenerateJoinToken generates a new join token, and signs it with the CA cert
// in the certificate manager.
func GenerateJoinToken(cm *CertificateManager) (JoinToken, error) {
	var jt JoinToken

	jt.version = V0
	jt.TokenID = uuid.MakeV4()
	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	jt.SharedSecret = randutil.RandBytes(r, joinTokenSecretLen)
	jt.sign(cm.CACert().FileContents)
	return jt, nil
}

// sign signs the provided CA cert using the shared secret, and sets the
// fingerprint field on the join token to the HMAC signature.
func (j *JoinToken) sign(caCert []byte) {
	signer := hmac.New(sha256.New, j.SharedSecret)
	_, _ = signer.Write(caCert)
	j.fingerprint = signer.Sum(nil)
}

// VerifySignature verifies that the fingerprint provided in the join token
// matches the signature of the provided CA cert with the join token's shared
// secret.
func (j *JoinToken) VerifySignature(caCert []byte) bool {
	signer := hmac.New(sha256.New, j.SharedSecret)
	_, _ = signer.Write(caCert)
	return hmac.Equal(signer.Sum(nil), j.fingerprint)
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (j *JoinToken) UnmarshalText(text []byte) error {
	// First byte will be the join token version.
	switch v := JoinTokenVersion(text[0]); v {
	case V0:
		decoder := base64.NewDecoder(base64.URLEncoding, bytes.NewReader(text[1:]))
		decoded, err := ioutil.ReadAll(decoder)
		if err != nil {
			return err
		}
		if len(decoded) <= uuid.Size+joinTokenSecretLen+4 {
			return errors.New("invalid join token")
		}
		expectedCSum := crc32.ChecksumIEEE(decoded[:len(decoded)-4])
		_, cSum, err := encoding.DecodeUint32Ascending(decoded[len(decoded)-4:])
		if err != nil {
			return err
		}
		if cSum != expectedCSum {
			return errors.New("invalid join token")
		}
		if err := j.TokenID.UnmarshalBinary(decoded[:uuid.Size]); err != nil {
			return err
		}
		decoded = decoded[uuid.Size:]
		j.SharedSecret = decoded[:joinTokenSecretLen]
		j.fingerprint = decoded[joinTokenSecretLen : len(decoded)-4]
		j.version = V0
	default:
		return &joinTokenVersionError{byte(v)}
	}

	return nil
}

// MarshalText implements the encoding.TextMarshaler interface.
func (j *JoinToken) MarshalText() ([]byte, error) {
	tokenID, err := j.TokenID.MarshalBinary()
	if err != nil {
		return nil, err
	}

	if len(j.SharedSecret) != joinTokenSecretLen {
		return nil, errors.New("join token shared secret not of the right size")
	}

	var b bytes.Buffer
	switch j.version {
	case V0:
		token := make([]byte, 0, len(tokenID)+len(j.SharedSecret)+len(j.fingerprint)+4)
		token = append(token, tokenID...)
		token = append(token, j.SharedSecret...)
		token = append(token, j.fingerprint...)
		// Checksum.
		cSum := crc32.ChecksumIEEE(token)
		token = encoding.EncodeUint32Ascending(token, cSum)
		encoder := base64.NewEncoder(base64.URLEncoding, &b)
		if _, err := encoder.Write(token); err != nil {
			return nil, err
		}
		if err := encoder.Close(); err != nil {
			return nil, err
		}
	default:
		return nil, &joinTokenVersionError{byte(j.version)}
	}

	// Prefix the encoded token with version number.
	version := []byte{byte(j.version)}
	return append(version, b.Bytes()...), nil
}
