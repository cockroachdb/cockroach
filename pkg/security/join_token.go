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

// joinTokenVersion is used to version the encoding of join tokens, and is the
// first byte in the marshaled token.
type joinTokenVersion byte

const (
	// V0 is the initial version of join token encoding.
	joinTokenV0 joinTokenVersion = '0'
)

var errInvalidJoinToken = errors.New("invalid join token")

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
}

// GenerateJoinToken generates a new join token, and signs it with the CA cert
// in the certificate manager.
func GenerateJoinToken(cm *CertificateManager) (JoinToken, error) {
	var jt JoinToken

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
	switch v := joinTokenVersion(text[0]); v {
	case joinTokenV0:
		decoder := base64.NewDecoder(base64.URLEncoding, bytes.NewReader(text[1:]))
		decoded, err := ioutil.ReadAll(decoder)
		if err != nil {
			return err
		}
		if len(decoded) <= uuid.Size+joinTokenSecretLen+4 {
			return errInvalidJoinToken
		}
		expectedCSum := crc32.ChecksumIEEE(decoded[:len(decoded)-4])
		_, cSum, err := encoding.DecodeUint32Ascending(decoded[len(decoded)-4:])
		if err != nil {
			return err
		}
		if cSum != expectedCSum {
			return errInvalidJoinToken
		}
		if err := j.TokenID.UnmarshalBinary(decoded[:uuid.Size]); err != nil {
			return err
		}
		decoded = decoded[uuid.Size:]
		j.SharedSecret = decoded[:joinTokenSecretLen]
		j.fingerprint = decoded[joinTokenSecretLen : len(decoded)-4]
	default:
		return errInvalidJoinToken
	}

	return nil
}

// MarshalText implements the encoding.TextMarshaler interface.
//
// The format of the text is:
// <version:1>base64(<TokenID:uuid.Size><SharedSecret:joinTokenSecretLen><fingerprint:variable><crc:4>)
func (j *JoinToken) MarshalText() ([]byte, error) {
	tokenID, err := j.TokenID.MarshalBinary()
	if err != nil {
		return nil, err
	}

	if len(j.SharedSecret) != joinTokenSecretLen {
		return nil, errors.New("join token shared secret not of the right size")
	}

	var b bytes.Buffer
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

	// Prefix the encoded token with version number.
	versionedToken := make([]byte, 0, len(b.Bytes())+1)
	versionedToken = append(versionedToken, byte(joinTokenV0))
	versionedToken = append(versionedToken, b.Bytes()...)
	return versionedToken, nil
}
