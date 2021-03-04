// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"hash/crc32"
	"io/ioutil"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	// Length of the join token shared secret.
	joinTokenSecretLen = 16

	// Default TTL for join tokens.
	joinTokenDefaultTTL = 10 * time.Minute
)

// JoinToken is a container for a TokenID and associated SharedSecret for use
// in certificate-free add/join operations.
type JoinToken struct {
	TokenID      uuid.UUID
	SharedSecret []byte
	Fingerprint  []byte
}

// Sign signs the provided CA cert using the shared secret, and sets the
// Fingerprint field on the join token to the HMAC signature.
func (j *JoinToken) Sign(caCert []byte) {
	signer := hmac.New(sha256.New, j.SharedSecret)
	_, _ = signer.Write(caCert)
	j.Fingerprint = signer.Sum(nil)
}

// VerifySignature verifies that the fingerprint provided in the join token
// matches the signature of the provided CA cert with the join token's shared
// secret.
func (j *JoinToken) VerifySignature(caCert []byte) bool {
	signer := hmac.New(sha256.New, j.SharedSecret)
	_, _ = signer.Write(caCert)
	return bytes.Equal(signer.Sum(nil), j.Fingerprint)
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (j *JoinToken) UnmarshalText(text []byte) error {
	decoder := base64.NewDecoder(base64.URLEncoding, bytes.NewReader(text))
	decoded, err := ioutil.ReadAll(decoder)
	if err != nil {
		return err
	}
	if len(decoded) <= uuid.Size + joinTokenSecretLen + 4 {
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
	j.Fingerprint = decoded[joinTokenSecretLen:len(decoded)-4]
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
	token := make([]byte, 0, len(tokenID)+len(j.SharedSecret)+len(j.Fingerprint)+4)
	token = append(token, tokenID...)
	token = append(token, j.SharedSecret...)
	token = append(token, j.Fingerprint...)
	// Checksum.
	cSum := crc32.ChecksumIEEE(token)
	token = encoding.EncodeUint32Ascending(token, cSum)

	var b bytes.Buffer
	encoder := base64.NewEncoder(base64.URLEncoding, &b)
	if _, err := encoder.Write(token); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// Checks the join token in the gossip store matches the marshalled form of
// the passed-in join token.
func (j *JoinToken) isValid(g *gossip.Gossip) (bool, error) {
	token, err := g.GetInfo(gossip.MakeJoinTokenKey(j.TokenID))
	if err != nil {
		return false, err
	}
	token2, err := j.MarshalText()
	if err != nil {
		return false, err
	}
	return bytes.Equal(token, token2), nil
}
