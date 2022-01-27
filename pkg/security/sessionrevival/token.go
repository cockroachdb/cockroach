// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessionrevival

import (
	"crypto/ed25519"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

const tokenLifetime = 10 * time.Minute

// CreateSessionRevivalToken creates a token that can be used to log in
// the given user.
func CreateSessionRevivalToken(
	cm *security.CertificateManager, user security.SQLUsername,
) ([]byte, error) {
	cert, err := cm.GetTenantSigningCert()
	if err != nil {
		return nil, err
	}
	key, err := security.PEMToPrivateKey(cert.KeyFileContents)
	if err != nil {
		return nil, err
	}

	now := timeutil.Now()
	issuedAt, err := pbtypes.TimestampProto(now)
	if err != nil {
		return nil, err
	}
	expiresAt, err := pbtypes.TimestampProto(now.Add(tokenLifetime))
	if err != nil {
		return nil, err
	}

	payload := &sessiondatapb.SessionRevivalToken_Payload{
		User:      user.Normalized(),
		Algorithm: cert.ParsedCertificates[0].PublicKeyAlgorithm.String(),
		IssuedAt:  issuedAt,
		ExpiresAt: expiresAt,
	}
	payloadBytes, err := protoutil.Marshal(payload)
	if err != nil {
		return nil, err
	}

	signature := ed25519.Sign(key.(ed25519.PrivateKey), payloadBytes)

	token := &sessiondatapb.SessionRevivalToken{
		Payload:   payloadBytes,
		Signature: signature,
	}
	tokenBytes, err := protoutil.Marshal(token)
	if err != nil {
		return nil, err
	}

	return tokenBytes, nil
}

// ValidateSessionRevivalToken checks if the given bytes are a valid
// session revival token for the user.
func ValidateSessionRevivalToken(
	cm *security.CertificateManager, user security.SQLUsername, tokenBytes []byte,
) error {
	cert, err := cm.GetTenantSigningCert()
	if err != nil {
		return err
	}

	token := &sessiondatapb.SessionRevivalToken{}
	payload := &sessiondatapb.SessionRevivalToken_Payload{}
	err = protoutil.Unmarshal(tokenBytes, token)
	if err != nil {
		return err
	}
	err = protoutil.Unmarshal(token.Payload, payload)
	if err != nil {
		return err
	}
	if err := validatePayloadContents(payload, user); err != nil {
		return err
	}
	for _, c := range cert.ParsedCertificates {
		if err := c.CheckSignature(c.SignatureAlgorithm, token.Payload, token.Signature); err == nil {
			return nil
		}
	}
	return errors.New("invalid signature")
}

func validatePayloadContents(
	payload *sessiondatapb.SessionRevivalToken_Payload, user security.SQLUsername,
) error {
	issuedAt, err := pbtypes.TimestampFromProto(payload.IssuedAt)
	if err != nil {
		return err
	}
	expiresAt, err := pbtypes.TimestampFromProto(payload.ExpiresAt)
	if err != nil {
		return err
	}

	now := timeutil.Now()
	if now.Before(issuedAt) {
		return errors.Errorf("token issue time is in the future (%v)", issuedAt)
	}
	if now.After(expiresAt) {
		return errors.Errorf("token expiration time is in the past (%v)", expiresAt)
	}
	// This check is so the token cannot be brute-forced by spoofing a very large
	// expiration date.
	if issuedAt.Add(tokenLifetime + 1*time.Minute).Before(expiresAt) {
		return errors.Errorf("token expiration time is too far in the future (%v)", expiresAt)
	}
	if user.Normalized() != payload.User {
		return errors.Errorf("token is for the wrong user %q, wanted %q", payload.User, user)
	}
	return nil
}
