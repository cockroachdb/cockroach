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
	pbtypes "github.com/gogo/protobuf/types"
)

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
	expiresAt, err := pbtypes.TimestampProto(now.Add(10 * time.Minute))
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
) (bool, error) {
	cert, err := cm.GetTenantSigningCert()
	if err != nil {
		return false, err
	}

	// All errors below are problems with the token, so "invalid token" is
	// returned rather than an error.
	token := &sessiondatapb.SessionRevivalToken{}
	payload := &sessiondatapb.SessionRevivalToken_Payload{}
	err = protoutil.Unmarshal(tokenBytes, token)
	if err != nil {
		return false, nil
	}
	err = protoutil.Unmarshal(token.Payload, payload)
	if err != nil {
		return false, nil
	}
	issuedAt, err := pbtypes.TimestampFromProto(payload.IssuedAt)
	if err != nil {
		return false, nil
	}
	expiresAt, err := pbtypes.TimestampFromProto(payload.ExpiresAt)
	if err != nil {
		return false, nil
	}

	now := timeutil.Now()
	if now.Before(issuedAt) {
		return false, nil
	}
	if now.After(expiresAt) {
		return false, nil
	}
	if user.Normalized() != payload.User {
		return false, nil
	}
	for _, c := range cert.ParsedCertificates {
		if err := c.CheckSignature(c.SignatureAlgorithm, token.Payload, token.Signature); err == nil {
			return true, nil
		}
	}
	return false, nil
}
