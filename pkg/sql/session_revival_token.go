// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"crypto/ed25519"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	pbtypes "github.com/gogo/protobuf/types"
)

func (p *planner) CreateSessionRevivalToken() (*tree.DBytes, error) {
	if !p.ExecCfg().AllowSessionRevival {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "session revival tokens are not supported on this cluster")
	}
	user := p.SessionData().User()
	if user.IsRootUser() {
		return nil, pgerror.New(pgcode.InsufficientPrivilege, "cannot create token for root user")
	}
	cm, err := p.ExecCfg().RPCContext.SecurityContext.GetCertificateManager()
	if err != nil {
		return nil, err
	}
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
	return tree.NewDBytes(tree.DBytes(tokenBytes)), nil
}
