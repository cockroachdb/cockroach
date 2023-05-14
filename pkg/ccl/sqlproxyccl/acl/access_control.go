// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package acl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ConnectionTags contains connection properties to match against the ACLs.
type ConnectionTags struct {
	// IP corresponds to the address of the connection.
	IP string
	// TODO(jaylim-crl): We're using TenantID to identify tenant for now. Once
	// all the directory cache APIs are using TenantNames, we should modify this
	// to use that too.
	TenantID roachpb.TenantID
	// EndpointID corresponds to the identifier of the private connection, if
	// one was used. This will be an empty string if the connection is from
	// the public internet. It is assumed that the connection is from a private
	// network if the PROXY headers include cloud provider endpoint identifiers.
	EndpointID string
}

type AccessController interface {
	// CheckConnection is used to indicate whether the given connection is
	// allowed to maintain a connection with the proxy. This returns an error
	// if the connection should be blocked, or nil otherwise.
	CheckConnection(context.Context, ConnectionTags) error
}
