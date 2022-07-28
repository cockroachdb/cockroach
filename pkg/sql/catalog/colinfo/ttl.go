// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colinfo

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"

// TTLDefaultExpirationColumnName is the column name representing the expiration
// column for TTL.
const TTLDefaultExpirationColumnName = "crdb_internal_expiration"

// DefaultTTLExpirationExpr is default TTL expression when
// ttl_expiration_expression is not specified
var DefaultTTLExpirationExpr = catpb.Expression(TTLDefaultExpirationColumnName)
