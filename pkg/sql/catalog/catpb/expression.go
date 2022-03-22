// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catpb

// Expression is a SQL expression encoded as a string.
// This type exists for use as a cast type in protobufs to annotate which
// fields hold SQL expressions.
//
// TODO(ajwerner): adopt this in descpb.
type Expression string
