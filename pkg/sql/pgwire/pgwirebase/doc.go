// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package pgwirebase contains type definitions and very basic protocol structures
to be used by both the pgwire package and by others (particularly by the sql
package). The contents of this package have been extracted from the pgwire
package for the implementation of the COPY IN protocol, which lives in sql.
*/
package pgwirebase
