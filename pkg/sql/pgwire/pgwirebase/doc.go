// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package pgwirebase contains type definitions and very basic protocol structures
to be used by both the pgwire package and by others (particularly by the sql
package). The contents of this package have been extracted from the pgwire
package for the implementation of the COPY IN protocol, which lives in sql.
*/
package pgwirebase
