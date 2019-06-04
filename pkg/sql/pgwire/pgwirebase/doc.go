// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

/*
Package pgwirebase contains type definitions and very basic protocol structures
to be used by both the pgwire package and by others (particularly by the sql
package). The contents of this package have been extracted from the pgwire
package for the implementation of the COPY IN protocol, which lives in sql.
*/
package pgwirebase
