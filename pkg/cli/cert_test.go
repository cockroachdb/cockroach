// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

func Example_cert() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	c.RunWithCAArgs([]string{"cert", "create-client", "foo"})
	c.RunWithCAArgs([]string{"cert", "create-client", "Ομηρος"})
	c.RunWithCAArgs([]string{"cert", "create-client", "0foo"})
	c.RunWithCAArgs([]string{"cert", "create-client", ",foo"})

	// Output:
	// cert create-client foo
	// cert create-client Ομηρος
	// cert create-client 0foo
	// cert create-client ,foo
	// ERROR: failed to generate client certificate and key: username is invalid
	// HINT: Usernames are case insensitive, must start with a letter, digit or underscore, may contain letters, digits, dashes, periods, or underscores, and must not exceed 63 characters.
}
