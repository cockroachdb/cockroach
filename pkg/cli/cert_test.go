// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

func Example_cert() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	c.RunWithCAArgs([]string{"cert", "create-client", "foo"})
	c.RunWithCAArgs([]string{"cert", "create-client", "Ομηρος"})
	c.RunWithCAArgs([]string{"cert", "create-client", "0foo"})
	c.RunWithCAArgs([]string{"cert", "create-client", "foo-1", "--tenant-scope", "1"})
	c.RunWithCAArgs([]string{"cert", "create-client", "foo-tenant2", "--tenant-name-scope", "tenant2"})
	c.RunWithCAArgs([]string{"cert", "create-client", "foo-1-tenant2", "--tenant-scope", "1", "--tenant-name-scope", "tenant2"})
	c.RunWithCAArgs([]string{"cert", "create-client", ",foo"})
	c.RunWithCAArgs([]string{"cert", "create-client", "--disable-username-validation", ",foo"})

	// Output:
	// cert create-client foo
	// cert create-client Ομηρος
	// cert create-client 0foo
	// cert create-client foo-1 --tenant-scope 1
	// cert create-client foo-tenant2 --tenant-name-scope tenant2
	// cert create-client foo-1-tenant2 --tenant-scope 1 --tenant-name-scope tenant2
	// cert create-client ,foo
	// ERROR: failed to generate client certificate and key: username is invalid
	// HINT: Usernames are case insensitive, must start with a letter, digit or underscore, may contain letters, digits, dashes, periods, or underscores, and must not exceed 63 characters.
	// cert create-client --disable-username-validation ,foo
	// warning: the specified identity ",foo" is not a valid SQL username.
	// Before it can be used to log in, an identity map rule will need to be set on the server.
}
