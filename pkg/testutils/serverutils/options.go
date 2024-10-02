// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverutils

import (
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/security/username"
)

// SQLConnOptions contains options for opening a SQL connection.
type SQLConnOptions struct {
	DBName         string
	User           *url.Userinfo
	ClientCerts    bool
	CertsDirPrefix string
}

// SQLConnOption is an option for opening a SQL connection.
type SQLConnOption func(result *SQLConnOptions)

// DefaultSQLConnOptions returns the default options for opening a SQL connection.
func DefaultSQLConnOptions() *SQLConnOptions {
	return &SQLConnOptions{
		DBName:         "",
		User:           url.User(username.RootUser),
		ClientCerts:    true,
		CertsDirPrefix: "openTestSQLConn",
	}
}

// DBName sets the database name to use when connecting to the server.
// Use catalogkeys.DefaultDatabaseName or catconstants.SystemDatabaseName when
// in doubt. An empty string results in DefaultDatabaseName being used.
func DBName(dbName string) SQLConnOption {
	return func(result *SQLConnOptions) {
		result.DBName = dbName
	}
}

// User sets the username to use when connecting to the server.
func User(username string) SQLConnOption {
	return func(result *SQLConnOptions) {
		result.User = url.User(username)
	}
}

// UserPassword sets the username and password to use when connecting to the
// server.
func UserPassword(username, password string) SQLConnOption {
	return func(result *SQLConnOptions) {
		result.User = url.UserPassword(username, password)
	}
}

// ClientCerts set whether the client certificates are loaded on-disk and in the
// connection URL.
func ClientCerts(clientCerts bool) SQLConnOption {
	return func(result *SQLConnOptions) {
		result.ClientCerts = clientCerts
	}
}

// CertsDirPrefix sets the prefix for the directory where the client certificates
// are stored.
func CertsDirPrefix(certsDirPrefix string) SQLConnOption {
	return func(result *SQLConnOptions) {
		result.CertsDirPrefix = certsDirPrefix
	}
}
