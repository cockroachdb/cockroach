// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package option

import (
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

type applyFunc func(any) any

func overwrite(v any) applyFunc {
	return func(_ any) any {
		return v
	}
}

// CustomOption encodes an option provided by the user to a roachtest
// cluster function. These options allow callers to customize certain
// parameters, typically using the "functional options" pattern.
type CustomOption struct {
	name  string
	apply applyFunc
}

// User allows the customization of the user to use when connecting to
// crdb.
func User(user string) CustomOption {
	return CustomOption{name: "User", apply: overwrite(user)}
}

// VirtualClusterName allows the customization of the virtual cluster
// to connect to. If not provided, it will default to the cluster's
// default virtual cluster (or `system` if that's not set.)
func VirtualClusterName(name string) CustomOption {
	return CustomOption{name: "VirtualClusterName", apply: overwrite(name)}
}

// SQLInstance allows the caller to indicate which sql instance to
// use. Only applicable in separate-process virtual clusters when more
// than one instance is running on the same node.
func SQLInstance(sqlInstance int) CustomOption {
	return CustomOption{name: "SQLInstance", apply: overwrite(sqlInstance)}
}

// ConnectionOption allows the caller to provide a custom connection
// option to be included in the pgurl.
func ConnectionOption(key, value string) CustomOption {
	// We use a custom `apply` function for this option since we want to
	// extend our map if this option is called multiple times.
	apply := func(existing any) any {
		options := existing.(map[string]string)
		if options == nil {
			options = make(map[string]string)
		}

		options[key] = value
		return options
	}

	return CustomOption{name: "ConnectionOption", apply: apply}
}

// ConnectTimeout allows callers to set a connection timeout.
func ConnectTimeout(t time.Duration) CustomOption {
	sec := int64(t.Seconds())
	if sec < 1 {
		sec = 1
	}
	return ConnectionOption("connect_timeout", fmt.Sprintf("%d", sec))
}

// DBName changes the database name used when connecting to crdb.
func DBName(dbName string) CustomOption {
	return CustomOption{name: "DBName", apply: overwrite(dbName)}
}

// AuthMode allows the callers to change the authentication mode used
// when connecting to crdb.
func AuthMode(authMode install.PGAuthMode) CustomOption {
	return CustomOption{name: "AuthMode", apply: overwrite(authMode)}
}

// Apply takes in a container data structure and a list of
// user-provided options, and applies those options to the
// container. The container should be a pointer to a struct containing
// the relevant fields -- in other words, the struct actually defines
// that custom options a function can take. The struct is expected to
// have a field for each custom option passed. If an unrecognized
// option is passed, an error is returned.
func Apply(container any, opts []CustomOption) (retErr error) {
	s := reflect.ValueOf(container)

	var currentField string
	defer func() {
		if r := recover(); r != nil {
			if currentField == "" {
				retErr = fmt.Errorf("option.Apply failed: %v", r)
			} else {
				retErr = fmt.Errorf("failed to set %q on %T: %v", currentField, container, r)
			}
		}
	}()

	for _, opt := range opts {
		currentField = opt.name
		f := s.Elem().FieldByName(currentField)
		if !f.IsValid() {
			return fmt.Errorf("invalid option %s for %T", opt.name, container)
		}

		f.Set(reflect.ValueOf(opt.apply(f.Interface())))
	}

	return nil
}

// VirtualClusterOptions contains the options to be accepted by
// functions that can be applied on a specific virtual cluster.
type VirtualClusterOptions struct {
	VirtualClusterName string
	SQLInstance        int
}

// ConnOptions contains connection-related options.
type ConnOptions struct {
	VirtualClusterOptions
	User             string
	DBName           string
	AuthMode         install.PGAuthMode
	ConnectionOption map[string]string
}
