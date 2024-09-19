// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package option

import (
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

// GlobalOptions is the set of all options that cluster functions can
// take. Specific functions should define their own options struct and
// call the `Apply` function to get the options that apply to
// them. Every options struct should contain, by necessity, a subset
// of the fields defined here.
type GlobalOptions struct {
	VirtualClusterOptions
	User              string
	DBName            string
	AuthMode          install.PGAuthMode
	ConnectionOptions map[string]string
}

type OptionFunc func(*GlobalOptions)

// User allows the customization of the user to use when connecting to
// crdb.
func User(user string) OptionFunc {
	return func(o *GlobalOptions) {
		o.User = user
	}
}

// VirtualClusterName allows the customization of the virtual cluster
// to connect to. If not provided, it will default to the cluster's
// default virtual cluster (or `system` if that's not set.)
func VirtualClusterName(name string) OptionFunc {
	return func(o *GlobalOptions) {
		o.VirtualClusterName = name
	}
}

// SQLInstance allows the caller to indicate which sql instance to
// use. Only applicable in separate-process virtual clusters when more
// than one instance is running on the same node.
func SQLInstance(sqlInstance int) OptionFunc {
	return func(o *GlobalOptions) {
		o.SQLInstance = sqlInstance
	}
}

// ConnectionOption allows the caller to provide a custom connection
// option to be included in the pgurl.
func ConnectionOption(key, value string) OptionFunc {
	return func(o *GlobalOptions) {
		if o.ConnectionOptions == nil {
			o.ConnectionOptions = make(map[string]string)
		}

		o.ConnectionOptions[key] = value
	}
}

// ConnectTimeout allows callers to set a connection timeout.
func ConnectTimeout(t time.Duration) OptionFunc {
	sec := int64(t.Seconds())
	if sec < 1 {
		sec = 1
	}
	return ConnectionOption("connect_timeout", fmt.Sprintf("%d", sec))
}

// DBName changes the database name used when connecting to crdb.
func DBName(dbName string) OptionFunc {
	return func(o *GlobalOptions) {
		o.DBName = dbName
	}
}

// AuthMode allows the callers to change the authentication mode used
// when connecting to crdb.
func AuthMode(authMode install.PGAuthMode) OptionFunc {
	return func(o *GlobalOptions) {
		o.AuthMode = authMode
	}
}

// Apply takes in an options struct and a list of user-provided
// options, and applies those options to the container. The container
// should be a pointer to a struct containing the relevant fields --
// in other words, the struct actually defines that custom options a
// function can take. The struct is expected to have a field for each
// custom option passed. An error is returned if an unrecognized
// option is passed.
func Apply(container any, opts ...OptionFunc) (retErr error) {
	var globalOptions GlobalOptions
	for _, opt := range opts {
		opt(&globalOptions)
	}

	// Many functions in the `reflect` package can panic if they
	// parameters are of unexpected types, so we wrap these panics as
	// errors to be returned to the caller.
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

	isEmbeddedStruct := func(f reflect.StructField) bool {
		return f.Type.Kind() == reflect.Struct && f.Anonymous
	}

	// Build a mapping from option to name to the corresponding
	// `reflect.Value`. We skip embedded struct fields because we are
	// only interested in the "flattened" view of the options
	// structs. This allows us to reuse options structs across multiple
	// functions (for instance, multiple functions need to take options
	// related to which virtual cluster to connect to).
	globalOptionsValue := reflect.ValueOf(globalOptions)
	globalFields := make(map[string]reflect.Value)
	for _, f := range reflect.VisibleFields(reflect.TypeOf(globalOptions)) {
		if isEmbeddedStruct(f) {
			continue
		}

		globalFields[f.Name] = globalOptionsValue.FieldByName(f.Name)
	}

	// We keep a set of fields from `globalOptions` that are actually
	// used by the container struct so that we can validate that the
	// caller didn't pass any options that are not applicable.
	containerFields := make(map[string]struct{})

	containerStruct := reflect.ValueOf(container).Elem()
	containerType := reflect.TypeOf(containerStruct.Interface())
	for _, structField := range reflect.VisibleFields(containerType) {
		if isEmbeddedStruct(structField) {
			continue
		}

		currentField = structField.Name
		f := containerStruct.FieldByName(currentField)

		// It is an error for the container struct to have fields that are
		// not present in the `GlobalOptions` struct.
		globalField, ok := globalFields[currentField]
		if !ok {
			return fmt.Errorf("options struct %T has unknown option %q", container, currentField)
		}

		// Update the field in the container struct with the value in
		// `GlobalOptions`.
		f.Set(globalField)
		containerFields[currentField] = struct{}{}
	}

	// Validate that the caller did not pass any options that do not
	// apply to the `container` struct.
	for name, f := range globalFields {
		if _, ok := containerFields[name]; ok {
			continue
		}

		if !f.IsZero() {
			return fmt.Errorf("non-applicable option %q for %T", name, container)
		}
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
	User              string
	DBName            string
	AuthMode          install.PGAuthMode
	ConnectionOptions map[string]string
}
