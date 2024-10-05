// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package syntheticprivilege

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/errors"
)

// Object represents an object that has its privileges stored
// in system.privileges.
type Object interface {
	privilege.Object
	// GetPath returns the path used to identify the object in
	// system.privileges.
	GetPath() string
	// GetFallbackPrivileges returns the default privileges for the synthetic
	// privilege object, if the system.privileges table usage is not yet
	// allowed by the version gate.
	GetFallbackPrivileges() *catpb.PrivilegeDescriptor
}

// Metadata for system privileges.
type Metadata struct {
	prefix string
	regex  *regexp.Regexp
	val    reflect.Type
}

var registry = []*Metadata{
	{
		prefix: "/global",
		regex:  regexp.MustCompile("(/global/)$"),
		val:    reflect.TypeOf((*GlobalPrivilege)(nil)),
	},
	{
		prefix: fmt.Sprintf("/%s", VirtualTablePathPrefix),
		regex:  regexp.MustCompile(fmt.Sprintf(`(/%s/((?P<SchemaName>.*))/((?P<TableName>.*)))$`, VirtualTablePathPrefix)),
		val:    reflect.TypeOf((*VirtualTablePrivilege)(nil)),
	},
	{
		prefix: "/externalconn",
		regex:  regexp.MustCompile(`(/externalconn/((?P<ConnectionName>.*)))$`),
		val:    reflect.TypeOf((*ExternalConnectionPrivilege)(nil)),
	},
}

func findMetadata(val string) *Metadata {
	for _, md := range registry {
		if strings.HasPrefix(val, md.prefix) {
			return md
		}
	}
	return nil
}

// Parse turns a privilege path string to a Object.
func Parse(privPath string) (Object, error) {
	md := findMetadata(privPath)
	if md == nil {
		return nil, errors.AssertionFailedf("no prefix match found for privilege path %s", privPath)
	}

	if !md.regex.MatchString(privPath) {
		return nil, errors.AssertionFailedf("%s does not match regex pattern %s", privPath, md.regex)
	}

	matches := md.regex.FindStringSubmatch(privPath)
	if matches == nil {
		return nil, errors.AssertionFailedf("no match found for privilege path %s", privPath)
	}
	vet := md.val.Elem()
	v := reflect.New(vet)

	for i := 0; i < vet.NumField(); i++ {
		f := vet.Field(i)
		tagField := f.Tag.Get("priv")
		if tagField == "" {
			continue
		}
		idx := md.regex.SubexpIndex(f.Name)
		if idx == -1 {
			return nil, errors.AssertionFailedf("no field found with name %s", f.Name)
		}
		val := reflect.ValueOf(matches[idx])
		val, err := unmarshal(val, f)
		if err != nil {
			return nil, err
		}
		v.Elem().Field(i).Set(val)
	}
	return v.Interface().(Object), nil
}

func unmarshal(val reflect.Value, f reflect.StructField) (reflect.Value, error) {
	switch f.Name {
	case "TableName":
		return val, nil
	case "SchemaName":
		return val, nil
	case "ConnectionName":
		return val, nil
	default:
		panic(errors.AssertionFailedf("unhandled type %v", f.Type))
	}
}
