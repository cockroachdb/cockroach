// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package systemprivilege

import (
	"reflect"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/errors"
)

// Metadata for system privileges.
type Metadata struct {
	prefix string
	regex  *regexp.Regexp
	val    reflect.Type
}

var registry = []*Metadata{
	{
		prefix: "/system",
		regex:  regexp.MustCompile("(/system/)$"),
		val:    reflect.TypeOf((*catalog.SystemClusterPrivilege)(nil)),
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

// Parse turns a privilege path string to a SystemPrivilegeObject.
func Parse(privPath string) (catalog.SystemPrivilegeObject, error) {
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
		v.Elem().Field(i).Set(reflect.ValueOf(matches[idx]))
	}
	return v.Interface().(catalog.SystemPrivilegeObject), nil
}
