// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syntheticprivilege

import (
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
		prefix: "/global",
		regex:  regexp.MustCompile("(/global/)$"),
		val:    reflect.TypeOf((*GlobalPrivilege)(nil)),
	},
	{
		prefix: "/vtable",
		regex:  regexp.MustCompile(`(/vtable/((?P<ID>\d+)))$`),
		val:    reflect.TypeOf((*VirtualTablePrivilege)(nil)),
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

// Parse turns a privilege path string to a SyntheticPrivilegeObject.
func Parse(privPath string) (catalog.SyntheticPrivilegeObject, error) {
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
	return v.Interface().(catalog.SyntheticPrivilegeObject), nil
}

func unmarshal(val reflect.Value, f reflect.StructField) (reflect.Value, error) {
	switch f.Type {
	case reflect.TypeOf(descpb.ID(0)):
		i, err := strconv.Atoi(val.String())
		if err != nil {
			return reflect.Value{}, errors.Wrap(err, "failed to cast value to uint32 (descpb.ID)")
		}
		return reflect.ValueOf(descpb.ID(i)), nil
	default:
		panic(errors.AssertionFailedf("unhandled type %v", f.Type))
	}
}
