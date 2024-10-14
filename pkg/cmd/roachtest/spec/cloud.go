// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spec

import (
	"fmt"
	"strings"
)

// Cloud indicates the cloud provider.
type Cloud int

const (
	// AnyCloud is a sentinel value for an unset Cloud value, which in most
	// contexts means "any cloud".
	AnyCloud Cloud = iota
	// Local is a faux cloud value assigned to tests that are run on a local
	// machine.
	Local
	// GCE stands for Google Compute Engine.
	GCE
	// AWS stands for Amazon Web Services.
	AWS
	// Azure is Microsoft's cloud.
	Azure
)

// IsSet returns true if the value is set. The meaning of an unset value depends
// on the context, but it usually means "any cloud".
func (c Cloud) IsSet() bool {
	return c != AnyCloud
}

func (c Cloud) String() string {
	switch c {
	case AWS:
		return "aws"
	case GCE:
		return "gce"
	case Azure:
		return "azure"
	case Local:
		return "local"
	default:
		panic("invalid cloud")
	}
}

// CloudFromString parses the cloud provider from a string (e.g. "aws" or
// "AWS").
func CloudFromString(s string) Cloud {
	c, ok := TryCloudFromString(s)
	if !ok {
		panic(fmt.Sprintf("invalid cloud %q", s))
	}
	return c
}

// TryCloudFromString parses the cloud provider from a string (e.g. "aws" or
// "AWS").
func TryCloudFromString(s string) (_ Cloud, ok bool) {
	switch strings.ToLower(s) {
	case "aws":
		return AWS, true
	case "gce":
		return GCE, true
	case "azure":
		return Azure, true
	case "local":
		return Local, true
	default:
		return AnyCloud, false
	}
}
