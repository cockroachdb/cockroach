// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgcryptocipherccl

import (
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

type cipherAlgorithm int

const (
	_ cipherAlgorithm = iota
	aesCipher
)

type cipherMode int

const (
	cbcMode cipherMode = iota
)

type cipherPadding int

const (
	pkcsPadding cipherPadding = iota
	noPadding
)

type cipherMethod struct {
	algorithm cipherAlgorithm
	mode      cipherMode
	padding   cipherPadding
}

var cipherMethodRE = regexp.MustCompile("^(?P<algorithm>[[:alpha:]]+)(?:-(?P<mode>[[:alpha:]]+))?(?:/pad:(?P<padding>[[:alpha:]]+))?$")

func parseCipherMethod(s string) (cipherMethod, error) {
	submatches := cipherMethodRE.FindStringSubmatch(s)
	if submatches == nil {
		return cipherMethod{}, pgerror.Newf(pgcode.InvalidParameterValue, `cipher method has wrong format: "%s"`, s)
	}

	ret := cipherMethod{}

	switch algorithm := submatches[cipherMethodRE.SubexpIndex("algorithm")]; strings.ToLower(algorithm) {
	case "aes":
		ret.algorithm = aesCipher
	case "bf":
		return cipherMethod{}, unimplemented.NewWithIssue(105466, "Blowfish is insecure and not supported")
	default:
		return cipherMethod{}, pgerror.Newf(pgcode.InvalidParameterValue, `cipher method has invalid algorithm: "%s"`, algorithm)
	}

	switch mode := submatches[cipherMethodRE.SubexpIndex("mode")]; strings.ToLower(mode) {
	case "", "cbc":
	case "ecb":
		return cipherMethod{}, unimplemented.NewWithIssue(105466, "ECB mode is insecure and not supported")
	default:
		return cipherMethod{}, pgerror.Newf(pgcode.InvalidParameterValue, `cipher method has invalid mode: "%s"`, mode)
	}

	switch padding := submatches[cipherMethodRE.SubexpIndex("padding")]; strings.ToLower(padding) {
	case "", "pkcs":
	case "none":
		ret.padding = noPadding
	default:
		return cipherMethod{}, pgerror.Newf(pgcode.InvalidParameterValue, `cipher method has invalid padding: "%s"`, padding)
	}

	return ret, nil
}
