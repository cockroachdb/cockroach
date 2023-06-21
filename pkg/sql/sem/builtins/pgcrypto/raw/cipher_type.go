// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raw

import (
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
)

type cipherAlgorithm int

const (
	_ cipherAlgorithm = iota
	aesCipher
)

type cipherMode int

const (
	cbcMode cipherMode = iota
	ecbMode
)

type cipherPadding int

const (
	pkcsPadding cipherPadding = iota
	noPadding
)

type cipherType struct {
	algorithm cipherAlgorithm
	mode      cipherMode
	padding   cipherPadding
}

func parseCipherType(s string) (cipherType, error) {
	cipherTypeRE := regexp.MustCompile("^(?P<algorithm>[[:alpha:]]+)(?:-(?P<mode>[[:alpha:]]+))?(?:/pad:(?P<padding>[[:alpha:]]+))?$")

	submatches := cipherTypeRE.FindStringSubmatch(s)
	if submatches == nil {
		return cipherType{}, errors.Newf(`cipher type has wrong format: "%s"`, s)
	}

	ret := cipherType{}

	switch algorithm := submatches[cipherTypeRE.SubexpIndex("algorithm")]; strings.ToLower(algorithm) {
	case "aes":
		ret.algorithm = aesCipher
	default:
		return cipherType{}, errors.Newf(`cipher type has unsupported algorithm: "%s"`, algorithm)
	}

	switch mode := submatches[cipherTypeRE.SubexpIndex("mode")]; strings.ToLower(mode) {
	case "", "cbc":
	case "ecb":
		ret.mode = ecbMode
	default:
		return cipherType{}, errors.Newf(`cipher type has unsupported mode: "%s"`, mode)
	}

	switch padding := submatches[cipherTypeRE.SubexpIndex("padding")]; strings.ToLower(padding) {
	case "", "pkcs":
	case "none":
		ret.padding = noPadding
	default:
		return cipherType{}, errors.Newf(`cipher type has unsupported padding: "%s"`, padding)
	}

	return ret, nil
}
