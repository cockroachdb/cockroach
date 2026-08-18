// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vm

import (
	"fmt"
	"strings"
)

// AddressMode controls whether newly created VMs receive public addresses.
type AddressMode string

const (
	// AddressModeAuto lets the provider choose based on its configuration.
	AddressModeAuto AddressMode = "auto"
	// AddressModePublic requests public addresses.
	AddressModePublic AddressMode = "public"
	// AddressModePrivate requests no public addresses.
	AddressModePrivate AddressMode = "private"
)

// NormalizeAddressMode validates mode and maps the zero value to the
// compatibility-preserving public mode.
func NormalizeAddressMode(mode AddressMode) (AddressMode, error) {
	mode = AddressMode(strings.ToLower(string(mode)))
	if mode == "" {
		return AddressModePublic, nil
	}
	switch mode {
	case AddressModeAuto, AddressModePublic, AddressModePrivate:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid address mode %q (expected auto, public, or private)", mode)
	}
}

// Set implements pflag.Value.
func (m *AddressMode) Set(value string) error {
	mode, err := NormalizeAddressMode(AddressMode(value))
	if err != nil {
		return err
	}
	*m = mode
	return nil
}

// String implements pflag.Value.
func (m AddressMode) String() string {
	if m == "" {
		return string(AddressModePublic)
	}
	return string(m)
}

// Type implements pflag.Value.
func (m AddressMode) Type() string { return "address-mode" }
