// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package apd

import "fmt"

// decomposer composes or decomposes a decimal value to and from individual parts.
// There are four separate parts: a boolean negative flag, a form byte with three possible states
// (finite=0, infinite=1, NaN=2),  a base-2 big-endian integer
// coefficient (also known as a significand) as a []byte, and an int32 exponent.
// These are composed into a final value as "decimal = (neg) (form=finite) coefficient * 10 ^ exponent".
// A zero length coefficient is a zero value.
// If the form is not finite the coefficient and scale should be ignored.
// The negative parameter may be set to true for any form, although implementations are not required
// to respect the negative parameter in the non-finite form.
//
// Implementations may choose to signal a negative zero or negative NaN, but implementations
// that do not support these may also ignore the negative zero or negative NaN without error.
// If an implementation does not support Infinity it may be converted into a NaN without error.
// If a value is set that is larger then what is supported by an implementation is attempted to
// be set, an error must be returned.
// Implementations must return an error if a NaN or Infinity is attempted to be set while neither
// are supported.
type decomposer interface {
	// Decompose returns the internal decimal state into parts.
	// If the provided buf has sufficient capacity, buf may be returned as the coefficient with
	// the value set and length set as appropriate.
	Decompose(buf []byte) (form byte, negative bool, coefficient []byte, exponent int32)

	// Compose sets the internal decimal value from parts. If the value cannot be
	// represented then an error should be returned.
	// The coefficent should not be modified. Successive calls to compose with
	// the same arguments should result in the same decimal value.
	Compose(form byte, negative bool, coefficient []byte, exponent int32) error
}

var _ decomposer = &Decimal{}

// Decompose returns the internal decimal state into parts.
// If the provided buf has sufficient capacity, buf may be returned as the coefficient with
// the value set and length set as appropriate.
func (d *Decimal) Decompose(buf []byte) (form byte, negative bool, coefficient []byte, exponent int32) {
	switch d.Form {
	default:
		panic(fmt.Errorf("unknown Form: %v", d.Form))
	case Finite:
		// Nothing, continue on.
	case Infinite:
		negative = d.Negative
		form = 1
		return
	case NaNSignaling, NaN:
		negative = d.Negative
		form = 2
		return
	}
	// Finite form.
	negative = d.Negative
	exponent = d.Exponent
	coefficient = d.Coeff.Bytes()
	return
}

// Compose sets the internal decimal value from parts. If the value cannot be
// represented then an error should be returned.
func (d *Decimal) Compose(form byte, negative bool, coefficient []byte, exponent int32) error {
	switch form {
	default:
		return fmt.Errorf("unknown form: %v", form)
	case 0:
		d.Form = Finite
		// Set rest of finite form below.
	case 1:
		d.Form = Infinite
		d.Negative = negative
		return nil
	case 2:
		d.Form = NaN
		d.Negative = negative
		return nil
	}
	// Finite form.
	d.Negative = negative
	d.Coeff.SetBytes(coefficient)
	d.Exponent = exponent
	return nil
}
