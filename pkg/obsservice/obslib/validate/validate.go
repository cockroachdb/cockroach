// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package validate

// Validator is the interface used to validate
// event-specific types.
//
// Validator implementations are expected to be thread-safe.
type Validator[T any] interface {
	Validate(T) error
}
