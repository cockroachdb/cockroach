// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

// flagsOptions are the options to `log.ApplyConfig`. This struct is typically
// not used directly. Instead, the functions mentioned on each field comment
// below are invoked as arguments to `log.ApplyConfig`. See the FlagsOption
// interface for a synopsis.
type flagsOptions struct {
	// managed is set by WithManaged. If set, managed indicates we are logging
	// from an instance that's run as part of a managed service. This impacts
	// certain log redaction policies. See safeManaged for additional details.
	managed bool
}

// FlagsOption is the interface satisfied by options to `log.ApplyConfig`.
// A synopsis of the options follows. For details, see their comments.
//
// - WithManaged: indicate we are logging from a managed service.
type FlagsOption interface {
	apply(flagsOptions) flagsOptions
}

type managedFlagsOption struct{}

// WithManaged configures logging to be aware that it's being run as
// part of a managed service. This impacts whether we consider arguments
// marked with SafeManaged as safe or unsafe. If WithManaged(true), any
// argument logged with SafeManaged will be considered safe from a
// redaction perspective, and unsafe otherwise. If the argument itself
// implements the redact.SafeFormatter interface, then we delegate to its
// implementation in either case.
//
// See SafeManaged for more details.
func WithManaged(managed bool) FlagsOption {
	if !managed {
		return nil
	}
	return managedFlagsSingleton
}

func (m managedFlagsOption) apply(opts flagsOptions) flagsOptions {
	opts.managed = true
	return opts
}

var managedFlagsSingleton = FlagsOption(managedFlagsOption{})
