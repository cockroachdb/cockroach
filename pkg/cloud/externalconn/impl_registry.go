// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package externalconn

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/errors"
)

// parseAndValidateFns maps a URI scheme to a constructor of instances of that external
// connection.
var parseAndValidateFns = map[string]connectionParserFactories{}

// RegisterConnectionDetailsFromURIFactory is used by every concrete
// implementation to register its factory method.
func RegisterConnectionDetailsFromURIFactory(
	providerScheme string, parseAndValidateFn connectionParserFactory,
) {
	parseAndValidateFns[providerScheme] = append(parseAndValidateFns[providerScheme], parseAndValidateFn)
}

// ExternalConnectionFromURI returns an ExternalConnection for the given URI,
// after validating that there's at least one registered consumer for it (e.g. it's a valid backup destination).
func ExternalConnectionFromURI(
	ctx context.Context, execCfg interface{}, user username.SQLUsername, uri string,
) (ExternalConnection, error) {
	externalConnectionURI, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	// Run all parseFn methods for the ExternalConnection provider. For the URI to be valid and saved,
	// at least one parseFn must succeed, and the result can't be ambiguous--every parseFn that succeeds
	// must create the same protobuf. Currently it's impossible for them to differ.
	return parseAndValidateFns[externalConnectionURI.Scheme].uniqueConnectionFromURI(ctx, execCfg, user, externalConnectionURI)
}

type connectionParserFactories []connectionParserFactory

// uniqueConnectionFromURI applies all factories for this scheme. If they all error, it returns all the errors using CombineErrors.
func (fns connectionParserFactories) uniqueConnectionFromURI(
	ctx context.Context, execCfg interface{}, user username.SQLUsername, uri *url.URL,
) (ExternalConnection, error) {
	switch len(fns) {
	case 0:
		return nil, errors.Newf("no parseFn found for external connection provider %s", uri.Scheme)
	case 1:
		return fns[0](ctx, execCfg, user, uri)
	default:
		var conns []ExternalConnection
		var combinedErr error
		for _, parseFn := range fns {
			conn, err := parseFn(ctx, execCfg, user, uri)
			if err != nil {
				combinedErr = errors.CombineErrors(combinedErr, err)
			} else {
				conns = append(conns, conn)
			}
		}
		if len(conns) == 0 {
			return nil, errors.Wrap(combinedErr, "all registered constructors returned errors")
		}
		return conns[0], connsDiffer(conns)
	}
}

func connsDiffer(conns []ExternalConnection) error {
	if len(conns) == 1 {
		return nil
	}
	scratch := make([]byte, conns[0].ConnectionProto().GetDetails().Size())
	_, err := conns[0].ConnectionProto().GetDetails().MarshalTo(scratch)
	if err != nil {
		return err
	}
	marshalled := string(scratch)
	for i := 1; i < len(conns); i++ {
		otherDetails := conns[i].ConnectionProto().GetDetails()
		if otherDetails.Size() > len(scratch) {
			return errors.AssertionFailedf("multiple constructors consider this a valid URI but parse it differently")
		}
		_, err := otherDetails.MarshalTo(scratch)
		if err != nil {
			return err
		}
		if marshalled != string(scratch) {
			return errors.AssertionFailedf("multiple constructors consider this a valid URI but parse it differently: %s vs %s",
				marshalled, string(scratch))
		}
	}
	return nil
}

// TestingKnobs provide fine-grained control over the external connection
// components for testing.
type TestingKnobs struct {
	// SkipCheckingExternalStorageConnection returns whether `CREATE EXTERNAL
	// CONNECTION` should skip the step that writes, lists and reads a sentinel
	// file from the underlying ExternalStorage.
	SkipCheckingExternalStorageConnection func() bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
