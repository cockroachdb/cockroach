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

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/errors"
)

type kmsConnectionContext struct {
	kmsEnv cloud.KMSEnv
}

// ExternalStorageContext implements the ConnectionContext interface.
func (k *kmsConnectionContext) ExternalStorageContext() cloud.ExternalStorageContext {
	panic("kmsConnectionContext cannot be used for External Storage initialization")
}

// KMSEnv implements the ConnectionContext interface.
func (k *kmsConnectionContext) KMSEnv() cloud.KMSEnv {
	return k.kmsEnv
}

var _ ConnectionContext = &kmsConnectionContext{}

func makeExternalConnectionKMS(
	ctx context.Context, uri string, env cloud.KMSEnv,
) (cloud.KMS, error) {
	kmsURI, err := url.ParseRequestURI(uri)
	if err != nil {
		return nil, err
	}
	if kmsURI.Host == "" {
		return nil, errors.Newf("host component of an external URI must refer to an "+
			"existing External Connection object: %s", kmsURI.String())
	}
	externalConnectionName := kmsURI.Host

	// TODO(adityamaru): Use the `user` in `cfg` to perform privilege checks on
	// the external connection object we are about to retrieve.

	// Retrieve the external connection object from the system table.
	var ec *ExternalConnection
	if err := env.DBHandle().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		ec, err = LoadExternalConnection(ctx, externalConnectionName, connectionpb.TypeKMS,
			env.InternalExecutor(), env.User(), txn)
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "failed to load external connection object")
	}

	// Construct an ExternalStorage handle for the underlying resource represented
	// by the external connection object.
	details := ec.ConnectionDetails()
	connDetails, err := MakeConnectionDetails(ctx, *details)
	if err != nil {
		return nil, err
	}
	connection, err := connDetails.Dial(ctx, &kmsConnectionContext{kmsEnv: env}, "" /* subdir */)
	if err != nil {
		return nil, errors.Wrap(err, "failed to Dial external connection")
	}

	var kms cloud.KMS
	var ok bool
	if kms, ok = connection.(cloud.KMS); !ok {
		return nil, errors.AssertionFailedf("cannot convert Connection to cloud.KMS")
	}

	return kms, nil
}

func init() {
	cloud.RegisterKMSFromURIFactory(makeExternalConnectionKMS, scheme)
}
