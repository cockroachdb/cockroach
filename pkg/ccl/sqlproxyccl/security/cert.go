// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

//go:generate mockgen -package=security -destination=mocks_generated.go -source=cert.go . Cert

package security

import "context"

// Cert is a generic presentation on managed certificate that can be reloaded.
type Cert interface {
	Reload(ctx context.Context)
	Err() error
	ClearErr()
}
