// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package httputil

import "github.com/cockroachdb/cockroach/pkg/settings"

// HSTSEnabled is a boolean that enables HSTS headers on the HTTP
// server. These instruct a valid user agent to use HTTPS *only*
// for all future connections to this host.
var HSTSEnabled = settings.RegisterBoolSetting(
	"server.hsts.enabled",
	"if true, HSTS headers will be sent along with all HTTP "+
		"requests. The headers will contain a max-age setting of one "+
		"year. Browsers honoring the header will always use HTTPS to "+
		"access the DB Console. Ensure that TLS is correctly configured "+
		"prior to enabling.",
	false,
).WithPublic()

// HstsHeaderKey is the header that's used to store the HSTS policy.
const HstsHeaderKey = "Strict-Transport-Security"

// HstsHeaderValue contains the static HSTS header value we return when
// HSTS is enabled via the clusters setting above. It sets the expiry
// of 1 year that the browser should remember to only use HTTPS for
// this site.
const HstsHeaderValue = "max-age=31536000"
