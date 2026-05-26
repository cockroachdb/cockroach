// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import DebugAnnotation from "src/views/shared/components/debugAnnotation";

/**
 * LicenseType is an indicator showing the current build license.
 */
export default function LicenseType(): React.ReactElement {
  return <DebugAnnotation label="License type" value="OSS" />;
}
