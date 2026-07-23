// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import OSSLicenseType from "oss/src/views/shared/components/licenseType";
import DebugAnnotation from "src/views/shared/components/debugAnnotation";
import swapByLicense from "src/views/shared/containers/licenseSwap";

export class CCLLicenseType extends React.Component<{}, {}> {
  render() {
    return <DebugAnnotation label="License type" value="CCL" />;
  }
}

/**
 * LicenseType is an indicator showing the current build license.
 */
const LicenseType = swapByLicense(OSSLicenseType, CCLLicenseType);

export default LicenseType;
