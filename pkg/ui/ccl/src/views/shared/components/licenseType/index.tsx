// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";

import DebugAnnotation from "src/views/shared/components/debugAnnotation";
import swapByLicense from "src/views/shared/containers/licenseSwap";
import OSSLicenseType from "oss/src/views/shared/components/licenseType";

class CCLLicenseType extends React.Component<{}, {}> {
  render() {
    return <DebugAnnotation label="License type" value="CCL" />;
  }
}

/**
 * LicenseType is an indicator showing the current build license.
 */
const LicenseType = swapByLicense(OSSLicenseType, CCLLicenseType);

export default LicenseType;
