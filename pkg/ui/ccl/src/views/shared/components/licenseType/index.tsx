// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";

import swapByLicense from "src/views/shared/containers/licenseSwap";
import OSSLicenseType from "oss/src/views/shared/components/licenseType";

class CCLLicenseType extends React.Component<{}, {}> {
  render() {
    return (
      <div>
        <h3>License type: CCL</h3>
      </div>
    );
  }
}

/**
 * LicenseType is an indicator showing the current build license.
 */
// tslint:disable-next-line:variable-name
const LicenseType = swapByLicense(OSSLicenseType, CCLLicenseType);

export default LicenseType;
