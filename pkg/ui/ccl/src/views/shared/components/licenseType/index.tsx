// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

import OSSLicenseType from "oss/src/views/shared/components/licenseType";
import React from "react";

/**
 * LicenseType is an indicator showing the current build license.
 */
export default class LicenseType extends React.Component<{}, {}> {
  render() {
    if (location.hash.indexOf("pretend-license-expired") !== -1) {
      return <OSSLicenseType/>;
    }
    return (
      <div>
        <h3>License type: CCL</h3>
      </div>
    );
  }
}
