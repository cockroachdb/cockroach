// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";

import DebugAnnotation from "src/views/shared/components/debugAnnotation";

/**
 * LicenseType is an indicator showing the current build license.
 */
export default class LicenseType extends React.Component<{}, {}> {
  render() {
    return <DebugAnnotation label="License type" value="OSS" />;
  }
}
