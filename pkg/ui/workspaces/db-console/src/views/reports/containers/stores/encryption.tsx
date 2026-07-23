// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import * as protos from "src/js/protos";

export interface EncryptionStatusProps {
  store: protos.cockroach.server.serverpb.IStoreDetails;
}

export default class EncryptionStatus {
  getEncryptionRows(): React.ReactElement<any> {
    return null;
  }
}
