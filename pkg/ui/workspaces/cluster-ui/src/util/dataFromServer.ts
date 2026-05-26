// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import FeatureFlags = cockroach.server.serverpb.FeatureFlags;

export interface DataFromServer {
  Insecure: boolean;
  LoggedInUser: string;
  Tag: string;
  Version: string;
  NodeID: string;
  OIDCAutoLogin: boolean;
  OIDCLoginEnabled: boolean;
  OIDCButtonText: string;
  OIDCGenerateJWTAuthTokenEnabled: boolean;
  FeatureFlags: FeatureFlags;
  LicenseType: string;
  SecondsUntilLicenseExpiry: number;
  IsManaged: boolean;
}

// Tell TypeScript about `window.dataFromServer`, which is set in a script
// tag in index.html, the contents of which are generated in a Go template
// server-side.
declare global {
  interface Window {
    dataFromServer: DataFromServer;
  }
}

export function getDataFromServer(): DataFromServer {
  return (
    window.dataFromServer ||
    ({
      FeatureFlags: {},
    } as DataFromServer)
  );
}
