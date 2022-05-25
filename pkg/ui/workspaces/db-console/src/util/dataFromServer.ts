// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { AdminUIState } from "oss/src/redux/state";

export interface DataFromServer {
  ExperimentalUseLogin: boolean;
  LoginEnabled: boolean;
  LoggedInUser: string;
  Tag: string;
  Version: string;
  NodeID: string;
  OIDCAutoLogin: boolean;
  OIDCLoginEnabled: boolean;
  OIDCButtonText: string;
}
// Tell TypeScript about `window.dataFromServer`, which is set in a script
// tag in index.html, the contents of which are generated in a Go template
// server-side.
declare global {
  interface Window {
    dataFromServer: DataFromServer;
  }
}

export function fetchDataFromServer(): Promise<DataFromServer> {
  return fetch("/", {
    method: "GET",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
  }).then(d => {
    return d.json();
  });
}

export const dataFromServerSelector = (s: AdminUIState) =>
  s.cachedData.dataFromServer;

export function getDataFromServer(): DataFromServer {
  return window.dataFromServer || ({} as DataFromServer);
}

export function setDataFromServer(d: DataFromServer) {
  window.dataFromServer = d;
}
