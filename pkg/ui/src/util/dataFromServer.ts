// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

export interface DataFromServer {
  ExperimentalUseLogin: boolean;
  LoginEnabled: boolean;
  LoggedInUser: string;
  Tag: string;
  Version: string;
  NodeID: string;
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
  return window.dataFromServer || {} as DataFromServer;
}
