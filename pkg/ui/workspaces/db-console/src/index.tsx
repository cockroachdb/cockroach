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
import * as ReactDOM from "react-dom";

import "src/polyfills";
import "src/protobufInit";
import { alertDataSync } from "src/redux/alerts";
import { App } from "src/app";
import { history } from "src/redux/history";
import { createAdminUIStore } from "src/redux/state";
import "src/redux/analytics";
import {
  DataFromServer,
  fetchDataFromServer,
  getDataFromServer,
  setDataFromServer,
} from "src/util/dataFromServer";
import { recomputeDocsURLs } from "src/util/docs";

async function fetchAndRender() {
  setDataFromServer(
    (await fetchDataFromServer().catch(() => {})) as DataFromServer,
  );

  const store = createAdminUIStore(history, getDataFromServer());
  recomputeDocsURLs();

  ReactDOM.render(
    <App history={history} store={store} />,
    document.getElementById("react-layout"),
  );

  store.subscribe(alertDataSync(store));
}

fetchAndRender();
