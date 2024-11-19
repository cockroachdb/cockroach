// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import React from "react";
import * as ReactDOM from "react-dom";

import "src/polyfills";
import "src/protobufInit";
import { App } from "src/app";
import { alertDataSync } from "src/redux/alerts";
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

  /* eslint react/no-deprecated: "off" */
  ReactDOM.render(
    <App history={history} store={store} />,
    document.getElementById("react-layout"),
  );

  store.subscribe(alertDataSync(store));
}

fetchAndRender();
