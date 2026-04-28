// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import React from "react";
import { createRoot } from "react-dom/client";

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

  const root = createRoot(document.getElementById("react-layout")!);
  root.render(<App history={history} store={store} />);

  store.subscribe(alertDataSync(store));
}

fetchAndRender();
