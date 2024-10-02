// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "./polyfills/object-assign";
import "whatwg-fetch";
import fetchMock from "fetch-mock";

fetchMock.configure({
  sendAsJson: false,
});

export default fetchMock;
