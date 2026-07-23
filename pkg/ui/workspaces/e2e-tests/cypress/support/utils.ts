// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Shared helper functions for e2e tests.

export const isTextGreaterThanZero = (ele: JQuery<HTMLElement>) => {
  const text = ele.get()[0].innerText;
  const textAsFloat = parseFloat(text);
  expect(textAsFloat).to.be.greaterThan(0);
};
