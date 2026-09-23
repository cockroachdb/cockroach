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

// The cluster ID shown in the masthead when no cluster name is configured.
const clusterIdPattern =
  /^Cluster id: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

// checkClusterId asserts that the masthead shows "Cluster id: <uuid>".
//
// The masthead splits that string across two DOM nodes: a "Cluster id: " text
// node and a nested span holding the UUID (see clusterTitle.tsx). That rules
// out findByText, which matches an element's direct text children only, so
// neither half matches the pattern on its own. contains matches an element's
// full text content instead, which includes the nested UUID.
export const checkClusterId = (
  options?: Partial<Cypress.Loggable & Cypress.Timeoutable>,
) => {
  cy.contains(clusterIdPattern, options);
};
