// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/**
 * Various things in DB Console use global fetch. `fetch` is generally
 * available in modern browsers but is not standard in NodeJS until
 * version 18.0.0. I am mocking here to generally not cause errors in
 * Jest when loading large parts of the application. If a test actually
 * requires fetching data, fetchMock should be used.
 */
Object.defineProperty(window, "fetch", {
  writable: true,
  value: jest.fn(),
});

Object.defineProperty(window, "matchMedia", {
  writable: true,
  value: query => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: jest.fn(), // deprecated
    removeListener: jest.fn(), // deprecated
    addEventListener: jest.fn(),
    removeEventListener: jest.fn(),
    dispatchEvent: jest.fn(),
  }),
});

afterAll(() => {
  jest.restoreAllMocks();
  delete window.matchMedia;
  delete window.fetch;
});
