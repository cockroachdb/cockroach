// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Jest uses jsdom to simulate a browser, which doesn’t include window.matchMedia.
// This property was created and added to `setupFilesAfterEnv` on jest.config.js to
// handle test cases where window.matchMedia and its functions (e.g. addEventListener)
// are used.
Object.defineProperty(window, "matchMedia", {
  writable: true,
  value: jest.fn().mockImplementation(query => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: jest.fn(), // deprecated
    removeListener: jest.fn(), // deprecated
    addEventListener: jest.fn(),
    removeEventListener: jest.fn(),
    dispatchEvent: jest.fn(),
  })),
});

export {};
