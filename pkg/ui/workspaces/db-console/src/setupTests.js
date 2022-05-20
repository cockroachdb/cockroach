// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import Enzyme from "enzyme";
import Adapter from "enzyme-adapter-react-16";

Enzyme.configure({ adapter: new Adapter() });

/**
 * Various thing in DB Console use global fetch. `fetch` is generally
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
