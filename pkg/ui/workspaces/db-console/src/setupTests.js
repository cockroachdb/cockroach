// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import Enzyme from "enzyme";
import Adapter from "enzyme-adapter-react-16";
// import * as v8 from "v8";
//
// if (process.env.BAZEL_TARGET) {
//   // TODO(barag): set Error.stackTraceLimit to be not 100 like it is now. Paths are *horridly* long under Bazel, and 100
//   // stack frames is a ton of data.
//   // console.log("stack trace limit = ", Error.stackTraceLimit);
//   // bisect log:
//   // 100: bad
//   // 50:  good
//   // 75:  bad
//   // 62:  good
//   // 68:  good
//   // 65:  good
//   // 66:  good
//   // 67:  bad
//   Error.stackTraceLimit = 100;
// }

//TODO(barag): does this need to all be in a beforeAll()/afterAll so we can restore all of these mocks?
Enzyme.configure({ adapter: new Adapter() });

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
