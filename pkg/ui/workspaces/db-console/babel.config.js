// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
/* globals module */

const presets = [
  [
    "@babel/env",
    {
      modules: "commonjs",
    },
  ],
  "@babel/react",
];

const plugins = [
  "@babel/proposal-class-properties",
  "@babel/proposal-object-rest-spread",
  // @babel/plugin-transform-runtime is required to support dynamic loading of cluster-ui package
  "@babel/plugin-transform-runtime",
];

const env = {
  test: {
    plugins: ["@babel/plugin-transform-modules-commonjs"],
  },
};

/*
  For dependencies that expect the value of `this` to be equivelant to
  the global `window` object (d3 specifically), we need Babel to process
  these dependencies as "script" rather than "module". Modules are run in
  "strict mode" where the value of a global `this` will be undefined where
  as in non-strict mode a global this will be equal to global window.

  Source type "unambiguous" will treat any file containing `import` or
  `export` as a module and any file without as a script (non-strict mode).
  See https://babeljs.io/docs/en/options#sourcetype for more details
*/
const sourceType = "unambiguous";

module.exports = { presets, plugins, env, sourceType };
