// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

const presets = [
  [
    "@babel/env",
    {
      "modules": "commonjs"
    },
  ],
  "@babel/react",
  [
    "@babel/typescript",
    {
      allowNamespaces: true,
    }
  ],
];

const plugins = [
  "@babel/proposal-class-properties",
  "@babel/proposal-object-rest-spread",
  // @babel/plugin-transform-runtime is required to support dynamic loading of cluster-ui package
  "@babel/plugin-transform-runtime",
];

const env = {
  test: {
    plugins: [
      [ "@babel/plugin-transform-runtime", { absoluteRuntime: true } ],
      "@babel/plugin-transform-modules-commonjs",
    ],
  }
}
module.exports = { presets, plugins, env };
