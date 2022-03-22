// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
  ["import", { "libraryName": "antd", "style": true }],
];

const env = {
  test: {
    plugins: ["@babel/plugin-transform-modules-commonjs"],
  }
}
module.exports = { presets, plugins, env };
