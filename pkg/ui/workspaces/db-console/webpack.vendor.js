// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

"use strict";

const path = require("path");
const webpack = require("webpack");

const pkg = require("./package.json");

const prodDependencies = Object.keys(pkg.dependencies);

// tslint:disable:object-literal-sort-keys
module.exports = (env, argv) => {
  const manifestPath = env && env.manifest;
  return {
    entry: {
      vendor: prodDependencies,
    },

    mode: "none",

    output: {
      filename: "vendor.oss.dll.js",
      path: path.resolve(__dirname, "dist"),
      library: "[name]_[hash]",
    },

    resolve: {
      modules: [ "node_modules" ],
    },

    plugins: [
      new webpack.DllPlugin({
        name: "[name]_[hash]",
        path:
          manifestPath || path.resolve(__dirname, "vendor.oss.manifest.json"),
      }),
    ],

    // Max size of is set to 4Mb to disable warning message and control
    // the growing size of bundle over time.
    performance: {
      maxEntrypointSize: 4000000,
      maxAssetSize: 4000000,
    },
  };
};
