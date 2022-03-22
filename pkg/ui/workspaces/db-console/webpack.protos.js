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

// tslint:disable:object-literal-sort-keys
module.exports = (env) => ({
  entry: {
    protos: [env.dist === "ccl" ? "./ccl/src/js/protos" : "./src/js/protos"],
  },

  mode: "none",

  output: {
    filename: `protos.${env.dist}.dll.js`,
    path: path.resolve(__dirname, "dist"),
    library: "[name]_[hash]",
  },

  module: {
    rules: [
      {
        test: /\.js$/,
        use: ["cache-loader", "thread-loader", "babel-loader"],
      },
    ],
  },

  plugins: [
    new webpack.DllPlugin({
      name: "[name]_[hash]",
      path: path.resolve(__dirname, `protos.${env.dist}.manifest.json`),
    }),
  ],

  // Max size of is set to 4Mb to disable warning message and control
  // the growing size of bundle over time.
  performance: {
    maxEntrypointSize: 4000000,
    maxAssetSize: 4000000,
  },
});
