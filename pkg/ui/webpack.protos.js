// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

"use strict";

const path = require("path");
const webpack = require("webpack");

// tslint:disable:object-literal-sort-keys
module.exports = (env) => ({
  entry: {
    protos: [env.dist === "ccl" ? "./ccl/src/js/protos" : "./src/js/protos"],
  },

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
});
