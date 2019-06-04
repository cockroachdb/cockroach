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

const pkg = require("./package.json");

const prodDependencies = Object.keys(pkg.dependencies);

// tslint:disable:object-literal-sort-keys
module.exports = {
  entry: {
    vendor: prodDependencies,
  },

  output: {
    filename: "vendor.oss.dll.js",
    path: path.resolve(__dirname, "dist"),
    library: "[name]_[hash]",
  },

  resolve: {
    modules: [path.resolve(__dirname, "node_modules")],
  },

  module: {
    rules: [
      {
        test: /\.js$/,
        // analytics-node is an es6 module and we must run it through babel. As
        // of 8/25/2017 there appears to be no better way to run babel only on
        // the specific packages in node_modules that actually use ES6.
        // https://github.com/babel/babel-loader/issues/171
        exclude: /node_modules\/(?!analytics-node)/,
        use: ["cache-loader", "thread-loader", "babel-loader"],
      },
    ],
  },

  plugins: [
    new webpack.DllPlugin({
      name: "[name]_[hash]",
      path: path.resolve(__dirname, "vendor.oss.manifest.json"),
    }),
  ],
};
