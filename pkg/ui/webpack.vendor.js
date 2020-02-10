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
module.exports = (env) => {
  return {
    entry: {
      vendor: prodDependencies.filter((dep) => {
        return (env.dist !== "oss" || dep !== "cockroachdb-protobuf-client-ccl")
      }),
    },

    output: {
      filename: "vendor.oss.dll.js",
      path: path.resolve(__dirname, "dist"),
      library: "[name]_[hash]",
    },

    mode: "none",

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
          //
          // davidh: Adding in the protobuf clients as of Jan/2020 to keep us from
          // having to add too much build infra to the protobuf scripts. Once the
          // Admin UI is managed separately we'll revisit this.
          exclude: /node_modules\/(?!((analytics-node)|(cockroachdb-protobuf-client-ccl)|(cockroachdb-protobuf-client-oss)))/,
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

    // Max size of is set to 4Mb to disable warning message and control
    // the growing size of bundle over time.
    performance: {
      maxEntrypointSize: 4000000,
      maxAssetSize: 4000000,
    },
  };
};
