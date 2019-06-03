// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
