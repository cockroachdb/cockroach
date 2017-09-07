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
    filename: "vendor.dll.js",
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
      path: path.resolve(__dirname, "vendor-manifest.json"),
    }),
  ],
};
