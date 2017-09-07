"use strict";

const path = require("path");
const webpack = require("webpack");

// tslint:disable:object-literal-sort-keys
module.exports = {
  entry: {
    protos: ["./src/js/protos"],
  },

  output: {
    filename: "protos.dll.js",
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
      path: path.resolve(__dirname, "protos-manifest.json"),
    }),
  ],
};
