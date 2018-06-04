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
