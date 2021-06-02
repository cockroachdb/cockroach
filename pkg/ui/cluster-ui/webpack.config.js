// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

const path = require("path");
const webpack = require("webpack");
const WebpackBar = require("webpackbar");
const MomentLocalesPlugin = require("moment-locales-webpack-plugin");

module.exports = {
  entry: "./src/index.ts",

  output: {
    path: path.resolve(__dirname, "dist"),
    filename: "main.js",
    library: "adminUI",
    libraryTarget: "umd",
  },

  // Enable sourcemaps for debugging webpack's output.
  devtool: "source-map",

  resolve: {
    modules: [
      'node_modules',
      path.join(__dirname, 'src/fonts'),
    ],
    extensions: [".ts", ".tsx", ".js", ".jsx", ".less"],
    alias: {
      src: path.resolve(__dirname, "src"),
    },
  },

  module: {
    rules: [
      {
        test: /\.(png|jpg|gif|svg|eot|ttf|woff|woff2)$/,
        use: {
          loader: "url-loader",
          options: {
            limit: true,
          }
        },
      },
      // Styles in current project use SCSS preprocessing language with CSS modules.
      // They have to follow file naming convention: [filename].module.scss
      {
        test: /\.module\.scss$/,
        exclude: /node_modules/,
        use: [
          "style-loader",
          {
            loader: "css-loader",
            options: {
              modules: {
                localIdentName: "[local]--[hash:base64:5]",
              }
            },
          },
          "sass-loader"
        ],
      },
      // Ant design styles defined as global styles with .scss files which don't follow
      // internal convention to have .module.scss extension. That's why we need one more sass
      // loader which matches SCSS files without .module suffix. Note, it doesn't exclude node_modules
      // dir so it can access external dependencies.
      {
        test: /(?<!\.module)\.scss/,
        use: ["style-loader", "css-loader", "sass-loader"],
      },
      {
        test: /\.(ts|js)x?$/,
        use: [
          "babel-loader",
          {
            loader: "astroturf/loader",
            options: { extension: ".module.scss" },
          },
        ],
        exclude: /node_modules/,
      },
      // Preprocess LESS styles required by external components
      // (react-select)
      {
        use: [
          "style-loader",
          "css-loader",
          {
            loader: "less-loader",
            options: {
              lessOptions: {
                javascriptEnabled: true
              }
            }
          },
        ],
        test: /\.less$/,
      },
      // All output '.js' files will have any sourcemaps re-processed by 'source-map-loader'.
      {
        enforce: "pre",
        test: /\.js$/,
        loader: "source-map-loader",
      },
      { test: /\.css$/, use: [ "style-loader", "css-loader" ] },
    ],
  },

  plugins: [
    new WebpackBar({
      name: "cluster-ui",
      color: "cyan",
      profile: true,
    }),
    new MomentLocalesPlugin(),
    new webpack.NormalModuleReplacementPlugin(
      /node_modules\/antd\/lib\/style\/index\.less/,
      path.resolve(__dirname, "src/core/antd-patch.less"),
    ),
  ],

  // When importing a module whose path matches one of the following, just
  // assume a corresponding global variable exists and use that instead.
  // This is important because it allows us to avoid bundling all of our
  // dependencies, which allows browsers to cache those libraries between builds.
  externals: {
    protobufjs: "protobufjs",
    react: {
      commonjs: "react",
      commonjs2: "react",
      amd: "react",
      root: "React",
    },
    "react-dom": {
      commonjs: "react-dom",
      commonjs2: "react-dom",
      amd: "react-dom",
      root: "ReactDom",
    },
    "react-router-dom": "react-router-dom",
    "react-redux": "react-redux",
    "redux-saga": "redux-saga",
    "redux": "redux",
  },
};
