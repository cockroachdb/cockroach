// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

const path = require("path");
const webpack = require("webpack");
const WebpackBar = require("webpackbar");
const MomentLocalesPlugin = require("moment-locales-webpack-plugin");
const MomentTimezoneDataPlugin = require("moment-timezone-data-webpack-plugin")
const { ESBuildMinifyPlugin } = require("esbuild-loader");

const { CopyEmittedFilesPlugin } = require("./build/webpack/copyEmittedFilesPlugin");

const currentYear = new Date().getFullYear();

// tslint:disable:object-literal-sort-keys
module.exports = (env, argv) => {
  env = env || {};

  return {
    context: __dirname,
    name: "cluster-ui",
    entry: path.resolve(__dirname, "./src/index.ts"),

    output: {
      path: path.resolve(__dirname, "dist/js"),
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
          exclude: /node_modules/,
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
            "sass-loader",
          ],
        },
        // Ant design styles defined as global styles with .scss files which don't follow
        // internal convention to have .module.scss extension. That's why we need one more sass
        // loader which matches SCSS files without .module suffix. Note, it doesn't exclude node_modules
        // dir so it can access external dependencies.
        {
          test: /(?<!\.module)\.scss/,
          use: ["style-loader", "css-loader", "sass-loader"],
          exclude: /node_modules/,
        },
        {
          test: /\.(ts|js)x?$/,
          use: [
            {
              loader: "esbuild-loader",
              options: {
                loader: "tsx",
                target: "es6",
              },
            },
            {
              loader: "astroturf/loader",
              options: {extension: ".module.scss"},
            },
          ],
          exclude: [
            /node_modules/,
            /db-console\/src\/js/,
          ],
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
          exclude: [
            /node_modules/,
            /db-console\/src\/js/,
          ],
        },
        {
          test: /\.css$/,
          use: [
            "style-loader",
            "css-loader",
            {
              loader: "esbuild-loader",
              options: {
                loader: "css",
                minify: true,
              },
            },
          ],
          exclude: /node_modules/,
        },
      ],
    },

    optimization: {
      minimizer: [
        new ESBuildMinifyPlugin({
          target: "es6",
        }),
      ],
    },

    plugins: [
      new WebpackBar({
        name: "cluster-ui",
        color: "cyan",
        reporters: [ "basic" ],
        profile: true,
      }),
      new MomentLocalesPlugin(),
      // Use MomentTimezoneDataPlugin to remove timezone data that we don't need.
      new MomentTimezoneDataPlugin({
        matchZones: ['Etc/UTC', 'America/New_York'],
        startYear: 2021,
        endYear: currentYear + 10,
        // We have to tell the plugin where to store the pruned file
        // otherwise webpack can't find it.
        cacheDir: path.resolve(__dirname, "timezones"),
      }),

      // When requested with --env.copy-to=foo, copy all emitted files to
      // arbitrary destination(s). Note that multiple destinations are supported
      // but providing --env.copy-to multiple times at the command-line.
      // This plugin does nothing in one-shot (i.e. non-watch) builds, or when
      // no destinations are provided.
      new CopyEmittedFilesPlugin({
        destinations: (function() {
          const copyTo = env["copy-to"] || [];
          return typeof copyTo === "string"
            ? [copyTo]
            : copyTo;
        })(),
      }),
    ],

    // When importing a module whose path matches one of the following, just
    // assume a corresponding global variable exists and use that instead.
    // This is important because it allows us to avoid bundling all of our
    // dependencies, which allows browsers to cache those libraries between builds.
    externals: {
      protobufjs: "protobufjs",
      // Importing protobufjs/minimal resolves to the protobufjs module, but webpack's
      // "externals" checking appears to be based on string comparisons.
      "protobufjs/minimal": "protobufjs/minimal",
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
      "react-router": "react-router",
      "react-router-dom": "react-router-dom",
      "react-redux": "react-redux",
      "redux-saga": "redux-saga",
      "redux": "redux",
    },
  };
};
