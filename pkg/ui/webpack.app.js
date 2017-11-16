"use strict";

const path = require("path");
const rimraf = require("rimraf");
const webpack = require("webpack");

const ForkTsCheckerWebpackPlugin = require("fork-ts-checker-webpack-plugin");
const HtmlWebpackPlugin = require("html-webpack-plugin");

const title = "Cockroach Console";

// Remove a broken dependency that Yarn insists upon installing before every
// Webpack compile. We also do this when installing dependencies via Make, but
// it"s common to run e.g. `yarn add` manually without re-running Make, which
// will reinstall the broken dependency. The error this dependency causes is
// horribly cryptic, so it"s important to remove it aggressively.
//
// See: https://github.com/yarnpkg/yarn/issues/2987
class RemoveBrokenDependenciesPlugin {
  apply(compiler) {
    compiler.plugin("compile", () => rimraf.sync("./node_modules/@types/node"));
  }
}

let DashboardPlugin;
try {
  DashboardPlugin = require("./opt/node_modules/webpack-dashboard/plugin");
} catch (e) {
  DashboardPlugin = class { apply() { /* no-op */ } };
}

// tslint:disable:object-literal-sort-keys
module.exports = (distDir, ...additionalRoots) => ({
  entry: ["./src/index.tsx"],
  output: {
    filename: "bundle.js",
    path: path.resolve(__dirname, distDir),
  },

  resolve: {
    // Add resolvable extensions.
    extensions: [".ts", ".tsx", ".js", ".json", ".styl", ".css"],
    // Resolve modules from src directory or node_modules.
    // The "path.resolve" is used to make these into absolute paths, meaning
    // that only the exact directory is checked. A relative path would follow
    // the resolution behavior used by node.js for "node_modules", which checks
    // for a "node_modules" child directory located in either the current
    // directory *or in any parent directory*.
    modules: [
      ...additionalRoots,
      path.resolve(__dirname),
      path.resolve(__dirname, "node_modules"),
    ],
    alias: {oss: path.resolve(__dirname)},
  },

  module: {
    rules: [
      { test: /\.css$/, use: [ "style-loader", "css-loader" ] },
      {
        test: /\.styl$/,
        use: [
          "cache-loader",
          "style-loader",
          "css-loader",
          {
            loader: "stylus-loader",
            options: {
              use: [require("nib")()],
            },
          },
        ],
      },
      {
        test: /\.(png|jpg|gif|svg|eot|ttf|woff|woff2)$/,
        loader: "url-loader",
        options: {
          limit: 10000,
        },
      },
      { test: /\.html$/, loader: "file-loader" },
      {
        test: /\.js$/,
        include: [...additionalRoots, path.resolve(__dirname, "src")],
        use: ["cache-loader", "babel-loader"],
      },
      {
        test: /\.tsx?$/,
        include: [...additionalRoots, path.resolve(__dirname, "src")],
        use: [
          "cache-loader",
          "babel-loader",
          { loader: "ts-loader", options: { happyPackMode: true } },
        ],
      },

      // All output ".js" files will have any sourcemaps re-processed by "source-map-loader".
      { enforce: "pre", test: /\.js$/, loader: "source-map-loader" },
    ],
  },

  plugins: [
    new RemoveBrokenDependenciesPlugin(),
    new HtmlWebpackPlugin({
      title: title,
      template: require("html-webpack-template"),
      scripts: ["protos.dll.js", "vendor.dll.js"],
      favicon: "favicon.ico",
      inject: false,
      appMountId: "react-layout",
    }),
    // See "DLLs for speedy builds" in the README for details.
    new webpack.DllReferencePlugin({
      manifest: require("./protos-manifest.json"),
    }),
    new webpack.DllReferencePlugin({
      manifest: require("./vendor-manifest.json"),
    }),
    new DashboardPlugin(),
  ],

  // https://webpack.js.org/configuration/stats/
  stats: {
    colors: true,
    chunks: false,
  },

  devServer: {
    contentBase: path.join(__dirname, distDir),
    proxy: [{
      context: ["/_admin", "/_status", "/ts"],
      secure: false,
      target: process.env.TARGET,
    }],
  },
});
