'use strict;'

const path = require('path');
const rimraf = require('rimraf');

const FaviconsWebpackPlugin = require('favicons-webpack-plugin');
const HtmlWebpackPlugin = require('html-webpack-plugin');

const title = 'Cockroach Console';

// Remove a broken dependency that Yarn insists upon installing before every
// Webpack compile. We also do this when installing dependencies via Make, but
// it's common to run e.g. `yarn add` manually without re-running Make, which
// will reinstall the broken dependency. The error this dependency causes is
// horribly cryptic, so it's important to remove it aggressively.
//
// See: https://github.com/yarnpkg/yarn/issues/2987
class RemoveBrokenDependenciesPlugin {
  apply(compiler) {
    compiler.plugin("compile", () => rimraf.sync("./node_modules/@types/node"));
  }
}

module.exports = {
  entry: './src/index.tsx',
  output: {
    filename: 'bundle.js',
    path: __dirname + '/dist',
  },

  resolve: {
    // Add resolvable extensions.
    extensions: ['.ts', '.tsx', '.js', '.json', '.styl', '.css'],
    // Resolve modules from src directory or node_modules.
    // The 'path.resolve' is used to make these into absolute paths, meaning
    // that only the exact directory is checked. A relative path would follow
    // the resolution behavior used by node.js for "node_modules", which checks
    // for a "node_modules" child directory located in either the current
    // directory *or in any parent directory*.
    modules: [path.resolve(__dirname), path.resolve(__dirname, "node_modules")]
  },

  module: {
    rules: [
      { test: /\.css$/, use: [ 'style-loader', 'css-loader' ] },
      {
        test: /\.styl$/,
        use: [
          'style-loader',
          'css-loader',
          {
            loader: 'stylus-loader',
            options: {
              use: [require('nib')()],
            },
          },
        ],
      },
      {
        test: /\.(png|jpg|gif|svg|eot|ttf|woff|woff2)$/,
        loader: 'url-loader',
        options: {
          limit: 10000,
        },
      },
      { test: /\.html$/, loader: 'file-loader' },

      // TODO(tamird,mrtracy,spencerkimball): remove this when all our code is
      // written in TS.
      {
        test: /\.js$/,
        exclude: /node_modules/,
        loader: 'babel-loader',
      },

      { test: /\.tsx?$/, loaders: ['babel-loader', 'ts-loader'] },

      // All output '.js' files will have any sourcemaps re-processed by 'source-map-loader'.
      { enforce: 'pre', test: /\.js$/, loader: 'source-map-loader' },
    ],
  },

  plugins: [
    new RemoveBrokenDependenciesPlugin(),
    new FaviconsWebpackPlugin({
      logo: './logo.png',
      persistentCache: false,
      inject: true,
      title: title,
      icons: {
        // Must explicitly override defaults. Sigh.
        android: false,
        appleIcon: true,
        appleStartup: false,
        favicons: true,
        firefox: false,
      },
    }),
    new HtmlWebpackPlugin({
      title: title,
      template: require('html-webpack-template'),
      inject: false,
      appMountId: 'react-layout',
    }),
  ],

  // https://webpack.js.org/configuration/stats/
  stats: {
    colors: true,
    chunks: false,
  },
};
