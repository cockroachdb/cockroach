'use strict;'

const FaviconsWebpackPlugin = require('favicons-webpack-plugin');
const HtmlWebpackPlugin = require('html-webpack-plugin');

const title = 'Cockroach Console';

module.exports = {
  entry: './src/index.tsx',
  output: {
    filename: 'bundle.js',
    path: __dirname + '/dist',
  },

  resolve: {
    // Add '.ts' and '.tsx' as resolvable extensions.
    extensions: ['.ts', '.tsx', '.js', '.json'],
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
};
