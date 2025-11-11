const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const CopyWebpackPlugin = require('copy-webpack-plugin');

module.exports = (env, argv) => {
  const isDevelopment = argv.mode === 'development';
  const outputDir = env.output || '../../../roachprod/ui/dist';
  // For Bazel builds, use fixed filename; for dev builds, use content hash
  const useBazel = env.bazel === 'true';

  return {
    entry: './src/index.tsx',
    output: {
      path: path.resolve(__dirname, outputDir),
      filename: useBazel ? 'bundle.js' : 'bundle.[contenthash].js',
      clean: true,
      publicPath: '/',
    },
    resolve: {
      extensions: ['.ts', '.tsx', '.js', '.jsx'],
      alias: {
        src: path.resolve(__dirname, 'src'),
      },
    },
    module: {
      rules: [
        {
          test: /\.tsx?$/,
          use: 'ts-loader',
          exclude: /node_modules/,
        },
        {
          test: /\.module\.scss$/,
          use: [
            'style-loader',
            {
              loader: 'css-loader',
              options: {
                modules: {
                  localIdentName: isDevelopment
                    ? '[name]__[local]--[hash:base64:5]'
                    : '[hash:base64]',
                },
              },
            },
            'sass-loader',
          ],
        },
        {
          test: /\.scss$/,
          exclude: /\.module\.scss$/,
          use: ['style-loader', 'css-loader', 'sass-loader'],
        },
        {
          test: /\.css$/,
          use: ['style-loader', 'css-loader'],
        },
      ],
    },
    plugins: [
      new HtmlWebpackPlugin({
        template: './public/index.html',
        title: 'Roachprod UI',
      }),
      new CopyWebpackPlugin([
        {
          from: 'public',
          to: '',
          ignore: ['index.html'],
        },
      ]),
    ],
    devServer: {
      port: 3000,
      hot: true,
      historyApiFallback: true,
      proxy: [
        {
          context: ['/api'],
          target: 'http://localhost:7763',
          changeOrigin: true,
        },
      ],
    },
    devtool: isDevelopment ? 'source-map' : false,
  };
};
