const proxy = require('http-proxy-middleware');

const target = process.env.TARGET;

module.exports = function expressMiddleware(router) {
  if (target) {
    router.use('/_status', proxy.createProxyMiddleware({
      target,
      changeOrigin: true
    }))
  }
}
