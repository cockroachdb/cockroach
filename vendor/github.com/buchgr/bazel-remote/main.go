package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof" // Register pprof handlers with DefaultServeMux.
	"net/url"
	"os"
	"runtime"
	"strconv"

	auth "github.com/abbot/go-http-auth"
	"github.com/buchgr/bazel-remote/cache"
	"github.com/buchgr/bazel-remote/cache/disk"
	"github.com/buchgr/bazel-remote/cache/gcsproxy"
	"github.com/buchgr/bazel-remote/cache/s3proxy"

	"github.com/buchgr/bazel-remote/cache/httpproxy"

	"github.com/buchgr/bazel-remote/config"
	"github.com/buchgr/bazel-remote/server"
	"github.com/buchgr/bazel-remote/utils/idle"
	"github.com/buchgr/bazel-remote/utils/rlimit"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	httpmetrics "github.com/slok/go-http-metrics/metrics/prometheus"
	middleware "github.com/slok/go-http-metrics/middleware"
	middlewarestd "github.com/slok/go-http-metrics/middleware/std"
	"github.com/urfave/cli/v2"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	logFlags = log.Ldate | log.Ltime | log.LUTC
)

// gitCommit is the version stamp for the server. The value of this var
// is set through linker options.
var gitCommit string

func main() {

	log.SetFlags(logFlags)

	maybeGitCommitMsg := ""
	if len(gitCommit) > 0 && gitCommit != "{STABLE_GIT_COMMIT}" {
		maybeGitCommitMsg = fmt.Sprintf(" from git commit %s", gitCommit)
	}
	log.Printf("bazel-remote built with %s%s.",
		runtime.Version(), maybeGitCommitMsg)

	app := cli.NewApp()
	app.Description = "A remote build cache for Bazel."
	app.Usage = "A remote build cache for Bazel"
	app.HideVersion = true
	app.HideHelpCommand = true

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "config_file",
			Value: "",
			Usage: "Path to a YAML configuration file. If this flag is specified then all other flags " +
				"are ignored.",
			EnvVars: []string{"BAZEL_REMOTE_CONFIG_FILE"},
		},
		&cli.StringFlag{
			Name:    "dir",
			Value:   "",
			Usage:   "Directory path where to store the cache contents. This flag is required.",
			EnvVars: []string{"BAZEL_REMOTE_DIR"},
		},
		&cli.Int64Flag{
			Name:    "max_size",
			Value:   -1,
			Usage:   "The maximum size of the remote cache in GiB. This flag is required.",
			EnvVars: []string{"BAZEL_REMOTE_MAX_SIZE"},
		},
		&cli.StringFlag{
			Name:    "host",
			Value:   "",
			Usage:   "Address to listen on. Listens on all network interfaces by default.",
			EnvVars: []string{"BAZEL_REMOTE_HOST"},
		},
		&cli.IntFlag{
			Name:    "port",
			Value:   8080,
			Usage:   "The port the HTTP server listens on.",
			EnvVars: []string{"BAZEL_REMOTE_PORT"},
		},
		&cli.IntFlag{
			Name:    "grpc_port",
			Value:   9092,
			Usage:   "The port the gRPC server listens on. Set to 0 to disable.",
			EnvVars: []string{"BAZEL_REMOTE_GRPC_PORT"},
		},
		&cli.StringFlag{
			Name:    "profile_host",
			Value:   "127.0.0.1",
			Usage:   "A host address to listen on for profiling, if enabled by a valid --profile_port setting.",
			EnvVars: []string{"BAZEL_REMOTE_PROFILE_HOST"},
		},
		&cli.IntFlag{
			Name:        "profile_port",
			Value:       0,
			Usage:       "If a positive integer, serve /debug/pprof/* URLs from http://profile_host:profile_port.",
			DefaultText: "0, ie profiling disabled",
			EnvVars:     []string{"BAZEL_REMOTE_PROFILE_PORT"},
		},
		&cli.DurationFlag{
			Name:        "http_read_timeout",
			Value:       0,
			Usage:       "The HTTP read timeout for a client request in seconds (does not apply to the proxy backends or the profiling endpoint)",
			DefaultText: "0s, ie disabled",
			EnvVars:     []string{"BAZEL_REMOTE_HTTP_READ_TIMEOUT"},
		},
		&cli.DurationFlag{
			Name:        "http_write_timeout",
			Value:       0,
			Usage:       "The HTTP write timeout for a server response in seconds (does not apply to the proxy backends or the profiling endpoint)",
			DefaultText: "0s, ie disabled",
			EnvVars:     []string{"BAZEL_REMOTE_HTTP_WRITE_TIMEOUT"},
		},
		&cli.StringFlag{
			Name:    "htpasswd_file",
			Value:   "",
			Usage:   "Path to a .htpasswd file. This flag is optional. Please read https://httpd.apache.org/docs/2.4/programs/htpasswd.html.",
			EnvVars: []string{"BAZEL_REMOTE_HTPASSWD_FILE"},
		},
		&cli.BoolFlag{
			Name:    "tls_enabled",
			Usage:   "This flag has been deprecated. Specify tls_cert_file and tls_key_file instead.",
			EnvVars: []string{"BAZEL_REMOTE_TLS_ENABLED"},
		},
		&cli.StringFlag{
			Name:    "tls_ca_file",
			Value:   "",
			Usage:   "Optional. Enables mTLS (authenticating client certificates), should be the certificate authority that signed the client certificates.",
			EnvVars: []string{"BAZEL_REMOTE_TLS_CA_FILE"},
		},
		&cli.StringFlag{
			Name:    "tls_cert_file",
			Value:   "",
			Usage:   "Path to a pem encoded certificate file.",
			EnvVars: []string{"BAZEL_REMOTE_TLS_CERT_FILE"},
		},
		&cli.StringFlag{
			Name:    "tls_key_file",
			Value:   "",
			Usage:   "Path to a pem encoded key file.",
			EnvVars: []string{"BAZEL_REMOTE_TLS_KEY_FILE"},
		},
		&cli.DurationFlag{
			Name:        "idle_timeout",
			Value:       0,
			Usage:       "The maximum period of having received no request after which the server will shut itself down.",
			DefaultText: "0s, ie disabled",
			EnvVars:     []string{"BAZEL_REMOTE_IDLE_TIMEOUT"},
		},
		&cli.IntFlag{
			Name:    "max_queued_uploads",
			Value:   1000000,
			Usage:   "When using proxy backends, sets the maximum number of objects in queue for upload. If the queue is full, uploads will be skipped until the queue has space again.",
			EnvVars: []string{"BAZEL_REMOTE_MAX_QUEUED_UPLOADS"},
		},
		&cli.IntFlag{
			Name:    "num_uploaders",
			Value:   100,
			Usage:   "When using proxy backends, sets the number of Goroutines to process parallel uploads to backend.",
			EnvVars: []string{"BAZEL_REMOTE_NUM_UPLOADERS"},
		},
		&cli.StringFlag{
			Name:    "s3.endpoint",
			Value:   "",
			Usage:   "The S3/minio endpoint to use when using S3 proxy backend.",
			EnvVars: []string{"BAZEL_REMOTE_S3_ENDPOINT"},
		},
		&cli.StringFlag{
			Name:    "s3.bucket",
			Value:   "",
			Usage:   "The S3/minio bucket to use when using S3 proxy backend.",
			EnvVars: []string{"BAZEL_REMOTE_S3_BUCKET"},
		},
		&cli.StringFlag{
			Name:    "s3.prefix",
			Value:   "",
			Usage:   "The S3/minio object prefix to use when using S3 proxy backend.",
			EnvVars: []string{"BAZEL_REMOTE_S3_PREFIX"},
		},
		&cli.StringFlag{
			Name:    "s3.access_key_id",
			Value:   "",
			Usage:   "The S3/minio access key to use when using S3 proxy backend.",
			EnvVars: []string{"BAZEL_REMOTE_S3_ACCESS_KEY_ID"},
		},
		&cli.StringFlag{
			Name:    "s3.secret_access_key",
			Value:   "",
			Usage:   "The S3/minio secret access key to use when using S3 proxy backend.",
			EnvVars: []string{"BAZEL_REMOTE_S3_SECRET_ACCESS_KEY"},
		},
		&cli.BoolFlag{
			Name:        "s3.disable_ssl",
			Usage:       "Whether to disable TLS/SSL when using the S3 proxy backend.",
			DefaultText: "false, ie enable TLS/SSL",
			EnvVars:     []string{"BAZEL_REMOTE_S3_DISABLE_SSL"},
		},
		&cli.StringFlag{
			Name:    "s3.iam_role_endpoint",
			Value:   "",
			Usage:   "Endpoint for using IAM security credentials. By default it will look for credentials in the standard locations for the AWS platform.",
			EnvVars: []string{"BAZEL_REMOTE_S3_IAM_ROLE_ENDPOINT"},
		},
		&cli.StringFlag{
			Name:    "s3.region",
			Value:   "",
			Usage:   "The AWS region. Required when not specifying S3/minio access keys.",
			EnvVars: []string{"BAZEL_REMOTE_S3_REGION"},
		},
		&cli.IntFlag{
			Name:        "s3.key_version",
			Usage:       "Set to 1 for the legacy flat key format, or 2 for the newer format that reduces the impact of S3 rate limits.",
			Value:       1,
			DefaultText: "1",
			EnvVars:     []string{"BAZEL_REMOTE_S3_KEY_VERSION"},
		},
		&cli.BoolFlag{
			Name:        "disable_http_ac_validation",
			Usage:       "Whether to disable ActionResult validation for HTTP requests.",
			DefaultText: "false, ie enable validation",
			EnvVars:     []string{"BAZEL_REMOTE_DISABLE_HTTP_AC_VALIDATION"},
		},
		&cli.BoolFlag{
			Name:        "disable_grpc_ac_deps_check",
			Usage:       "Whether to disable ActionResult dependency checks for gRPC GetActionResult requests.",
			DefaultText: "false, ie enable ActionCache dependency checks",
			EnvVars:     []string{"BAZEL_REMOTE_DISABLE_GRPS_AC_DEPS_CHECK"},
		},
		&cli.BoolFlag{
			Name:        "enable_ac_key_instance_mangling",
			Usage:       "Whether to enable mangling ActionCache keys with non-empty instance names.",
			DefaultText: "false, ie disable mangling",
			EnvVars:     []string{"BAZEL_REMOTE_ENABLE_AC_KEY_INSTANCE_MANGLING"},
		},
		&cli.BoolFlag{
			Name:        "enable_endpoint_metrics",
			Usage:       "Whether to enable metrics for each HTTP/gRPC endpoint.",
			DefaultText: "false, ie disable metrics",
			EnvVars:     []string{"BAZEL_REMOTE_ENABLE_ENDPOINT_METRICS"},
		},
		&cli.BoolFlag{
			Name:        "experimental_remote_asset_api",
			Usage:       "Whether to enable the experimental remote asset API implementation.",
			DefaultText: "false, ie disable remote asset API",
			EnvVars:     []string{"BAZEL_REMOTE_EXPERIMENTAL_REMOTE_ASSET_API"},
		},
	}

	app.Action = func(ctx *cli.Context) error {
		configFile := ctx.String("config_file")
		var c *config.Config
		var err error
		if configFile != "" {
			c, err = config.NewFromYamlFile(configFile)
		} else {
			var s3 *config.S3CloudStorageConfig
			if ctx.String("s3.bucket") != "" {
				s3 = &config.S3CloudStorageConfig{
					Endpoint:        ctx.String("s3.endpoint"),
					Bucket:          ctx.String("s3.bucket"),
					Prefix:          ctx.String("s3.prefix"),
					AccessKeyID:     ctx.String("s3.access_key_id"),
					SecretAccessKey: ctx.String("s3.secret_access_key"),
					DisableSSL:      ctx.Bool("s3.disable_ssl"),
					IAMRoleEndpoint: ctx.String("s3.iam_role_endpoint"),
					Region:          ctx.String("s3.region"),
					KeyVersion:      ctx.Int("s3.key_version"),
				}
			}
			c, err = config.New(
				ctx.String("dir"),
				ctx.Int("max_size"),
				ctx.String("host"),
				ctx.Int("port"),
				ctx.Int("grpc_port"),
				ctx.String("profile_host"),
				ctx.Int("profile_port"),
				ctx.String("htpasswd_file"),
				ctx.Int("max_queued_uploads"),
				ctx.Int("num_uploaders"),
				ctx.String("tls_ca_file"),
				ctx.String("tls_cert_file"),
				ctx.String("tls_key_file"),
				ctx.Duration("idle_timeout"),
				s3,
				ctx.Bool("disable_http_ac_validation"),
				ctx.Bool("disable_grpc_ac_deps_check"),
				ctx.Bool("enable_ac_key_instance_mangling"),
				ctx.Bool("enable_endpoint_metrics"),
				ctx.Bool("experimental_remote_asset_api"),
				ctx.Duration("http_read_timeout"),
				ctx.Duration("http_write_timeout"),
			)
		}

		if err != nil {
			fmt.Fprintf(ctx.App.Writer, "%v\n\n", err)
			cli.ShowAppHelp(ctx)
			return cli.Exit("", 1)
		}

		if ctx.NArg() > 0 {
			fmt.Fprintf(ctx.App.Writer,
				"Error: bazel-remote does not take positional aguments\n")
			for i := 0; i < ctx.NArg(); i++ {
				fmt.Fprintf(ctx.App.Writer, "arg: %s\n", ctx.Args().Get(i))
			}
			fmt.Fprintf(ctx.App.Writer, "\n")

			cli.ShowAppHelp(ctx)
			return cli.Exit("", 1)
		}

		rlimit.Raise()

		accessLogger := log.New(os.Stdout, "", logFlags)
		errorLogger := log.New(os.Stderr, "", logFlags)

		var proxyCache cache.Proxy
		if c.GoogleCloudStorage != nil {
			proxyCache, err = gcsproxy.New(c.GoogleCloudStorage.Bucket,
				c.GoogleCloudStorage.UseDefaultCredentials, c.GoogleCloudStorage.JSONCredentialsFile,
				accessLogger, errorLogger, c.NumUploaders, c.MaxQueuedUploads)
			if err != nil {
				log.Fatal(err)
			}
		} else if c.HTTPBackend != nil {
			httpClient := &http.Client{}
			var baseURL *url.URL
			baseURL, err = url.Parse(c.HTTPBackend.BaseURL)
			if err != nil {
				log.Fatal(err)
			}
			proxyCache = httpproxy.New(baseURL,
				httpClient, accessLogger, errorLogger, c.NumUploaders, c.MaxQueuedUploads)
		} else if c.S3CloudStorage != nil {
			proxyCache = s3proxy.New(c.S3CloudStorage, accessLogger, errorLogger, c.NumUploaders, c.MaxQueuedUploads)
		}

		diskCache, err := disk.New(c.Dir, int64(c.MaxSize)*1024*1024*1024, proxyCache)
		if err != nil {
			log.Fatal(err)
		}

		var tlsConfig *tls.Config
		if len(c.TLSCaFile) != 0 {
			caCertPool := x509.NewCertPool()
			caCert, err := ioutil.ReadFile(c.TLSCaFile)
			if err != nil {
				log.Fatalf("Error reading TLS CA File: %v", err)
			}
			added := caCertPool.AppendCertsFromPEM(caCert)
			if !added {
				log.Fatalf("Failed to add certificate to cert pool.")
			}

			readCert, err := tls.LoadX509KeyPair(
				c.TLSCertFile,
				c.TLSKeyFile,
			)
			if err != nil {
				log.Fatalf("Error reading certificate/key pair: %v", err)
			}

			tlsConfig = &tls.Config{
				Certificates: []tls.Certificate{readCert},
				ClientCAs:    caCertPool,
				ClientAuth:   tls.RequireAndVerifyClientCert,
			}
		} else if len(c.TLSCertFile) != 0 && len(c.TLSKeyFile) != 0 {
			readCert, err := tls.LoadX509KeyPair(
				c.TLSCertFile,
				c.TLSKeyFile,
			)
			if err != nil {
				log.Fatalf("Error reading certificate/key pair: %v", err)
			}

			tlsConfig = &tls.Config{
				Certificates: []tls.Certificate{readCert},
			}
		}

		mux := http.NewServeMux()
		httpServer := &http.Server{
			Addr:         c.Host + ":" + strconv.Itoa(c.Port),
			Handler:      mux,
			ReadTimeout:  c.HTTPReadTimeout,
			TLSConfig:    tlsConfig,
			WriteTimeout: c.HTTPWriteTimeout,
		}

		validateAC := !c.DisableHTTPACValidation
		h := server.NewHTTPCache(diskCache, accessLogger, errorLogger, validateAC, c.EnableACKeyInstanceMangling, gitCommit)

		var htpasswdSecrets auth.SecretProvider
		cacheHandler := h.CacheHandler
		if c.HtpasswdFile != "" {
			htpasswdSecrets = auth.HtpasswdFileProvider(c.HtpasswdFile)
			cacheHandler = wrapAuthHandler(cacheHandler, htpasswdSecrets, c.Host)
		}

		var idleTimer *idle.Timer
		if c.IdleTimeout > 0 {
			idleTimer = idle.NewTimer(c.IdleTimeout)
			cacheHandler = wrapIdleHandler(cacheHandler, idleTimer, accessLogger, httpServer)
		}

		acKeyManglingStatus := "disabled"
		if c.EnableACKeyInstanceMangling {
			acKeyManglingStatus = "enabled"
		}
		log.Println("Mangling non-empty instance names with AC keys:", acKeyManglingStatus)

		if c.EnableEndpointMetrics {
			metricsMdlw := middleware.New(middleware.Config{
				Recorder: httpmetrics.NewRecorder(httpmetrics.Config{
					DurationBuckets: c.MetricsDurationBuckets,
				}),
			})
			mux.Handle("/metrics", middlewarestd.Handler("metrics", metricsMdlw, promhttp.Handler()))
			mux.Handle("/status", middlewarestd.Handler("status", metricsMdlw, http.HandlerFunc(h.StatusPageHandler)))
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				middlewarestd.Handler(r.Method, metricsMdlw, http.HandlerFunc(cacheHandler)).ServeHTTP(w, r)
			})
		} else {
			mux.HandleFunc("/status", h.StatusPageHandler)
			mux.HandleFunc("/", cacheHandler)
		}

		if c.GRPCPort > 0 {

			if c.GRPCPort == c.Port {
				log.Fatalf("Error: gRPC and HTTP ports (%d) conflict", c.Port)
			}

			go func() {
				addr := c.Host + ":" + strconv.Itoa(c.GRPCPort)

				opts := []grpc.ServerOption{}
				streamInterceptors := []grpc.StreamServerInterceptor{}
				unaryInterceptors := []grpc.UnaryServerInterceptor{}

				if c.EnableEndpointMetrics {
					streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
					unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
					grpc_prometheus.EnableHandlingTimeHistogram(grpc_prometheus.WithHistogramBuckets(c.MetricsDurationBuckets))
				}

				if tlsConfig != nil {
					opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
				}

				if htpasswdSecrets != nil {
					gba := server.NewGrpcBasicAuth(htpasswdSecrets)
					streamInterceptors = append(streamInterceptors, gba.StreamServerInterceptor)
					unaryInterceptors = append(unaryInterceptors, gba.UnaryServerInterceptor)
				}

				if idleTimer != nil {
					it := server.NewGrpcIdleTimer(idleTimer)
					streamInterceptors = append(streamInterceptors, it.StreamServerInterceptor)
					unaryInterceptors = append(unaryInterceptors, it.UnaryServerInterceptor)
				}

				opts = append(opts, grpc.ChainStreamInterceptor(streamInterceptors...))
				opts = append(opts, grpc.ChainUnaryInterceptor(unaryInterceptors...))

				log.Printf("Starting gRPC server on address %s", addr)

				validateAC := !c.DisableGRPCACDepsCheck
				validateStatus := "disabled"
				if validateAC {
					validateStatus = "enabled"
				}
				log.Println("gRPC AC dependency checks:", validateStatus)

				enableRemoteAssetAPI := c.ExperimentalRemoteAssetAPI
				remoteAssetStatus := "disabled"
				if enableRemoteAssetAPI {
					remoteAssetStatus = "enabled"
				}
				log.Println("experimental gRPC remote asset API:", remoteAssetStatus)

				err3 := server.ListenAndServeGRPC(addr, opts,
					validateAC,
					c.EnableACKeyInstanceMangling,
					enableRemoteAssetAPI,
					diskCache, accessLogger, errorLogger)
				if err3 != nil {
					log.Fatal(err3)
				}
			}()
		}

		if c.ProfilePort > 0 {
			go func() {
				// Allow access to /debug/pprof/ URLs.
				profileAddr := c.ProfileHost + ":" +
					strconv.Itoa(c.ProfilePort)
				log.Printf("Starting HTTP server for profiling on address %s",
					profileAddr)
				log.Fatal(http.ListenAndServe(profileAddr, nil))
			}()
		}

		validateStatus := "disabled"
		if validateAC {
			validateStatus = "enabled"
		}

		if len(c.TLSCertFile) > 0 && len(c.TLSKeyFile) > 0 {
			log.Printf("Starting HTTPS server on address %s", httpServer.Addr)
			log.Println("HTTP AC validation:", validateStatus)
			return httpServer.ListenAndServeTLS(c.TLSCertFile, c.TLSKeyFile)
		}

		if idleTimer != nil {
			log.Printf("Starting idle timer with value %v", c.IdleTimeout)
			idleTimer.Start()
		}

		log.Printf("Starting HTTP server on address %s", httpServer.Addr)
		log.Println("HTTP AC validation:", validateStatus)
		return httpServer.ListenAndServe()
	}

	serverErr := app.Run(os.Args)
	if serverErr != nil {
		log.Fatal("bazel-remote terminated:", serverErr)
	}
}

func wrapIdleHandler(handler http.HandlerFunc, idleTimer *idle.Timer, accessLogger cache.Logger, httpServer *http.Server) http.HandlerFunc {

	tearDown := make(chan struct{})
	idleTimer.Register(tearDown)

	go func() {
		<-tearDown
		accessLogger.Printf("Shutting down after idle timeout")
		httpServer.Shutdown(context.Background())
	}()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idleTimer.ResetTimer()
		handler(w, r)
	})
}

func wrapAuthHandler(handler http.HandlerFunc, secrets auth.SecretProvider, host string) http.HandlerFunc {
	authenticator := auth.NewBasicAuthenticator(host, secrets)
	return auth.JustCheck(authenticator, handler)
}
