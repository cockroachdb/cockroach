
# How to run?

In one terminal, run a cluster:
```
./cockroach demo movr --nodes=9 --with-load --geo-partitioned-replicas --insecure --multitenant=false --log='file-defaults: {dir: /tmp/cockroach-logs}'
```

In another terminal run a reloading bazel command:
```
ibazel --run_output run //build/dev:cockroach-db-console -- --host=127.0.0.1:26357	
```

Then open the browser to `http://localhost:9080` and you should see the new DB Console.
If you `./dev build` it's also served at `http://localhost:8080/future`.

# Limitations

The HTTP client which requests certain pieces of data from CRDB (notably Databases page stuff) is hardcoded to localhost:8080 at the moment just because I haven't parameterized it. So it will work with the configuration above but not otherwise at the moment.

