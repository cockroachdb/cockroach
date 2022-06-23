# cockroach-go Testserver

The `testserver` package helps running cockroachDB binary with tests. It 
automatically downloads the latest stable cockroach binary for your runtimeOS 
(Linux-amd64 and Darwin-amd64 only for now), or attempts to run "cockroach" from your PATH.

### Example
To run the test server, call `NewTestServer(opts)` and with test server options. 

Here's an example of starting a test server without server options (i.e. in `Insecure` 
mode).

```go
  import "github.com/cockroachdb/cockroach-go/v2/testserver"
  import "testing"
  import "time"

  func TestRunServer(t *testing.T) {
     ts, err := testserver.NewTestServer()
     if err != nil {
       t.Fatal(err)
     }
     defer ts.Stop()

     db, err := sql.Open("postgres", ts.PGURL().String())
     if err != nil {
       t.Fatal(err)
     }
   }
```

**Note: please always use `testserver.NewTestServer()` to start a test server. Never use 
`testserver.Start()`.**

### Test Server Options

The default configuration is :

- in insecure mode, so not using TLS certificates to encrypt network;
- storing data to memory, with 20% of hard drive space assigned to the node;
- auto-downloading the latest stable release of cockroachDB.

You can also choose from the following options and pass them to `testserver.
NewTestServer()`.

- **Secure Mode**: run a secure multi-node cluster locally, using TLS certificates to encrypt network communication.
    See also https://www.cockroachlabs.com/docs/stable/secure-a-cluster.html.
  - Usage: ```NewTestServer(testserver.SecureOpt())```
- **Set Root User's Password**: set the given password for the root user for the
  PostgreSQL server, so to avoid having to use client certificates. This option can 
  only be passed under secure mode. 
  - Usage: ```NewTestServer(testserver.RootPasswordOpt
    (your_password))```
- **Store On Disk**: store the database to the local disk. By default, the database is 
  saved at `/tmp/cockroach-testserverxxxxxxxx`, with randomly generated `xxxxxxxx` 
  postfix. 
  - Usage: ```NewTestServer(testserver.StoreOnDiskOpt())```
- **Set Memory Allocation for Databse Storage**: set the maximum percentage of 
  total memory space assigned to store the database. 
  See also https://www.cockroachlabs. com/docs/stable/cockroach-start.html. 
  - Usage: 
    ```NewTestServer(testserver.SetStoreMemSizeOpt(0.3))```

### Test Server for Multi Tenants
The usage of test server as a tenant server is still under development. Please 
check `testserver/tenant.go` for more information.