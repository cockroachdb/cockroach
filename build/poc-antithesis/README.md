## Background

Antithesis Proof-of-Concept evaluates the capabilities of Antithesis' _deterministic_ testing platform which uses coverage-guided failure injection. To run experiments,
the platform requires an instrumented crdb binary, a workload generator, both dockerized, and a self-contained docker-compose configuration which creates a cluster
and runs a workload against it.

The following instructions are for building and deploying docker containers encapsulating an _instrumented_ crdb binary and a regular (i.e., uninstrumented)
crdb binary, used as a workload generator. Both binaries are built using custom Make targets. The instrumentation library is _not_ OSS, hence it's stored
in the private repository. The docker container used by the builder.sh is stored in GCR; see instructions below on how to updated it.
The build steps assume you're in possession of the zipfile containing the instrumentation binary (`goinstrumentor`), `go.mod`, `wrappers.go`
and a native shared library, `libvoidstar.so`.

The deploy steps package an instrumented binary, workload binary, and a docker-compose configuration along with bash scripts. The corresponding CI
scripts execute build and deploy steps, followed by tagging the resulting docker containers and pushing them both into Cockroach Labs GCR and Antithesis' docker repository.


## Build

### Install Antithesis Instrumentor

```
unzip antithesis_instrumentation*.zip -d /opt/antithesis/
cp /opt/antithesis/instrumentation/go/wrappers/wrappers.go build/poc-antithesis/
cp /opt/antithesis/instrumentation/go/wrappers/go.mod build/poc-antithesis/
cp /opt/antithesis/bin/goinstrumentor build/poc-antithesis/
cp /opt/antithesis/lib/libvoidstar.so build/poc-antithesis/
```

**NOTE**: `wrappers.go` contains CGO headers and linker flags. To simplify the build process, we assume that the shared library is installed at `/opt/antithesis/lib/`.

Update `wrappers.go` with `-L/opt/antithesis/lib/`. The first line comment after the copyright preamble should look like this,

```
// #cgo LDFLAGS: -lpthread -ldl -lc -lm -L/opt/antithesis/lib/ -lvoidstar
```

### Setup Env. Vars

```
export gcr_repository="gcr.io/cockroach-testeng-infra"
export docker_registry="gcr.io/cockroach-testeng-infra"
export build_tag=`git rev-parse --abbrev-ref HEAD`
```

**NOTE**: All subsequent steps assume the current directory is `poc-antithesis` (under `cockroach/build`).

### Authorize Docker GCR

In order to push docker containers into GCR, you will need to authorize docker,

```
gcloud auth configure-docker
```

**NOTE**: gce-worker project is already authorized to read/write from `gcr.io/cockroach-testeng-infra`. If you're building from your local workstation, then your GCP credentials
will be used; those may not be sufficient. If that's the case, please request help in the [`test-eng`](https://cockroachlabs.slack.com/archives/C023S0V4YEB) slack channel.


### Builder Image (Optional)

The builder image is rarely updated. Unless the following patch results in `Dockerfile` which differs
from the one in git, you probably don't need to rebuild the builder image; i.e., skip this step.


Apply patch to crdb builder which adds Antithesis instrumentor,

```
patch ../builder/Dockerfile Dockerfile.patch  -o Dockerfile
```

Build the docker container to encapsulate crdb builder,

```
docker build . --tag=cockroachdb/builder:antithesis-latest
```

Tag and Push,

```
docker tag cockroachdb/builder:antithesis-latest $gcr_repository/cockroachdb/builder:antithesis-latest
docker push $gcr_repository/cockroachdb/builder:antithesis-latest
```

### Build

Pull the latest builder image,

```
docker pull $gcr_repository/cockroachdb/builder:antithesis-latest
```

Apply patch to `builder.sh` which adds Antithesis instrumentor,

```
patch ../builder.sh builder.sh.patch  -o builder.sh
```

Run `make` inside the builder image,

```
./builder.sh mkrelease amd64-linux-gnu instrumentshort INSTRUMENTATION_TMP=/go/src/github.com/instrument
```

### Deploy

Copy artifacts into `build/deploy`,

```
./prepare-deploy.sh
```

Create instrumented crdb containter,

```
docker build \
  --no-cache \
  --tag="cockroachdb/cockroach-instrumented:$build_tag" \
  --memory 30g \
  --memory-swap -1 \
  -f deploy/Dockerfile_antithesis_instrumented \
  deploy
```

Create workload container,

```
docker build \
  --no-cache \
  --tag="cockroachdb/workload:$build_tag" \
  --memory 30g \
  --memory-swap -1 \
  -f deploy/Dockerfile_antithesis_workload \
  deploy
```

Create container with docker-compose config.,

```
docker build \
  --no-cache \
  --tag="cockroachdb/config:$build_tag" \
  --memory 30g \
  --memory-swap -1 \
  -f deploy/Dockerfile_antithesis_config \
  deploy
```

Tag and Push,

```
docker tag cockroachdb/cockroach-instrumented:$build_tag $gcr_repository/cockroachdb/cockroach-instrumented:$build_tag
docker tag cockroachdb/workload:$build_tag  $gcr_repository/cockroachdb/workload:$build_tag
docker tag cockroachdb/config:$build_tag  $gcr_repository/cockroachdb/config:$build_tag

docker push $gcr_repository/cockroachdb/cockroach-instrumented:$build_tag
docker push $gcr_repository/cockroachdb/workload:$build_tag
docker push $gcr_repository/cockroachdb/config:$build_tag
```

Verify the images have been properly tagged and pushed,

```
docker images
```

should display something like this,

```
REPOSITORY                                                                                                 TAG                               IMAGE ID       CREATED         SIZE
cockroachdb/config                                                                                         unexpected_commit_investigation   f75a85854266   7 minutes ago   3.58kB
gcr.io/cockroach-testeng-infra/cockroachdb/config                                                          unexpected_commit_investigation   f75a85854266   7 minutes ago   3.58kB
cockroachdb/workload                                                                                       unexpected_commit_investigation   153e9bbd779b   8 minutes ago   360MB
gcr.io/cockroach-testeng-infra/cockroachdb/workload                                                        unexpected_commit_investigation   153e9bbd779b   8 minutes ago   360MB
cockroachdb/cockroach-instrumented                                                                         unexpected_commit_investigation   acdb3d9242fe   9 minutes ago   459MB
gcr.io/cockroach-testeng-infra/cockroachdb/cockroach-instrumented                                          unexpected_commit_investigation   acdb3d9242fe   9 minutes ago   459MB
```

Test docker-compose locally,

```
docker-compose -f deploy/docker-compose.yml up
```

After verifying everyting works as expected, tear down docker-compose,
```
docker-compose -f deploy/docker-compose.yml down
```

### CI

To build and deploy from CI, use the following [job](https://teamcity.cockroachdb.com/buildConfiguration/Cockroach_ScratchProjectPutTcExperimentsInHere_AntithesisPoc). 

**NOTE**: if you're triggering the job from a PR branch, be sure to specify `env.BUILD_TAG`, otherwise it will attempt to detect the branch name and fall back to `HEAD` (github PRs don't expose a branch name).
