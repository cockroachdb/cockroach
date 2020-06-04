### How to run regression visual tests
- start `movr` demo server with command and dev env
```
cockroach demo movr --nodes 4 --insecure
make ui-watch TARGET=http://localhost:{PORT}
```
- run tests
```
yarn cypress:run-visual-regression
```

#### To update test snapshots:
```
yarn cypress:update-snapshots
```
