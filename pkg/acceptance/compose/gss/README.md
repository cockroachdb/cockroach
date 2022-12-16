## Updating the `kdc`/`psql`/`python` images

- (One-time setup) Depending on how your Docker instance is configured, you may have to run `docker run --privileged --rm tonistiigi/binfmt --install all`. This will install `qemu` emulators on your system for platforms besides your native one.
- Build the image for both platforms and publish the cross-platform manifest. Note that the non-native build for your image will be very slow since it will have to emulate.
```
    ./build-push-gss.sh kdc
```
- Pass `python` or `psql` instead of `kdc` if that's what you need.
