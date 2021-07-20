#!/usr/bin/bash
# Without this `umask`, the image will create build artifacts that the host
# won't have permission to delete (which can pollute the workspace with files
# that can't be cleaned up).
umask 0000
exec "$@"
