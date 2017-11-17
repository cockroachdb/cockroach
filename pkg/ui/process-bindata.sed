# This script post-processes the output of go-bindata to install an init hook
# that injects the embedded assets from the distccl or distoss package into the
# ui package.

# NB: We can't permanently add the init hook to the distccl and distoss 
# packages, as convenient as that would be, as those packages must be buildable
# when bindata.go has not been generated, and the init hooks reference functions
# that are not available until bindata.go has been generated. You might think
# that distccl need not be buildable when building the OSS binary, for example,
# but `go test pkg/...` matches every package in the pkg directory and has no
# facility for exclusions. We could, of course, instead require generating both
# the CCL and OSS UI before running `go test pkg/...`, but that would slow Go
# developers down tremendously when the Go tests do not in fact test the UI.

# TODO(benesch): this script would be much shorter and could fold into the
# Makefile if goimports could automatically add the pkg/ui import, but goimports
# currently can't handle files as large as bindata.go.

# Add pkg/ui import statement in import block.
/^import (/a\
\	"github.com/cockroachdb/cockroach/pkg/ui"

# Add init function to install hooks at the bottom of file.
$a\
func init() {\
\	ui.Asset = Asset\
\	ui.AssetDir = AssetDir\
\	ui.AssetInfo = AssetInfo\
}
