# This script post-processes the output of go-bindata.

# TODO(benesch): this script would be much shorter and could fold into the
# Makefile if goimports could automatically add the pkg/ui import, but goimports
# currently can't handle files as large as bindata.go.

# Add comment recognized by Reviewable to the top of the file.
1i\
// GENERATED FILE DO NOT EDIT

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
