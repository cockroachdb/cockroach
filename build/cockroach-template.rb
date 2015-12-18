require "language/go"

class Cockroach < Formula
  desc "Distributed SQL database"
  homepage "https://www.cockroachlabs.com"
  url "https://github.com/cockroachdb/cockroach/archive/v@VERSION@.tar.gz"
  version "@VERSION@"
  sha256 "@SHA256@"

  head do
    url "https://github.com/cockroachdb/cockroach.git"
  end

  depends_on "go" => :build
  ### GO RESOURCES ###
  def install
    ENV["GOBIN"] = bin
    ENV["GOPATH"] = buildpath
    ENV["GOHOME"] = buildpath

    mkdir_p buildpath/"src/github.com/cockroachdb/"
    ln_sf buildpath, buildpath/"src/github.com/cockroachdb/cockroach"
    # The build environment is weird in that we symlinked the
    # cockroach source to the correct location on the line above. But
    # cgo needs to find the location of the various c-* packages. This
    # is done via relative paths but those paths step up out of the
    # cockroach symlink so we need to help the build tool and place
    # symlinks to the c-* packages in the location it expects them.
    ln_sf buildpath/"src/github.com/cockroachdb/c-lz4", buildpath/".."
    ln_sf buildpath/"src/github.com/cockroachdb/c-protobuf", buildpath/".."
    ln_sf buildpath/"src/github.com/cockroachdb/c-rocksdb", buildpath/".."
    ln_sf buildpath/"src/github.com/cockroachdb/c-snappy", buildpath/".."
    Language::Go.stage_deps resources, buildpath/"src"

    # TODO(peter): We should be using the cockroach makefile so that
    # we can get the build information baked into the binary.
    system "go", "build", "-v", "-o", bin/"cockroach", "main.go"
  end

  test do
    system "#{bin}/cockroach"
  end

  # TODO(peter): Need to figure out how set cockroach to running.
end
