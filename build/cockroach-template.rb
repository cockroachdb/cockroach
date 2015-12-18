require "language/go"

class Cockroach < Formula
  desc "Distributed SQL database"
  homepage "https://www.cockroachlabs.com"
  url "https://github.com/cockroachdb/cockroach.git",
      :tag => "@VERSION@",
      :revision => "@REVISION@"
  head "https://github.com/cockroachdb/cockroach.git"
  
  depends_on "go" => :build
  ### GO RESOURCES ###
  def install
    ENV["GOBIN"] = bin
    ENV["GOPATH"] = buildpath
    ENV["GOHOME"] = buildpath

    files = Dir.glob('*')
    mkdir_p buildpath/"src/github.com/cockroachdb/cockroach"
    mv files, buildpath/"src/github.com/cockroachdb/cockroach"
    Language::Go.stage_deps resources, buildpath/"src"
    
    # TODO(peter): We should be using the cockroach makefile so that
    # we can get the build information baked into the
    # binary. Unfortunately, this is dying with some weird dwarf
    # linker error:
    #
    #   make -C src/github.com/cockroachdb/cockroach build SKIP_BOOTSTRAP=1
    system "go", "build", "-v", "-o", bin/"cockroach", "github.com/cockroachdb/cockroach"
  end

  test do
    system "#{bin}/cockroach", "version"
  end

  # TODO(peter): Need to figure out how set cockroach to running.
end
