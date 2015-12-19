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

    # Move everything in the current directory (i.e. the cockroach
    # repo), except for .brew_home, to where it would reside in a
    # normal Go source layout.
    files = Dir.glob("*") + Dir.glob(".[a-z]*")
    files.delete(".brew_home")
    mkdir_p buildpath/"src/github.com/cockroachdb/cockroach"
    mv files, buildpath/"src/github.com/cockroachdb/cockroach"
    Language::Go.stage_deps resources, buildpath/"src"

    # We use `xcrun make` instead of `make` to avoid homebrew mucking
    # with the HOMEBREW_CCCFG variable which in turn causes the C
    # compiler to behave in a way that is not supported by cgo.
    #
    # TODO(peter): build/depvers is returning nothing. Figure out why.
    system "xcrun", "make", "-C",
           "src/github.com/cockroachdb/cockroach", "build",
           "GOFLAGS=-v", "SKIP_BOOTSTRAP=1"
    bin.install "src/github.com/cockroachdb/cockroach/cockroach" => "cockroach"
  end

  test do
    system "#{bin}/cockroach", "version"
  end

  # TODO(peter): Need to figure out how set cockroach to running.
end
