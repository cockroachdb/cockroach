require "language/go"

class Cockroach < Formula
  desc "Distributed SQL database"
  homepage "https://www.cockroachlabs.com"
  url "https://github.com/cockroachdb/cockroach.git",
      :tag => "v0.1-alpha",
      :revision => "26088f81e5ecfb2fd63f8f15f524102c9a0c1c05"
  head "https://github.com/cockroachdb/cockroach.git"

  depends_on "go" => :build

  def install
    # Move everything in the current directory (i.e. the cockroach
    # repo), except for .brew_home, to where it would reside in a
    # normal Go source layout.
    files = Dir.glob("*") + Dir.glob(".[a-z]*")
    files.delete(".brew_home")
    mkdir_p buildpath/"src/github.com/cockroachdb/cockroach"
    mv files, buildpath/"src/github.com/cockroachdb/cockroach"

    # The only go binary we need to install is glock.
    ENV["GOBIN"] = buildpath/"bin"
    ENV["GOPATH"] = buildpath
    ENV["GOHOME"] = buildpath

    # Install glock and all of our dependencies. We don't bother
    # installing commands (e.g. protoc) because we don't need them for
    # building the cockroach binary.
    system "go", "get", "github.com/robfig/glock"
    system "grep -v ^cmd src/github.com/cockroachdb/cockroach/GLOCKFILE | " \
           "bin/glock sync -n"

    # We use `xcrun make` instead of `make` to avoid homebrew mucking
    # with the HOMEBREW_CCCFG variable which in turn causes the C
    # compiler to behave in a way that is not supported by cgo.
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
