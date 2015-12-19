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

    # We can't use build/depvers.sh in the homebrew environment
    # because dependencies have not had their git repos copied
    # over. But we already have all of the revision information
    # available, we just have to build up the same command line "make
    # build" would create.
    deps = format("%s:%s ", "github.com/cockroachdb/cockroach", active_spec.specs[:revision])
    deps += resources.grep(Resource::Go) do |resource|
      format("%s:%s", resource.name, resource.specs[:revision])
    end.sort.join(" ")
    tag = `git -C src/github.com/cockroachdb/cockroach describe --dirty --tags`.strip
    date = `date -u '+%Y/%m/%d %H:%M:%S'`.strip

    system "go", "build", "-v", "-o", bin/"cockroach", "-ldflags",
           "-X \"github.com/cockroachdb/cockroach/util.buildTag=#{tag}\"" \
           " -X \"github.com/cockroachdb/cockroach/util.buildTime=#{date}\"" \
           " -X \"github.com/cockroachdb/cockroach/util.buildDeps=#{deps}\"",
           "github.com/cockroachdb/cockroach"
  end

  test do
    system "#{bin}/cockroach", "version"
  end

  # TODO(peter): Need to figure out how set cockroach to running.
end
