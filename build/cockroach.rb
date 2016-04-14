require "language/go"

class Cockroach < Formula
  desc "Distributed SQL database"
  homepage "https://www.cockroachlabs.com"
  url "https://github.com/cockroachdb/cockroach.git",
      :tag => "beta-20160414",
      :revision => "852d6b6b3d3feec24f82ace7fc07e67a436d212c"
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

    # We use `xcrun make` instead of `make` to avoid homebrew mucking
    # with the HOMEBREW_CCCFG variable which in turn causes the C
    # compiler to behave in a way that is not supported by cgo.
    system "xcrun", "make", "GOFLAGS=-v", "-C",
           "src/github.com/cockroachdb/cockroach", "build"
    bin.install "src/github.com/cockroachdb/cockroach/cockroach" => "cockroach"

    # TODO(pmattis): Debug the launchctl stuff
    # (prefix+'com.cockroachlabs.cockroachdb.plist').write startup_plist
    # (prefix+'com.cockroachlabs.cockroachdb.plist').chmod 0644
  end

  def caveats
    <<-EOS.undent
    Start the cockroach server:
        cockroach start --store=#{var}/cockroach
    EOS
  end

  test do
    system "#{bin}/cockroach", "version"
  end

  #   def startup_plist
  #     return <<-EOS
  # <?xml version="1.0" encoding="UTF-8"?>
  # <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
  # <plist version="1.0">
  # <dict>
  #   <key>Label</key>
  #   <string>com.cockroachlabs.cockroachdb</string>
  #   <key>ProgramArguments</key>
  #   <array>
  #     <string>#{bin}/cockroach</string>
  #     <string>start</string>
  #   </array>
  #   <key>RunAtLoad</key>
  #   <true/>
  #   <key>KeepAlive</key>
  #   <false/>
  #   <key>UserName</key>
  #   <string>#{`whoami`.chomp}</string>
  #   <key>WorkingDirectory</key>
  #   <string>#{HOMEBREW_PREFIX}</string>
  #   <key>StandardErrorPath</key>
  #   <string>#{var}/log/cockroachdb/output.log</string>
  #   <key>StandardOutPath</key>
  #   <string>#{var}/log/cockroachdb/output.log</string>
  # </dict>
  # </plist>
  # EOS
  #   end
end
