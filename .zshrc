export GOPATH=~/go
export GOROOT="$(brew --prefix go)/libexec"
autoload -Uz compinit && compinit
alias repo='cd /Users/nasetlur/go/src/github.com/cockroachdb/cockroach'
alias zshell='vim ~/.zshrc'
alias create-cluster='roachprod create $CLUSTER; roachprod stage $CLUSTER workload'
alias start-cluster='roachprod start $CLUSTER'
alias stop-cluster='roachprod stop $CLUSTER'
alias destroy-cluster='roachprod destroy $CLUSTER'
alias put-build='roachprod put $CLUSTER artifacts/cockroach'
alias refresh='source ~/.zshrc'

# The next line updates PATH for the Google Cloud SDK.
if [ -f '/opt/homebrew/share/google-cloud-sdk/path.zsh.inc' ]; then . '/opt/homebrew/share/google-cloud-sdk/path.zsh.inc'; fi

# The next line enables shell command completion for gcloud.
if [ -f '/opt/homebrew/share/google-cloud-sdk/completion.zsh.inc' ]; then . '/opt/homebrew/share/google-cloud-sdk/completion.zsh.inc'; fi

export PATH="/Users/nasetlur/go/src/github.com/cockroachdb/cockroach/bin:/Users/nasetlur/go/src/github.com/cockroachdb/cockroach:$PATH"
export CLUSTER="naveensetlur-test"
export CLUSTER2="naveensetlur-test2"
export COCKROACH_DEV_LICENSE="crl-0-EI32gMgHGAEiI0NvY2tyb2FjaCBMYWJzIC0gUHJvZHVjdGlvbiBUZXN0aW5n"

### MANAGED BY RANCHER DESKTOP START (DO NOT EDIT)
export PATH="/Users/nasetlur/.rd/bin:$PATH"
### MANAGED BY RANCHER DESKTOP END (DO NOT EDIT)
