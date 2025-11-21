#!/bin/bash
# Script to generate and publish code coverage report for a Go package
#
# Usage:
#   ./generate_coverage.sh <github-username> <package-path> [--push]
#
# Arguments:
#   github-username  Your GitHub username (e.g., wenyihu6) - REQUIRED
#   package-path     Package path to test (e.g., pkg/kv/kvserver/allocator/mmaprototype) - REQUIRED
#
# Options:
#   --push           Automatically commit and push the coverage report to the current branch
#
# Examples:
#   # For mmaprototype package (from this directory):
#   ./generate_coverage.sh wenyihu6 pkg/kv/kvserver/allocator/mmaprototype --push
#   
#   # For any other package (from repository root):
#   ./pkg/kv/kvserver/allocator/mmaprototype/generate_coverage.sh wenyihu6 pkg/some/other/package --push

set -e  # Exit on error

# Find and change to repository root (needed for git and bazel commands)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR" && git rev-parse --show-toplevel 2>/dev/null || echo "")"

if [[ -z "$REPO_ROOT" ]]; then
    echo "Error: Not inside a git repository"
    exit 1
fi

cd "$REPO_ROOT"
echo "Working from repository root: $REPO_ROOT"
echo ""

# Parse arguments
PUSH=false
GITHUB_USERNAME=""
PACKAGE_PATH=""

for arg in "$@"; do
    if [[ "$arg" == "--push" ]]; then
        PUSH=true
    elif [[ "$arg" == *"/"* ]] && [[ -z "$PACKAGE_PATH" ]]; then
        # Argument contains slash, likely a package path
        PACKAGE_PATH="$arg"
    elif [[ -z "$GITHUB_USERNAME" ]]; then
        GITHUB_USERNAME="$arg"
    fi
done

# Check if GitHub username was provided
if [[ -z "$GITHUB_USERNAME" ]]; then
    echo "Error: GitHub username is required"
    echo "Usage: ./generate_coverage.sh <github-username> <package-path> [--push]"
    echo "Example: ./generate_coverage.sh wenyihu6 pkg/kv/kvserver/allocator/mmaprototype --push"
    exit 1
fi

# Check if package path was provided
if [[ -z "$PACKAGE_PATH" ]]; then
    echo "Error: Package path is required"
    echo "Usage: ./generate_coverage.sh <github-username> <package-path> [--push]"
    echo "Example: ./generate_coverage.sh wenyihu6 pkg/kv/kvserver/allocator/mmaprototype --push"
    exit 1
fi

# Remove leading/trailing slashes from package path
PACKAGE_PATH="${PACKAGE_PATH#/}"
PACKAGE_PATH="${PACKAGE_PATH%/}"

# Extract package name from path (last component)
PACKAGE_NAME=$(basename "$PACKAGE_PATH")

echo "========================================"
echo "Generating Code Coverage Report"
echo "GitHub username: $GITHUB_USERNAME"
echo "Package: $PACKAGE_PATH"
echo "========================================"

# Verify lcov and genhtml are available
if ! command -v lcov &> /dev/null; then
    echo "Error: lcov is not installed"
    echo "Install with: brew install lcov"
    exit 1
fi

if ! command -v genhtml &> /dev/null; then
    echo "Error: genhtml is not installed (part of lcov package)"
    echo "Install with: brew install lcov"
    exit 1
fi

# Step 1: Find test targets
echo ""
echo "Step 1/6: Finding test targets in package..."
TEST_TARGETS=$(bazel query "kind('.*_test', //${PACKAGE_PATH}:*)" 2>/dev/null || echo "")

if [[ -z "$TEST_TARGETS" ]]; then
    echo "Error: No test targets found in //${PACKAGE_PATH}"
    echo "Trying to find available targets..."
    bazel query "//${PACKAGE_PATH}:*" 2>/dev/null || true
    exit 1
fi

echo "Found test targets:"
echo "$TEST_TARGETS"

# Use the first test target found
TEST_TARGET=$(echo "$TEST_TARGETS" | head -1)
echo ""
echo "Using test target: $TEST_TARGET"

# Step 2: Run Bazel coverage
echo ""
echo "Step 2/6: Running Bazel coverage tests..."
echo "Note: Using --instrumentation_filter=//pkg/... to ensure coverage data is collected"
echo "This will be filtered to $PACKAGE_PATH in the next step"
echo ""

if ! bazel coverage --combined_report=lcov --instrumentation_filter=//pkg/... "$TEST_TARGET"; then
    echo ""
    echo "Error: Bazel coverage command failed"
    echo "This could be due to:"
    echo "  1. Test failures"
    echo "  2. Build errors"
    echo "  3. Missing dependencies"
    echo ""
    echo "Try running the test directly first:"
    echo "  bazel test $TEST_TARGET"
    exit 1
fi

# Step 3: Find and filter the coverage report
echo ""
echo "Step 3/6: Filtering coverage data for $PACKAGE_NAME package..."
COVERAGE_FILE=$(find /private/var/tmp/_bazel_$USER -name "_coverage_report.dat" -path "*/bazel-out/_coverage/*" -mtime -1 | head -1)

if [[ -z "$COVERAGE_FILE" ]]; then
    echo "Error: Could not find coverage report file"
    echo "Searched in: /private/var/tmp/_bazel_$USER"
    exit 1
fi

echo "Found coverage file: $COVERAGE_FILE"

# Check if file has data
FILE_SIZE=$(stat -f%z "$COVERAGE_FILE" 2>/dev/null || stat -c%s "$COVERAGE_FILE" 2>/dev/null || echo "0")
if [[ "$FILE_SIZE" -lt 100 ]]; then
    echo "Warning: Coverage file is very small ($FILE_SIZE bytes)"
    echo "This might indicate no coverage was collected"
fi

# Extract coverage for the specific package
# First, verify the files exist in the coverage data
echo "Verifying package files exist in coverage data..."
FOUND_FILES=$(grep "^SF:" "$COVERAGE_FILE" | grep "$PACKAGE_PATH" | wc -l | tr -d ' ')
if [[ "$FOUND_FILES" -eq 0 ]]; then
    echo "Error: No files found for package $PACKAGE_PATH in coverage data"
    echo ""
    echo "Available packages with 'allocator' in the path:"
    grep "^SF:" "$COVERAGE_FILE" | grep allocator | head -20 || true
    exit 1
fi
echo "Found $FOUND_FILES source files for $PACKAGE_PATH"

# Extract using multiple pattern attempts (lcov pattern matching can be finicky)
echo "Extracting coverage data..."
EXTRACT_SUCCESS=false

# Try pattern 1: With wildcards
if lcov --extract "$COVERAGE_FILE" "*/${PACKAGE_PATH}/*" --output-file coverage_${PACKAGE_NAME}.lcov --ignore-errors empty,unused 2>/dev/null; then
    if [[ -s coverage_${PACKAGE_NAME}.lcov ]]; then
        EXTRACT_SUCCESS=true
        echo "✓ Extracted using pattern: */${PACKAGE_PATH}/*"
    fi
fi

# Try pattern 2: Without leading wildcard
if [[ "$EXTRACT_SUCCESS" == "false" ]]; then
    if lcov --extract "$COVERAGE_FILE" "${PACKAGE_PATH}/*" --output-file coverage_${PACKAGE_NAME}.lcov --ignore-errors empty,unused 2>/dev/null; then
        if [[ -s coverage_${PACKAGE_NAME}.lcov ]]; then
            EXTRACT_SUCCESS=true
            echo "✓ Extracted using pattern: ${PACKAGE_PATH}/*"
        fi
    fi
fi

# Try pattern 3: Exact match with pkg prefix
if [[ "$EXTRACT_SUCCESS" == "false" ]]; then
    if lcov --extract "$COVERAGE_FILE" "pkg/${PACKAGE_PATH#pkg/}/*" --output-file coverage_${PACKAGE_NAME}.lcov --ignore-errors empty,unused 2>/dev/null; then
        if [[ -s coverage_${PACKAGE_NAME}.lcov ]]; then
            EXTRACT_SUCCESS=true
            echo "✓ Extracted using pattern: pkg/${PACKAGE_PATH#pkg/}/*"
        fi
    fi
fi

if [[ "$EXTRACT_SUCCESS" == "false" ]]; then
    echo "Error: Failed to extract coverage data with all attempted patterns"
    echo ""
    echo "Attempted patterns:"
    echo "  1. */${PACKAGE_PATH}/*"
    echo "  2. ${PACKAGE_PATH}/*"
    echo "  3. pkg/${PACKAGE_PATH#pkg/}/*"
    echo ""
    echo "Files in coverage data for this package:"
    grep "^SF:" "$COVERAGE_FILE" | grep "$PACKAGE_PATH" || true
    exit 1
fi

# Verify the extracted file has data
EXTRACTED_FILES=$(grep "^SF:" coverage_${PACKAGE_NAME}.lcov 2>/dev/null | wc -l | tr -d ' ')
echo "Successfully extracted coverage for $EXTRACTED_FILES files"

# Step 4: Generate HTML report
echo ""
echo "Step 4/6: Generating HTML report..."
if ! genhtml coverage_${PACKAGE_NAME}.lcov --output-directory coverage_html; then
    echo "Error: Failed to generate HTML report"
    exit 1
fi

# Print coverage summary
echo ""
echo "========================================"
echo "Coverage Summary:"
echo "========================================"
lcov --summary coverage_${PACKAGE_NAME}.lcov

# Step 5: Optionally commit and push
if [[ "$PUSH" == "true" ]]; then
    echo ""
    echo "Step 5/6: Committing coverage report..."
    git add coverage_html/ coverage_${PACKAGE_NAME}.lcov
    
    if git diff --cached --quiet; then
        echo "No changes to commit (coverage report unchanged)"
    else
        git commit -m "Update code coverage report for $PACKAGE_NAME package"
        
        echo ""
        echo "Step 6/6: Pushing to remote..."
        
        # Find the remote that matches the GitHub username
        REMOTE=""
        echo "Looking for remote matching GitHub username: $GITHUB_USERNAME"
        # Try to find a remote with the username in the URL
        for remote in $(git remote); do
            remote_url=$(git remote get-url "$remote" 2>/dev/null || echo "")
            if [[ "$remote_url" =~ github\.com[:/]${GITHUB_USERNAME}/ ]]; then
                REMOTE="$remote"
                echo "✓ Found remote matching username $GITHUB_USERNAME: $REMOTE ($remote_url)"
                break
            fi
        done
        
        # If no remote matches username, error out with helpful message
        if [[ -z "$REMOTE" ]]; then
            echo ""
            echo "Error: No git remote found matching username '$GITHUB_USERNAME'"
            echo ""
            echo "Available remotes:"
            git remote -v
            echo ""
            echo "Please ensure you have a remote pointing to your fork:"
            echo "  git remote add $GITHUB_USERNAME git@github.com:$GITHUB_USERNAME/cockroach.git"
            echo ""
            echo "Or push manually to the correct remote:"
            echo "  git push <your-remote-name> HEAD"
            exit 1
        fi
        
        echo "Pushing to remote: $REMOTE"
        if ! git push "$REMOTE" HEAD; then
            echo ""
            echo "Error: Failed to push to $REMOTE"
            echo ""
            echo "Available remotes:"
            git remote -v
            echo ""
            echo "You may need to push manually:"
            echo "  git push $REMOTE HEAD"
            exit 1
        fi
        echo "✓ Successfully pushed to $REMOTE"
    fi
else
    echo ""
    echo "Step 5/6: Skipping commit (run with --push to commit and push)"
    echo ""
    
    # Find the best remote for manual push instructions
    SUGGESTED_REMOTE=""
    for remote in $(git remote); do
        remote_url=$(git remote get-url "$remote" 2>/dev/null || echo "")
        if [[ "$remote_url" =~ github\.com[:/]${GITHUB_USERNAME}/ ]]; then
            SUGGESTED_REMOTE="$remote"
            break
        fi
    done
    
    if [[ -z "$SUGGESTED_REMOTE" ]]; then
        SUGGESTED_REMOTE="<your-remote-name>"
    fi
    
    echo "To manually commit and push:"
    echo "  git add coverage_html/ coverage_${PACKAGE_NAME}.lcov"
    echo "  git commit -m 'Update code coverage report for $PACKAGE_NAME package'"
    echo "  git push $SUGGESTED_REMOTE HEAD"
    
    if [[ "$SUGGESTED_REMOTE" == "<your-remote-name>" ]]; then
        echo ""
        echo "Note: No remote found for $GITHUB_USERNAME. Available remotes:"
        git remote -v | sed 's/^/  /'
    fi
fi

# Step 6: Display public URL
echo ""
echo "========================================"
echo "✅ Coverage report generated successfully!"
echo "========================================"
echo ""
echo "Local report: file://$(pwd)/coverage_html/index.html"
echo ""

BRANCH=$(git branch --show-current)
if [[ -n "$BRANCH" ]]; then
    if [[ "$PUSH" == "true" ]]; then
        echo "Public URL:"
        echo "https://htmlpreview.github.io/?https://github.com/$GITHUB_USERNAME/cockroach/blob/$BRANCH/coverage_html/index.html"
        echo ""
        echo "Direct link to $PACKAGE_NAME coverage:"
        echo "https://htmlpreview.github.io/?https://github.com/$GITHUB_USERNAME/cockroach/blob/$BRANCH/coverage_html/${PACKAGE_NAME}/index.html"
    else
        echo "After pushing to remote, the public URL will be:"
        echo "https://htmlpreview.github.io/?https://github.com/$GITHUB_USERNAME/cockroach/blob/$BRANCH/coverage_html/index.html"
    fi
fi
echo ""

