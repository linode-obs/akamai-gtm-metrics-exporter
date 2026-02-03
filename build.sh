#!/bin/bash
set -e

# Configuration
BINARY_NAME="akamai-gtm-metrics-exporter"
VERSION=$1

if [[ -z "$VERSION" ]]; then
    echo "Usage: ./build.sh <version>"
    exit 1
fi


PLATFORMS=(
    "linux/amd64"
    "linux/386"
    "linux/arm"
    "linux/arm64"
    "linux/mips"
    "linux/mipsle"
    "linux/mips64"
    "linux/mips64le"
    "linux/ppc64"
    "linux/ppc64le"
    "linux/s390x"
    "openbsd/amd64"
    "windows/amd64"
    "windows/386"
    "darwin/arm64"
    "darwin/amd64"
)

mkdir -p build

echo ">> Starting build for version $VERSION..."

# The Build Loop
for PLATFORM in "${PLATFORMS[@]}"; do
    OS=${PLATFORM%/*}
    ARCH=${PLATFORM#*/}
    
    echo ">> Compiling for ${OS}/${ARCH}..."

    # Handle Windows .exe extension
    EXT=""
    if [ "$OS" == "windows" ]; then EXT=".exe"; fi

    # Target output name
    TARGET_FILE="build/${BINARY_NAME}-${VERSION}.${OS}-${ARCH}${EXT}"

    # Build using make + environment overrides
    # CGO_ENABLED=0 ensures the binaries are portable/static
    GOOS=$OS GOARCH=$ARCH CGO_ENABLED=0 make build PROMU_FLAGS="-trimpath"

    # Check common output locations (root or bin/)
    if [ -f "./${BINARY_NAME}${EXT}" ]; then
        mv "./${BINARY_NAME}${EXT}" "$TARGET_FILE"
    elif [ -f "./bin/${BINARY_NAME}${EXT}" ]; then
        mv "./bin/${BINARY_NAME}${EXT}" "$TARGET_FILE"
    else
        echo "!! Error: Could not find binary for ${OS}/${ARCH}"
        exit 1
    fi

    # Generate the .sig file
    shasum -a 256 "$TARGET_FILE" | awk '{print $1}' > "${TARGET_FILE}.sig"
done

echo "-------------------------------------------------------"
echo "Done! Generated $(ls build/*.sig | wc -l) binaries and signatures."
echo "Check the 'build' directory."