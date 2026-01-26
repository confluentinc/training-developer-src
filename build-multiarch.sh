#!/bin/bash
# Build and push multi-architecture Docker images (AMD64 + ARM64)
# This creates manifest lists so a single tag works on both architectures.
#
# Prerequisites:
#   - docker buildx with multi-platform support
#   - Logged in to Docker Hub: docker login
#
# Usage:
#   ./build-multiarch.sh         # Build and push all images
#   ./build-multiarch.sh --local # Build for local architecture only (no push)

set -e

PLATFORMS="linux/amd64,linux/arm64"
PUSH_FLAG="--push"

if [ "$1" == "--local" ]; then
    PLATFORMS="linux/$(uname -m | sed 's/x86_64/amd64/' | sed 's/aarch64/arm64/')"
    PUSH_FLAG="--load"
    echo "🔨 Building for local platform only ($PLATFORMS)..."
else
    echo "🚀 Building multi-arch images (amd64 + arm64) and pushing to Docker Hub..."
    echo "⚠️  Make sure you're logged in: docker login"
    echo ""
fi

# Create/use a buildx builder that supports multi-platform
BUILDER_NAME="multiarch-builder"
if ! docker buildx inspect "$BUILDER_NAME" > /dev/null 2>&1; then
    echo "📦 Creating buildx builder: $BUILDER_NAME"
    docker buildx create --name "$BUILDER_NAME" --use --bootstrap
else
    docker buildx use "$BUILDER_NAME"
fi

# Java Producer (Avro)
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 Building cnfltraining/java-producer-avro:2.0"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker buildx build \
    --platform "$PLATFORMS" \
    -t cnfltraining/java-producer-avro:2.0 \
    -t cnfltraining/java-producer-avro:latest \
    -f solution/java-producer-avro/Dockerfile \
    $PUSH_FLAG \
    solution/java-producer-avro

# Webserver (plain)
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 Building cnfltraining/node-webserver:3.0"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker buildx build \
    --platform "$PLATFORMS" \
    -t cnfltraining/node-webserver:3.0 \
    -t cnfltraining/node-webserver:latest \
    -f webserver/Dockerfile \
    $PUSH_FLAG \
    webserver

# Webserver Avro
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 Building cnfltraining/node-webserver-avro:3.0"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker buildx build \
    --platform "$PLATFORMS" \
    -t cnfltraining/node-webserver-avro:3.0 \
    -t cnfltraining/node-webserver-avro:latest \
    -f webserver-avro/Dockerfile \
    $PUSH_FLAG \
    webserver-avro

# Webserver Streams
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 Building cnfltraining/node-webserver-streams:1.0"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker buildx build \
    --platform "$PLATFORMS" \
    -t cnfltraining/node-webserver-streams:1.0 \
    -t cnfltraining/node-webserver-streams:latest \
    -f webserver-streams/Dockerfile \
    $PUSH_FLAG \
    webserver-streams

echo ""
echo "✅ All images built successfully!"
echo ""

if [ "$1" != "--local" ]; then
    echo "🔍 Verify multi-arch support with:"
    echo "   docker manifest inspect cnfltraining/node-webserver:3.0"
    echo ""
    echo "📋 Expected output shows both architectures:"
    echo "   - linux/amd64"
    echo "   - linux/arm64"
fi

