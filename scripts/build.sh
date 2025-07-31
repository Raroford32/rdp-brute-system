#!/bin/bash

# Exit on error
set -e

# Get the project root directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." &> /dev/null && pwd )"

echo "Changing to project root: $DIR"
cd "$DIR"

echo "Creating bin directory..."
mkdir -p bin

echo "Building server..."
go build -o bin/server ./server/cmd/server

echo "Building client..."
go build -o bin/client ./client/cmd/client

echo "Build complete."
