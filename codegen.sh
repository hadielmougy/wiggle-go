#!/usr/bin/env bash
#
# Regenerates the gRPC stubs in internal/pb/ from the vendored proto (proto/wiggle.proto).
# The proto is the wire contract with the Wiggle server; keep proto/wiggle.proto in sync with the
# canonical copy in the engine repo (wiggle: proto/src/main/proto/wiggle.proto) and re-run this.
#
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#   brew install protobuf        # provides protoc
#   ./codegen.sh
set -euo pipefail
cd "$(dirname "$0")"
export PATH="$PATH:$(go env GOPATH)/bin"

protoc -I proto \
	--go_out=. --go_opt=module=github.com/hadielmougy/wiggle-go \
	--go-grpc_out=. --go-grpc_opt=module=github.com/hadielmougy/wiggle-go \
	proto/wiggle.proto

echo "regenerated stubs in internal/pb"
