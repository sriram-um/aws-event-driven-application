#!/usr/bin/env bash
# Build the AWS Common Runtime Lambda layer (layers/crt/python/) - exercise 9.
# PutEvents through an EventBridge global endpoint is signed with SigV4a, which botocore only
# supports when awscrt is importable; the Lambda Python runtime doesn't include it. awscrt is a
# native extension, so the wheel must match Lambda (Linux, arm64, CPython 3.12) - not the
# machine running this script. --no-deps: awscrt has no dependencies we need to bundle.
# Run before `sam build` (the CI workflow does this too).
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET=layers/crt/python
PYTHON=${PYTHON:-python}
rm -rf "$TARGET"
"$PYTHON" -m pip install --quiet --no-deps --target "$TARGET" \
  --platform manylinux2014_aarch64 --implementation cp --python-version 3.12 --only-binary=:all: \
  "awscrt==0.38.0"
find "$TARGET" -name "__pycache__" -type d -prune -exec rm -rf {} +
echo "CRT layer built: $(du -sh "$TARGET" | cut -f1) in $TARGET"
