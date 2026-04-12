#!/bin/bash
# One-time setup: link git hooks so dependencies auto-install on git pull
cp scripts/post-merge .git/hooks/post-merge
chmod +x .git/hooks/post-merge
echo "✓ Git hooks installed. Dependencies will auto-install on every git pull."
