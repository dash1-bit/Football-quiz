#!/usr/bin/env bash
set -euo pipefail

: "${CLOUDFLARE_API_TOKEN:?CLOUDFLARE_API_TOKEN is required}"
: "${CLOUDFLARE_ACCOUNT_ID:?CLOUDFLARE_ACCOUNT_ID is required}"
: "${CLOUDFLARE_PAGES_PROJECT_NAME:?CLOUDFLARE_PAGES_PROJECT_NAME is required}"

if npx wrangler pages project create "$CLOUDFLARE_PAGES_PROJECT_NAME" --production-branch main; then
  echo "Cloudflare Pages project created: $CLOUDFLARE_PAGES_PROJECT_NAME"
else
  echo "Project create command failed (likely already exists). Continuing."
fi

