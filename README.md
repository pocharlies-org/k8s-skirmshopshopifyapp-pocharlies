# k8s-skirmshopshopifyapp-pocharlies

GitOps manifests for **"Pocharlies Catalog RAG Sync"** (the legacy
`skirmshopshopifyapp` Remix app) deployed to the `skirmshop` namespace, managed
by ArgoCD.

This app is **embedded at the root** of `skirmshop.e-dani.com` (Shopify
`application_url = https://skirmshop.e-dani.com/app/rag`, OAuth at `/auth`).
It syncs the Shopify catalog into the brain RAG and exposes a RAG-status UI.

## Layout
- `k8s/kustomization.yaml` — overlay on the shared
  `k8s-shopify-framework-pocharlies` base (Deployment `rag-app` + Service +
  edge IngressRoute), same pattern as `k8s-shopify-sii-pocharlies`.

## Wiring
- Image: `harbor.lan.e-dani.com/homelab/skirmshopshopifyapp` (built by the app
  repo's `release.yml` on a `v*` tag).
- DB: `postgres-shared-rw.databases.svc.cluster.local/skirmshop` (CNPG), creds
  from the `shared-postgres-app` secret.
- Catalog RAG target: `skirmshop-brain.skirmshop-brain-prod` (brain v2).
- SII cert: `sii-certificate` secret mounted at `/app/secrets` (shared with the
  sii app).
- App env: legacy settings still come from the out-of-band `rag-secrets`, but
  catalog API authentication is explicitly overridden from the Vault-backed
  `catalog-api-server` Secret so it has a defined owner and rotation window.
- Public route: IngressRoute `rag-app` on `traefik-edge`,
  `Host(skirmshop.e-dani.com)` (root). The legacy edge catch-all that pointed at
  the dead `sauvage:3456` host port was removed from
  `k8s-infra-pocharlies/networking/traefik-edge/legacy-public-routes.yaml`.

## Runs on
Edge node (`role: edge`), like the other root-domain Shopify apps.
