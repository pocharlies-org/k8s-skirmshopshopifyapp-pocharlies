# ARCHITECTURE — k8s-skirmshopshopifyapp-pocharlies

Repo: `pocharlies-org/k8s-skirmshopshopifyapp-pocharlies` · tronco real: `main` (default branch y `targetRevision` de la Application viva `skirmshopshopifyapp`) · workflows `ci.yml`, `pr-review.yml`.
Manifiestos GitOps de la app «Pocharlies Catalog RAG Sync». **Repo de aplicación:** `pocharlies-org/skirmshopshopifyapp` (Remix; imagen `harbor.lan.e-dani.com/homelab/skirmshopshopifyapp`).

## Clientes y versiones
- Sin clientes propios. Despliega la app Shopify embebida (Deployment `rag-app` + Service + IngressRoute, venidos de la base compartida) y un conjunto de CronJobs del catálogo.
- Imagen: `k8s/kustomization.yaml` → `images: shopify-app` → `harbor.lan.e-dani.com/homelab/skirmshopshopifyapp`, `newTag: v1.5.129` + `digest`. La construye `release.yml` de la app con un tag `v*`.
- Namespace `skirmshop`, nodo edge (`role: edge`); `namePrefix: rag-` y etiqueta `app.kubernetes.io/instance: rag`.
- URL pública: raíz de `skirmshop.e-dani.com` (IngressRoute parcheada a `Host(skirmshop.e-dani.com)` sin prefijo); OAuth en `/auth`. El App Proxy `/chatbot` del mismo host lo sirve el chatbot, no esta app.

## Dependencias en ambos sentidos
- Depende de: base remota `https://github.com/pocharlies/k8s-shopify-framework-pocharlies.git//base?ref=deploy/prod` (Deployment `app` + Service + IngressRoute; fuente en la cuenta personal `pocharlies`, no en la org); Postgres CNPG `postgres-shared-rw.databases…/skirmshop` (Secret `shared-postgres-app`); secretos `rag-secrets` (gestionado a mano, fuera de banda), `catalog-api-server` (Vault/ESO: `CATALOG_SYNC_TOKEN` y `CATALOG_SYNC_TOKEN_PREVIOUS`), `rag-litellm-key` (`externalsecret-litellm.yaml`, Vault `secret/litellm`), `sii-certificate` (compartido con la app SII).
- Servicios que usa la app: cerebro `skirmshop-brain.skirmshop-brain-prod…` (`POCHARLIES_RAG_URL`), LiteLLM `litellm.litellm…:4000` (modelo `tooling`), Picqer y Shopify.
- De él dependen: las rutas `/api/*` de la app que llaman synapse, el motor del crawler de competencia y los MCPs; el borrado de la ruta heredada `sauvage:3456` está en `k8s-infra-pocharlies/networking/traefik-edge/legacy-public-routes.yaml` (README).
- Apps hermanas con el mismo patrón: `k8s-shopify-sii-pocharlies`, `k8s-shopify-serial-numbers-pocharlies`, `k8s-shopify-label-pocharlies` (otra tanda).

## Stack con versiones
- Kustomize (con `configMapGenerator` para scripts Node y `patches` JSON/strategic-merge); `node:20-alpine` para los CronJobs de script; imagen de la app por digest. Sin Helm.
- `SCOPES` de Shopify declarados en el Deployment (más amplios que `shopify.app.toml`: añade `read_all_orders`, `read_returns`): deben alinearse con la app al cambiar scopes.

## Componentes compartidos
- Publica: Deployment/Service `rag-app` y IngressRoute `app` (nombres tras el prefijo), CronJobs `canonicalize` (02:30), `health-report`, `taxonomy-verify`, `proposals-digest`, `reconcile-summary`, `picqer-price-sync` (04:30 diario), `nl-llm-matcher`, `competitor-llm-matcher`, `provider-sourcing-backfill`, `redirect-health`, `parent-health`, y `product-weight-edge.yaml` (rate-limit de `/api/product-weight`).
- Consume: scripts versionados aquí (`k8s/picqer-price-sync.js`, `nl-llm-matcher.js`, `competitor-llm-matcher.js`, `redirect-health.js`, `parent-health.js`; montados como ConfigMap con hash) y la imagen de la app para los jobs que ejecutan código de la app.
- Los scripts de cron son lógica de negocio que vive en el chart, no en el repo de aplicación: duplicación potencial con `skirmshopshopifyapp/scripts/`.

## Cómo se construye
- Versión nueva de la app: tag `v*` en `skirmshopshopifyapp` → `release.yml` publica → PR aquí con el nuevo `newTag` y `digest`. Un job nuevo = `cronjob-<x>.yaml` + entrada en `resources` y, si lleva script, entrada en `configMapGenerator`.
- Los flags de comportamiento autónomo (`RECONCILE_APPLY_ENABLED`, `CATALOG_CREATE_DRAFT_ENABLED`, `CATALOG_IMPORT_ENABLED`, `DEV_MODE=false`) están en el patch del Deployment con su kill-switch comentado: tocarlos es un cambio de comportamiento en producción.
- `migrate deploy` de Prisma corre al arrancar el pod (`npx prisma migrate deploy && remix-serve`).

## Tests
- Solo validación de manifiestos: `ci.yml` → `reusable-ci.yml@main` de `k8s-gitops-pocharlies` (`arc-k8s`, `kustomize_paths: ". k8s"`, sin Node ni docker build). La base remota se descarga al renderizar: un fallo de red o un cambio en su `deploy/prod` rompe el render.

## CI/CD y despliegue
- ArgoCD Application `skirmshopshopifyapp`, `targetRevision: main`, path `k8s`, namespace `skirmshop`. PR contra `main`; devops hace el merge y valida con el estado de la Application y `https://skirmshop.e-dani.com/app`.

## Decisiones y trampas
- La base `?ref=deploy/prod` está en otra cuenta/repo (`pocharlies/k8s-shopify-framework-pocharlies`): un cambio allí afecta a este chart y a sus hermanos sin tocar este repo.
- `rag-secrets` es manual (no ESO); solo `CATALOG_SYNC_TOKEN*` tienen propietario Vault. Rotar `rag-secrets` no pasa por GitOps.
- `LITELLM_MODEL=tooling`: nombre de capacidad, no de modelo (comentario del manifiesto); no sustituir por un nombre concreto.
- El Deployment lee la URL pública `SHOPIFY_APP_URL` y debe coincidir exactamente con la IngressRoute (invariante de auth).

## Referencias cruzadas
- App: `skirmshopshopifyapp` (`shopify.app.toml`, `app/routes/api.*`, `release.yml`); cerebro: `skirmshop-brain-k8s`; chatbot en el mismo host: `skirmshop-chatbot`; wiki-chart para enrutado admin: `k8s-skirmshop-wiki-pocharlies`.

## Hallazgo C5
- No es duplicado ni abandonado (tag v1.5.129 en uso). Riesgo, no archivado: lógica de cron en JS dentro del chart y base remota en cuenta personal.
