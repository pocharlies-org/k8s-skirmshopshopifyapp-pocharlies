// Daily guard for the SEO recovery of the parent collections (plan fluffy-koala).
// Read-only. Exits non-zero so a break shows up in job history / alerting.
//
//  1. THE TEMPLATE. `templates/collection.parent.json` in the LIVE theme must list
//     `main-collection` in its `order`. Without it a parent page renders only the
//     subcollection tiles — no product grid, no CollectionPage JSON-LD — so every
//     301 aimed at it lands on a soft-404. This regressed on 2026-07-29 when
//     pocharlies.5 was orphaned: another session published .6 built from the older
//     live .4, silently dropping the fix. The theme went .6→.7→.8→.9 in three days
//     and every publish is another chance to lose it — hence daily, not weekly.
//
//  2. THE MEMBERSHIP. The 60 parents materialised on 2026-07-29 (transitive closure
//     of their descendants, ~11k memberships) must keep their products. An empty
//     parent is the same soft-404 by another route. Two are legitimately empty
//     (`magazine-parts`, `srs-buckings` — their descendants genuinely have none),
//     so this compares against a cap instead of demanding zero.

const STORE = process.env.SHOPIFY_STORE;
const TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const VERSION = process.env.SHOPIFY_API_VERSION || '2025-07';
const CAP = Number(process.env.EMPTY_PARENT_CAP || 4);
const API = `https://${STORE}/admin/api/${VERSION}/graphql.json`;

if (!STORE || !TOKEN) {
  console.error('faltan SHOPIFY_STORE / SHOPIFY_ACCESS_TOKEN');
  process.exit(1);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function gql(query, variables) {
  const res = await fetch(API, {
    method: 'POST',
    headers: { 'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables: variables || {} }),
    signal: AbortSignal.timeout(60_000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${await res.text().catch(() => '')}`);
  const json = await res.json();
  if (json.errors) throw new Error(`GraphQL: ${JSON.stringify(json.errors)}`);
  return json.data;
}

async function checkTemplate() {
  console.log('[1/2] plantilla collection.parent.json del tema VIVO');
  const data = await gql(`{
    themes(first: 1, roles: [MAIN]) {
      nodes {
        name
        files(filenames: ["templates/collection.parent.json"], first: 1) {
          nodes { body { ... on OnlineStoreThemeFileBodyText { content } } }
        }
      }
    }
  }`);
  const theme = (data.themes?.nodes || [])[0];
  if (!theme) throw new Error('no hay tema con rol MAIN');
  const file = (theme.files?.nodes || [])[0];
  if (!file) {
    console.error(`[ALERTA] el tema vivo (${theme.name}) NO tiene templates/collection.parent.json`);
    return false;
  }
  // El fichero lleva una cabecera /* ... */ que no es JSON valido: se lee por regex.
  const m = String(file.body.content).match(/"order"\s*:\s*\[([^\]]*)\]/);
  if (!m) {
    console.error(`[ALERTA] ${theme.name}: collection.parent.json sin bloque "order"`);
    return false;
  }
  const order = m[1].split(',').map((s) => s.trim().replace(/^"|"$/g, '')).filter(Boolean);
  console.log(`  tema=${theme.name} order=[${order.join(', ')}]`);
  if (!order.includes('main-collection')) {
    console.error(`[ALERTA] ${theme.name}: collection.parent.json NO lleva "main-collection" en order.`);
    console.error('  Las paginas padre solo renderizan azulejos: sin rejilla de producto y sin');
    console.error('  CollectionPage JSON-LD, asi que los 301 que apuntan a ellas caen en soft-404.');
    console.error('  Reaplica el fix sobre el tema VIVO con el flujo pocharlies.N (skill');
    console.error('  skirmshop-theme-update) — NO parchees asset a asset.');
    return false;
  }
  console.log('  OK: la rejilla sigue en la plantilla');
  return true;
}

// Bulk en vez de paginar con after:endCursor, que en esta tienda es inestable.
async function bulkFetch(innerQuery) {
  const start = await gql(
    `mutation($q: String!) {
       bulkOperationRunQuery(query: $q) {
         bulkOperation { id status } userErrors { field message } } }`,
    { q: innerQuery },
  );
  const errs = start.bulkOperationRunQuery?.userErrors || [];
  if (errs.length) throw new Error(`bulk userErrors: ${JSON.stringify(errs)}`);

  for (let i = 0; i < 60; i++) {
    const cur = (await gql('{ currentBulkOperation { status url errorCode } }')).currentBulkOperation || {};
    if (cur.status === 'COMPLETED') {
      if (!cur.url) return []; // resultado vacio: Shopify no publica url
      const res = await fetch(cur.url, { signal: AbortSignal.timeout(120_000) });
      if (!res.ok) throw new Error(`descarga del bulk: HTTP ${res.status}`);
      const text = await res.text();
      return text.split('\n').filter(Boolean).map((l) => JSON.parse(l));
    }
    if (cur.status === 'FAILED' || cur.status === 'CANCELED') {
      throw new Error(`bulk ${cur.status} ${cur.errorCode || ''}`);
    }
    await sleep(5000);
  }
  throw new Error('bulk timeout');
}

async function checkMembership() {
  console.log('[2/2] los padres siguen conteniendo producto');
  const rows = await bulkFetch(
    '{ collections { edges { node { handle templateSuffix productsCount { count } } } } }',
  );
  const parents = rows.filter((o) => (o.templateSuffix || '') === 'parent');
  if (!parents.length) throw new Error('el bulk no devolvio ninguna coleccion parent');
  // <= 0 y no === 0: productsCount es un contador incremental almacenado y puede
  // quedar NEGATIVO (inner-barrels estaba en -1 el 29-jul-2026, con 0 productos).
  const empty = parents.filter((o) => (o.productsCount?.count ?? 0) <= 0).map((o) => o.handle).sort();
  console.log(`  parents=${parents.length} vacios=${empty.length} (cap ${CAP})`);
  if (empty.length) console.log(`  vacios: ${empty.join(', ')}`);
  if (empty.length > CAP) {
    console.error(`[ALERTA] ${empty.length} colecciones padre vacias (> cap ${CAP}).`);
    console.error('  Alguien esta vaciando los padres materializados. Sospechoso historico: el');
    console.error('  reconcile de product-creator arrancaba las colecciones parent de cada producto');
    console.error('  al editarle titulo/body/tags (arreglado el 31-jul-2026).');
    console.error('  Rematerializar: skirmshopshopifyapp scripts/legacy-url-map/materialize-parents.py');
    return false;
  }
  console.log('  OK: los padres conservan su producto');
  return true;
}

(async () => {
  // Los dos checks corren SIEMPRE, aunque el primero falle: si se han roto ambos
  // quiero verlo en un solo run, no descubrir el segundo la noche siguiente.
  const results = [];
  for (const [name, fn] of [['plantilla', checkTemplate], ['membresia', checkMembership]]) {
    try {
      results.push(await fn());
    } catch (err) {
      console.error(`[ERROR] check ${name}: ${err.message}`);
      results.push(false);
    }
  }
  if (results.every(Boolean)) {
    console.log('OK: plantilla y membresia de los padres intactas');
    process.exit(0);
  }
  process.exit(1);
})();
