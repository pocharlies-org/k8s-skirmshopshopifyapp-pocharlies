// Weekly redirect-health audit (SEO recovery, plan fluffy-koala 2026-07-29).
// Read-only: bulk-dumps every urlRedirect plus the live product/collection sets,
// cross-checks them in memory, and FAILS the job when broken targets exceed the
// cap so it shows up in job history / alerting. Never writes to Shopify.
//
// Rationale: 426 deleted collections + 435 deleted products silently broke ~10k of
// the 26.569 redirects after the Feb-Mar 2026 migration. This catches the same
// regression the next time someone deletes a product or a collection.

const STORE = process.env.SHOPIFY_STORE;
const TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const VERSION = process.env.SHOPIFY_API_VERSION || '2025-07';
// 130 y no 50: el suelo medido el 31-jul-2026 es 105 rotos (target404=97 +
// targetEmpty=8), destinos genuinamente borrados que la campana de reparacion
// dejo a proposito porque borrar un 301 roto lo convierte en 404. El CronJob lo
// sobreescribe por env; este default esta alineado para que ejecutarlo a mano no
// de un falso positivo.
const CAP = Number(process.env.BROKEN_CAP || 130);
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

// OJO: Shopify solo admite UNA bulk query por app+tienda, y `currentBulkOperation`
// es GLOBAL — devuelve la operacion en curso sea de quien sea. Si no comparas el
// id, te comes el resultado de OTRO job y lo interpretas como tuyo.
async function bulkFetch(innerQuery) {
  let startedId = null;
  for (let attempt = 0; attempt < 30; attempt++) {
    const payload = (await gql(
      `mutation($q: String!) {
         bulkOperationRunQuery(query: $q) {
           bulkOperation { id status } userErrors { field message } } }`,
      { q: innerQuery },
    )).bulkOperationRunQuery || {};
    if (payload.bulkOperation?.id) { startedId = payload.bulkOperation.id; break; }
    const errs = payload.userErrors || [];
    const busy = errs.some((e) => /already in progress/i.test(e.message || ''));
    if (!busy) throw new Error(`bulk userErrors: ${JSON.stringify(errs)}`);
    console.log('  otra bulk operation ocupa la tienda, esperando...');
    await sleep(10_000);
  }
  if (!startedId) throw new Error('otra bulk operation ocupo la tienda todo el rato');

  for (let i = 0; i < 120; i++) {
    const cur = (await gql('{ currentBulkOperation { id status url errorCode } }')).currentBulkOperation || {};
    if (cur.id !== startedId) { await sleep(5000); continue; } // no es la nuestra
    if (cur.status === 'COMPLETED') {
      if (!cur.url) return [];
      const res = await fetch(cur.url, { signal: AbortSignal.timeout(300_000) });
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

const norm = (p) => String(p || '').toLowerCase().split('?')[0].replace(/\/$/, '');
const RES = /^(?:\/[a-z]{2}(?:-[a-z]{2})?)?\/(products|collections|pages)\/([^/]+)$/;

(async () => {
  console.log('[1/3] bulk dump: urlRedirects + products + collections');
  const rows = await bulkFetch(
    '{ urlRedirects { edges { node { id path target } } } ' +
    'products { edges { node { id handle status } } } ' +
    'collections { edges { node { id handle productsCount { count } } } } }',
  );
  console.log(`  ${rows.length} objetos`);

  console.log('[2/3] cruce en memoria');
  const products = new Map();
  const cols = new Map();
  const redirects = [];
  for (const o of rows) {
    const id = o.id || '';
    if (id.includes('/Product/')) products.set(o.handle, o.status);
    else if (id.includes('/Collection/')) cols.set(o.handle, o.productsCount?.count ?? 0);
    else if (id.includes('/UrlRedirect/')) redirects.push(o);
  }
  if (!redirects.length) throw new Error('el bulk no devolvio ningun urlRedirect');

  const paths = new Set(redirects.map((r) => norm(r.path)));
  let chains = 0, t404 = 0, tEmpty = 0;
  for (const r of redirects) {
    const nt = norm(r.target);
    if (paths.has(nt)) { chains++; continue; }
    const m = nt.match(RES);
    if (!m) continue;
    if (m[1] === 'products') {
      if (products.get(m[2]) !== 'ACTIVE') t404++;
    } else if (m[1] === 'collections') {
      if (!cols.has(m[2])) t404++;
      // <= 0 y no === 0: productsCount es un contador incremental almacenado y
      // puede quedar NEGATIVO (inner-barrels estaba en -1 el 29-jul-2026). Con
      // === 0 una coleccion vacia con el contador desincronizado pasaba el audit.
      else if (cols.get(m[2]) <= 0) tEmpty++;
    }
  }
  console.log(`  redirects=${redirects.length} chains=${chains} target404=${t404} targetEmpty=${tEmpty}`);

  console.log('[3/3] veredicto');
  const broken = t404 + tEmpty;
  if (broken > CAP) {
    console.error(`[ALERTA] ${broken} redirects rotos (> cap ${CAP}) — alguien ha borrado`);
    console.error('  productos/colecciones con redirects apuntandoles. Ejecutar');
    console.error('  scripts/legacy-url-map/{dump,audit,build-repair-plan}.');
    process.exit(1);
  }
  console.log(`OK: ${broken} rotos (cap ${CAP})`);
})().catch((err) => {
  console.error(`[ERROR] ${err.message}`);
  process.exit(1);
});
