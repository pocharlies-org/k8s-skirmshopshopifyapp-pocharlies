/**
 * picqer-price-sync (durable CronJob) — keeps the Prices tab's Coste/Margen + NL
 * fresh for the whole catalog. Daily, self-contained (runs in the rag-app image —
 * @prisma/client — with this script from a ConfigMap; env from existing secrets).
 * SC-5827: reads and writes `catalogo` directly (was: brain prices/comparison +
 * push-ingest). Same row shapes the app's writers use, so the exporter
 * (app/services/prices/export.server.ts) finds them.
 *
 * COST: GAP-FILL only — keyed by productcode_supplier (manufacturer SKU — the same
 *   key the synapse driver uses) with productcode fallback, so a write MERGES into
 *   the driver's catalogo.manufacturer_product row instead of duplicating. Never
 *   overwrites products that already show cost. Filter es>0 AND cost>0 (real
 *   negative margins kept). → manufacturer_product + product_sourced_from.
 *
 * NL: REFRESH-ON-CHANGE from the Picqer NET price (single source of truth, self-heals
 *   drift, but only writes products whose net actually changed vs the stored NL
 *   offer — so the nightly load stays light). ES and NL are separate Picqer products
 *   sharing a productcode_supplier; the cheapest NL-sibling Picqer `price` IS the NL
 *   net (ex-VAT). We store that net with vat_incl:false so the read layer's ×1.21
 *   yields the correct ES-comparable value. → channel_offer + product_channel_offer.
 *   v1 BUG (fixed 2026-06): it stored the skirmshop.nl storefront RETAIL price (already
 *   VAT-incl) as vat_incl:false, inflating NL ~+20%. Now uses the Picqer net directly.
 *   Keyed channel:NL:<es-sku> (idempotent).
 *
 * Set APPLY=1 to write.
 *
 * SOLO mode (SC-5887, B2b de SC-5806 — «Events, never polling»): with
 * SOLO=<productcode> (the event Job from workflow
 * skirmshop.k8s.rag-picqer-price-sync) it does the SAME rules over one product
 * and its family instead of the full sweep: resolve the product in Picqer, take
 * its productcode_supplier, refresh every ES sibling (SP-…) that shares it
 * (the NL case: the NL sibling's price changed and each ES's channel:NL: must
 * follow), reading the current catalogo row per ES by sku (so the SOLO mode
 * needs no storefront map). Irrelevant (no SP- sibling) → exit 0 in
 * seconds. Without SOLO → the sweep above, untouched (it is the declared net:
 * gap-fill of never-edited products is not an event, C-1).
 */
if(!process.env.CATALOGO_DATABASE_URL){console.error("CATALOGO_DATABASE_URL is required");process.exit(1);}
const { PrismaClient } = require("@prisma/client");
const CATALOGO_URL = process.env.CATALOGO_DATABASE_URL + (process.env.CATALOGO_DATABASE_URL.includes("?") ? "&" : "?") + "connection_limit=3&pool_timeout=15&connect_timeout=10&application_name=picqer-price-sync";
const db = new PrismaClient({ datasources: { db: { url: CATALOGO_URL } }, log: ["error"] });
const PKEY=process.env.PICQER_API_KEY||""; const SUB=process.env.PICQER_SUBDOMAIN||"skirmshop";
const PBASE=`https://${SUB}.picqer.com/api/v1`; const PAUTH="Basic "+Buffer.from(PKEY+":X").toString("base64");
const APPLY=process.env.APPLY==="1";
// Cost plausibility guard: a Shopify SKU must resolve to the SAME product in Picqer.
// If the Picqer net price is wildly off our ES ex-VAT price (e.g. a 495€ rifle whose
// productcode collides with a 32€ magazine SKU), the SKU maps to a DIFFERENT product —
// skip the cost rather than stamp a wrong one (the dominant wrong-cost source on the
// Prices tab). Env-tunable band; 0 disables.
const COST_PRICE_MINR=Number(process.env.COST_PRICE_MINR||0.15); const COST_PRICE_MAXR=Number(process.env.COST_PRICE_MAXR||6);
const sleep=ms=>new Promise(r=>setTimeout(r,ms)); const norm=s=>(s||"").toString().trim().toLowerCase();
const isOtherChan=c=>/^(sp-|uk-?|us-?)/i.test(c); const SOLO=(process.env.SOLO||"").trim();
async function jget(url,opts={}){ let last; for(let a=0;a<6;a++){ let r; try{ r=await fetch(url,opts); }catch(e){ last=e; await sleep(1500*(a+1)); continue; } /* retry transient network errors ("fetch failed") */ if(r.status===429||r.status>=500){ last=new Error("HTTP "+r.status+" "+url); await sleep(1500*(a+1)); continue; } if(!r.ok) throw new Error("HTTP "+r.status+" "+url); return r.json(); } throw last||new Error("retries exhausted "+url); }
async function storeMap(host){ const m=new Map(); for(let p=1;p<=80;p++){ const j=await jget(`https://${host}/products.json?limit=250&page=${p}`,{headers:{Accept:"application/json"}}); const ps=j.products||[]; if(!ps.length)break; for(const pr of ps){ const prim=(pr.variants||[])[0]||{}; for(const v of (pr.variants||[])) if(v.sku) m.set(norm(v.sku),{handle:pr.handle, skuOrig:String(v.sku).trim(), price:Number(prim.price)||null}); } if(ps.length<250)break; await sleep(450);} return m; }
// Current state per product handle, straight from catalogo (replaces the brain's
// /prices/comparison): ES price, best cost via product_sourced_from (tier order of
// the read layer aside — gap-fill only needs "has cost"), latest NL net.
// skuN (SOLO): only the product whose sku matches, case-insensitive.
async function currentPrices(skuN){
  const rows=await db.$queryRawUnsafe(`SELECT p.id AS handle, p.sku AS sku, p.price AS es,
    (SELECT mp.cost FROM catalogo.product_sourced_from psf
       JOIN catalogo.manufacturer_product mp ON mp.id = psf.manufacturer_product_id
      WHERE psf.product_id = p.id AND mp.cost IS NOT NULL
      ORDER BY psf.match_score DESC NULLS LAST LIMIT 1) AS cost,
    (SELECT co.price FROM catalogo.product_channel_offer pco
       JOIN catalogo.channel_offer co ON co.id = pco.channel_offer_id
      WHERE pco.product_id = p.id AND co.channel = 'NL'
      ORDER BY co.observed_at DESC NULLS LAST LIMIT 1) AS nl_raw
    FROM catalogo.product p WHERE p.deleted_at IS NULL${skuN?" AND lower(trim(p.sku)) = $1":""}`, ...(skuN?[skuN]:[]));
  const m=new Map();
  for(const r of rows) m.set(r.handle,{sku:r.sku,es:r.es==null?null:Number(r.es),cost:r.cost==null?null:Number(r.cost),nlRaw:r.nl_raw==null?null:Number(r.nl_raw)});
  return m;
}
async function writeCost(key,cost,handle){
  await db.$executeRawUnsafe(`INSERT INTO catalogo.manufacturer_product (id, sku, cost, currency) VALUES ($1,$1,$2,'EUR')
    ON CONFLICT (id) DO UPDATE SET sku = EXCLUDED.sku, cost = EXCLUDED.cost`, key, cost);
  await db.$executeRawUnsafe(`INSERT INTO catalogo.product_sourced_from (product_id, manufacturer_product_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, handle, key);
}
async function writeNl(sku,price,handle,hash){
  const id=`channel:NL:${sku}`; const now=new Date().toISOString();
  await db.$executeRawUnsafe(`INSERT INTO catalogo.channel_offer (id, sku, channel, kind, price, currency, vat_incl, observed_at, content_hash)
    VALUES ($1,$2,'NL','channel',$3,'EUR',false,$4::timestamptz,$5)
    ON CONFLICT (id) DO UPDATE SET sku = EXCLUDED.sku, price = EXCLUDED.price,
      observed_at = EXCLUDED.observed_at, content_hash = EXCLUDED.content_hash`, id, sku, price, now, hash);
  await db.$executeRawUnsafe(`INSERT INTO catalogo.product_channel_offer (product_id, channel_offer_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, handle, id);
}
// SOLO mode (SC-5887, B2b de SC-5806): the sweep's rules over one product and its
// supplier family, in seconds and without the storefront map. The event Job runs
// this; the CronJob (no SOLO) keeps running the full sweep as the declared net.
async function solo(code){
  const rows=await jget(`${PBASE}/products?productcode=${encodeURIComponent(code)}`,{headers:{Authorization:PAUTH,Accept:"application/json","User-Agent":"event"}});
  const hit=(rows||[]).find(r=>norm(r.productcode)===norm(code)&&r.active!==false);
  if(!hit){ console.log(`SOLO=${code}: no esta (o esta inactivo) en Picqer — nada que hacer`); return; }
  const supRaw=String(hit.productcode_supplier||"").trim(); const sk=norm(supRaw);
  // Family = every Picqer product sharing the supplier. /products has no working
  // productcode_supplier filter (verified 16-11); search= matches product and
  // supplier codes fuzzily and across warehouses, so the exact match is made here.
  let family=[hit];
  if(sk){
    const found=await jget(`${PBASE}/products?search=${encodeURIComponent(supRaw)}`,{headers:{Authorization:PAUTH,Accept:"application/json","User-Agent":"event"}});
    family=(found||[]).filter(r=>r.active!==false&&norm(String(r.productcode_supplier||"").trim())===sk);
    if(!family.some(r=>norm(r.productcode)===norm(code))) family.push(hit);
  }
  const esSibs=family.filter(r=>/^sp-/i.test(String(r.productcode||"")));
  if(!esSibs.length){ console.log(`SOLO=${code}: sin hermano SP- (supplier ${supRaw||"sin supplier"}) — irrelevante, salida rapida`); return; }
  // NL net (ex-VAT) = the cheapest non-ES/UK/US sibling price under this supplier — the sweep's rule.
  let supNl=null;
  for(const r of family){ const price=Number(r.price)||0; if(!isOtherChan(norm(r.productcode))&&price>0&&(supNl==null||price<supNl)) supNl=price; }
  const costRows=[], nlRows=[];
  for(const e of esSibs){
    const codeN=norm(e.productcode);
    // Current catalogo row for this ES sku (its id IS the handle, so SOLO needs no storefront map).
    const hit1=[...(await currentPrices(codeN))][0];
    if(!hit1){ continue; } // not in catalogo — the sweep skips it the same way
    const [handle,b]=hit1;
    const skuOrig=String(b.sku||e.productcode).trim();
    const sup=String(e.productcode_supplier||"").trim();
    const cost=Number(e.fixedstockprice)||0, pqPrice=Number(e.price)||0;
    // COST gap-fill with the same plausibility guard (a Shopify↔Picqer SKU collision must not stamp a wrong cost).
    if(cost>0&&b.cost==null&&b.es!=null&&b.es>0){
      const esEx=b.es/1.21;
      const mismatch=pqPrice>0&&esEx>0&&(pqPrice>esEx*COST_PRICE_MAXR||pqPrice<esEx*COST_PRICE_MINR);
      if(!mismatch){ const key=(sup||skuOrig);
        costRows.push({ key, cost, handle }); }
    }
    // NL refresh-on-change from the Picqer net (ex-VAT), same 0.01 band as the sweep.
    if(supNl!=null&&(b.nlRaw==null||Math.abs(Number(supNl)-Number(b.nlRaw))>0.01)){
      nlRows.push({ sku: skuOrig, price: Number(supNl), handle, hash: `picqer-nl-net:${supNl}` });
    }
    await sleep(110);
  }
  console.log(`SOLO=${code}: es_siblings=${esSibs.length} cost=${costRows.length} nl=${nlRows.length} APPLY=${APPLY}`);
  if(!costRows.length&&!nlRows.length){ console.log("nada que escribir"); return; }
  if(!APPLY){ console.log("DRY. sample:",JSON.stringify([...costRows,...nlRows].slice(0,4))); return; }
  for(const r of costRows) await writeCost(r.key,r.cost,r.handle);
  for(const r of nlRows) await writeNl(r.sku,r.price,r.handle,r.hash);
  console.log(`WRITTEN cost_rows=${costRows.length} nl_rows=${nlRows.length}`);
  console.log("DONE");
}
(async()=>{
  if(SOLO){ return solo(SOLO); }
  const es=await storeMap("skirmshop.es");
  const cur=await currentPrices();
  // Picqer: productcode -> {cost, sup}; supplier -> [sibling productcodes]
  const pq=new Map(); const bySup=new Map(); const supNl=new Map(); let poff=0;
  for(;;){ const rows=await jget(`${PBASE}/products?offset=${poff}`,{headers:{Authorization:PAUTH,Accept:"application/json","User-Agent":"backfill"}}); if(!rows.length)break;
    for(const p of rows){
      // DISCONTINUED guard (defensive): never let an inactive product be a cost/NL
      // source (SP-SBA-SSS-50 MDR-X bug: an inactive code collided with a live SKU →
      // wrong cost). NOTE (verified 2026-06-28): Picqer /products is ACTIVE-ONLY by
      // default, so deactivating already DROPS the product from these rows — this
      // guard is belt-and-suspenders in case the fetch ever includes ?inactive=true.
      if(p.active===false) continue;
      const c=Number(p.fixedstockprice)||0; const supRaw=String(p.productcode_supplier||"").trim(); const code=norm(p.productcode); const price=Number(p.price)||0;
      pq.set(code,{cost:c, sup:supRaw, price});
      const sk=norm(supRaw); if(sk){ if(!bySup.has(sk))bySup.set(sk,[]); bySup.get(sk).push(code);
        // NL net = the cheapest non-ES/UK/US Picqer sibling price (ex-VAT) under this supplier.
        if(!isOtherChan(code) && price>0){ if(!supNl.has(sk)||price<supNl.get(sk)) supNl.set(sk,price); } } }
    poff+=rows.length; if(rows.length<100)break; await sleep(110); }
  console.log(`maps: es=${es.size} supNl=${supNl.size} catalogo=${cur.size} picqer=${pq.size} suppliers=${bySup.size} APPLY=${APPLY}`);

  // NL net resolver: the cheapest NL-sibling Picqer price (ex-VAT) under the same supplier.
  function nlPriceFor(esSku, sup){ const sk=norm(sup); return (sk && supNl.has(sk)) ? supNl.get(sk) : null; }

  const costRows=[], nlRows=[]; let nlMatched=0, costSkippedMismatch=0;
  for(const [sku, info] of es){
    const b=cur.get(info.handle); if(!b) continue;
    const e=pq.get(sku); const cost=e&&e.cost; const sup=e&&e.sup; const pqPrice=e&&e.price;
    // COST gap-fill — merge into driver row via productcode_supplier.
    if(cost>0 && b.cost==null && b.es!=null && b.es>0){
      // Plausibility guard: skip when the Picqer product the SKU resolves to is clearly
      // a DIFFERENT product (its net price is wildly off our ES ex-VAT price) — a
      // Shopify↔Picqer SKU collision (e.g. a magazine SKU resolving to a 495€ rifle).
      const esEx = b.es / 1.21;
      const mismatch = pqPrice > 0 && esEx > 0 &&
        (pqPrice > esEx * COST_PRICE_MAXR || pqPrice < esEx * COST_PRICE_MINR);
      if(mismatch){ costSkippedMismatch++; }
      else {
        const key=(sup||info.skuOrig);
        costRows.push({ key, cost, handle: info.handle });
      }
    }
    // NL REFRESH-ON-CHANGE from the Picqer net (ex-VAT): write only when the net is new
    // or differs from the stored NL price — keeps self-heal, light nightly load.
    const nlp=nlPriceFor(sku, sup);
    if(nlp!=null && (b.nlRaw==null || Math.abs(Number(nlp)-Number(b.nlRaw))>0.01)){ nlMatched++;
      nlRows.push({ sku: info.skuOrig, price: Number(nlp), handle: info.handle, hash: `picqer-nl-net:${nlp}` });
    }
  }
  console.log(`candidates: cost=${costRows.length} nl=${nlRows.length} (nl to-update=${nlMatched}) cost-skipped(sku-mismatch)=${costSkippedMismatch}`);
  if(!APPLY){ console.log("DRY. sample nl:",JSON.stringify(nlRows.slice(0,4))); return; }
  let c=0,n=0;
  for(const r of costRows){ await writeCost(r.key,r.cost,r.handle); c++; process.stdout.write("c"); }
  for(const r of nlRows){ await writeNl(r.sku,r.price,r.handle,r.hash); n++; process.stdout.write("n"); }
  console.log(`\nWRITTEN cost_rows=${c} nl_rows=${n}`);
  console.log("DONE");
})().catch(e=>{console.error("ERR",e.message);process.exit(1)}).finally(()=>db.$disconnect());
