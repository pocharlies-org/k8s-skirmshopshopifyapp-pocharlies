/**
 * picqer-price-sync (durable CronJob) — keeps the Prices tab's Coste/Margen + NL
 * fresh for the whole catalog. Daily, self-contained (node:20 + this script from a
 * ConfigMap; env from existing secrets). GAP-FILL: only fills products missing
 * cost/NL; never overwrites the synapse driver's priced products.
 *
 * NL uses the Picqer MULTICHANNEL link: ES and NL are separate Picqer products
 * sharing a productcode_supplier; the NL storefront uses its own productcode (e.g.
 * ES sp-socom556m-fde ↔ NL agun-49). Resolver: supplier-direct → sibling-on-NL →
 * SP-strip. Verified: siblings are the same product. Covers ~1,480 NL-connected
 * products (the rest are ES-only / NL-only — the two storefronts are partly
 * different catalogs). Cost = Picqer fixedstockprice for ~the whole catalog.
 *
 * KNOWN v1 LIMITATION: cost/NL frozen at first sync for products already filled
 * (gap-fill skips them); the driver refreshes its own. A v2 can add refresh.
 */
/**
 * Backfill Cost (Picqer fixedstockprice) + NL (skirmshop.nl) into the brain, GAP-FILL.
 *
 * COST: keyed by productcode_supplier (manufacturer SKU — same key the synapse
 *   driver uses) with productcode fallback, so a push MERGES into the driver's
 *   ManufacturerProduct node instead of duplicating. Never overwrites products
 *   that already show cost. Filter es>0 AND cost>0 (real negative margins kept).
 *
 * NL: ES and NL are separate Picqer products that share a productcode_supplier;
 *   the NL storefront uses its OWN productcode (e.g. ES sp-socom556m-fde ↔ NL
 *   agun-49, both supplier socom556m-fde). So we resolve the NL price by, in
 *   order: (1) NL sku == supplier code, (2) any Picqer SIBLING productcode under
 *   the same supplier that exists on the NL storefront, (3) SP-strip fallback.
 *   This is the real multichannel link (verified: siblings are the same product).
 *
 * Set APPLY=1 to write. Cost → ManufacturerProduct+SOURCED_FROM; NL → ChannelOffer.
 */
const BRAIN=(process.env.POCHARLIES_RAG_URL||process.env.BRAIN_URL||"http://skirmshop-brain-v2:5001").replace(/\/+$/,"");
const BKEY=process.env.BRAIN_API_KEY||""; const PKEY=process.env.PICQER_API_KEY||""; const SUB=process.env.PICQER_SUBDOMAIN||"skirmshop";
const PBASE=`https://${SUB}.picqer.com/api/v1`; const PAUTH="Basic "+Buffer.from(PKEY+":X").toString("base64");
const APPLY=process.env.APPLY==="1"; const CHUNK=200;
const sleep=ms=>new Promise(r=>setTimeout(r,ms)); const norm=s=>(s||"").toString().trim().toLowerCase(); const stripSP=s=>norm(s).replace(/^sp-/,"");
async function jget(url,opts={}){ for(let a=0;a<6;a++){ const r=await fetch(url,opts); if(r.status===429){await sleep(1500*(a+1));continue;} if(!r.ok) throw new Error("HTTP "+r.status+" "+url); return r.json(); } throw new Error("429 "+url); }
async function push(adapter,documents){ const r=await fetch(`${BRAIN}/instances/skirmshop/push-ingest`,{method:"POST",headers:{"Content-Type":"application/json",Accept:"application/json","X-API-Key":BKEY},body:JSON.stringify({adapter,documents})}); const t=await r.text(); if(!r.ok) throw new Error("push "+adapter+" HTTP "+r.status+": "+t.slice(0,200)); return JSON.parse(t); }
async function storeMap(host){ const m=new Map(); for(let p=1;p<=80;p++){ const j=await jget(`https://${host}/products.json?limit=250&page=${p}`,{headers:{Accept:"application/json"}}); const ps=j.products||[]; if(!ps.length)break; for(const pr of ps){ const prim=(pr.variants||[])[0]||{}; for(const v of (pr.variants||[])) if(v.sku) m.set(norm(v.sku),{handle:pr.handle, skuOrig:String(v.sku).trim(), price:Number(prim.price)||null}); } if(ps.length<250)break; await sleep(450);} return m; }
function chunk(a,n){const o=[];for(let i=0;i<a.length;i+=n)o.push(a.slice(i,i+n));return o;}
(async()=>{
  const es=await storeMap("skirmshop.es");
  const nlRaw=await storeMap("skirmshop.nl"); const nl=new Map(); for(const [k,v] of nlRaw) if(v.price!=null) nl.set(k, v.price);
  const brain=new Map(); let off=0,total=0;
  for(;;){ const j=await jget(`${BRAIN}/instances/skirmshop/prices/comparison?limit=200&offset=${off}`,{headers:{"X-API-Key":BKEY,Accept:"application/json"}}); const items=j.items||[]; total=j.total; for(const it of items) if(it.id) brain.set(it.id,{es:it.es_price,cost:it.cost,nl:it.nl_price}); off+=items.length; if(items.length<200||off>=total)break; }
  // Picqer: productcode -> {cost, sup}; supplier -> [sibling productcodes]
  const pq=new Map(); const bySup=new Map(); let poff=0;
  for(;;){ const rows=await jget(`${PBASE}/products?offset=${poff}`,{headers:{Authorization:PAUTH,Accept:"application/json","User-Agent":"backfill"}}); if(!rows.length)break;
    for(const p of rows){ const c=Number(p.fixedstockprice)||0; const supRaw=String(p.productcode_supplier||"").trim(); const code=norm(p.productcode);
      pq.set(code,{cost:c, sup:supRaw});
      const sk=norm(supRaw); if(sk){ if(!bySup.has(sk))bySup.set(sk,[]); bySup.get(sk).push(code); } }
    poff+=rows.length; if(rows.length<100)break; await sleep(110); }
  console.log(`maps: es=${es.size} nl=${nl.size} brain=${brain.size} picqer=${pq.size} suppliers=${bySup.size} APPLY=${APPLY}`);

  // NL multichannel resolver: supplier-direct -> sibling-on-NL -> SP-strip.
  function nlPriceFor(esSku, sup){
    const sk=norm(sup);
    if(sk && nl.has(sk)) return nl.get(sk);
    if(sk && bySup.has(sk)){ for(const c of bySup.get(sk)){ if(nl.has(c)) return nl.get(c); } }
    const st=stripSP(esSku); if(nl.has(st)) return nl.get(st);
    return null;
  }

  const costDocs=[], nlDocs=[]; let nlDirect=0,nlSibling=0,nlStrip=0;
  for(const [sku, info] of es){
    const b=brain.get(info.handle); if(!b) continue;
    const e=pq.get(sku); const cost=e&&e.cost; const sup=e&&e.sup;
    // COST gap-fill — merge into driver node via productcode_supplier.
    if(cost>0 && b.cost==null && b.es!=null && b.es>0){
      const key=(sup||info.skuOrig);
      costDocs.push({ source_id:`manufacturer:${key}`, content:`Picqer cost for ${key}: ${cost} EUR`,
        metadata:{ source:"manufacturer", sku:key, cost_eur:cost, our_product_handle:info.handle } });
    }
    // NL gap-fill via multichannel resolver.
    if(b.nl==null){
      const nlp=nlPriceFor(sku, sup);
      if(nlp!=null){
        const sk=norm(sup);
        if(sk && nl.has(sk)) nlDirect++; else if(sk && bySup.has(sk) && bySup.get(sk).some(c=>nl.has(c))) nlSibling++; else nlStrip++;
        nlDocs.push({ source_id:`channel:NL:${info.skuOrig}`, content:`NL channel price for ${info.skuOrig}: €${nlp} (ex-VAT)`,
          metadata:{ source:"channel_offer", channel:"NL", sku:info.skuOrig, price:Number(nlp), currency:"EUR", vat_incl:false, our_product_handle:info.handle } });
      }
    }
  }
  console.log(`candidates: cost=${costDocs.length} nl=${nlDocs.length} (nl direct=${nlDirect} sibling=${nlSibling} strip=${nlStrip})`);
  if(!APPLY){ console.log("DRY. sample nl:",JSON.stringify(nlDocs.slice(0,4).map(d=>d.metadata))); return; }
  let cIng=0,nIng=0;
  for(const c of chunk(costDocs,CHUNK)){ const r=await push("manufacturer",c); cIng+=r.chunks_ingested??r.documents_received??0; process.stdout.write("c"); }
  for(const c of chunk(nlDocs,CHUNK)){ const r=await push("channel",c); nIng+=r.chunks_ingested??r.documents_received??0; process.stdout.write("n"); }
  console.log(`\nPUSHED cost_docs=${costDocs.length} (ingested ${cIng}) | nl_docs=${nlDocs.length} (ingested ${nIng})`);
  console.log("DONE");
})().catch(e=>{console.error("ERR",e.message);process.exit(1)});
