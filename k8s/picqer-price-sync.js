/**
 * picqer-price-sync (durable CronJob) — keeps the Prices tab's Coste/Margen + NL
 * fresh for the whole catalog. Daily, self-contained (node:20 + this script from a
 * ConfigMap; env from existing secrets).
 *
 * COST: GAP-FILL only — keyed by productcode_supplier (manufacturer SKU — the same
 *   key the synapse driver uses) with productcode fallback, so a push MERGES into
 *   the driver's ManufacturerProduct node instead of duplicating. Never overwrites
 *   products that already show cost. Filter es>0 AND cost>0 (real negative margins
 *   kept). → ManufacturerProduct + SOURCED_FROM.
 *
 * NL: ALWAYS-REFRESH from the Picqer NET price (single source of truth, self-heals
 *   drift). ES and NL are separate Picqer products sharing a productcode_supplier;
 *   the cheapest NL-sibling Picqer `price` IS the NL net (ex-VAT). We store that net
 *   with vat_incl:false so the read layer's ×1.21 yields the correct ES-comparable
 *   value (and nl_price_raw = the true ex-VAT net). → ChannelOffer.
 *   v1 BUG (fixed 2026-06): it stored the skirmshop.nl storefront RETAIL price (already
 *   VAT-incl) as vat_incl:false, inflating NL ~+20%. Now uses the Picqer net directly,
 *   matching the one-off Part B correction. Keyed channel:NL:<es-sku> (idempotent).
 *
 * Set APPLY=1 to write.
 */
const BRAIN=(process.env.POCHARLIES_RAG_URL||process.env.BRAIN_URL||"http://skirmshop-brain-v2:5001").replace(/\/+$/,"");
const BKEY=process.env.BRAIN_API_KEY||""; const PKEY=process.env.PICQER_API_KEY||""; const SUB=process.env.PICQER_SUBDOMAIN||"skirmshop";
const PBASE=`https://${SUB}.picqer.com/api/v1`; const PAUTH="Basic "+Buffer.from(PKEY+":X").toString("base64");
const APPLY=process.env.APPLY==="1"; const CHUNK=200;
const sleep=ms=>new Promise(r=>setTimeout(r,ms)); const norm=s=>(s||"").toString().trim().toLowerCase();
async function jget(url,opts={}){ for(let a=0;a<6;a++){ const r=await fetch(url,opts); if(r.status===429){await sleep(1500*(a+1));continue;} if(!r.ok) throw new Error("HTTP "+r.status+" "+url); return r.json(); } throw new Error("429 "+url); }
async function push(adapter,documents){ let last; for(let a=0;a<6;a++){ let r; try{ r=await fetch(`${BRAIN}/instances/skirmshop/push-ingest`,{method:"POST",headers:{"Content-Type":"application/json",Accept:"application/json","X-API-Key":BKEY},body:JSON.stringify({adapter,documents})}); }catch(e){ last=e; await sleep(1500*(a+1)); continue; } /* retry transient network errors (e.g. "fetch failed") */ if(r.status===429||r.status>=500){ last=new Error("push "+adapter+" HTTP "+r.status); await sleep(1500*(a+1)); continue; } const t=await r.text(); if(!r.ok) throw new Error("push "+adapter+" HTTP "+r.status+": "+t.slice(0,200)); /* 4xx = fatal, no retry */ return JSON.parse(t); } throw last; }
async function storeMap(host){ const m=new Map(); for(let p=1;p<=80;p++){ const j=await jget(`https://${host}/products.json?limit=250&page=${p}`,{headers:{Accept:"application/json"}}); const ps=j.products||[]; if(!ps.length)break; for(const pr of ps){ const prim=(pr.variants||[])[0]||{}; for(const v of (pr.variants||[])) if(v.sku) m.set(norm(v.sku),{handle:pr.handle, skuOrig:String(v.sku).trim(), price:Number(prim.price)||null}); } if(ps.length<250)break; await sleep(450);} return m; }
function chunk(a,n){const o=[];for(let i=0;i<a.length;i+=n)o.push(a.slice(i,i+n));return o;}
(async()=>{
  const es=await storeMap("skirmshop.es");
  const brain=new Map(); let off=0,total=0;
  for(;;){ const j=await jget(`${BRAIN}/instances/skirmshop/prices/comparison?limit=200&offset=${off}`,{headers:{"X-API-Key":BKEY,Accept:"application/json"}}); const items=j.items||[]; total=j.total; for(const it of items) if(it.id) brain.set(it.id,{es:it.es_price,cost:it.cost,nl:it.nl_price}); off+=items.length; if(items.length<200||off>=total)break; }
  // Picqer: productcode -> {cost, sup}; supplier -> [sibling productcodes]
  const pq=new Map(); const bySup=new Map(); const supNl=new Map(); let poff=0;
  const isOtherChan=c=>/^(sp-|uk-?|us-?)/i.test(c);
  for(;;){ const rows=await jget(`${PBASE}/products?offset=${poff}`,{headers:{Authorization:PAUTH,Accept:"application/json","User-Agent":"backfill"}}); if(!rows.length)break;
    for(const p of rows){ const c=Number(p.fixedstockprice)||0; const supRaw=String(p.productcode_supplier||"").trim(); const code=norm(p.productcode); const price=Number(p.price)||0;
      pq.set(code,{cost:c, sup:supRaw});
      const sk=norm(supRaw); if(sk){ if(!bySup.has(sk))bySup.set(sk,[]); bySup.get(sk).push(code);
        // NL net = the cheapest non-ES/UK/US Picqer sibling price (ex-VAT) under this supplier.
        if(!isOtherChan(code) && price>0){ if(!supNl.has(sk)||price<supNl.get(sk)) supNl.set(sk,price); } } }
    poff+=rows.length; if(rows.length<100)break; await sleep(110); }
  console.log(`maps: es=${es.size} supNl=${supNl.size} brain=${brain.size} picqer=${pq.size} suppliers=${bySup.size} APPLY=${APPLY}`);

  // NL net resolver: the cheapest NL-sibling Picqer price (ex-VAT) under the same supplier.
  function nlPriceFor(esSku, sup){ const sk=norm(sup); return (sk && supNl.has(sk)) ? supNl.get(sk) : null; }

  const costDocs=[], nlDocs=[]; let nlMatched=0;
  for(const [sku, info] of es){
    const b=brain.get(info.handle); if(!b) continue;
    const e=pq.get(sku); const cost=e&&e.cost; const sup=e&&e.sup;
    // COST gap-fill — merge into driver node via productcode_supplier.
    if(cost>0 && b.cost==null && b.es!=null && b.es>0){
      const key=(sup||info.skuOrig);
      costDocs.push({ source_id:`manufacturer:${key}`, content:`Picqer cost for ${key}: ${cost} EUR`,
        metadata:{ source:"manufacturer", sku:key, cost_eur:cost, our_product_handle:info.handle } });
    }
    // NL ALWAYS-REFRESH from the Picqer net (ex-VAT) — self-heals drift, single source of truth.
    const nlp=nlPriceFor(sku, sup);
    if(nlp!=null){ nlMatched++;
      nlDocs.push({ source_id:`channel:NL:${info.skuOrig}`, content:`NL net (Picqer) for ${info.skuOrig}: ${nlp} EUR ex-VAT`,
        metadata:{ source:"channel_offer", channel:"NL", sku:info.skuOrig, price:Number(nlp), currency:"EUR", vat_incl:false, our_product_handle:info.handle, content_hash:`picqer-nl-net:${nlp}` } });
    }
  }
  console.log(`candidates: cost=${costDocs.length} nl=${nlDocs.length} (nl matched=${nlMatched})`);
  if(!APPLY){ console.log("DRY. sample nl:",JSON.stringify(nlDocs.slice(0,4).map(d=>d.metadata))); return; }
  let cIng=0,nIng=0;
  for(const c of chunk(costDocs,CHUNK)){ const r=await push("manufacturer",c); cIng+=r.chunks_ingested??r.documents_received??0; process.stdout.write("c"); }
  for(const c of chunk(nlDocs,CHUNK)){ const r=await push("channel",c); nIng+=r.chunks_ingested??r.documents_received??0; process.stdout.write("n"); }
  console.log(`\nPUSHED cost_docs=${costDocs.length} (ingested ${cIng}) | nl_docs=${nlDocs.length} (ingested ${nIng})`);
  console.log("DONE");
})().catch(e=>{console.error("ERR",e.message);process.exit(1)});
