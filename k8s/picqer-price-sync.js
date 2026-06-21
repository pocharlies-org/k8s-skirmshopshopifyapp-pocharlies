/**
 * picqer-price-sync (durable CronJob) — keeps the Prices tab's Coste/Margen + NL
 * fresh for the whole catalog. Daily, self-contained (node:20 + this script from a
 * ConfigMap; env from existing secrets). GAP-FILL semantics: pushes Cost
 * (Picqer fixedstockprice) + NL (skirmshop.nl) only for products that DON'T yet
 * have them in the brain — never overwrites the synapse manufacturer-sync driver's
 * data, never creates duplicate nodes. New catalog products get priced on the next
 * run. Filter: es>0 AND cost>0 (real negative margins are kept as valid signals).
 *
 * KNOWN v1 LIMITATION: cost is frozen at first sync for Picqer-only products
 * (gap-fill skips products that already have a cost). The driver refreshes its own
 * ~650; a v2 (cost-refresh with MSRP preservation, or a brain-side cost-source
 * priority) can make Picqer the authoritative live-cost source for all products.
 */
/**
 * Backfill Cost (Picqer fixedstockprice) + NL (skirmshop.nl) into the brain,
 * GAP-FILL only. Cost docs are keyed by productcode_supplier (the manufacturer
 * SKU — the SAME key the synapse driver uses) with productcode fallback, so a
 * push MERGES into the driver's existing ManufacturerProduct node (setting cost
 * on a driver node that had null cost) instead of creating a duplicate. Never
 * overwrites products that already show cost (the driver's ~536). MSRP is
 * preserved (MERGE). Filter: es>0 AND cost>0 (real negative margins kept).
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
  const nlRaw=await storeMap("skirmshop.nl"); const nl=new Map(); for(const [k,v] of nlRaw) nl.set(k, v.price);
  const brain=new Map(); let off=0,total=0;
  for(;;){ const j=await jget(`${BRAIN}/instances/skirmshop/prices/comparison?limit=200&offset=${off}`,{headers:{"X-API-Key":BKEY,Accept:"application/json"}}); const items=j.items||[]; total=j.total; for(const it of items) if(it.id) brain.set(it.id,{es:it.es_price,cost:it.cost,nl:it.nl_price}); off+=items.length; if(items.length<200||off>=total)break; }
  const pq=new Map(); let poff=0; for(;;){ const rows=await jget(`${PBASE}/products?offset=${poff}`,{headers:{Authorization:PAUTH,Accept:"application/json","User-Agent":"backfill"}}); if(!rows.length)break; for(const p of rows){ const c=Number(p.fixedstockprice)||0; pq.set(norm(p.productcode),{cost:c, sup:String(p.productcode_supplier||"").trim()}); } poff+=rows.length; if(rows.length<100)break; await sleep(110); }
  console.log(`maps: es=${es.size} nl=${nl.size} brain=${brain.size} picqer=${pq.size} APPLY=${APPLY}`);

  const costDocs=[], nlDocs=[];
  for(const [sku, info] of es){
    const b=brain.get(info.handle); if(!b) continue;
    // COST gap-fill — key by manufacturer SKU (productcode_supplier) to MERGE into
    // the driver's node; fall back to the ES productcode when supplier code is absent.
    const e=pq.get(sku); const cost=e&&e.cost;
    if(cost>0 && b.cost==null && b.es!=null && b.es>0){
      const key=(e.sup||info.skuOrig);
      costDocs.push({ source_id:`manufacturer:${key}`, content:`Picqer cost for ${key}: ${cost} EUR`,
        metadata:{ source:"manufacturer", sku:key, cost_eur:cost, our_product_handle:info.handle } });
    }
    // NL gap-fill (no dual-node issue: brain has no null-priced NL nodes).
    const nlp=nl.get(stripSP(sku));
    if(nlp!=null && b.nl==null){
      nlDocs.push({ source_id:`channel:NL:${info.skuOrig}`, content:`NL channel price for ${info.skuOrig}: €${nlp} (ex-VAT)`,
        metadata:{ source:"channel_offer", channel:"NL", sku:info.skuOrig, price:Number(nlp), currency:"EUR", vat_incl:false, our_product_handle:info.handle } });
    }
  }
  console.log(`candidates: cost=${costDocs.length} nl=${nlDocs.length}`);
  if(!APPLY){ console.log("DRY. sample cost:",JSON.stringify(costDocs.slice(0,4))); return; }
  let cIng=0,nIng=0;
  for(const c of chunk(costDocs,CHUNK)){ const r=await push("manufacturer",c); cIng+=r.chunks_ingested??r.documents_received??0; process.stdout.write("c"); }
  for(const c of chunk(nlDocs,CHUNK)){ const r=await push("channel",c); nIng+=r.chunks_ingested??r.documents_received??0; process.stdout.write("n"); }
  console.log(`\nPUSHED cost_docs=${costDocs.length} (ingested ${cIng}) | nl_docs=${nlDocs.length} (ingested ${nIng})`);
  console.log("DONE");
})().catch(e=>{console.error("ERR",e.message);process.exit(1)});
