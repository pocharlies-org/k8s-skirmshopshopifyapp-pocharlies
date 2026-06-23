/**
 * nl-llm-matcher (durable CronJob) — daily LLM pass that links remaining ES⇄NL
 * products one-by-one and writes the NL price into the brain ONLY on a strict
 * 100%-identical match (2-stage LLM judge). Complements the deterministic
 * Picqer-multichannel link (shared productcode_supplier, ~1,482 pairs); this
 * catches any remaining pair whose codes differ but products are unmistakably
 * identical. Verified: 0 false positives on 95 products + a negative control;
 * matches clearly-identical titles, conservatively skips ambiguous (colour/
 * edition) ones. Most of the ES residue is ES-only, so expect a LOW match yield.
 * Resumable: each decided ES product gets an NL ChannelOffer (match) or an
 * NL_NOMATCH marker (skip) so the next run never re-LLMs it. Match provenance in
 * ChannelOffer.content_hash = "llm:<nl_sku>:<conf>" (reversible). Audit = stdout.
 * v1: NL_NOMATCH markers are permanent (a v2 can expire them after N days).
 */
/**
 * nl-llm-matcher — match ES⇄NL products one-by-one with an LLM and link the NL
 * price into the brain ONLY when they are 100% the same product.
 *
 * Pipeline per unmatched ES product (es_price>0, no NL offer yet, not already
 * decided): TF-IDF candidate retrieval over the NL storefront by name (idsupplier
 * is NOT shared ES⇄NL, so text is the only cross-store signal) → enrich both sides
 * with Picqer (name, weight, productcode_supplier; storefront body_html) → LLM
 * judge (stage 1: pick the identical candidate; stage 2: strict yes/no confirm of
 * that single pair) → accept ONLY if confident & unique → push the NL ChannelOffer
 * for the ES handle (price from the NL storefront), provenance in content_hash.
 * Non-matches get an NL_NOMATCH marker so they are skipped on the next run
 * (resumable). DRY=1 logs decisions without writing. APPLY=1 writes.
 *
 * Env: BRAIN_URL, BRAIN_API_KEY, LITELLM_KEY, LITELLM_URL(opt), PICQER_API_KEY,
 *      PICQER_SUBDOMAIN, BATCH(opt, default 120), TOPK(opt, 6), MODEL(opt, tooling),
 *      MIN_CONF(opt, 0.9), APPLY(=1 to write).
 */
const BRAIN=(process.env.BRAIN_URL||process.env.POCHARLIES_RAG_URL||"http://skirmshop-brain.skirmshop-brain-prod.svc.cluster.local").replace(/\/+$/,"");
const BKEY=process.env.BRAIN_API_KEY||"";
const LURL=(process.env.LITELLM_URL||"http://litellm.litellm.svc.cluster.local:4000").replace(/\/+$/,"");
const LKEY=process.env.LITELLM_KEY||"";
const PKEY=process.env.PICQER_API_KEY||""; const SUB=process.env.PICQER_SUBDOMAIN||"skirmshop";
const PBASE=`https://${SUB}.picqer.com/api/v1`; const PAUTH="Basic "+Buffer.from(PKEY+":X").toString("base64");
const BATCH=Number(process.env.BATCH||120); const TOPK=Number(process.env.TOPK||6);
const MODEL=process.env.MODEL||"tooling"; const MIN_CONF=Number(process.env.MIN_CONF||0.9);
const APPLY=process.env.APPLY==="1";
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
// resilient fetch: retry on network error + 429 + 5xx (transient brain/litellm blips)
async function rfetch(url,opts={},tries=5){let last;for(let a=0;a<tries;a++){try{const r=await fetch(url,opts);if(r.status===429||r.status>=500){last=new Error("HTTP "+r.status);await sleep(1500*(a+1));continue;}return r;}catch(e){last=e;await sleep(1500*(a+1));}}throw last||new Error("rfetch failed "+url);}
const norm=s=>(s||"").toString().trim().toLowerCase();
const STOP=new Set("the a an of for with and or to in on at by de la el los las para con sin y o un una pcs set kit new para".split(/\s+/));
function toks(s){return [...new Set((s||"").toLowerCase().replace(/<[^>]+>/g," ").replace(/[^a-z0-9]+/g," ").split(/\s+/).filter(t=>t.length>1&&!STOP.has(t)))];}
function stripHtml(s){return (s||"").replace(/<[^>]+>/g," ").replace(/\s+/g," ").trim().slice(0,600);}
async function jget(url,opts={}){const r=await rfetch(url,opts);if(!r.ok)throw new Error("HTTP "+r.status+" "+url);return r.json();}
async function cypher(query){const r=await rfetch(`${BRAIN}/graph/skirmshop/cypher`,{method:"POST",headers:{"Content-Type":"application/json","X-API-Key":BKEY},body:JSON.stringify({query,limit:100000})});if(!r.ok)throw new Error("cypher HTTP "+r.status+": "+(await r.text()).slice(0,200));const j=await r.json();return j.rows||j.result||j.data||j;}
async function pushBrain(adapter,documents){if(!documents.length)return;const r=await rfetch(`${BRAIN}/instances/skirmshop/push-ingest`,{method:"POST",headers:{"Content-Type":"application/json","X-API-Key":BKEY},body:JSON.stringify({adapter,documents})});if(!r.ok)throw new Error("push HTTP "+r.status+": "+(await r.text()).slice(0,200));}
async function storeMap(host){const m=new Map();for(let p=1;p<=80;p++){const j=await jget(`https://${host}/products.json?limit=250&page=${p}`,{headers:{Accept:"application/json"}});const ps=j.products||[];if(!ps.length)break;for(const pr of ps){const prim=(pr.variants||[])[0]||{};const sku=norm(prim.sku);m.set(pr.handle,{handle:pr.handle,title:pr.title||"",body:stripHtml(pr.body_html),vendor:pr.vendor||"",sku,price:Number(prim.price)||null});}if(ps.length<250)break;await sleep(450);}return m;}
async function llm(messages,max_tokens=300){const r=await rfetch(`${LURL}/v1/chat/completions`,{method:"POST",headers:{"Content-Type":"application/json",Authorization:"Bearer "+LKEY},body:JSON.stringify({model:MODEL,messages,temperature:0,max_tokens})});if(!r.ok)throw new Error("llm HTTP "+r.status+": "+(await r.text()).slice(0,150));const j=await r.json();return (j.choices&&j.choices[0]&&j.choices[0].message&&j.choices[0].message.content)||"";}
function parseJson(s){const m=(s||"").match(/\{[\s\S]*\}/);if(!m)return null;try{return JSON.parse(m[0]);}catch{return null;}}

(async()=>{
  console.log(`nl-llm-matcher start APPLY=${APPLY} BATCH=${BATCH} TOPK=${TOPK} MODEL=${MODEL} MIN_CONF=${MIN_CONF}`);
  // 1. NL storefront candidates + price
  const nlMap=await storeMap("skirmshop.nl"); const nl=[...nlMap.values()];
  const esMap=await storeMap("skirmshop.es");
  // Picqer enrich: sku -> {name, weight, sup}
  const pq=new Map(); let poff=0;
  for(;;){const rows=await jget(`${PBASE}/products?offset=${poff}`,{headers:{Authorization:PAUTH,Accept:"application/json","User-Agent":"matcher"}});if(!rows.length)break;for(const p of rows)pq.set(norm(p.productcode),{name:p.name||"",weight:p.weight||null,sup:p.productcode_supplier||""});poff+=rows.length;if(rows.length<100)break;await sleep(110);}
  console.log(`nl_candidates=${nl.length} es_store=${esMap.size} picqer=${pq.size}`);
  // TF-IDF over NL names (storefront title + picqer name)
  const df=new Map(); const nlTok=nl.map(c=>{const t=toks(c.title+" "+((pq.get(c.sku)||{}).name||""));t.forEach(w=>df.set(w,(df.get(w)||0)+1));return t;});
  const N=nl.length; const idf=w=>Math.log((N+1)/((df.get(w)||0)+1));
  const post=new Map(); nlTok.forEach((t,i)=>t.forEach(w=>{if(!post.has(w))post.set(w,[]);post.get(w).push(i);}));
  function candidates(esText){const q=toks(esText);const sc=new Map();for(const w of q){const idfw=idf(w);if(idfw<0.5)continue;for(const i of (post.get(w)||[]))sc.set(i,(sc.get(i)||0)+idfw);}return [...sc.entries()].sort((a,b)=>b[1]-a[1]).slice(0,TOPK).map(([i])=>nl[i]);}

  // 2. ES unmatched targets: es_price>0 AND no NL offer, minus already-decided (NL or NL_NOMATCH marker)
  const decidedRows=await cypher("MATCH (p:Product)-[:HAS_CHANNEL_PRICE]->(co:ChannelOffer) WHERE co.channel IN ['NL','NL_NOMATCH'] RETURN p.id AS id, p.sku AS sku, co.price AS price, co.channel AS ch");
  const decided=new Set((decidedRows||[]).map(r=>r.id));
  const targets=[]; let off=0,total=0;
  for(;;){const j=await jget(`${BRAIN}/instances/skirmshop/prices/comparison?limit=200&offset=${off}`,{headers:{"X-API-Key":BKEY,Accept:"application/json"}});const items=j.items||[];total=j.total;for(const it of items){if(it.es_price!=null&&it.es_price>0&&it.nl_price==null&&!decided.has(it.id))targets.push(it);}off+=items.length;if(items.length<200||off>=total)break;}
  console.log(`unmatched_targets=${targets.length} (decided_skip=${decided.size}); processing batch=${Math.min(BATCH,targets.length)}`);

  let matched=0,nomatch=0,nocand=0; const offers=[],markers=[]; const decisions=[];
  for(const t of targets.slice(0,BATCH)){
    const esStore=esMap.get(t.id)||{}; const esPq=pq.get(norm(t.sku))||{};
    const esText=`${esStore.title||t.title||""} ${esPq.name||""}`.trim();
    if(!esText){ nocand++; markers.push(mk(t)); continue; }
    const cands=candidates(esText);
    if(!cands.length){ nocand++; nomatch++; markers.push(mk(t)); decisions.push({es:t.id,result:"no-candidate"}); continue; }
    // Stage 1: pick identical candidate
    const esBlock=`ES PRODUCT:\n- title: ${esStore.title||t.title}\n- picqer_name: ${esPq.name||"-"}\n- weight_g: ${esPq.weight??"-"}\n- vendor: ${esStore.vendor||t.vendor||"-"}\n- description: ${esStore.body||"-"}`;
    const candBlock=cands.map((c,i)=>{const cp=pq.get(c.sku)||{};return `[${i}] title: ${c.title} | picqer_name: ${cp.name||"-"} | weight_g: ${cp.weight??"-"} | vendor: ${c.vendor||"-"} | desc: ${c.body||"-"}`;}).join("\n");
    let pick=null;
    try{
      const out=await llm([
        {role:"system",content:"You match identical airsoft products across two stores (Spanish ES and Dutch NL, titles may be in different languages). Two products MATCH only if they are the EXACT same product: same brand, same model, same variant (colour/size/length/material/edition). Different colour, size, length, version or bundle = NOT a match. When unsure, do NOT match. Reply ONLY with JSON."},
        {role:"user",content:`${esBlock}\n\nNL CANDIDATES:\n${candBlock}\n\nReturn JSON {"match_index": <index of the identical candidate, or null if none is the exact same product>, "confidence": <0..1>, "reason": "<short>"}.`}
      ]);
      pick=parseJson(out);
    }catch(e){ decisions.push({es:t.id,result:"llm-error-stage1",err:e.message}); markers.push(mk(t)); nomatch++; continue; }
    const idx=pick&&Number.isInteger(pick.match_index)?pick.match_index:null;
    if(idx==null||idx<0||idx>=cands.length||!(Number(pick.confidence)>=MIN_CONF)){
      nomatch++; markers.push(mk(t)); decisions.push({es:t.id,es_title:esStore.title||t.title,result:"no-match",conf:pick&&pick.confidence,reason:pick&&pick.reason}); continue;
    }
    const cand=cands[idx]; const cp=pq.get(cand.sku)||{};
    // Stage 2: strict yes/no confirm of the single chosen pair
    let conf2=null;
    try{
      const out2=await llm([
        {role:"system",content:"Answer ONLY JSON. Two airsoft products are the SAME only if identical brand+model+variant (colour/size/length/version/edition). Be strict: any difference = not same."},
        {role:"user",content:`A: title="${esStore.title||t.title}" name="${esPq.name||""}" weight=${esPq.weight??"-"}\nB: title="${cand.title}" name="${cp.name||""}" weight=${cp.weight??"-"}\nAre A and B the exact same product? Return {"same": true|false, "confidence":0..1, "reason":"<short>"}.`}
      ],150);
      conf2=parseJson(out2);
    }catch(e){ markers.push(mk(t)); nomatch++; decisions.push({es:t.id,result:"llm-error-stage2",err:e.message}); continue; }
    if(conf2&&conf2.same===true&&Number(conf2.confidence)>=MIN_CONF&&cand.price!=null){
      matched++;
      offers.push({source_id:`channel:NL:${t.sku}`,content:`LLM-matched NL price for ${t.sku} (nl ${cand.sku})`,metadata:{source:"channel_offer",channel:"NL",sku:t.sku,price:Number(cand.price),currency:"EUR",vat_incl:false,our_product_handle:t.id,title:esStore.title||t.title,content_hash:`llm:${cand.sku}:${(conf2.confidence).toFixed(2)}`}});
      decisions.push({es:t.id,es_title:esStore.title||t.title,nl:cand.sku,nl_title:cand.title,nl_price:cand.price,conf:conf2.confidence,result:"MATCH",reason:conf2.reason});
    }else{
      nomatch++; markers.push(mk(t)); decisions.push({es:t.id,es_title:esStore.title||t.title,nl_cand:cand.title,result:"reject-stage2",conf:conf2&&conf2.confidence,reason:conf2&&conf2.reason});
    }
  }
  function mk(t){return {source_id:`channel:NL_NOMATCH:${t.sku}`,content:`no NL match for ${t.sku}`,metadata:{source:"channel_offer",channel:"NL_NOMATCH",sku:t.sku,price:0,currency:"EUR",vat_incl:false,our_product_handle:t.id,content_hash:"llm-nomatch"}};}
  console.log(`\nDECISIONS (matched=${matched} nomatch=${nomatch} nocand=${nocand}):`);
  for(const d of decisions) console.log(JSON.stringify(d));
  if(!APPLY){ console.log(`\nDRY — would push ${offers.length} NL offers + ${markers.length} no-match markers`); return; }
  await pushBrain("channel",offers); await pushBrain("channel",markers);
  console.log(`\nAPPLIED: ${offers.length} NL matches + ${markers.length} markers pushed.`);
  console.log("DONE");
})().catch(e=>{console.error("FATAL",e.message);process.exit(1)});
