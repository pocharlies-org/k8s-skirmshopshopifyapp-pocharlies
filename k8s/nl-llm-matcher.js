/**
 * nl-llm-matcher (durable CronJob) — daily LLM pass that links remaining ES⇄NL
 * products one-by-one and writes the NL price into `catalogo` ONLY on a strict
 * 100%-identical match (2-stage LLM judge). Complements the deterministic
 * Picqer-multichannel link (shared productcode_supplier, ~1,482 pairs); this
 * catches any remaining pair whose codes differ but products are unmistakably
 * identical. Verified: 0 false positives on 95 products + a negative control;
 * matches clearly-identical titles, conservatively skips ambiguous (colour/
 * edition) ones. Most of the ES residue is ES-only, so expect a LOW match yield.
 * Resumable: each decided ES product gets an NL channel_offer (match) or an
 * NL_NOMATCH marker (skip) so the next run never re-LLMs it. Match provenance in
 * channel_offer.content_hash = "llm:<nl_sku>:<conf>" (reversible). Audit = stdout.
 * v1: NL_NOMATCH markers are permanent (a v2 can expire them after N days).
 * SC-5827: reads and writes `catalogo` directly (was: brain channel-offers/
 * comparison + push-ingest); runs in the rag-app image (@prisma/client) with this
 * script from a ConfigMap.
 */
/**
 * nl-llm-matcher — match ES⇄NL products one-by-one with an LLM and link the NL
 * price into catalogo ONLY when they are 100% the same product.
 *
 * Pipeline per unmatched ES product (es_price>0, no NL offer yet, not already
 * decided): TF-IDF candidate retrieval over the NL storefront by name (idsupplier
 * is NOT shared ES⇄NL, so text is the only cross-store signal) → enrich both sides
 * with Picqer (name, weight, productcode_supplier; storefront body_html) → LLM
 * judge (stage 1: pick the identical candidate; stage 2: strict yes/no confirm of
 * that single pair) → accept ONLY if confident & unique → write the NL
 * channel_offer for the ES handle (price from the NL storefront), provenance in
 * content_hash. Non-matches get an NL_NOMATCH marker so they are skipped on the
 * next run (resumable). DRY=1 logs decisions without writing. APPLY=1 writes.
 *
 * Env: CATALOGO_DATABASE_URL, LITELLM_KEY, LITELLM_URL(opt), PICQER_API_KEY,
 *      PICQER_SUBDOMAIN, BATCH(opt, default 50), TOPK(opt, 6), MODEL(opt, tooling),
 *      MIN_CONF(opt, 0.9), FETCH_TIMEOUT_MS(opt, default 30000),
 *      FETCH_ATTEMPTS(opt, default 2), RUN_BUDGET_MS(opt, default 2700000),
 *      COMPLETION_RESERVE_MS(opt, default 600000),
 *      MAX_LLM_ERRORS(opt, default 2), APPLY(=1 to write).
 */
if(!process.env.CATALOGO_DATABASE_URL){console.error("CATALOGO_DATABASE_URL is required");process.exit(1);}
const { PrismaClient } = require("@prisma/client");
const CATALOGO_URL = process.env.CATALOGO_DATABASE_URL + (process.env.CATALOGO_DATABASE_URL.includes("?") ? "&" : "?") + "connection_limit=3&pool_timeout=15&connect_timeout=10&application_name=nl-llm-matcher";
const db = new PrismaClient({ datasources: { db: { url: CATALOGO_URL } }, log: ["error"] });
const LURL=(process.env.LITELLM_URL||"http://litellm.litellm.svc.cluster.local:4000").replace(/\/+$/,"");
const LKEY=process.env.LITELLM_KEY||"";
const PKEY=process.env.PICQER_API_KEY||""; const SUB=process.env.PICQER_SUBDOMAIN||"skirmshop";
const PBASE=`https://${SUB}.picqer.com/api/v1`; const PAUTH="Basic "+Buffer.from(PKEY+":X").toString("base64");
const BATCH=Number(process.env.BATCH||50); const TOPK=Number(process.env.TOPK||6);
const MODEL=process.env.MODEL||"tooling"; const MIN_CONF=Number(process.env.MIN_CONF||0.9);
const FETCH_TIMEOUT_MS=Math.max(1000,Number(process.env.FETCH_TIMEOUT_MS||30000));
const FETCH_ATTEMPTS=Math.max(1,Number(process.env.FETCH_ATTEMPTS||2));
const RUN_BUDGET_MS=Math.max(60000,Number(process.env.RUN_BUDGET_MS||2700000));
const COMPLETION_RESERVE_MS=Math.max(1000,Number(process.env.COMPLETION_RESERVE_MS||600000));
const MAX_LLM_ERRORS=Math.max(0,Number(process.env.MAX_LLM_ERRORS||2));
const APPLY=process.env.APPLY==="1";
const RUN_STARTED_AT=Date.now();
const runRemainingMs=()=>RUN_BUDGET_MS-(Date.now()-RUN_STARTED_AT);
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
// Bound every I/O attempt so a stalled upstream cannot consume the Job deadline.
async function rfetch(url,opts={},tries=FETCH_ATTEMPTS){
  let last;
  const target=new URL(url).origin+new URL(url).pathname;
  for(let a=0;a<tries;a++){
    const remaining=runRemainingMs();
    if(remaining<=0)throw new Error(`run budget exhausted before ${target}`);
    const controller=new AbortController();
    const timeoutMs=Math.min(FETCH_TIMEOUT_MS,remaining);
    const timeout=setTimeout(()=>controller.abort(new Error(`timeout after ${timeoutMs}ms`)),timeoutMs);
    try{
      const response=await fetch(url,{...opts,signal:controller.signal});
      if(response.status===429||response.status>=500){
        last=new Error(`HTTP ${response.status} ${target}`);
        console.warn(`retryable response target=${target} attempt=${a+1}/${tries} status=${response.status}`);
        await response.body?.cancel();
        await sleep(Math.min(1500*(a+1),Math.max(0,runRemainingMs())));
        continue;
      }
      return {status:response.status,body:await response.text()};
    }catch(e){
      last=new Error(`request ${target} attempt=${a+1}/${tries}: ${e.message}`);
      console.warn(last.message);
      await sleep(Math.min(1500*(a+1),Math.max(0,runRemainingMs())));
    }finally{
      clearTimeout(timeout);
    }
  }
  throw last||new Error(`rfetch failed ${target}`);
}
const norm=s=>(s||"").toString().trim().toLowerCase();
const STOP=new Set("the a an of for with and or to in on at by de la el los las para con sin y o un una pcs set kit new para".split(/\s+/));
function toks(s){return [...new Set((s||"").toLowerCase().replace(/<[^>]+>/g," ").replace(/[^a-z0-9]+/g," ").split(/\s+/).filter(t=>t.length>1&&!STOP.has(t)))];}
function stripHtml(s){return (s||"").replace(/<[^>]+>/g," ").replace(/\s+/g," ").trim().slice(0,600);}
async function jget(url,opts={}){const r=await rfetch(url,opts);if(r.status<200||r.status>=300)throw new Error("HTTP "+r.status+" "+url);return JSON.parse(r.body);}
// Write one channel_offer (NL match or NL_NOMATCH marker) + its product edge into
// catalogo — the same shape app/services/catalogo/channel-offers.server.ts uses.
async function writeChannelOffer(id,sku,channel,price,handle,title,hash){
  const now=new Date().toISOString();
  await db.$executeRawUnsafe(`INSERT INTO catalogo.channel_offer (id, title, sku, channel, kind, price, currency, vat_incl, observed_at, content_hash)
    VALUES ($1,$2,$3,$4,'channel',$5,'EUR',false,$6::timestamptz,$7)
    ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, sku = EXCLUDED.sku,
      channel = EXCLUDED.channel, price = EXCLUDED.price,
      observed_at = EXCLUDED.observed_at, content_hash = EXCLUDED.content_hash`,
    id, title, sku, channel, price, now, hash);
  await db.$executeRawUnsafe(`INSERT INTO catalogo.product_channel_offer (product_id, channel_offer_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, handle, id);
}
async function storeMap(host){const m=new Map();for(let p=1;p<=80;p++){const j=await jget(`https://${host}/products.json?limit=250&page=${p}`,{headers:{Accept:"application/json"}});const ps=j.products||[];if(!ps.length)break;for(const pr of ps){const prim=(pr.variants||[])[0]||{};const sku=norm(prim.sku);m.set(pr.handle,{handle:pr.handle,title:pr.title||"",body:stripHtml(pr.body_html),vendor:pr.vendor||"",sku,price:Number(prim.price)||null});}if(ps.length<250)break;await sleep(450);}return m;}
async function llm(messages,max_tokens=300){const r=await rfetch(`${LURL}/v1/chat/completions`,{method:"POST",headers:{"Content-Type":"application/json",Authorization:"Bearer "+LKEY},body:JSON.stringify({model:MODEL,messages,temperature:0,max_tokens})});if(r.status<200||r.status>=300)throw new Error("llm HTTP "+r.status+": "+r.body.slice(0,150));const j=JSON.parse(r.body);return (j.choices&&j.choices[0]&&j.choices[0].message&&j.choices[0].message.content)||"";}
function parseJson(s){const m=(s||"").match(/\{[\s\S]*\}/);if(!m)return null;try{return JSON.parse(m[0]);}catch{return null;}}

(async()=>{
  console.log(`nl-llm-matcher start APPLY=${APPLY} BATCH=${BATCH} TOPK=${TOPK} MODEL=${MODEL} MIN_CONF=${MIN_CONF} FETCH_TIMEOUT_MS=${FETCH_TIMEOUT_MS} FETCH_ATTEMPTS=${FETCH_ATTEMPTS} RUN_BUDGET_MS=${RUN_BUDGET_MS} COMPLETION_RESERVE_MS=${COMPLETION_RESERVE_MS}`);
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

  // 2. ES unmatched targets: es_price>0 AND no NL offer AND no NL_NOMATCH marker
  // (both edges live in catalogo), straight from catalogo.product (replaces the
  // brain's channel-offers + /prices/comparison reads).
  const targets=await db.$queryRawUnsafe(`SELECT p.id, p.sku, p.price AS es_price, p.title, p.vendor
    FROM catalogo.product p
    WHERE p.deleted_at IS NULL AND p.price IS NOT NULL AND p.price > 0 AND p.sku IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM catalogo.product_channel_offer pco
                        JOIN catalogo.channel_offer co ON co.id = pco.channel_offer_id
                       WHERE pco.product_id = p.id AND co.channel IN ('NL','NL_NOMATCH'))
    ORDER BY p.updated_at DESC NULLS LAST`);
  console.log(`unmatched_targets=${targets.length}; processing batch=${Math.min(BATCH,targets.length)}`);

  let matched=0,nomatch=0,nocand=0,llmErrors=0,budgetStopped=false,processed=0; const offers=[],markers=[]; const decisions=[];
  for(const t of targets.slice(0,BATCH)){
    if(runRemainingMs()<=COMPLETION_RESERVE_MS){
      budgetStopped=true;
      console.warn(`run_budget_stop remaining_ms=${runRemainingMs()} reserve_ms=${COMPLETION_RESERVE_MS} processed=${processed}`);
      break;
    }
    processed++;
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
    }catch(e){ llmErrors++; decisions.push({es:t.id,result:"llm-error-stage1",err:e.message}); continue; }
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
    }catch(e){ llmErrors++; decisions.push({es:t.id,result:"llm-error-stage2",err:e.message}); continue; }
    if(conf2&&conf2.same===true&&Number(conf2.confidence)>=MIN_CONF&&cand.price!=null){
      matched++;
      offers.push({id:`channel:NL:${t.sku}`,sku:t.sku,channel:"NL",price:Number(cand.price),handle:t.id,title:esStore.title||t.title,hash:`llm:${cand.sku}:${(conf2.confidence).toFixed(2)}`});
      decisions.push({es:t.id,es_title:esStore.title||t.title,nl:cand.sku,nl_title:cand.title,nl_price:cand.price,conf:conf2.confidence,result:"MATCH",reason:conf2.reason});
    }else{
      nomatch++; markers.push(mk(t)); decisions.push({es:t.id,es_title:esStore.title||t.title,nl_cand:cand.title,result:"reject-stage2",conf:conf2&&conf2.confidence,reason:conf2&&conf2.reason});
    }
  }
  function mk(t){return {id:`channel:NL_NOMATCH:${t.sku}`,sku:t.sku,channel:"NL_NOMATCH",price:0,handle:t.id,title:null,hash:"llm-nomatch"};}
  if(budgetStopped&&processed===0&&targets.length)throw new Error("run budget exhausted before processing any target");
  console.log(`\nDECISIONS (processed=${processed} matched=${matched} nomatch=${nomatch} nocand=${nocand} llm_errors=${llmErrors} budget_stopped=${budgetStopped}):`);
  for(const d of decisions) console.log(JSON.stringify(d));
  if(!APPLY){ console.log(`\nDRY — would write ${offers.length} NL offers + ${markers.length} no-match markers`); return; }
  for(const o of [...offers,...markers]){ await writeChannelOffer(o.id,o.sku,o.channel,o.price,o.handle,o.title,o.hash); }
  if(llmErrors>MAX_LLM_ERRORS)throw new Error(`llm errors exceeded threshold: ${llmErrors}/${processed} (max ${MAX_LLM_ERRORS})`);
  console.log(`\nAPPLIED: ${offers.length} NL matches + ${markers.length} markers written to catalogo.`);
  console.log("DONE");
})().catch(e=>{console.error("FATAL",e.message);process.exit(1)}).finally(()=>db.$disconnect());
