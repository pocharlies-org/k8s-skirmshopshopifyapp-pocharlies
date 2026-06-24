/**
 * competitor-llm-matcher — link our Shopify products to competitor products
 * (scraped `competitor:*` CompetitorProduct nodes) one-by-one with an LLM, ONLY
 * on a strict 100%-identical match (2-stage judge). Mirrors nl-llm-matcher.
 *
 * Pipeline per our product (es_price>0, has vendor, not already decided):
 *   brand-scope competitor nodes by normalised vendor==brand → TF-IDF candidate
 *   retrieval by title → LLM stage1 (pick the identical candidate) → stage2
 *   (strict yes/no confirm) → accept ONLY if confident+unique → emit a
 *   `competitor_match` push-ingest doc so the brain materialises
 *   (Product)-[:PRODUCT_MATCH]->(CompetitorProduct). DRY logs; APPLY=1 writes.
 *
 * Env: BRAIN_URL, BRAIN_API_KEY, LITELLM_KEY, LITELLM_URL(opt), MODEL(opt tooling),
 *      MIN_CONF(opt .9), TOPK(opt 6), BATCH(opt 120), BRAND(opt — sample 1 brand),
 *      LIMIT(opt — cap products scanned), APPLY(=1 to write).
 */
const BRAIN=(process.env.BRAIN_URL||process.env.POCHARLIES_RAG_URL||"http://skirmshop-brain.skirmshop-brain-prod.svc.cluster.local").replace(/\/+$/,"");
const BKEY=process.env.BRAIN_API_KEY||"";
const LURL=(process.env.LITELLM_URL||"http://litellm.litellm.svc.cluster.local:4000").replace(/\/+$/,"");
const LKEY=process.env.LITELLM_KEY||"";
const MODEL=process.env.MODEL||"tooling"; const MIN_CONF=Number(process.env.MIN_CONF||0.9);
const TOPK=Number(process.env.TOPK||6); const BATCH=Number(process.env.BATCH||120);
const BRAND=(process.env.BRAND||"").trim().toLowerCase(); const LIMIT=Number(process.env.LIMIT||0);
const APPLY=process.env.APPLY==="1";
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function rfetch(url,opts={},tries=5){let last;for(let a=0;a<tries;a++){try{const r=await fetch(url,opts);if(r.status===429||r.status>=500){last=new Error("HTTP "+r.status);await sleep(1500*(a+1));continue;}return r;}catch(e){last=e;await sleep(1500*(a+1));}}throw last||new Error("rfetch failed "+url);}
const norm=s=>(s||"").toString().trim().toLowerCase();
const normBrand=s=>norm(s).replace(/[^a-z0-9]+/g," ").trim();
const STOP=new Set("the a an of for with and or to in on at by de la el los las para con sin y o un una pcs set kit new for airsoft".split(/\s+/));
function toks(s){return [...new Set((s||"").toLowerCase().replace(/<[^>]+>/g," ").replace(/[^a-z0-9]+/g," ").split(/\s+/).filter(t=>t.length>1&&!STOP.has(t)))];}
async function jget(url,opts={}){const r=await rfetch(url,opts);if(!r.ok)throw new Error("HTTP "+r.status+" "+url);return r.json();}
async function cypher(query){const r=await rfetch(`${BRAIN}/graph/skirmshop/cypher`,{method:"POST",headers:{"Content-Type":"application/json","X-API-Key":BKEY},body:JSON.stringify({query,limit:100000})});if(!r.ok)throw new Error("cypher HTTP "+r.status+": "+(await r.text()).slice(0,200));const j=await r.json();return j.rows||j.result||j.data||j;}
// The cypher HTTP endpoint hard-caps each response at 500 rows; paginate via SKIP/LIMIT.
async function cypherAll(matchReturn,orderBy){let out=[];for(let s=0;;s+=500){const rows=await cypher(`${matchReturn} ORDER BY ${orderBy} SKIP ${s} LIMIT 500`);out=out.concat(rows||[]);if(!rows||rows.length<500)break;}return out;}
async function pushBrain(adapter,documents){if(!documents.length)return;for(const c of chunk(documents,100)){const r=await rfetch(`${BRAIN}/instances/skirmshop/push-ingest`,{method:"POST",headers:{"Content-Type":"application/json","X-API-Key":BKEY},body:JSON.stringify({adapter,documents:c})});if(!r.ok)throw new Error("push HTTP "+r.status+": "+(await r.text()).slice(0,200));process.stdout.write("p");}}
function chunk(a,n){const o=[];for(let i=0;i<a.length;i+=n)o.push(a.slice(i,i+n));return o;}
async function llm(messages,max_tokens=250){const r=await rfetch(`${LURL}/v1/chat/completions`,{method:"POST",headers:{"Content-Type":"application/json",Authorization:"Bearer "+LKEY},body:JSON.stringify({model:MODEL,messages,temperature:0,max_tokens})});if(!r.ok)throw new Error("llm HTTP "+r.status+": "+(await r.text()).slice(0,150));const j=await r.json();return (j.choices&&j.choices[0]&&j.choices[0].message&&j.choices[0].message.content)||"";}
function parseJson(s){const m=(s||"").match(/\{[\s\S]*\}/);if(!m)return null;try{return JSON.parse(m[0]);}catch{return null;}}

(async()=>{
  console.log(`competitor-llm-matcher start APPLY=${APPLY} MODEL=${MODEL} MIN_CONF=${MIN_CONF} TOPK=${TOPK} BATCH=${BATCH} BRAND='${BRAND||"*"}' LIMIT=${LIMIT}`);
  // 1. competitor priced nodes -> brand index (paginated; ~21.7k nodes)
  const comp=await cypherAll("MATCH (c:CompetitorProduct) WHERE c.id STARTS WITH 'competitor:' AND c.brand IS NOT NULL AND c.price IS NOT NULL RETURN c.id AS id, c.title AS title, c.brand AS brand, c.price AS price, c.domain AS domain, c.url AS url","c.id");
  console.log(`competitor_nodes=${comp.length}`);
  // Global TF-IDF over ALL competitor titles+brand. Our Product.vendor does NOT
  // align with competitor.brand naming, so we retrieve by TITLE (brand+model tokens
  // live in both titles) and let the strict 2-stage LLM judge precision.
  const df=new Map(); const cTok=comp.map(c=>{const t=toks(`${c.title} ${c.brand}`);t.forEach(w=>df.set(w,(df.get(w)||0)+1));return t;});
  const Nc=comp.length||1; const idf=w=>Math.log((Nc+1)/((df.get(w)||0)+1));
  const post=new Map(); cTok.forEach((t,i)=>t.forEach(w=>{if(!post.has(w))post.set(w,[]);post.get(w).push(i);}));
  function candidatesFor(it){const q=toks(`${it.vendor||""} ${it.title}`);const sc=new Map();for(const w of q){const iw=idf(w);if(iw<0.5)continue;for(const i of (post.get(w)||[]))sc.set(i,(sc.get(i)||0)+iw);}return [...sc.entries()].sort((a,b)=>b[1]-a[1]).slice(0,TOPK).map(([i])=>comp[i]);}
  // 2. already-decided (existing PRODUCT_MATCH or checked marker)
  const decRows=await cypherAll("MATCH (p:Product) WHERE p.competitor_checked_at IS NOT NULL RETURN p.id AS id","p.id");
  const decided=new Set((decRows||[]).map(r=>r.id));
  const matRows=await cypherAll("MATCH (p:Product)-[:PRODUCT_MATCH]->(:CompetitorProduct) RETURN DISTINCT p.id AS id","p.id");
  for(const r of (matRows||[])) decided.add(r.id);
  // 3. our products (active, es_price>0, vendor present) from comparison feed
  const targets=[]; let off=0,total=0;
  for(;;){const j=await jget(`${BRAIN}/instances/skirmshop/prices/comparison?status=active&limit=200&offset=${off}`,{headers:{"X-API-Key":BKEY,Accept:"application/json"}});const items=j.items||[];total=j.total;for(const it of items){if(it.es_price>0&&!decided.has(it.id)){if(BRAND&&normBrand(it.vendor)!==BRAND)continue;targets.push(it);}}off+=items.length;if(items.length<200||off>=total)break;}
  console.log(`targets(active es>0, undecided)=${targets.length} decided_skip=${decided.size}`);

  let matched=0,nomatch=0,nocand=0; const docs=[],markers=[]; const decisions=[];
  const slice=LIMIT>0?targets.slice(0,LIMIT):targets.slice(0,BATCH);
  const CONC=Number(process.env.CONC||6);
  function mk(t){return {source_id:`match_nomatch:${t.id}`,content:`competitor checked, no match for ${t.id}`,metadata:{source:"competitor_match",our_product_handle:t.id,no_match:true,match_method:"llm"}};}

  // Process ONE product: brand-blind global candidates -> 2-stage strict LLM judge.
  // Returns {kind:'match'|'nomatch'|'nocand', decision, doc?, marker?}.
  async function processOne(t){
    const cands=candidatesFor(t);
    if(!cands.length) return {kind:"nocand", decision:{p:t.id,result:"no-candidate"}, marker:mk(t)};
    const esBlock=`OUR PRODUCT (Spanish store):\n- title: ${t.title}\n- vendor: ${t.vendor}\n- sku: ${t.sku||"-"}`;
    const candBlock=cands.map((c,i)=>`[${i}] title: ${c.title} | brand: ${c.brand} | store: ${c.domain} | price: ${c.price}`).join("\n");
    let pick=null;
    try{
      const out=await llm([
        {role:"system",content:"You match identical airsoft/tactical products between our Spanish store and a competitor store. Competitor titles may be in English or German. Two products MATCH only if they are the EXACT same product: same brand, same model, same variant (colour/size/length/material/edition/version/capacity). Different colour, size, length, version, capacity or bundle = NOT a match. When unsure, do NOT match. Reply ONLY with JSON."},
        {role:"user",content:`${esBlock}\n\nCOMPETITOR CANDIDATES:\n${candBlock}\n\nReturn JSON {"match_index": <index of the exact same product, or null>, "confidence": <0..1>, "reason": "<short>"}.`}
      ]);
      pick=parseJson(out);
    }catch(e){return {kind:"nomatch", decision:{p:t.id,result:"llm-error-1",err:e.message}, marker:mk(t)};}
    const idx=pick&&Number.isInteger(pick.match_index)?pick.match_index:null;
    if(idx==null||idx<0||idx>=cands.length||!(Number(pick.confidence)>=MIN_CONF))
      return {kind:"nomatch", decision:{p:t.id,title:t.title,result:"no-match",conf:pick&&pick.confidence,reason:pick&&pick.reason}, marker:mk(t)};
    const cand=cands[idx];
    let c2=null;
    try{
      const out2=await llm([
        {role:"system",content:"Answer ONLY JSON. Two airsoft/tactical products are the SAME only if identical brand+model+variant (colour/size/length/version/capacity/edition). Be strict: any difference = not same."},
        {role:"user",content:`A (ours): "${t.title}" vendor="${t.vendor}"\nB (competitor): "${cand.title}" brand="${cand.brand}"\nAre A and B the exact same product? Return {"same": true|false, "confidence":0..1, "reason":"<short>"}.`}
      ],150);
      c2=parseJson(out2);
    }catch(e){return {kind:"nomatch", decision:{p:t.id,result:"llm-error-2",err:e.message}, marker:mk(t)};}
    if(c2&&c2.same===true&&Number(c2.confidence)>=MIN_CONF)
      return {kind:"match",
        doc:{source_id:`match:${t.id}:${cand.id}`,content:`PRODUCT_MATCH ${t.title} == ${cand.title} (${cand.domain})`,metadata:{source:"competitor_match",our_product_handle:t.id,competitor_id:cand.id,confidence:Number(c2.confidence),match_method:"llm",domain:cand.domain,competitor_price:Number(cand.price)}},
        decision:{p:t.id,title:t.title,comp:cand.id,comp_title:cand.title,price:cand.price,domain:cand.domain,conf:c2.confidence,result:"MATCH",reason:c2.reason}};
    return {kind:"nomatch", decision:{p:t.id,title:t.title,cand:cand.title,result:"reject-2",conf:c2&&c2.confidence,reason:c2&&c2.reason}, marker:mk(t)};
  }

  // Concurrency pool + incremental push (so progress survives a crash; resumable via markers).
  for(let i=0;i<slice.length;i+=CONC){
    const group=slice.slice(i,i+CONC);
    const res=await Promise.all(group.map(t=>processOne(t).catch(e=>({kind:"nomatch",decision:{p:t.id,result:"fatal",err:e.message},marker:mk(t)}))));
    const gdocs=[],gmarkers=[];
    for(const r of res){ decisions.push(r.decision); if(r.kind==="match"){matched++; if(r.doc)gdocs.push(r.doc);} else if(r.kind==="nocand"){nocand++; if(r.marker)gmarkers.push(r.marker);} else {nomatch++; if(r.marker)gmarkers.push(r.marker);} }
    if(APPLY){ await pushBrain("competitor",gdocs); await pushBrain("competitor",gmarkers); }
    process.stdout.write(`\r${Math.min(i+CONC,slice.length)}/${slice.length} matched=${matched} nomatch=${nomatch} nocand=${nocand}   `);
  }
  console.log(`\nMATCHES (matched=${matched} nomatch=${nomatch} nocand=${nocand} of ${slice.length}):`);
  for(const d of decisions) if(d.result==="MATCH") console.log(JSON.stringify(d));
  if(!APPLY){console.log(`\nDRY — would push ${matched} PRODUCT_MATCH + ${nomatch+nocand} markers`); return;}
  console.log(`\nAPPLIED (incremental): ${matched} matches + ${nomatch+nocand} markers pushed.`);
  console.log("DONE");
})().catch(e=>{console.error("FATAL",e.message);process.exit(1)});
