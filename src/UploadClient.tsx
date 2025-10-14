import React, { useEffect, useMemo, useRef, useState } from "react";

/** =========================================================
 *  Chunk Uploader — Clean Multi-Batch UI (Real-time)
 *  - One clean drag area per batch
 *  - Server limits shown (via GET /limits if present, else learned from errors)
 *  - Speed + ETA: WS first, 1s polling fallback, client PUT fallback
 *  - Correctly flips from "queued" to "uploading" if server already received bytes
 *  - Resumable (createOrResumeBatch + /uploads/:id/status)
 *  - Waits for all parts on server before /complete
 *  - Clear error surfacing (incl. /complete 400s)
 *  - Repairs missing parts on /complete errors, then retries /complete
 *  - NEW (fix): Cleans local resume/progress/WS artifacts on done/cancel/error/remove/clear
 *  - NEW: Treat batch_not_ready & Bethel transient errors as soft success
 *  - NEW: Backoff when re-finalizing after repair
 * ========================================================= */

type ServerSession = { id: string; wsToken?: string; partSize: number; batchId: string };
type FileKey = string;

type FileItem = {
  key: FileKey;
  file: File;
  status: "queued" | "uploading" | "paused" | "done" | "error" | "canceled";
  message?: string;
  uploadId?: string;
  partSize?: number;
  bytesSent: number;
  partsDone: number;
  partsTotal: number;
  speedBps: number;
  etaMs: number;
  createdAt: number;
  controller?: AbortController;
  startedAt?: number;
  finalAvgBps?: number;
  totalTimeMs?: number;
};

type UploaderSettings = {
  serverUrl: string;
  token: string;
  partSizeBytes: number;
  fileConcurrency: number;
  finalizeChecksum: boolean;
};

type Batch = {
  id: string;
  title: string;
  createdAt: number;
  expanded: boolean;
  files: FileItem[];
  // user-supplied batch metadata
  did?: string;
  docid?: string;
  doctype?: string;
  docname?: string;   // server may map to "dockname"
  path?: string;
};

type ServerHints = {
  partSizeMin?: number;
  partSizeMax?: number;
  maxConcurrentFiles?: number;
  maxChunksPerUpload?: number;
  globalUploadLimit?: number;
  maxBytesPerMinute?: number;
  fileSizeMax?: number;
  // NEW
  minPartsPerFile?: number;
};


const LS_SETTINGS = "chunkDash:v9:settings";
const LS_RESUMES  = "chunkDash:v9:resumes";
const LS_BATCHES  = "chunkDash:v9:batches";

const ENV_SERVER =
  (import.meta as any)?.env?.VITE_SERVER_URL ||
  (typeof process !== "undefined" ? (process as any)?.env?.VITE_SERVER_URL : "") ||
  "http://localhost:3000";
const ENV_TOKEN =
  (import.meta as any)?.env?.VITE_UPLOAD_TOKEN ||
  (typeof process !== "undefined" ? (process as any)?.env?.VITE_UPLOAD_TOKEN : "") ||
  "";

// ---------------- Utils ----------------
const fmtBytes = (n: number) => {
  if (!Number.isFinite(n) || n <= 0) return "0 B";
  const u = ["B","KB","MB","GB","TB"];
  let i=0, x=n; while (x>=1024 && i<u.length-1) { x/=1024; i++; }
  return `${x.toFixed(i?1:0)} ${u[i]}`;
};
const fmtTime = (ms: number) => {
  if (!Number.isFinite(ms) || ms <= 0) return "—";
  const s = Math.round(ms/1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s/60), r = s%60;
  if (m < 60) return `${m}m ${r}s`;
  const h = Math.floor(m/60), rm=m%60;
  return `${h}h ${rm}m`;
};
const keyOf = (f: File): FileKey => `${f.name}::${f.size}::${f.lastModified}`;

async function sha256(buf: ArrayBuffer){ return crypto.subtle.digest("SHA-256", buf); }
function toBase64(buf: ArrayBuffer){
  const bytes = new Uint8Array(buf);
  const CHUNK = 0x8000;
  let s = "";
  for (let i = 0; i < bytes.length; i += CHUNK) {
    s += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
  }
  return btoa(s);
}
function toHex(buf: ArrayBuffer){ return [...new Uint8Array(buf)].map(b=>b.toString(16).padStart(2,"0")).join(""); }

class SpeedWindow {
  samples: Array<[number,number]> = [];
  total = 0;
  constructor(public win=10_000){}
  push(bytes:number){ const t=Date.now(); this.samples.push([t,bytes]); this.total+=bytes; this.gc(); }
  gc(){ const t=Date.now(); while(this.samples.length && t-this.samples[0][0]>this.win){ this.total-=this.samples[0][1]; this.samples.shift(); } }
  bps(){ this.gc(); if (!this.samples.length) return 0; const span=Math.max(1, Date.now()-this.samples[0][0]); return (this.total*1000)/span; }
}

const baseUrl = (s: string) => s.replace(/\/$/, "");
function buildFilesInputForBatch(batch: Batch){
  return batch.files.map((f, idx) => ({
    index: idx,
    key: `file-${idx}`,
    name: f.file.name,
    contentType: f.file.type || "application/octet-stream",
  }));
}

// ---------------- API helpers ----------------
function mergeServerHints(setter: React.Dispatch<React.SetStateAction<ServerHints>>, payload: any){
  if (!payload || typeof payload !== "object") return;
  setter(prev => {
    const next: ServerHints = {
      partSizeMin: payload.partSizeMin ?? prev.partSizeMin,
      partSizeMax: payload.partSizeMax ?? prev.partSizeMax,
      maxConcurrentFiles: payload.maxConcurrentFiles ?? prev.maxConcurrentFiles,
      maxChunksPerUpload: payload.maxChunksPerUpload ?? prev.maxChunksPerUpload,
      globalUploadLimit: payload.globalUploadLimit ?? prev.globalUploadLimit,
      maxBytesPerMinute: payload.maxBytesPerMinute ?? prev.maxBytesPerMinute,
      fileSizeMax: payload.fileSizeMax ?? prev.fileSizeMax,
      // NEW
      minPartsPerFile: payload.minPartsPerFile ?? prev.minPartsPerFile,
    };
    if (Number.isFinite(payload?.min)) next.partSizeMin = payload.min;
    if (Number.isFinite(payload?.max)) next.partSizeMax = payload.max;
    if (Number.isFinite(payload?.limit)) next.maxChunksPerUpload ??= payload.limit;
    if (Number.isFinite(payload?.limitPerMinute)) next.maxBytesPerMinute = payload.limitPerMinute;
    // alias guards
    if (Number.isFinite(payload?.minParts)) next.minPartsPerFile = payload.minParts;
    return next;
  });
}

async function fetchLimitsIfAny(base: string): Promise<Partial<ServerHints>>{
  try{
    const r = await fetch(`${baseUrl(base)}/limits`, { credentials:"omit", mode:"cors" });
    if (!r.ok) return {};
    return await r.json();
  }catch{ return {}; }
}

async function createOrResumeBatch(
  base: string,
  token: string,
  file: File,
  requestedPartSize: number,
  batch: Batch,
  resumeId: string | null,
  onHint?: (p:any)=>void
): Promise<ServerSession>{
  const filesPayload = resumeId ? [{ resumeId }] : [{
    filename: file.name,
    contentType: file.type || "application/octet-stream",
    totalBytes: file.size,
    partSize: requestedPartSize,
  }];

  const body: any = {
    batchId: batch.id,
    files: filesPayload,
    // user-provided metadata ONLY (server generates job/jobId/type internally)
    did: (batch.did || "").trim() || undefined,
    docid: (batch.docid || "").trim() || undefined,
    doctype: (batch.doctype || "").trim() || undefined,
    docname: (batch.docname || "").trim() || undefined,
    path: (batch.path || "").trim() || undefined,
    filesInput: buildFilesInputForBatch(batch),
  };

  const r = await fetch(`${baseUrl(base)}/uploads/batch`, {
    method: "POST",
    headers: { "Content-Type":"application/json", Authorization:`Bearer ${token.trim()}` },
    body: JSON.stringify(body),
    credentials: "omit", mode: "cors",
  });

  const text = await r.text().catch(()=> "");
  let j:any = {}; try{ j = JSON.parse(text||"{}"); }catch{}
  if (!r.ok || !j?.created?.length){
    onHint?.(j);
    const err: any = new Error(j?.error || `Batch create/resume failed (${r.status})`);
    (err.body = j);
    throw err;
  }
  const c = j.created[0];
  return { id:c.id, wsToken:c.wsToken, partSize:c.partSize, batchId: j.batchId || c.batchId };
}

async function getStatus(base:string, token:string, uploadId:string){
  const r = await fetch(`${baseUrl(base)}/uploads/${uploadId}`, {
    headers: { Authorization:`Bearer ${token.trim()}` }, credentials:"omit", mode:"cors"
  });
  const j = await r.json().catch(()=> ({}));
  if (!r.ok) {
    const err: any = new Error(j?.error || `Status failed (${r.status})`);
    (err.body = j);
    throw err;
  }
  return j;
}

async function getPartToken(
  base:string, token:string, uploadId:string, pn:number, len:number, s256b64:string, onHint?:(p:any)=>void
){
  const r = await fetch(`${baseUrl(base)}/uploads/${uploadId}/parts/${pn}/token?len=${len}&s256=${encodeURIComponent(s256b64)}`, {
    headers: { Authorization:`Bearer ${token.trim()}` }, credentials:"omit", mode:"cors"
  });
  const text = await r.text().catch(()=> "");
  let j:any = {}; try{ j=JSON.parse(text||"{}"); }catch{}
  if (!r.ok || !j?.token){
    onHint?.(j);
    const err: any = new Error(j?.error || `Token p${pn} failed (${r.status})`);
    (err.body = j);
    throw err;
  }
  return j.token as string;
}

async function putPart(
  base:string, token:string, uploadId:string, pn:number, blob:Blob, chunkToken:string, s256b64:string, signal?:AbortSignal, onHint?:(p:any)=>void
){
  const r = await fetch(`${baseUrl(base)}/uploads/${uploadId}/parts/${pn}`, {
    method:"PUT",
    headers:{
      Authorization:`Bearer ${token.trim()}`,
      "x-chunk-token": chunkToken,
      "x-checksum-sha256": s256b64,
      "Content-Type":"application/octet-stream",
    },
    body: blob, credentials:"omit", mode:"cors", signal
  });
  if (!r.ok){
    const retryAfter = Number(r.headers.get("retry-after") || "");
    const text = await r.text().catch(()=> "");
    let j:any = {}; try{ j=JSON.parse(text||"{}"); }catch{}
    onHint?.(j);
    const err: any = new Error(`Part ${pn} failed: ${r.status} ${j?.error || text}`);
    (err.retryAfterMs = Number.isFinite(retryAfter) ? retryAfter*1000 : undefined);
    (err.body = j);
    throw err;
  }
}

async function completeUpload(base:string, token:string, uploadId:string, finalHex?:string){
  const r = await fetch(`${baseUrl(base)}/uploads/${uploadId}/complete`, {
    method:"POST",
    headers:{ Authorization:`Bearer ${token.trim()}`, "Content-Type":"application/json" },
    body: JSON.stringify(finalHex ? { finalSha256Hex: finalHex } : {}),
    credentials:"omit", mode:"cors"
  });
  const j = await r.json().catch(()=> ({}));
  if (!r.ok) {
    const err: any = new Error(j?.error || `Complete failed (${r.status})`);
    (err.body = j);
    throw err;
  }
  return j;
}

async function abortUpload(base:string, token:string, uploadId:string){
  await fetch(`${baseUrl(base)}/uploads/${uploadId}`, { method:"DELETE", headers:{ Authorization:`Bearer ${token.trim()}` }, credentials:"omit", mode:"cors" }).catch(()=>{});
}
async function heartbeat(base:string, token:string, uploadId:string){
  await fetch(`${baseUrl(base)}/heartbeat/${uploadId}`, { method:"POST", headers:{ Authorization:`Bearer ${token.trim()}` }, credentials:"omit", mode:"cors" }).catch(()=>{});
}

// ---------- NEW: parts listing + repair helpers ----------
async function listParts(base: string, token: string, uploadId: string) {
  const r = await fetch(`${baseUrl(base)}/uploads/${uploadId}/parts`, {
    headers: { Authorization: `Bearer ${token.trim()}` },
    credentials: "omit",
    mode: "cors",
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) {
    const err: any = new Error(j?.error || `List parts failed (${r.status})`);
    (err.body = j);
    throw err;
  }
  // returns: { uploadId, parts: [{ partNumber, size, exists, cid, ipfsStatus }, ...] }
  return j;
}

// ---------- NEW: classify finalize errors ----------
function isBatchNotReadyError(e: any) {
  const body = e?.body || {};
  const err  = String(body?.error || "");
  const detail = String(body?.detail || "");
  // 409 path (?requireBethelNow=1) or 5xx payloads flagging batch-not-ready
  return err === "batch_not_ready_for_bethel" || detail === "batch_not_ready";
}
function isBethelTransientError(e: any) {
  const body = e?.body || {};
  if (body?.bethel === "failed") {
    const reason = String(body?.reason || "");
    const detail = String(body?.detail || "");
    return (
      reason === "bethel_error" ||
      /timeout|bad gateway|network|502|503|504|fetch failed|ECONNRESET|EAI_AGAIN/i.test(detail)
    );
  }
  return false;
}

function parseMissingPartsFromError(e: any): number[] {
  const body = e?.body || {};
  const err = body?.error || "";
  if (Array.isArray(body?.missing) && body.missing.length) {
    return body.missing.map((x: any) => Number(x)).filter((n: number) => Number.isFinite(n) && n > 0);
  }
  const m = /^missing_part_(\d+)$/i.exec(err);
  if (m) return [Number(m[1])];
  return [];
}

async function reuploadSpecificParts(opts: {
  base: string; token: string; uploadId: string; file: File; partSize: number;
  partNumbers: number[]; controller?: AbortSignal; onHint?: (p: any) => void;
}) {
  const { base, token, uploadId, file, partSize, partNumbers, controller, onHint } = opts;

  async function sha256B64OfSlice(start: number, end: number) {
    const blob = file.slice(start, end);
    const buf = await blob.arrayBuffer();
    const digest = await crypto.subtle.digest("SHA-256", buf);
    const bytes = new Uint8Array(digest);
    let s = "", CHUNK = 0x8000;
    for (let i = 0; i < bytes.length; i += CHUNK) s += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
    return { blob, b64: btoa(s), len: end - start };
  }

  for (const pn of partNumbers) {
    const start = (pn - 1) * partSize;
    const end = Math.min(file.size, start + partSize);
    const { blob, b64, len } = await sha256B64OfSlice(start, end);

    const tokenResp = await getPartToken(base, token, uploadId, pn, len, b64, onHint);
    await putPart(base, token, uploadId, pn, blob, tokenResp, b64, controller, onHint);
  }
}

async function repairThenComplete({
  base, token, uploadId, file, partSize, finalHex, controller, onHint,
}: {
  base: string; token: string; uploadId: string; file: File; partSize: number;
  finalHex?: string; controller?: AbortSignal; onHint?: (p: any) => void;
}) {
  // 1) ask the server which parts exist
  const lp = await listParts(base, token, uploadId);
  const expected = Math.ceil(file.size / partSize);
  const missing: number[] = [];
  for (let i = 1; i <= expected; i++) {
    const row = lp.parts.find((p: any) => p.partNumber === i);
    if (!row || !row.exists) missing.push(i);
  }

  // 2) if any are missing, re-upload them
  if (missing.length) {
    await reuploadSpecificParts({
      base, token, uploadId, file, partSize, partNumbers: missing, controller, onHint,
    });
  }

  // 3) finalize once more (with backoff) — soft accept batch-not-ready / Bethel transient
  return await withBackoff(async () => {
    try {
      return await completeUpload(base, token, uploadId, finalHex);
    } catch (e: any) {
      if (isBatchNotReadyError(e) || isBethelTransientError(e)) {
        return { ok: true, soft: true };
      }
      throw e;
    }
  }, 4);
}

// Simple exponential backoff with optional server-provided retryAfterMs
async function withBackoff<T>(
  fn: (attempt: number) => Promise<T>,
  max = 5
): Promise<T> {
  let attempt = 0;
  let lastErr: any;
  while (attempt < max) {
    attempt++;
    try {
      return await fn(attempt);
    } catch (e: any) {
      lastErr = e;
      if (attempt >= max) break;
      const jitter = Math.random() * 200;
      const serverBackoff = Number(e?.retryAfterMs || 0);
      const backoff = serverBackoff || Math.min(30_000, 600 * 2 ** (attempt - 1)) + jitter;
      await new Promise((r) => setTimeout(r, backoff));
    }
  }
  throw lastErr;
}


// ---------------- UI subcomponents ----------------
function FieldLabel({ children, hint }:{ children:React.ReactNode; hint?:string }){
  return <label className="fieldlbl" title={hint}>{children}</label>;
}
function Section({ children }:{ children:React.ReactNode }){ return <section className="card">{children}</section>; }
function StatusBadge({ kind, label }:{ kind:"ok"|"warn"|"err"|"idle"; label:string }){
  return <span className={`badge ${kind}`}>{label}</span>;
}

// ---------------- Main component ----------------
export default function UploadDashboard(){
  const [theme,setTheme]=useState(()=> localStorage.getItem("cd-theme")==="light"?"light":"dark");
  useEffect(()=>{ document.documentElement.dataset.theme=theme; localStorage.setItem("cd-theme", theme); },[theme]);

  const [settings,setSettings]=useState<UploaderSettings>(()=> {
    try{ const raw=localStorage.getItem(LS_SETTINGS); if (raw) return JSON.parse(raw); }catch{}
    return { serverUrl:ENV_SERVER, token:ENV_TOKEN, partSizeBytes:5*1024*1024, fileConcurrency:2, finalizeChecksum:true };
  });
  useEffect(()=>{ localStorage.setItem(LS_SETTINGS, JSON.stringify(settings)); },[settings]);

  const [hints,setHints]=useState<ServerHints>({});
  useEffect(()=>{ (async()=>{
    if (!settings.serverUrl) return;
    const l = await fetchLimitsIfAny(settings.serverUrl);
    if (Object.keys(l).length) setHints(prev => ({...prev, ...l}));
  })(); },[settings.serverUrl]);

  const [storageText,setStorageText]=useState("—");
  useEffect(()=>{ (async()=>{
    try{
      // @ts-ignore
      if (navigator?.storage?.estimate){
        // @ts-ignore
        const { usage=0, quota=0 } = await navigator.storage.estimate();
        const free = Math.max(0, quota-usage);
        setStorageText(`${fmtBytes(free)} free`);
      }
    }catch{ setStorageText("—"); }
  })(); },[]);

  const readResumes = ():Record<string,{partSize:number}> => {
    try{ return JSON.parse(localStorage.getItem(LS_RESUMES) || "{}") || {}; }catch{ return {}; }
  };
  const resumesRef = useRef<Record<string,{partSize:number}>>(readResumes());
  const saveResumes = () => localStorage.setItem(LS_RESUMES, JSON.stringify(resumesRef.current));

  const [batches,setBatches] = useState<Batch[]>(()=>{
    try{
      const raw=localStorage.getItem(LS_BATCHES);
      if (raw){
        const saved = JSON.parse(raw)||[];
        return (saved as any[]).map(b=>({
          id: b.id, title: b.title, createdAt: b.createdAt, expanded: !!b.expanded,
          files: [],
          did: b.did || "", docid: b.docid || "", doctype: b.doctype || "",
          docname: b.docname || b.dockname || "", path: b.path || "",
        }));
      }
    }catch{}
    const id = makeId();
    const initial:Batch = {
      id, title: humanTitle(new Date()), createdAt: Date.now(), expanded:true, files:[],
      did:"", docid:"", doctype:"", docname:"", path:""
    };
    persistShells([initial]);
    return [initial];
  });
  function persistShells(list:Batch[]){
    const shells=list.map(b=>({
      id:b.id,title:b.title,createdAt:b.createdAt,expanded:b.expanded,
      did:b.did||"", docid:b.docid||"", doctype:b.doctype||"", docname:b.docname||"", path:b.path||"",
    }));
    localStorage.setItem(LS_BATCHES, JSON.stringify(shells));
  }
  function humanTitle(d=new Date()){
    const pad = (n:number)=> String(n).padStart(2,"0");
    return `Batch ${pad(d.getMonth()+1)}/${pad(d.getDate())}/${d.getFullYear()}, ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }
  function makeId(){ const a=new Uint8Array(8); crypto.getRandomValues(a); return [...a].map(b=>b.toString(16).padStart(2, "0")).join(""); }
  const addBatch = ()=> {
    const id=makeId();
    const b:Batch={ id, title:humanTitle(), createdAt:Date.now(), expanded:true, files:[],
      did:"", docid:"", doctype:"", docname:"", path:""
    };
    setBatches(prev=>{ const next=[b, ...prev]; persistShells(next); return next; });
  };

  // PROGRESS + SPEED (WS + fallback)
  const speedRefs = useRef<Map<string,SpeedWindow>>(new Map());
  const lastServerBytes = useRef<Map<string, { t:number; b:number }>>(new Map());
  const wsMap = useRef<Map<string,WebSocket>>(new Map());
  const serverProgress = useRef<Map<string,{percent:number;bytesReceived:number;receivedParts:number;expectedParts:number;batchId:string}>>(new Map());
  const lastPolledAt = useRef<Map<string, number>>(new Map());

  useEffect(() => () => { wsMap.current.forEach(ws => { try { ws.close(); } catch {} }); wsMap.current.clear(); }, []);
  useEffect(()=>{
    const iv=setInterval(()=>{
      batches.forEach(batch=>batch.files.forEach(f=>{
        if (f.uploadId && (f.status==="uploading"||f.status==="paused")) heartbeat(settings.serverUrl, settings.token, f.uploadId);
      }));
    }, 30_000);
    return ()=>clearInterval(iv);
  },[batches, settings.serverUrl, settings.token]);

  function ensureWs(uploadId:string, wsToken?:string){
    if (!wsToken || wsMap.current.has(uploadId)) return;
    try{
      const u=new URL(baseUrl(settings.serverUrl));
      u.protocol=u.protocol.replace("http","ws");
      u.pathname="/ws";
      u.searchParams.set("uploadId", uploadId);
      u.searchParams.set("wsToken", wsToken);
      const ws=new WebSocket(u.toString());
      ws.onmessage=(ev)=>{
        try{
          const m=JSON.parse(String(ev.data));
          if (m?.type === "progress" && m.uploadId === uploadId){
            const now = Date.now();
            const cur = Number(m.bytesReceived) || 0;
            serverProgress.current.set(uploadId, {
              percent: Number(m.percent) || 0,
              bytesReceived: cur,
              receivedParts: Number(m.receivedParts) || 0,
              expectedParts: Number(m.expectedParts) || 0,
              batchId: String(m.batchId || ""),
            });
            const sw = speedRefs.current.get(uploadId) ?? new SpeedWindow(10_000);
            speedRefs.current.set(uploadId, sw);
            const prev = lastServerBytes.current.get(uploadId);
            if (prev) {
              const delta = Math.max(0, cur - prev.b);
              if (delta > 0) sw.push(delta);
            }
            lastServerBytes.current.set(uploadId, { t: now, b: cur });
            setBatches(prev => prev.map(b => ({
              ...b,
              files: b.files.map(f => {
                if (f.uploadId !== uploadId) return f;
                if ((f.status === "queued" || f.status === "paused" || f.status === "error") && cur > 0 && (Number(m.percent)||0) < 100){
                  return { ...f, status: "uploading", startedAt: f.startedAt ?? Date.now() };
                }
                if (f.status !== "done" && (Number(m.percent)||0) >= 100){
                  const startedAtVal = f.startedAt ?? f.createdAt;
                  const elapsed = Math.max(0, now - startedAtVal);
                  const avgBps = elapsed > 0 ? (f.file.size * 1000) / elapsed : 0;
                  return { ...f, status: "done", finalAvgBps: avgBps, totalTimeMs: elapsed, message: f.message || "Completed" };
                }
                return f;
              })
            })));
          }
        } catch {}
      };
      ws.onclose = () => { wsMap.current.delete(uploadId); };
      ws.onerror = () => {};
      wsMap.current.set(uploadId, ws);
    } catch {}
  }

  // ---------- CLEANUP ARTIFACTS (NEW) ----------
  function cleanupUploadArtifacts(uploadId?: string) {
    if (!uploadId) return;
    try { wsMap.current.get(uploadId)?.close(); } catch {}
    wsMap.current.delete(uploadId);
    serverProgress.current.delete(uploadId);
    lastServerBytes.current.delete(uploadId);
    lastPolledAt.current.delete(uploadId);
    if (resumesRef.current[uploadId]) {
      delete resumesRef.current[uploadId];
      saveResumes();
    }
  }

  // POLLING FALLBACK (1s)
  useEffect(()=>{
    const tick = async () => {
      const now = Date.now();
      const toPoll: Array<{uploadId:string; file: FileItem; batchId:string}> = [];

      batches.forEach(b => b.files.forEach(f => {
        if (!f.uploadId) return;
        const sp = serverProgress.current.get(f.uploadId);
        const done = sp ? sp.percent >= 100 : (f.status === 'done');
        const hasWs = wsMap.current.has(f.uploadId);
        if (!done && !hasWs && (f.status === 'uploading' || f.status === 'queued')){
          const last = lastPolledAt.current.get(f.uploadId) || 0;
          if (now - last >= 1000) {
            lastPolledAt.current.set(f.uploadId, now);
            toPoll.push({ uploadId: f.uploadId, file: f, batchId: b.id });
          }
        }
      }));

      const MAX_P = 4;
      for (let i = 0; i < toPoll.length && i < MAX_P; i++){
        const { uploadId, file, batchId } = toPoll[i];
        try{
          const st = await getStatus(settings.serverUrl, settings.token, uploadId);
          const total = Number(st?.receivedParts?.reduce?.((a:number,b:any)=>a+b.size,0) || 0);
          const expectedParts = Number(st?.expectedParts||0);
          const receivedParts = Number(st?.receivedParts?.length||0);
          const percent = file.file.size > 0 ? Math.floor((total / file.file.size) * 100) : 0;

          serverProgress.current.set(uploadId, {
            percent,
            bytesReceived: total,
            receivedParts,
            expectedParts,
            batchId: String(st?.batchId || batchId),
          });

          const sw = speedRefs.current.get(uploadId) ?? new SpeedWindow(10_000);
          speedRefs.current.set(uploadId, sw);
          const prev = lastServerBytes.current.get(uploadId);
          if (prev){
            const delta = Math.max(0, total - prev.b);
            if (delta > 0) sw.push(delta);
          }
          lastServerBytes.current.set(uploadId, { t: Date.now(), b: total });

          setBatches(prev => prev.map(B => ({
            ...B,
            files: B.files.map(x => {
              if (x.uploadId !== uploadId) return x;
              if ((x.status === 'queued' || x.status === 'paused' || x.status === 'error') && total > 0 && percent < 100){
                return { ...x, status: 'uploading', startedAt: x.startedAt ?? Date.now() };
              }
              if (x.status !== 'done' && percent >= 100){
                const startedAtVal = x.startedAt ?? x.createdAt;
                const elapsed = Math.max(0, Date.now() - startedAtVal);
                const avgBps = elapsed > 0 ? (x.file.size * 1000) / elapsed : 0;
                return { ...x, status: 'done', finalAvgBps: avgBps, totalTimeMs: elapsed, message: x.message || 'Completed' };
              }
              return x;
            })
          })));
        }catch{}
      }
    };

    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [batches, settings.serverUrl, settings.token]);

  const onAddFilesToBatch = (batchId:string, incoming:File[]) => {
    setBatches(prev=>prev.map(b=>{
      if (b.id!==batchId) return b;
      const next=[...b.files];
      for (const f of incoming){
        const key=keyOf(f);
        if (next.some(x=>x.key===key)) continue;

        // NEW: respect server's minPartsPerFile when estimating parts before session creation
        const minParts = Number(hints.minPartsPerFile || 0);
        let estPartSize = Math.max(1, settings.partSizeBytes);
        if (minParts > 0) {
          const cap = Math.max(1, Math.floor(f.size / minParts)); // force >= minParts
          estPartSize = Math.min(estPartSize, cap);
        }
        const estTotal = Math.ceil(f.size / estPartSize);

        next.push({
          key, file:f, status:"queued", bytesSent:0, partsDone:0,
          partsTotal: estTotal,
          speedBps:0, etaMs:NaN, createdAt:Date.now(),
        });
      }
      return {...b, files:next};
    }));
  };


  const startAll = ()=> batches.forEach(b=>startBatch(b.id));

  const startBatch = async (batchId:string)=>{
    const batch=batches.find(b=>b.id===batchId); if (!batch) return;
    const queue=batch.files.filter(f=>["queued","paused","error"].includes(f.status)).map(f=>f.key);
    if (!queue.length) return;

    setBatches(prev=>prev.map(b=>b.id!==batchId?b:{...b, files:b.files.map(it=>{
      if (!queue.includes(it.key)) return it;
      return {
        ...it,
        status:"uploading",
        controller:new AbortController(),
        message:"",
        startedAt: it.startedAt ?? Date.now(),
      };
    })}));

    const worker=async()=>{
      while(true){
        const nextKey=queue.shift(); if (!nextKey) break;
        try{ await uploadOne(batchId, nextKey); }
        catch(e:any){
          setBatches(prev=>prev.map(b=>b.id!==batchId?b:{...b, files:b.files.map(x=>x.key===nextKey?{...x,status:(x.status==="canceled"?"canceled":"error"),message:String(e?.message||e)}:x)}));
        }
      }
    };
    const runners=Array.from({length: Math.max(1, settings.fileConcurrency)}, ()=>worker());
    await Promise.allSettled(runners);
  };

  // Upload one file
  const uploadOne = async (batchId:string, key:FileKey)=>{
    const batch=batches.find(bb=>bb.id===batchId); if (!batch) return;
    let item=batch.files.find(x=>x.key===key); if (!item) return;

    const sp=new SpeedWindow();
    const update=(patch:Partial<FileItem>)=>{
      setBatches(prev=>prev.map(B=>B.id!==batchId?B:{...B, files:B.files.map(x=>x.key===key?{...x,...patch}:x)}));
    };

    let controller=item.controller!; if (!controller){ controller=new AbortController(); update({controller}); }

    let uploadId=item.uploadId ?? null;
    let partSize=item.partSize ?? settings.partSizeBytes;

    try{
      const sess = await createOrResumeBatch(
        settings.serverUrl, settings.token, item.file, settings.partSizeBytes, batch, uploadId,
        (p)=>mergeServerHints(setHints,p)
      );
      uploadId=sess.id; partSize=sess.partSize;
      // UPDATED: set partsTotal based on the server-enforced partSize
      update({ uploadId, partSize, partsTotal: Math.ceil(item.file.size / partSize) });
      resumesRef.current[uploadId]={ partSize }; saveResumes();
      ensureWs(uploadId, sess.wsToken);
    }catch(e:any){
      const msg = e?.body?.detail || e?.message || "Create/resume failed";
      throw new Error(msg);
    }


    const totalParts = Math.ceil(item.file.size / partSize);
    let have = new Set<number>();
    try{
      const st = await getStatus(settings.serverUrl, settings.token, uploadId!);
      have = new Set((st.receivedParts||[]).map((p:any)=>Number(p.partNumber)));
    }catch{}

    let partsDone=have.size, bytesSent=0;
    update({ partsTotal: totalParts, partsDone, bytesSent });

    const pending:number[]=[]; for(let pn=1;pn<=totalParts;pn++) if(!have.has(pn)) pending.push(pn);
    const PART_WORKERS = Math.max(1, Math.min((hints.maxChunksPerUpload ?? 4), 8));

    async function uploadPart(pn:number){
      if (controller.signal.aborted) return;
      const start = (pn - 1) * partSize!;
      const end   = Math.min(item!.file.size, start + partSize!);
      const blob  = item!.file.slice(start, end);
      const len   = end - start;

      const buf     = await blob.arrayBuffer();
      const digest  = await sha256(buf);
      const s256b64 = toBase64(digest);

      const token = await withBackoff(
        async () => await getPartToken(
          settings.serverUrl, settings.token, uploadId!, pn, len, s256b64,
          (p)=>mergeServerHints(setHints,p)
        ),
        5
      );

      await withBackoff(async () => {
        await putPart(
          settings.serverUrl, settings.token, uploadId!, pn, blob, token, s256b64,
          controller.signal, (p)=>mergeServerHints(setHints,p)
        );
        partsDone += 1;
        bytesSent += len;
        sp.push(len);
        const bps    = sp.bps();
        const remain = item!.file.size - bytesSent;
        const eta    = bps > 0 ? (remain / bps) * 1000 : NaN;
        update({ partsDone, bytesSent, speedBps: bps, etaMs: eta });
      }, 5);
    }

    const workers = Array.from({ length: PART_WORKERS }, async () => {
      while (pending.length && !controller.signal.aborted) {
        const pn = pending.shift()!;
        await uploadPart(pn);
      }
    });

    try{
      await Promise.all(workers);

      if (controller.signal.aborted){
        const paused=batches.find(bb=>bb.id===batchId)?.files.find(x=>x.key===key)?.status==="paused";
        update({ status: paused ? "paused" : "canceled" });
        return;
      }

      update({ message: "Finalizing… verifying parts on server" });
      await waitForAllParts(settings.serverUrl, settings.token, uploadId!, totalParts);

      let finalHex: string | undefined;
      const FINALIZE_RAM_CAP = 512 * 1024 * 1024;
      if (settings.finalizeChecksum && item.file.size <= FINALIZE_RAM_CAP){
        const all=await item.file.arrayBuffer();
        finalHex=toHex(await sha256(all));
      }

      // FIRST attempt to complete, then repair on "missing_part_*" or similar
      try {
        await completeUpload(settings.serverUrl, settings.token, uploadId!, finalHex);
      } catch (e: any) {
        const missing = parseMissingPartsFromError(e);
        if (missing.length) {
          await repairThenComplete({
            base: settings.serverUrl,
            token: settings.token,
            uploadId: uploadId!,
            file: item.file,
            partSize: partSize!,
            finalHex,
            controller: controller.signal,
            onHint: (p) => mergeServerHints(setHints, p),
          });
        } else if (isBatchNotReadyError(e) || isBethelTransientError(e)) {
          // Soft success – file is uploaded; batch will finalize later
        } else {
          throw e;
        }
      }

      const startedAtVal = item.startedAt ?? item.createdAt;
      const elapsed = Math.max(0, Date.now() - startedAtVal);
      const avgBps = elapsed > 0 ? (item.file.size * 1000) / elapsed : 0;
      update({
        status:"done", message:"Completed", speedBps:0, etaMs:0,
        partsDone: totalParts, bytesSent: item.file.size,
        finalAvgBps: avgBps, totalTimeMs: elapsed
      });

      // --------- CLEANUP after success (NEW) ---------
      cleanupUploadArtifacts(uploadId!);

    }catch(e:any){
      if (controller.signal.aborted){
        const paused=batches.find(bb=>bb.id===batchId)?.files.find(x=>x.key===key)?.status==="paused";
        update({ status: paused ? "paused" : "canceled" });
      }else{
        // NEW: soften batch-level transient errors; treat as done
        if (isBatchNotReadyError(e) || isBethelTransientError(e)) {
          const startedAtVal = item.startedAt ?? item.createdAt;
          const elapsed = Math.max(0, Date.now() - startedAtVal);
          const avgBps = elapsed > 0 ? (item.file.size * 1000) / elapsed : 0;
          update({
            status: "done",
            message: "Uploaded. Waiting for batch finalize…",
            speedBps: 0,
            etaMs: 0,
            partsDone: Math.ceil(item.file.size / partSize!),
            bytesSent: item.file.size,
            finalAvgBps: avgBps,
            totalTimeMs: elapsed
          });
          cleanupUploadArtifacts(uploadId!);
        } else {
          const msg = e?.body?.detail || e?.message || String(e);
          update({ status:"error", message: msg });
          // --------- CLEANUP after error (NEW) ---------
          cleanupUploadArtifacts(uploadId!);
        }
      }
    }
  };

  // ---------- waitForAllParts ----------
  async function waitForAllParts(
    base: string,
    token: string,
    uploadId: string,
    expectedParts: number,
    timeoutMs = 180_000,
    intervalMs = 1_500
  ) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const st = await getStatus(base, token, uploadId);
      const have = Array.isArray(st?.receivedParts) ? st.receivedParts.length : 0;
      if (have >= expectedParts) return st;
      await new Promise((r) => setTimeout(r, intervalMs));
    }
    throw new Error(`timeout_waiting_for_parts (expected ${expectedParts})`);
  }

  async function withBackoff<T>(fn:(attempt:number)=>Promise<T>, max=5):Promise<T>{
    let attempt=0, last:any;
    while(attempt<max){
      attempt++;
      try{ return await fn(attempt); }
      catch(e:any){
        last=e; if (attempt>=max) break;
        const jitter=Math.random()*200;
        const serverBackoff=Number((e as any)?.retryAfterMs||0);
        const backoff=serverBackoff || Math.min(30_000, 600*2**(attempt-1)) + jitter;
        await new Promise(r=>setTimeout(r, backoff));
      }
    }
    throw last;
  }

  const pauseFile = (batchId:string, key:FileKey) =>
    setBatches(prev=>prev.map(B=>B.id!==batchId?B:{...B, files:B.files.map(x=>x.key!==key?x:(x.controller?.abort(), {...x,status:"paused",message:"Paused"}))}));

  const resumeFile = (batchId:string, key:FileKey)=>{
    setBatches(prev=>prev.map(B=>B.id!==batchId?B:{...B, files:B.files.map(x=>x.key===key?{
      ...x, status:"uploading", controller:new AbortController(), message:"", startedAt: x.startedAt ?? Date.now(),
    }:x)}));
    uploadOne(batchId,key).catch(e=> setBatches(prev=>prev.map(B=>B.id!==batchId?B:{...B, files:B.files.map(x=>x.key===key?{...x,status:"error",message:String(e)}:x)})));
  };

  const cancelFile = async (batchId:string, key:FileKey)=>{
    const it=batches.find(b=>b.id===batchId)?.files.find(x=>x.key===key);
    if (it?.controller) it.controller.abort();
    if (it?.uploadId) {
      await abortUpload(settings.serverUrl, settings.token, it.uploadId);
      // --------- CLEANUP after cancel (NEW) ---------
      cleanupUploadArtifacts(it.uploadId);
    }
    setBatches(prev=>prev.map(B=>B.id!==batchId?B:{...B, files:B.files.map(x=>x.key!==key?x:{...x,status:"canceled",message:"Canceled"})}));
  };

  const removeFile = (batchId:string, key:FileKey)=>{
    const up=batches.find(b=>b.id===batchId)?.files.find(f=>f.key===key);
    if (up?.uploadId) cleanupUploadArtifacts(up.uploadId); // NEW
    setBatches(prev=>prev.map(B=>B.id!==batchId?B:{...B, files:B.files.filter(x=>x.key!==key)}));
  };

  const canStart = useMemo(()=> settings.serverUrl.trim() && settings.token.trim()
    && batches.some(b=>b.files.some(f=>["queued","paused","error"].includes(f.status))), [batches,settings]);

  const queuedCount = batches.reduce((a,b)=> a + b.files.filter(f=>f.status==="queued").length, 0);
  const activeCount = batches.reduce((a,b)=> a + b.files.filter(f=>f.status==="uploading").length, 0);

  // ⬇️ inlined batch meta updater (persisted) ⬇️
  const updateBatchMeta = (batchId:string, patch: Partial<Pick<Batch, "did"|"docid"|"doctype"|"docname"|"path">>) => {
    setBatches(prev => {
      const next = prev.map(b => b.id !== batchId ? b : ({ ...b, ...patch }));
      persistShells(next);
      return next;
    });
  };

  return (
    <div className="page">
      <style>{css}</style>

      <header className="topbar">
        <div className="brand">
          <div className="logo" aria-hidden>⬆</div>
          <div>
            <div className="title">Chunk Uploader</div>
            <div className="subtitle">Multi-batch • Resumable • Parallel</div>
          </div>
        </div>

        <div className="topactions">
          <div className="summary">
            <StatusBadge kind="idle" label={`Queued ${queuedCount}`} />
            <StatusBadge kind="ok" label={`Active ${activeCount}`} />
          </div>
          <div className="sysstatus">
            <span className="dot">●</span>
            <span>Online</span>
            <span className="sep">•</span>
            <span>HB: 30s</span>
            <span className="sep">•</span>
            <span>Storage: {storageText}</span>
          </div>
          <button
            className="btn ghost"
            onClick={()=>{
              // --------- CLEANUP for ALL (NEW) ---------
              batches.forEach(b => b.files.forEach(f => cleanupUploadArtifacts(f.uploadId)));
              wsMap.current.forEach(ws => { try { ws.close(); } catch {} });
              wsMap.current.clear();
              setBatches([]);
              persistShells([]);
              localStorage.removeItem(LS_RESUMES);
            }}
          >
            Clear All
          </button>
          <button className="btn" disabled={!canStart} onClick={startAll}>Start All</button>
          <button className="btn ghost toggle" onClick={()=>setTheme(t=>t==="dark"?"light":"dark")}>{theme==="dark"?"☼":"☾"}</button>
        </div>
      </header>

      <Section>
        <div className="grid2">
          <div>
            <FieldLabel hint="Your upload service base URL">Server URL</FieldLabel>
            <input type="text" value={settings.serverUrl} onChange={e=>setSettings(s=>({...s, serverUrl:e.target.value}))} placeholder={ENV_SERVER} spellCheck={false}/>
          </div>
          <div>
            <FieldLabel hint="Bearer token for authentication">Bearer Token</FieldLabel>
            <input type="text" value={settings.token} onChange={e=>setSettings(s=>({...s, token:e.target.value}))} placeholder={ENV_TOKEN||"paste JWT"} spellCheck={false}/>
          </div>
        </div>

        <details className="adv" open>
          <summary>Advanced settings</summary>
          <div className="grid3">
            <div>
              <FieldLabel hint="Size of each chunk in bytes">Part size (bytes)</FieldLabel>
              <input
                type="number" min={1024*1024} step={1024*1024}
                value={settings.partSizeBytes}
                onChange={e=>setSettings(s=>({...s, partSizeBytes: Math.max(1024*1024, parseInt(e.target.value||"0",10))}))}
              />
              <div className="hint">Usually 5–50 MiB; server min/max apply</div>
            </div>
            <div>
              <FieldLabel hint="How many files upload at once (across batches)">Parallel files</FieldLabel>
              <input
                type="number" min={1} max={8}
                value={settings.fileConcurrency}
                onChange={e=>setSettings(s=>({...s, fileConcurrency: Math.min(8, Math.max(1, parseInt(e.target.value||"1",10))) }))}
              />
            </div>
            <div className="checkbox">
              <FieldLabel hint="Send final file checksum to server when complete">Finalize with SHA-256</FieldLabel>
              <div className="row">
                <input id="finalize" type="checkbox" checked={settings.finalizeChecksum}
                       onChange={e=>setSettings(s=>({...s, finalizeChecksum:e.target.checked }))}/>
                <label htmlFor="finalize" className="hint">Send final file checksum to server on complete (browser will skip huge files)</label>
              </div>
            </div>
          </div>

          {/* Batch metadata (user-editable) */}
          {batches[0] && (
            <div className="limits" style={{ marginTop: 14 }}>
              <div className="limits-title">Batch metadata</div>
              <div className="grid3">
                <div>
                  <FieldLabel hint="Decentralized Identifier for ownership context">DID</FieldLabel>
                  <input type="text" value={batches[0]?.did||""} onChange={e=>updateBatchMeta(batches[0].id,{did:e.target.value})} placeholder="did:bethel:main:8788c111-..." />
                </div>
                <div>
                  <FieldLabel hint="Document ID">Doc ID</FieldLabel>
                  <input type="text" value={batches[0]?.docid||""} onChange={e=>updateBatchMeta(batches[0].id,{docid:e.target.value})} placeholder="DOC-123" />
                </div>
                <div>
                  <FieldLabel hint="Classification or type of the document">Doc Type</FieldLabel>
                  <input type="text" value={batches[0]?.doctype||""} onChange={e=>updateBatchMeta(batches[0].id,{doctype:e.target.value})} placeholder="invoice, video, ..." />
                </div>
              </div>
              <div className="grid3" style={{marginTop:8}}>
                <div>
                  <FieldLabel hint="Human-friendly document name">Doc Name</FieldLabel>
                  <input type="text" value={batches[0]?.docname||""} onChange={e=>updateBatchMeta(batches[0].id,{docname:e.target.value})} placeholder="Q3 Billing"/>
                </div>
                <div>
                  <FieldLabel hint="Virtual path / folder for the batch">Path</FieldLabel>
                  <input type="text" value={batches[0]?.path||""} onChange={e=>updateBatchMeta(batches[0].id,{path:e.target.value})} placeholder="/org/acme/2025/q3"/>
                </div>
                <div>
                  <FieldLabel hint="Files descriptor sent for hashing (auto)">Files Input</FieldLabel>
                  <input type="text" value={`${batches[0]?.files.length ?? 0} file(s)`} readOnly/>
                </div>
              </div>
            </div>
          )}

          {/* Limits (read-only) */}
          <div className="limits">
            <div className="limits-title">
              Server limits (read-only)
              <button
                className="btn ghost sm"
                style={{ marginLeft: 8 }}
                onClick={async () => {
                  const newLimits = await fetchLimitsIfAny(settings.serverUrl);
                  setHints((prev) => ({ ...prev, ...newLimits }));
                }}
              >
                Sync
              </button>
            </div>
            <div className="limits-grid">
              <div><span>Part size min</span><b>{hints.partSizeMin ? fmtBytes(hints.partSizeMin) : "—"}</b></div>
              <div><span>Part size max</span><b>{hints.partSizeMax ? fmtBytes(hints.partSizeMax) : "—"}</b></div>
              <div><span>Max chunks/upload</span><b>{hints.maxChunksPerUpload ?? "—"}</b></div>
              <div><span>Max files/user</span><b>{hints.maxConcurrentFiles ?? "—"}</b></div>
              <div><span>Global chunk limit</span><b>{hints.globalUploadLimit ?? "—"}</b></div>
              <div><span>Per-minute user cap</span><b>{hints.maxBytesPerMinute ? fmtBytes(hints.maxBytesPerMinute) : "—"}</b></div>
              <div><span>Max file size</span><b>{hints.fileSizeMax ? fmtBytes(hints.fileSizeMax) : "—"}</b></div>
              {/* NEW */}
              <div><span>Min parts / file</span><b>{hints.minPartsPerFile ?? "—"}</b></div>
            </div>
            <div className="hint">Auto-fills from <code>/limits</code> if present; otherwise learns from error payloads.</div>
          </div>
        </details>
      </Section>

      <div className="batchbar">
        <button className="btn" onClick={addBatch}>+ New Batch</button>
      </div>

      {batches.length===0 && <Section><div className="empty">No batches — click <b>New Batch</b> to begin.</div></Section>}

      {batches.map(batch=>{
        const totalBytes = batch.files.reduce((a,f)=>a+f.file.size,0);
        const uploaded = batch.files.reduce((a,f)=>{
          const sv = f.uploadId ? serverProgress.current.get(f.uploadId) : undefined;
          const server = sv?.bytesReceived ?? 0;
          const client = f.bytesSent ?? 0;
          return a + Math.max(server, client);
        },0);
        const pct = totalBytes>0 ? Math.floor((uploaded/totalBytes)*100) : 0;
        const queued = batch.files.filter(f=>f.status==="queued").length;
        const active = batch.files.filter(f=>f.status==="uploading").length;

        return (
          <Section key={batch.id}>
            <div className="batchhead">
              <button className="fold" onClick={()=>{
                setBatches(prev=>{ const next=prev.map(b=>b.id===batch.id?{...b,expanded:!b.expanded}:b); persistShells(next); return next; });
              }}>{batch.expanded?"▾":"▸"}</button>
              <div className="bmeta">
                <div className="btitle">{batch.title}</div>
                <div className="bsub"><span>{queued} queued</span><span className="sep">•</span><span>{active} active</span><span className="sep">•</span><span>{fmtBytes(uploaded)} / {fmtBytes(totalBytes)}</span></div>
              </div>
              <div className="bactions">
                <button
                  className="btn ghost"
                  onClick={()=>{
                    // --------- CLEANUP when clearing a batch (NEW) ---------
                    batch.files.forEach(f => cleanupUploadArtifacts(f.uploadId));
                    setBatches(prev=>prev.map(b=>b.id!==batch.id?b:{...b, files:[]}));
                  }}
                >
                  Clear Batch
                </button>
                <button className="btn" disabled={!batch.files.some(f=>["queued","paused","error"].includes(f.status)) || !settings.token || !settings.serverUrl} onClick={()=>startBatch(batch.id)}>Start Batch</button>
              </div>
            </div>

            <div className="progressrow batchbarrow">
              <progress value={pct} max={100}/><span className="pct">{pct}%</span>
              <button className="btn ghost sm" onClick={()=>{
                batch.files.forEach(async f=>{
                  if (!f.uploadId) return;
                  try{
                    const st=await getStatus(settings.serverUrl, settings.token, f.uploadId);
                    const total=Number(st?.receivedParts?.reduce?.((a:number,b:any)=>a+b.size,0) || 0);
                    serverProgress.current.set(f.uploadId,{
                      percent: Math.floor((total/f.file.size)*100) || 0,
                      bytesReceived: total,
                      receivedParts: Number(st?.receivedParts?.length||0),
                      expectedParts: Number(st?.expectedParts||0),
                      batchId: String(st?.batchId||batch.id),
                    });
                    const sw = speedRefs.current.get(f.uploadId) ?? new SpeedWindow(10_000);
                    speedRefs.current.set(f.uploadId, sw);
                    const prev = lastServerBytes.current.get(f.uploadId);
                    if (prev){ const delta = Math.max(0, total - prev.b); if (delta>0) sw.push(delta); }
                    lastServerBytes.current.set(f.uploadId, { t: Date.now(), b: total });
                    setBatches(prev=>[...prev]);
                  }catch{}
                });
              }}>Refresh Files</button>
            </div>

            {batch.expanded && (
              <>
                <div className="droparea"
                     onDragOver={e=>{e.preventDefault(); e.currentTarget.classList.add("dragging");}}
                     onDragLeave={e=>{e.currentTarget.classList.remove("dragging");}}
                     onDrop={e=>{
                       e.preventDefault();
                       e.currentTarget.classList.remove("dragging");
                       const fl=Array.from(e.dataTransfer?.files||[]);
                       if (fl.length) onAddFilesToBatch(batch.id, fl);
                     }}>
                  <div className="dropinner">
                    <div className="dropicon">📁</div>
                    <div className="droptitle">Drop files here</div>
                    <div className="hint">Batch: {batch.title}</div>
                    <div className="hint">or</div>
                    <label className="btn filebtn">Choose Files
                      <input type="file" multiple style={{display:"none"}} onChange={e=>{const fl=Array.from(e.target.files||[]); if(fl.length) onAddFilesToBatch(batch.id, fl); e.currentTarget.value="";}}/>
                    </label>
                  </div>
                </div>

                <div className="table">
                  <div className="thead"><div>File</div><div className="right">Size</div><div style={{width:520}}>Progress</div><div className="right">Speed</div><div className="right">ETA</div><div className="right">Actions</div></div>
                  {batch.files.length===0 && <div className="empty">No files yet — add some above.</div>}
                  {batch.files.map(fi=>{
                    const sv = fi.uploadId ? serverProgress.current.get(fi.uploadId) : undefined;
                    const serverPct = sv?.percent ?? 0;
                    const clientPct = fi.partsTotal ? Math.floor((fi.partsDone/fi.partsTotal)*100) : 0;
                    const displayPct = Math.max(Number.isFinite(serverPct) ? serverPct : 0, clientPct);
                    const partsText = sv
                      ? `${Math.max(sv.receivedParts||0, fi.partsDone||0)}/${sv.expectedParts||fi.partsTotal} parts`
                      : `${fi.partsDone}/${fi.partsTotal} parts`;

                    const serverBytes = sv?.bytesReceived ?? 0;
                    const bytesDone   = Math.max(serverBytes, fi.bytesSent);
                    const bytesText   = `${fmtBytes(bytesDone)} / ${fmtBytes(fi.file.size)}`;

                    const wsSw = fi.uploadId ? speedRefs.current.get(fi.uploadId) : undefined;
                    const wsBps = wsSw?.bps() || 0;
                    const currBps = wsBps > 0 ? wsBps : fi.speedBps;
                    const remain = Math.max(0, fi.file.size - bytesDone);
                    const currEtaMs = currBps > 0 ? (remain / currBps) * 1000 : NaN;

                    let speedCell: React.ReactNode = "—";
                    let etaCell: React.ReactNode = "—";
                    if (displayPct < 100) {
                      if (currBps > 0) {
                        speedCell = `${fmtBytes(currBps)}/s`;
                        etaCell = fmtTime(currEtaMs);
                      }
                    } else {
                      const bpsToShow = fi.finalAvgBps && fi.finalAvgBps > 0 ? fi.finalAvgBps : currBps;
                      speedCell = bpsToShow > 0 ? `${fmtBytes(bpsToShow)}/s` : '—';
                      etaCell = fi.totalTimeMs && fi.totalTimeMs > 0 ? fmtTime(fi.totalTimeMs) : '—';
                    }

                    return (
                      <div key={fi.key} className="trow">
                        <div className="filecell">
                          <div className="filename" title={fi.file.name}>{fi.file.name}</div>
                          <div className="muted small">{fi.uploadId ? `ID: ${fi.uploadId}` : "—"}</div>
                          <div className="badges"><span className={`chip ${fi.status}`}>{fi.status}</span></div>
                        </div>
                        <div className="right">{fmtBytes(fi.file.size)}</div>
                        <div>
                          <div className="progressrow"><progress value={displayPct} max={100}/><span className="pct">{Math.floor(displayPct)}%</span></div>
                          <div className="muted small">{partsText} • {bytesText}</div>
                          {fi.message && <div className={`small ${fi.status==="error"?"err":"muted"}`}>{fi.message}</div>}
                        </div>
                        <div className="right">{speedCell}</div>
                        <div className="right">{etaCell}</div>
                        <div className="right actions">
                          {fi.status==="queued" && <button className="btn ghost" onClick={()=>resumeFile(batch.id, fi.key)}>Start</button>}
                          {fi.status==="uploading" && (<><button className="btn ghost" onClick={()=>pauseFile(batch.id, fi.key)}>Pause</button><button className="btn ghost warn" onClick={()=>cancelFile(batch.id, fi.key)}>Cancel</button></>)}
                          {fi.status==="paused" && (<><button className="btn ghost" onClick={()=>resumeFile(batch.id, fi.key)}>Resume</button><button className="btn ghost warn" onClick={()=>cancelFile(batch.id, fi.key)}>Cancel</button></>)}
                          {["error","canceled","done"].includes(fi.status) && <button className="btn ghost" onClick={()=>removeFile(batch.id, fi.key)}>Remove</button>}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </>
            )}
          </Section>
        );
      })}
    </div>
  );
}

const css = `
:root{ --bg:#0b0c10; --fg:#e8ecf1; --muted:#97a1b3; --card:#12141a; --line:#1e2533; --brand:#6ea8fe; --ok:#10b981; --warn:#f59e0b; --err:#ef4444; --chip:#1b2536; --chipText:#cbd5e1;}
:root[data-theme="light"]{ --bg:#f7f9fc; --fg:#0b1220; --muted:#55637a; --card:#ffffff; --line:#e6ebf2; --brand:#3466f6; --chip:#eef3fb; --chipText:#274060;}
*{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 Inter,system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif}
.page{max-width:1120px;margin:0 auto;padding:16px}
.topbar{position:sticky;top:0;z-index:2;backdrop-filter:saturate(1.4) blur(6px);background:linear-gradient(to bottom,color-mix(in oklab,var(--bg) 60%,transparent),transparent);display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;padding:8px 0}
.brand{display:flex;gap:10px;align-items:center}
.logo{width:36px;height:36px;border-radius:10px;background:#1a2233;display:flex;align-items:center;justify-content:center;font-size:18px;box-shadow:0 8px 24px rgba(0,0,0,.25)}
.title{font-weight:800} .subtitle{font-size:12px;color:var(--muted)}
.topactions{display:flex;gap:10px;align-items:center}
.summary{display:flex;gap:6px;margin-right:8px}
.sysstatus{display:flex;align-items:center;gap:8px;font-size:12px;color:var(--muted);white-space:nowrap}
.sysstatus .dot{color:var(--ok)} .sysstatus .sep{opacity:.7}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:16px;box-shadow:0 8px 22px rgba(0,0,0,.18);margin-bottom:12px}
.fieldlbl{display:block;color:var(--muted);font-size:12px;margin-bottom:6px;font-weight:600}
input[type=text],input[type=number]{width:100%;padding:11px 12px;border-radius:12px;border:1px solid var(--line);background:color-mix(in oklab,var(--card) 94%, black 6%);color:var(--fg);outline:none}
input[type=text]:focus,input[type=number]:focus{border-color:color-mix(in oklab,var(--brand) 70%, white 30%);box-shadow:0 0 0 3px color-mix(in oklab,var(--brand) 25%, transparent)}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.grid3{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}
.checkbox .row{display:flex;gap:8px;align-items:center}
.hint{font-size:12px;color:var(--muted)}
.btn{background:var(--brand);color:#04101f;border:0;border-radius:12px;padding:9px 12px;cursor:pointer;font-weight:800;letter-spacing:.2px;box-shadow:0 6px 18px color-mix(in oklab,var(--brand) 25%, transparent)}
.btn.ghost{background:transparent;color:var(--fg);border:1px solid var(--line);box-shadow:none}
.btn.warn{border-color:color-mix(in oklab,var(--warn) 50%, var(--line));color:color-mix(in oklab,var(--warn) 85%, var(--fg) 15%)}
.btn.toggle{width:38px;padding:8px 0}
.btn.sm{padding:6px 9px;border-radius:10px}
.adv>summary{cursor:pointer;list-style:none;font-weight:700;margin:8px 0}
.adv>summary::-webkit-details-marker{display:none}
.batchbar{display:flex;gap:8px;margin:6px 0 10px}
.droparea{border:2px dashed var(--line);border-radius:14px;display:flex;align-items:center;justify-content:center;padding:18px;margin-top:10px;min-height:120px;text-align:center;background:color-mix(in oklab,var(--card) 93%, black 7%);transition:border-color .2s ease, background .2s ease}
.droparea.dragging{border-color:var(--brand);background:color-mix(in oklab,var(--card) 85%, var(--brand) 15%)}
.dropinner{display:flex;flex-direction:column;align-items:center}
.dropicon{font-size:28px;margin-bottom:4px}
.droptitle{font-weight:800;margin-bottom:4px}
.filebtn{margin-top:6px;display:inline-block}
.badge{padding:6px 8px;border-radius:999px;border:1px solid var(--line);font-size:12px;color:var(--muted);background:color-mix(in oklab,var(--card) 92%, black 8%)}
.badge.ok{color:var(--ok)} .badge.warn{color:var(--warn)} .badge.err{color:var(--err)}
.chip{display:inline-block;padding:2px 8px;border-radius:999px;background:var(--chip);color:var(--chipText);font-size:11px;text-transform:uppercase;letter-spacing:.06em}
.chip.uploading{background:color-mix(in oklab,var(--brand) 25%, var(--chip))}
.chip.done{background:color-mix(in oklab,var(--ok) 40%, var(--chip))}
.chip.error{background:color-mix(in oklab,var(--err) 40%, var(--chip))}
.chip.paused{background:color-mix(in oklab,var(--warn) 40%, var(--chip))}
.table{display:flex;flex-direction:column;gap:8px}
.thead,.trow{display:grid;grid-template-columns:1.4fr .6fr 2.4fr .6fr .6fr .8fr;gap:10px;align-items:center}
.thead{padding:6px 8px;color:var(--muted);text-transform:uppercase;font-size:11px;letter-spacing:.06em}
.trow{padding:10px;border:1px solid var(--line);border-radius:12px;background:color-mix(in oklab,var(--card) 93%, black 7%)}
.trow:hover{border-color:color-mix(in oklab,var(--brand) 40%, var(--line))}
.right{text-align:right}
.filecell .filename{font-weight:800}
.muted{color:var(--muted)} .small{font-size:12px} .err{color:var(--err)}
.progressrow{display:flex;align-items:center;gap:10px}
.batchbarrow{margin-top:6px}
progress{width:100%;height:10px;appearance:none;border:none;background:#1b2536;border-radius:999px;overflow:hidden}
progress::-webkit-progress-bar{background:transparent}
progress::-webkit-progress-value{background:linear-gradient(90deg,var(--brand),#8ed0ff);transition:width .25s ease}
.pct{min-width:38px;text-align:right;color:#cfe2ff}
.empty{padding:16px;border:1px dashed var(--line);border-radius:10px;color:#94a3b8;text-align:center}
.limits{margin-top:10px;padding-top:10px;border-top:1px dashed var(--line)}
.limits-title{font-weight:800;margin-bottom:6px;display:flex;align-items:center}
.limits-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}
.limits-grid div{display:flex;align-items:center;justify-content:space-between;background:color-mix(in oklab,var(--card) 92%, black 8%);border:1px solid var(--line);border-radius:10px;padding:8px 10px}
.batchhead{display:flex;align-items:center;gap:10px}
.bmeta{flex:1 1 auto;min-width:0}
.btitle{font-weight:800}
.bsub{font-size:12px;color:var(--muted)} .bsub .sep{margin:0 8px;opacity:.7}
.bactions{display:flex;gap:8px}
.fold{border:1px solid var(--line);background:transparent;color:var(--fg);border-radius:10px;padding:6px 10px;cursor:pointer}
@media (max-width:980px){
  .grid2{grid-template-columns:1fr}
  .grid3{grid-template-columns:1fr 1fr}
  .thead,.trow{grid-template-columns:1.3fr .7fr 2fr .7fr .7fr .9fr}
  .sysstatus{display:none}
  .limits-grid{grid-template-columns:repeat(2,minmax(0,1fr))}
}
`;
