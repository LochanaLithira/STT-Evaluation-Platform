'use strict';

// ─── Constants ───────────────────────────────────────────────────────────────
const TARGET_SR = 16000;
const CHUNK_SAMPLES = 1280;            // 80 ms at 16 kHz
const FINALIZE_TIMEOUT_MS = 12000;     // give up waiting for the server's "done"

const SOURCES = ['u35', 's37'];
const EXPECTED_MODEL = {
  u35: 'universal-3-5-pro',
  s37: 'assemblyai-stt-3-7-realtime-agent'
};
const LANG_TAG = {
  'en': 'language_codes=["en"]',
  'fr': 'language_codes=["fr"]',
  'fr-bilingual': 'language_codes=["fr","en"]',
  'auto': 'no language steering'
};

// ─── State ───────────────────────────────────────────────────────────────────
// Only the language is user-selectable. Mode and voice focus use the server
// defaults (balanced, off) — see routes/compare.py.
const cfg = { lang: 'en' };
const APPLIED_DEFAULTS = { mode: 'balanced', voice_focus: '' };
let state = 'idle';                    // idle | running | finalizing
let ws = null, runId = 0, finalizeTimer = null;
let actx = null, procNode = null, stream = null;
let run = {};                          // per-model results for the current/last run

function newRunData() {
  return {
    finals: [],          // [{ text, turn, el, stopped }]
    finalByTurn: {},     // turn_order → index in finals (a turn may be re-finalized)
    finalLat: [],        // ms, one per finalized turn
    live: null,          // DOM element of the in-progress turn
    score: null          // alignment result once scored
  };
}

// ─── Init ────────────────────────────────────────────────────────────────────
document.querySelectorAll('.ltog button[data-g]').forEach(function (b) {
  b.addEventListener('click', function () { setOpt(b.dataset.g, b.dataset.v); });
});
document.getElementById('startBtn').addEventListener('click', function () {
  if (state === 'idle') start(); else if (state === 'running') stop();
});
document.getElementById('refText').addEventListener('input', function () {
  // Re-score the last run if the reference is edited afterwards.
  if (state === 'idle' && run.u35) scoreRun();
});
SOURCES.forEach(function (s) { run[s] = newRunData(); });

function setOpt(group, value) {
  if (state !== 'idle') return;
  cfg[group] = value;
  document.querySelectorAll('.ltog button[data-g="' + group + '"]').forEach(function (b) {
    b.className = b.dataset.v === value ? 'on' : '';
  });
  if (group === 'lang') {
    document.querySelectorAll('[data-lang-tag]').forEach(function (t) { t.textContent = LANG_TAG[value]; });
  }
}

// ─── Start / Stop ────────────────────────────────────────────────────────────
async function start() {
  hideErr();
  resetRun();

  try {
    // Raw mic: no browser noise suppression or auto-gain, which can strip
    // speech detail the models rely on. Echo cancellation stays on.
    stream = await navigator.mediaDevices.getUserMedia({
      audio: { channelCount: 1, echoCancellation: true, noiseSuppression: false, autoGainControl: false }
    });
  } catch (e) {
    showErr('Microphone access denied. Please allow mic access in your browser and try again.');
    return;
  }

  const id = ++runId;
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const qs = new URLSearchParams({ lang: cfg.lang });
  const sock = new WebSocket(proto + '://' + location.host + '/ws/compare?' + qs);
  sock.binaryType = 'arraybuffer';
  ws = sock;
  SOURCES.forEach(function (s) { setSession(s, 'Connecting…', false); });
  setState('running');

  sock.onopen = function () {
    if (id !== runId) return;
    startMic();
  };
  sock.onmessage = function (e) { if (id === runId) handleMessage(JSON.parse(e.data)); };
  sock.onerror = function () { if (id === runId) showErr('Connection error — check the server is running and the API key is set.'); };
  sock.onclose = function () { if (id === runId) finish(); };
}

// User pressed Stop.
function stop() {
  if (state !== 'running') return;
  stopAudio();
  if (ws && ws.readyState === WebSocket.OPEN) {
    // Server sends Terminate to both models so they finalize the last
    // sentence, then replies {"type":"done"}.
    ws.send(JSON.stringify({ type: 'stop' }));
    setState('finalizing');
    finalizeTimer = setTimeout(finish, FINALIZE_TIMEOUT_MS);
  } else {
    finish();
  }
}

// Run is over: tidy up, keep any unfinished text, score.
function finish() {
  if (state === 'idle') return;
  clearTimeout(finalizeTimer);
  stopAudio();
  runId++;                                   // ignore anything else from this socket
  if (ws) { try { ws.close(); } catch (e) { } ws = null; }

  SOURCES.forEach(function (src) {
    const r = run[src];
    if (!r.live) return;
    const text = r.live.querySelector('.ltx').textContent.trim();
    r.live.remove();
    r.live = null;
    if (text) addFinal(src, text, null, null, null, true);
  });

  setState('idle');
  scoreRun();
}

function stopAudio() {
  if (procNode) { procNode.disconnect(); procNode = null; }
  if (actx) { actx.close(); actx = null; }
  if (stream) { stream.getTracks().forEach(function (t) { t.stop(); }); stream = null; }
  meter(0);
}

// ─── Microphone source ───────────────────────────────────────────────────────
function startMic() {
  try { actx = new AudioContext({ sampleRate: TARGET_SR }); }
  catch (e) { actx = new AudioContext(); }
  const src = actx.createMediaStreamSource(stream);
  const proc = actx.createScriptProcessor(1024, 1, 1);
  const ratio = actx.sampleRate / TARGET_SR;
  let buf = [];

  proc.onaudioprocess = function (e) {
    if (state !== 'running' || !ws || ws.readyState !== WebSocket.OPEN) return;
    const input = e.inputBuffer.getChannelData(0);

    // Downsample to 16 kHz only if the browser refused a 16 kHz context.
    if (ratio === 1) {
      for (let i = 0; i < input.length; i++) buf.push(input[i]);
    } else {
      const outLen = Math.floor(input.length / ratio);
      for (let j = 0; j < outLen; j++) {
        const pos = j * ratio, idx = Math.floor(pos), frac = pos - idx;
        const s0 = input[idx] || 0, s1 = input[idx + 1] || s0;
        buf.push(s0 + frac * (s1 - s0));
      }
    }
    while (buf.length >= CHUNK_SAMPLES) {
      const chunk = buf.splice(0, CHUNK_SAMPLES);
      const i16 = new Int16Array(CHUNK_SAMPLES);
      for (let k = 0; k < CHUNK_SAMPLES; k++) i16[k] = Math.max(-32768, Math.min(32767, Math.round(chunk[k] * 32767)));
      sendChunk(i16);
    }
  };
  src.connect(proc);
  proc.connect(actx.destination);
  procNode = proc;
}

function sendChunk(i16) {
  let sq = 0;
  for (let i = 0; i < i16.length; i++) sq += i16[i] * i16[i];
  meter(Math.round(Math.sqrt(sq / i16.length)));
  ws.send(i16.buffer);
}

// ─── Messages from the server ────────────────────────────────────────────────
function handleMessage(m) {
  if (m.type === 'done') { finish(); return; }
  if (m.error && !m.source) { showErr('Server error: ' + m.error); finish(); return; }
  if (!run[m.source]) return;
  if (m.error) { showPanelErr(m.source, m.error); return; }
  if (m.session) { showSession(m.source, m.session); return; }
  onTurn(m.source, m);
}

function onTurn(src, m) {
  if (!m.is_final) showLive(src, m.transcript, m.latency_ms);
  else addFinal(src, m.transcript, m.latency_ms, m.language, m.turn_order, false);
}

// Each Turn re-transcribes the whole turn, so the live line is replaced.
function showLive(src, text, latency_ms) {
  const r = run[src], a = out(src);
  clearPlaceholder(a);
  if (!r.live) {
    r.live = document.createElement('div');
    r.live.className = 'live';
    r.live.innerHTML = '<span class="lbl">LIVE<span class="live-lat"></span></span><span class="ltx"></span><span class="cur"></span>';
    a.appendChild(r.live);
  }
  r.live.querySelector('.ltx').textContent = text;
  if (latency_ms != null) {
    const ll = r.live.querySelector('.live-lat');
    ll.textContent = ' ' + latency_ms + 'ms';
    ll.style.color = 'var(--' + ({ fast: 'green', mid: 'amber', slow: 'red' })[latClass(latency_ms)] + ')';
  }
  a.scrollTop = a.scrollHeight;
}

function addFinal(src, text, latency_ms, language, turn, stopped) {
  const r = run[src], a = out(src);
  clearPlaceholder(a);
  if (r.live) { r.live.remove(); r.live = null; }
  if (!text || !text.trim()) return;

  const el = document.createElement('div');
  el.className = 'fin';
  if (language) {
    const lt = document.createElement('span');
    lt.className = 'lang-tag';
    lt.textContent = language;
    el.appendChild(lt);
  }
  el.appendChild(document.createTextNode(text));
  if (stopped) {
    const cut = document.createElement('span');
    cut.className = 'stopped-tag';
    cut.textContent = '[no final — stopped]';
    el.appendChild(cut);
  } else if (latency_ms != null) {
    const badge = document.createElement('span');
    badge.className = 'lat-badge lat-' + latClass(latency_ms);
    badge.textContent = latency_ms + 'ms';
    el.appendChild(badge);
  }

  // Same turn finalized again → replace it rather than duplicate it.
  if (turn != null && r.finalByTurn[turn] != null) {
    const prev = r.finals[r.finalByTurn[turn]];
    prev.el.replaceWith(el);
    prev.el = el;
    prev.text = text;
  } else {
    if (turn != null) r.finalByTurn[turn] = r.finals.length;
    r.finals.push({ text: text, turn: turn, el: el, stopped: stopped });
    if (latency_ms != null) r.finalLat.push(latency_ms);
    a.appendChild(el);
  }
  document.getElementById(src + '-note').textContent = stopped ? 'Stopped' : 'Final turn received';
  updatePanelStats(src, latency_ms);
  a.scrollTop = a.scrollHeight;
}

// ─── Latency stats ───────────────────────────────────────────────────────────
function latClass(ms) {
  if (ms == null) return 'na';
  if (ms < 800) return 'fast';
  if (ms < 2000) return 'mid';
  return 'slow';
}

// Nearest-rank percentile.
function pct(arr, p) {
  if (!arr.length) return null;
  const s = arr.slice().sort(function (a, b) { return a - b; });
  return s[Math.max(0, Math.ceil(p / 100 * s.length) - 1)];
}

function updatePanelStats(src, last) {
  const lat = run[src].finalLat;
  const vals = { last: last, p50: pct(lat, 50), p90: pct(lat, 90), max: lat.length ? Math.max.apply(null, lat) : null };
  Object.keys(vals).forEach(function (k) {
    const el = document.getElementById(src + '-' + k);
    if (vals[k] == null) { if (k !== 'last') { el.textContent = '—'; el.className = 'ls-val na'; } return; }
    el.textContent = vals[k] + 'ms';
    el.className = 'ls-val ' + latClass(vals[k]);
  });
}

// ─── Accuracy (WER) ──────────────────────────────────────────────────────────
// Lowercase, unify apostrophes, treat hyphens/slashes as spaces, drop other
// punctuation. Accented letters are kept (they matter in French).
function normWords(t) {
  return t.normalize('NFC').toLowerCase()
    .replace(/[’‘`´]/g, "'")
    .replace(/[-‐‑–—_/]/g, ' ')
    .replace(/[^\p{L}\p{N}'\s]/gu, ' ')
    .replace(/(^|\s)'+|'+(?=\s|$)/g, ' ')
    .split(/\s+/).filter(Boolean);
}

// Word-level Levenshtein alignment → ops: ok | sub | del | ins.
function alignWords(ref, hyp) {
  const n = ref.length, m = hyp.length, W = m + 1;
  const d = new Uint32Array((n + 1) * W);
  for (let i = 0; i <= n; i++) d[i * W] = i;
  for (let j = 0; j <= m; j++) d[j] = j;
  for (let i = 1; i <= n; i++) {
    for (let j = 1; j <= m; j++) {
      const c = ref[i - 1] === hyp[j - 1] ? 0 : 1;
      d[i * W + j] = Math.min(d[(i - 1) * W + j - 1] + c, d[(i - 1) * W + j] + 1, d[i * W + j - 1] + 1);
    }
  }
  const ops = [];
  let i = n, j = m;
  while (i > 0 || j > 0) {
    if (i > 0 && j > 0 && d[i * W + j] === d[(i - 1) * W + j - 1] + (ref[i - 1] === hyp[j - 1] ? 0 : 1)) {
      ops.push({ op: ref[i - 1] === hyp[j - 1] ? 'ok' : 'sub', r: ref[i - 1], h: hyp[j - 1] }); i--; j--;
    } else if (i > 0 && d[i * W + j] === d[(i - 1) * W + j] + 1) {
      ops.push({ op: 'del', r: ref[i - 1] }); i--;
    } else {
      ops.push({ op: 'ins', h: hyp[j - 1] }); j--;
    }
  }
  return ops.reverse();
}

function scoreRun() {
  const ref = normWords(document.getElementById('refText').value || '');
  SOURCES.forEach(function (src) {
    const r = run[src], box = document.getElementById(src + '-align');
    if (!ref.length || !r.finals.length) { r.score = null; box.hidden = true; return; }
    const hyp = normWords(r.finals.map(function (f) { return f.text; }).join(' '));
    const ops = alignWords(ref, hyp);
    const c = { sub: 0, del: 0, ins: 0, ok: 0 };
    ops.forEach(function (o) { c[o.op]++; });
    r.score = { n: ref.length, hypN: hyp.length, sub: c.sub, del: c.del, ins: c.ins, wer: (c.sub + c.del + c.ins) / ref.length };
    renderAlign(src, ops, r.score);
  });
}

function renderAlign(src, ops, s) {
  const box = document.getElementById(src + '-align');
  box.innerHTML = '';
  const head = document.createElement('div');
  head.className = 'align-head';
  head.innerHTML = '<strong>WER ' + (s.wer * 100).toFixed(1) + '%</strong> vs reference · ' +
    '<span class="a-sub">substituted</span> <span class="a-del">missed</span> <span class="a-ins">extra</span>';
  box.appendChild(head);
  const body = document.createElement('div');
  body.className = 'align-body';
  ops.forEach(function (o) {
    const w = document.createElement('span');
    if (o.op === 'ok') { w.textContent = o.h; }
    else if (o.op === 'sub') { w.className = 'a-sub'; w.textContent = o.h; w.title = 'expected: ' + o.r; }
    else if (o.op === 'del') { w.className = 'a-del'; w.textContent = o.r; w.title = 'missed'; }
    else { w.className = 'a-ins'; w.textContent = o.h; w.title = 'not in reference'; }
    body.appendChild(w);
    body.appendChild(document.createTextNode(' '));
  });
  box.appendChild(body);
  box.hidden = false;
}

// ─── Session info ────────────────────────────────────────────────────────────
// Show what the server actually applied; flag anything that differs from the request.
function showSession(src, s) {
  const vf = s.voice_focus || '';
  const txt = 'model=' + (s.model || '?') + ' · mode=' + (s.mode || '?') + ' · voice_focus=' + (vf || 'off');
  const mismatch = (s.model && s.model !== EXPECTED_MODEL[src]) ||
    (s.mode && s.mode !== APPLIED_DEFAULTS.mode) || vf !== APPLIED_DEFAULTS.voice_focus;
  setSession(src, (mismatch ? '⚠ server applied ' : '✓ ') + txt, mismatch);
}

function setSession(src, txt, warn) {
  const el = document.getElementById(src + '-session');
  el.textContent = txt;
  el.className = 'session-info' + (warn ? ' warn' : '');
}

// ─── UI helpers ──────────────────────────────────────────────────────────────
function out(src) { return document.getElementById(src + '-out'); }
function clearPlaceholder(a) { const ph = a.querySelector('.ph2'); if (ph) ph.remove(); }

function resetRun() {
  SOURCES.forEach(function (src) {
    run[src] = newRunData();
    out(src).innerHTML = '<span class="ph2">Transcription appears here live…</span>';
    document.getElementById(src + '-note').textContent = '';
    document.getElementById(src + '-align').hidden = true;
    ['last', 'p50', 'p90', 'max'].forEach(function (k) {
      const el = document.getElementById(src + '-' + k);
      el.textContent = '—'; el.className = 'ls-val na';
    });
  });
}

function showPanelErr(src, msg) {
  const box = document.createElement('div');
  box.className = 'panel-err';
  box.innerHTML = '<strong>Connection failed</strong><br><code style="font-size:.72rem"></code><br><br>Check the server terminal and API key.';
  box.querySelector('code').textContent = msg;
  out(src).innerHTML = '';
  out(src).appendChild(box);
  setSession(src, 'Error', true);
}

function meter(rms) {
  const f = document.getElementById('mf'), s = document.getElementById('vs');
  const p = Math.min(rms / 4000 * 100, 100).toFixed(1);
  const c = rms < 200 ? 'var(--text-muted)' : rms < 1000 ? 'var(--amber)' : 'var(--green)';
  f.style.width = p + '%';
  f.style.background = c;
  if (state === 'running') s.textContent = 'Level ' + rms;
  s.style.color = c;
}

function setState(v) {
  state = v;
  const btn = document.getElementById('startBtn');
  document.getElementById('startBtnTxt').textContent = { idle: 'Start', running: 'Stop', finalizing: 'Finalizing…' }[v];
  btn.className = v === 'running' ? 'stop' : '';
  btn.disabled = v === 'finalizing';
  document.querySelectorAll('.ltog button[data-g]').forEach(function (b) { b.disabled = v !== 'idle'; });
  if (v === 'finalizing') document.getElementById('vs').textContent = 'Waiting for final transcripts…';
  if (v === 'idle') { document.getElementById('vs').textContent = 'Not recording'; document.getElementById('vs').style.color = ''; }
}

function showErr(m) {
  document.getElementById('errTxt').textContent = m;
  document.getElementById('errEl').classList.add('show');
}
function hideErr() { document.getElementById('errEl').classList.remove('show'); }
