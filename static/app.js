// ─── Constants ───────────────────────────────────────────────────────────────
var OLD_VAD = 1000, NEW_VAD = 650, MAX_RMS = 4000;
var TARGET_SR = 16000; // We resample everything to 16 kHz before sending
// Chunk size: Deepgram docs say "80ms chunks strongly recommended" for Flux.
// 80ms × 16000 Hz = 1280 samples = 2560 bytes per chunk.
// Nova-3 works fine with smaller chunks; we use the same buffer for simplicity.
var CHUNK_SAMPLES = 1280; // 80ms at 16kHz (Deepgram recommended)

// ─── Latency tracking ────────────────────────────────────────────────────────
var latStats = {
  nova3: { samples: [], ids: { last: 'n3-last', avg: 'n3-avg', min: 'n3-min', max: 'n3-max' } },
  flux: { samples: [], ids: { last: 'fl-last', avg: 'fl-avg', min: 'fl-min', max: 'fl-max' } },
  aai: { samples: [], ids: { last: 'aa-last', avg: 'aa-avg', min: 'aa-min', max: 'aa-max' } },
  oai: { samples: [], ids: { last: 'oa-last', avg: 'oa-avg', min: 'oa-min', max: 'oa-max' } },
  voxtral: { samples: [], ids: { last: 'vx-last', avg: 'vx-avg', min: 'vx-min', max: 'vx-max' } }
};

function latClass(ms) {
  if (ms == null) return 'na';
  if (ms < 800) return 'fast';
  if (ms < 2000) return 'mid';
  return 'slow';
}

function latBadgeClass(ms) {
  if (ms == null) return '';
  if (ms < 800) return 'lat-badge lat-fast';
  if (ms < 2000) return 'lat-badge lat-mid';
  return 'lat-badge lat-slow';
}

function updateLatStats(src, ms) {
  if (ms == null) return;
  var s = latStats[src];
  s.samples.push(ms);
  var avg = Math.round(s.samples.reduce(function (a, b) { return a + b; }, 0) / s.samples.length);
  var min = Math.min.apply(null, s.samples);
  var max = Math.max.apply(null, s.samples);
  var ids = s.ids;
  function setEl(id, val) {
    var el = document.getElementById(id);
    if (!el) return;
    el.textContent = val + 'ms';
    el.className = 'ls-val ' + latClass(val);
    // Flash animation: remove then re-add so it retriggers each time
    el.classList.remove('lat-flash');
    void el.offsetWidth; // force reflow
    el.classList.add('lat-flash');
  }
  setEl(ids.last, ms);
  setEl(ids.avg, avg);
  setEl(ids.min, min);
  setEl(ids.max, max);
}

function resetLatStats() {
  ['nova3', 'flux', 'aai', 'oai', 'voxtral'].forEach(function (src) {
    latStats[src].samples = [];
    var ids = latStats[src].ids;
    ['last', 'avg', 'min', 'max'].forEach(function (k) {
      var el = document.getElementById(ids[k]);
      if (el) { el.textContent = '—'; el.className = 'ls-val na'; }
    });
  });
}

// ─── State ───────────────────────────────────────────────────────────────────
var lang = 'en', recording = false, ws = null;
var actx = null, workletNode = null, stream = null;
var panels = {
  nova3: { aId: 'a1', live: null },
  flux: { aId: 'a2', live: null },
  aai: { aId: 'a3', live: null },
  oai: { aId: 'a4', live: null },
  voxtral: { aId: 'a5', live: null }
};

// ─── VAD threshold lines ─────────────────────────────────────────────────────
(function () {
  var w = document.getElementById('mw');
  [
    { v: NEW_VAD, c: 'var(--amber)', lbl: 'New (650)' },
    { v: OLD_VAD, c: 'var(--red)', lbl: 'Old (1000)' }
  ].forEach(function (t) {
    var p = (Math.min(t.v / MAX_RMS * 100, 100)).toFixed(2);
    var ln = document.createElement('div');
    ln.className = 'vline';
    ln.style.cssText = 'left:' + p + '%;background:' + t.c;
    w.appendChild(ln);
    var lb = document.createElement('div');
    lb.className = 'vlabel';
    lb.style.cssText = 'left:' + p + '%;color:' + t.c;
    lb.textContent = t.lbl;
    w.appendChild(lb);
  });
})();

// ─── Language toggle ─────────────────────────────────────────────────────────
function setLang(l) {
  if (recording) return;
  lang = l;
  document.getElementById('btnEn').className = (l === 'en') ? 'on' : '';
  document.getElementById('btnFr').className = (l === 'fr') ? 'on' : '';
  document.getElementById('btnFrBi').className = (l === 'fr-bilingual') ? 'on' : '';

  var isFr = l === 'fr';
  var isFrBi = l === 'fr-bilingual';
  var isFrAny = isFr || isFrBi;

  document.getElementById('t1').innerHTML = isFrAny
    ? '❌ Nova-3 — Streaming STT (French)'
    : '❌ Nova-3 — Streaming STT (English)';
  document.getElementById('t2').innerHTML = isFr
    ? '✅ Flux Multi — FR Monolingual (single hint=fr)'
    : isFrBi
      ? '✅ Flux Multi — FR+EN Bilingual (hint=fr+en)'
      : '✅ Flux General EN — Conversational STT (English)';
  document.getElementById('t3').innerHTML = isFr
    ? '🟣 AssemblyAI U3 Pro — FR Monolingual'
    : isFrBi
      ? '🟣 AssemblyAI U3 Pro — FR+EN Multilingual'
      : '🟣 AssemblyAI U3 Pro — Streaming STT (English)';

  document.getElementById('l1tag').textContent = isFrAny ? 'language=fr' : 'language=en';
  document.getElementById('l2tag').textContent = isFr
    ? 'language_hint=fr (monolingual ✓)'
    : isFrBi
      ? 'language_hint=fr+en (bilingual)'
      : '(no language_hint)';
  document.getElementById('fluxModelTag').textContent = isFrAny
    ? 'model=flux-general-multi'
    : 'model=flux-general-en';
  document.getElementById('l3tag').textContent = isFr
    ? 'prompt=Transcribe French.'
    : isFrBi
      ? 'prompt=Transcribe FR+EN'
      : '(default English prompt)';
  document.getElementById('t4').innerHTML = isFr
    ? '⚡ OpenAI gpt-4o-transcribe — FR (language=fr)'
    : isFrBi
      ? '⚡ OpenAI gpt-4o-transcribe — FR+EN (auto-detect)'
      : '⚡ OpenAI gpt-4o-transcribe — Realtime STT (English)';
  document.getElementById('t5').innerHTML = '🌊 Voxtral — Realtime STT (auto-detect)';
  document.getElementById('l4tag').textContent = isFr
    ? 'language=fr'
    : isFrBi
      ? 'language=auto (bilingual)'
      : 'language=en';
  // Clear detected language and notes on mode change
  document.getElementById('detectedLang').textContent = '';
  document.getElementById('aaiNote').textContent = '';
  document.getElementById('oaiNote').textContent = '';
  document.getElementById('vxNote').textContent = '';
}

// ─── Start / Stop ────────────────────────────────────────────────────────────
function toggle() { recording ? stop() : start(); }

function start() {
  hideErr(); clear(); resetLatStats();
  navigator.mediaDevices
    .getUserMedia({ audio: { channelCount: 1, sampleRate: { ideal: TARGET_SR }, echoCancellation: true, noiseSuppression: true } })
    .then(function (s) {
      stream = s;
      // Create AudioContext at 16kHz if supported, else we resample manually
      try { actx = new AudioContext({ sampleRate: TARGET_SR }); }
      catch (e) { actx = new AudioContext(); }

      var proto = location.protocol === 'https:' ? 'wss' : 'ws';
      var url = proto + '://' + location.host + '/ws/compare5?lang=' + lang;
      ws = new WebSocket(url);
      ws.binaryType = 'arraybuffer';
      ws.onopen = function () { setupAudio(); setState(true); };
      ws.onerror = function () { showErr('Connection error — check server is running and API key is correct.'); };
      ws.onclose = function () { if (recording) stop(); };
      ws.onmessage = handleMessage;
    })
    .catch(function () {
      showErr('Microphone access denied. Please allow mic access in your browser and try again.');
    });
}

// ─── Audio pipeline ──────────────────────────────────────────────────────────
// Strategy:
//   1. Capture mic at browser native rate (usually 48kHz)
//   2. Use ScriptProcessorNode to get raw float32 frames
//   3. Downsample to 16kHz using linear interpolation
//   4. Buffer into 80ms chunks (1280 samples) then send over WS
//   Deepgram docs: "80ms audio chunks strongly recommended" for Flux.

var resampleBuf = []; // accumulator of 16kHz samples

function setupAudio() {
  var src = actx.createMediaStreamSource(stream);
  // ScriptProcessorNode bufferSize 1024 = ~21ms at 48kHz, ~10ms at 96kHz
  // We want small but not so small it causes glitches. 1024 is a good balance.
  var proc = actx.createScriptProcessor(1024, 1, 1);
  var nativeSR = actx.sampleRate;
  var ratio = nativeSR / TARGET_SR; // e.g. 3.0 for 48kHz→16kHz

  proc.onaudioprocess = function (e) {
    if (!recording || !ws || ws.readyState !== WebSocket.OPEN) return;

    var input = e.inputBuffer.getChannelData(0); // float32 at native SR

    // ── RMS meter (on native samples, fast) ──
    var sq = 0;
    for (var i = 0; i < input.length; i++) sq += input[i] * input[i];
    meter(Math.round(Math.sqrt(sq / input.length) * 32767));

    // ── Downsample to 16kHz via linear interpolation ──
    var outLen = Math.floor(input.length / ratio);
    for (var j = 0; j < outLen; j++) {
      var pos = j * ratio;
      var idx = Math.floor(pos);
      var frac = pos - idx;
      var s0 = input[idx] || 0;
      var s1 = input[idx + 1] || s0;
      resampleBuf.push(s0 + frac * (s1 - s0));
    }

    // ── Emit 20ms chunks (320 samples at 16kHz) ──
    while (resampleBuf.length >= CHUNK_SAMPLES) {
      var chunk = resampleBuf.splice(0, CHUNK_SAMPLES);
      var i16 = new Int16Array(CHUNK_SAMPLES);
      for (var k = 0; k < CHUNK_SAMPLES; k++) {
        i16[k] = Math.max(-32768, Math.min(32767, Math.round(chunk[k] * 32767)));
      }
      ws.send(i16.buffer);
    }
  };

  src.connect(proc);
  proc.connect(actx.destination);
  workletNode = proc; // store ref for cleanup
}

function stop() {
  setState(false);
  resampleBuf = [];
  if (workletNode) { workletNode.disconnect(); workletNode = null; }
  if (actx) { actx.close(); actx = null; }
  if (stream) { stream.getTracks().forEach(function (t) { t.stop(); }); stream = null; }
  meter(0);

  // Promote any live (interim) blocks to committed .fin lines instead of silently discarding.
  // This ensures in-progress text from ALL models is preserved when the user clicks Stop.
  Object.keys(panels).forEach(function (k) {
    if (panels[k].live) {
      var liveEl = panels[k].live;
      var ltx = liveEl.querySelector('.ltx');
      var text = ltx ? ltx.textContent.trim() : '';
      if (text) {
        var a = document.getElementById(panels[k].aId);
        var d = document.createElement('div');
        d.className = 'fin';
        d.textContent = text;
        // small visual cue that this was promoted (cut short), not a clean final
        var cut = document.createElement('span');
        cut.style.cssText = 'font-size:.6rem;margin-left:6px;opacity:.5;font-family:"Fira Code",monospace;';
        cut.textContent = '[stopped]';
        d.appendChild(cut);
        a.appendChild(d);
        a.scrollTop = a.scrollHeight;
      }
      liveEl.remove();
      panels[k].live = null;
    }
  });
  var el = document.getElementById('fluxListen'); if (el) el.remove();

  // ── Grace-period close for in-flight OAI transcripts ────────────────────────
  var wsRef = ws;
  ws = null;
  if (wsRef) {
    setTimeout(function () {
      try { if (wsRef.readyState === WebSocket.OPEN) wsRef.close(); } catch (e) { }
    }, 2500);
  }
}

// ─── Message handler ─────────────────────────────────────────────────────────
function handleMessage(e) {
  var m = JSON.parse(e.data);

  // Global server error
  if (m.error && !m.source) { showErr('Server error: ' + m.error); stop(); return; }
  // Per-panel connection error
  if (m.error && m.source) { showPanelErr(m.source, m.error); return; }

  // Nova-3: speech just detected by VAD → show mini indicator
  if (m.speech_started && m.source === 'nova3') { showSpeechPing(); return; }
  // OpenAI: speech started event
  if (m.speech_started && m.source === 'oai') {
    var oaiNote = document.getElementById('oaiNote');
    if (oaiNote) oaiNote.textContent = '🎤 Speech detected';
    return;
  }

  // Flux: user resumed speaking after an EagerEndOfTurn
  if (m.turn_resumed) {
    var p = panels['flux'];
    if (p.live) { p.live.remove(); p.live = null; }
    showFluxListening();
    return;
  }

  if (m.source === 'nova3') addLine('nova3', m.transcript, m.is_final, [], m.latency_ms);
  if (m.source === 'flux') addLine('flux', m.transcript, m.is_final, m.languages || [], m.latency_ms);
  if (m.source === 'aai') addLine('aai', m.transcript, m.is_final, [], m.latency_ms);
  if (m.source === 'oai') addLine('oai', m.transcript, m.is_final, [], m.latency_ms);
  if (m.source === 'voxtral') addLine('voxtral', m.transcript, m.is_final, [], m.latency_ms);
}

// ─── Transcript rendering ─────────────────────────────────────────────────────
function addLine(src, text, fin, langs, latency_ms) {
  if (src === 'flux') hideFluxListening();
  if (src === 'aai' && fin) {
    var noteEl = document.getElementById('aaiNote');
    if (noteEl) noteEl.textContent = 'Final turn received ✓';
  }
  if (src === 'oai' && fin) {
    var oaiEl = document.getElementById('oaiNote');
    if (oaiEl) oaiEl.textContent = 'Final turn received ✓';
  }
  if (src === 'voxtral' && fin) {
    var vxEl = document.getElementById('vxNote');
    if (vxEl) vxEl.textContent = 'Flushed ✓';
  }
  // Update latency stats on any message with a value
  if (latency_ms != null) updateLatStats(src, latency_ms);
  var p = panels[src];
  var a = document.getElementById(p.aId);
  var ph = a.querySelector('.ph2'); if (ph) ph.remove();

  // Update detected language badge in Flux panel header (on any turn with language data)
  if (src === 'flux' && langs && langs.length) {
    var langNames = {
      en: 'English', fr: 'French', es: 'Spanish', de: 'German',
      hi: 'Hindi', ru: 'Russian', pt: 'Portuguese', ja: 'Japanese',
      it: 'Italian', nl: 'Dutch'
    };
    var display = langs.map(function (c) { return (langNames[c] || c.toUpperCase()); }).join(', ');
    var dlEl = document.getElementById('detectedLang');
    if (dlEl) dlEl.textContent = '🌍 Detected: ' + display;
  }

  if (!fin) {
    // Interim / speculative — update live block
    if (!p.live) {
      p.live = document.createElement('div');
      p.live.className = 'live';
      p.live.innerHTML = '<span class="lbl">● LIVE</span><span class="ltx"></span><span class="cur"></span>';
      a.appendChild(p.live);
    }
    p.live.querySelector('.ltx').textContent = text;
    // Show latency on the live block too
    var liveLat = p.live.querySelector('.live-lat');
    if (latency_ms != null) {
      if (!liveLat) {
        liveLat = document.createElement('span');
        liveLat.className = 'live-lat';
        liveLat.style.cssText = 'font-family:"Fira Code",monospace;font-size:.6rem;margin-left:8px;opacity:.75;';
        p.live.querySelector('.lbl').appendChild(liveLat);
      }
      liveLat.textContent = ' · ' + latency_ms + 'ms';
      liveLat.style.color = latency_ms < 800 ? 'var(--green)' : latency_ms < 2000 ? 'var(--amber)' : 'var(--red)';
    }
    a.scrollTop = a.scrollHeight;
  } else {
    // Final — commit to history
    if (p.live) { p.live.remove(); p.live = null; }
    if (text.trim()) {
      var d = document.createElement('div');
      d.className = 'fin';
      // For Flux multilingual: append detected lang code tag inline
      if (src === 'flux' && langs && langs.length) {
        d.textContent = text;
        var ltag = document.createElement('span');
        ltag.style.cssText = 'font-size:.6rem;margin-left:7px;color:var(--green);font-family:"Fira Code",monospace;opacity:.8;';
        ltag.textContent = '[' + langs.join(',') + ']';
        d.appendChild(ltag);
      } else {
        d.textContent = text;
      }
      // Latency badge on final line
      if (latency_ms != null) {
        var badge = document.createElement('span');
        badge.className = latBadgeClass(latency_ms);
        badge.textContent = latency_ms + 'ms';
        d.appendChild(badge);
      }
      a.appendChild(d);
      a.scrollTop = a.scrollHeight;
    }
    if (src === 'flux' && recording) showFluxListening();
  }
}

// ─── Nova-3 speech-ping ───────────────────────────────────────────────────────
var _pingTimer = null;
function showSpeechPing() {
  var a = document.getElementById('a1');
  var old = a.querySelector('.speech-ping'); if (old) old.remove();
  if (panels['nova3'].live) return; // already showing live text
  var d = document.createElement('div');
  d.className = 'speech-ping';
  d.innerHTML = '<span style="font-size:.85rem">🎤</span><span>Speech detected…</span>';
  a.appendChild(d);
  a.scrollTop = a.scrollHeight;
  clearTimeout(_pingTimer);
  _pingTimer = setTimeout(function () { if (d.parentNode) d.remove(); }, 2000);
}

// ─── Flux listening indicator ─────────────────────────────────────────────────
function showFluxListening() {
  var a = document.getElementById('a2');
  if (document.getElementById('fluxListen')) return;
  var ph = a.querySelector('.ph2'); if (ph) ph.remove();
  var d = document.createElement('div');
  d.id = 'fluxListen'; d.className = 'listening';
  d.innerHTML = '<span class="ldot"></span><span>Listening… transcript appears after you stop speaking</span>';
  a.appendChild(d);
  a.scrollTop = a.scrollHeight;
}
function hideFluxListening() {
  var el = document.getElementById('fluxListen'); if (el) el.remove();
}

// ─── Panel errors ─────────────────────────────────────────────────────────────
function showPanelErr(src, msg) {
  var aMap = { nova3: 'a1', flux: 'a2', aai: 'a3', oai: 'a4', voxtral: 'a5' };
  var aId = aMap[src] || 'a1';
  document.getElementById(aId).innerHTML =
    '<div class="panel-err"><strong>Connection failed</strong><br>' +
    '<code style="font-size:.72rem">' + msg + '</code><br><br>' +
    'Check the server terminal. The model name or params may need updating in the pipeline configs.</div>';
}

// ─── VAD meter ────────────────────────────────────────────────────────────────
function meter(rms) {
  var f = document.getElementById('mf'), s = document.getElementById('vs');
  var p = Math.min(rms / MAX_RMS * 100, 100).toFixed(1);
  var c, t;
  if (rms < NEW_VAD) { c = 'var(--red)'; t = 'Level ' + rms + ' — Dropped by both old and new VAD'; }
  else if (rms < OLD_VAD) { c = 'var(--amber)'; t = 'Level ' + rms + ' — Old VAD blocks this · New VAD (650) PASSES ✓'; }
  else { c = 'var(--green)'; t = 'Level ' + rms + ' — Passes both thresholds ✓'; }
  f.style.width = p + '%';
  f.style.background = c;
  s.textContent = t;
  s.style.color = c;
}

// ─── UI state ─────────────────────────────────────────────────────────────────
function setState(v) {
  recording = v;
  var b = document.getElementById('startBtn');
  var t = document.getElementById('startBtnTxt');
  t.textContent = v ? '⏹ Stop' : '▶ Start';
  b.className = v ? 'stop' : '';
  document.getElementById('btnEn').disabled = v;
  document.getElementById('btnFr').disabled = v;
  document.getElementById('btnFrBi').disabled = v;
  if (v) {
    showFluxListening();
  } else {
    document.getElementById('vs').textContent = '⏸ Not recording';
    document.getElementById('vs').style.color = '';
  }
}

function clear() {
  hideFluxListening();
  Object.keys(panels).forEach(function (k) { panels[k].live = null; });
  document.getElementById('a1').innerHTML = '<span class="ph2">Transcription appears here live…</span>';
  document.getElementById('a2').innerHTML = '<span class="ph2">Transcription appears here live…</span>';
  document.getElementById('a3').innerHTML = '<span class="ph2">Transcription appears here live…</span>';
  document.getElementById('a4').innerHTML = '<span class="ph2">Transcription appears here live…</span>';
  document.getElementById('a5').innerHTML = '<span class="ph2">Transcription appears here live…</span>';
  var n = document.getElementById('aaiNote'); if (n) n.textContent = '';
  var o = document.getElementById('oaiNote'); if (o) o.textContent = '';
  var v = document.getElementById('vxNote'); if (v) v.textContent = '';
}

function showErr(m) {
  var e = document.getElementById('errEl');
  document.getElementById('errTxt').textContent = m;
  e.classList.add('show');
}
function hideErr() { document.getElementById('errEl').classList.remove('show'); }
