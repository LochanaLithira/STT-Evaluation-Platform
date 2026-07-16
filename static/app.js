// ─── Constants ───────────────────────────────────────────────────────────────
var TARGET_SR = 16000;
var CHUNK_SAMPLES = 1280; // 80ms at 16kHz

// ─── Latency tracking ────────────────────────────────────────────────────────
var latStats = {
  aai:     { samples: [], ids: { last: 'aa-last', avg: 'aa-avg', min: 'aa-min', max: 'aa-max' } },
  voxtral: { samples: [], ids: { last: 'vx-last', avg: 'vx-avg', min: 'vx-min', max: 'vx-max' } },
  soniox:  { samples: [], ids: { last: 'sx-last', avg: 'sx-avg', min: 'sx-min', max: 'sx-max' } },
  gladia:  { samples: [], ids: { last: 'gl-last', avg: 'gl-avg', min: 'gl-min', max: 'gl-max' } }
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
  if (!s) return;
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
    el.classList.remove('lat-flash');
    void el.offsetWidth;
    el.classList.add('lat-flash');
  }
  setEl(ids.last, ms);
  setEl(ids.avg, avg);
  setEl(ids.min, min);
  setEl(ids.max, max);
}

function resetLatStats() {
  ['aai', 'voxtral', 'soniox', 'gladia'].forEach(function (src) {
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
  aai:     { aId: 'aAAI', live: null },
  voxtral: { aId: 'aVX',  live: null },
  soniox:  { aId: 'aSOX', live: null },
  gladia:  { aId: 'aGLA', live: null }
};

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
  var langLabel = isFrAny ? 'French' : 'English';

  document.getElementById('tAAI').textContent = 'AssemblyAI U3 Pro — Streaming STT (' + langLabel + ')';
  document.getElementById('lAAItag').textContent = isFr
    ? 'prompt=Transcribe French.'
    : isFrBi
      ? 'prompt=Transcribe FR+EN'
      : '(default English prompt)';

  document.getElementById('tVX').textContent = 'Voxtral — Realtime STT (auto-detect)';
  document.getElementById('tSOX').textContent = 'Soniox — STT RT v5 (' + langLabel + ')';
  document.getElementById('tGLA').textContent = isFr
    ? 'Gladia Solaria — Realtime STT (French)'
    : isFrBi
      ? 'Gladia Solaria — Realtime STT (FR+EN)'
      : 'Gladia Solaria — Realtime STT (English)';

  document.getElementById('aaiNote').textContent = '';
  document.getElementById('vxNote').textContent = '';
  document.getElementById('soxNote').textContent = '';
  document.getElementById('glaNote').textContent = '';
}

// ─── Start / Stop ────────────────────────────────────────────────────────────
function toggle() { recording ? stop() : start(); }

function start() {
  hideErr(); clear(); resetLatStats();
  navigator.mediaDevices
    .getUserMedia({ audio: { channelCount: 1, sampleRate: { ideal: TARGET_SR }, echoCancellation: true, noiseSuppression: true } })
    .then(function (s) {
      stream = s;
      try { actx = new AudioContext({ sampleRate: TARGET_SR }); }
      catch (e) { actx = new AudioContext(); }

      var proto = location.protocol === 'https:' ? 'wss' : 'ws';
      var url = proto + '://' + location.host + '/ws/compare4?lang=' + lang;
      ws = new WebSocket(url);
      ws.binaryType = 'arraybuffer';
      ws.onopen = function () { setupAudio(); setState(true); };
      ws.onerror = function () { showErr('Connection error — check server is running and API keys are set.'); };
      ws.onclose = function () { if (recording) stop(); };
      ws.onmessage = handleMessage;
    })
    .catch(function () {
      showErr('Microphone access denied. Please allow mic access in your browser and try again.');
    });
}

// ─── Audio pipeline ──────────────────────────────────────────────────────────
var resampleBuf = [];

function setupAudio() {
  var src = actx.createMediaStreamSource(stream);
  var proc = actx.createScriptProcessor(1024, 1, 1);
  var nativeSR = actx.sampleRate;
  var ratio = nativeSR / TARGET_SR;

  proc.onaudioprocess = function (e) {
    if (!recording || !ws || ws.readyState !== WebSocket.OPEN) return;

    var input = e.inputBuffer.getChannelData(0);

    // ── RMS meter ──
    var sq = 0;
    for (var i = 0; i < input.length; i++) sq += input[i] * input[i];
    meter(Math.round(Math.sqrt(sq / input.length) * 32767));

    // ── Downsample to 16kHz ──
    var outLen = Math.floor(input.length / ratio);
    for (var j = 0; j < outLen; j++) {
      var pos = j * ratio;
      var idx = Math.floor(pos);
      var frac = pos - idx;
      var s0 = input[idx] || 0;
      var s1 = input[idx + 1] || s0;
      resampleBuf.push(s0 + frac * (s1 - s0));
    }

    // ── Emit chunks ──
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
  workletNode = proc;
}

function stop() {
  setState(false);
  resampleBuf = [];
  if (workletNode) { workletNode.disconnect(); workletNode = null; }
  if (actx) { actx.close(); actx = null; }
  if (stream) { stream.getTracks().forEach(function (t) { t.stop(); }); stream = null; }
  meter(0);

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

  if (m.error && !m.source) { showErr('Server error: ' + m.error); stop(); return; }
  if (m.error && m.source) { showPanelErr(m.source, m.error); return; }

  if (m.source === 'aai')     addLine('aai',     m.transcript, m.is_final, m.latency_ms);
  if (m.source === 'voxtral') addLine('voxtral', m.transcript, m.is_final, m.latency_ms);
  if (m.source === 'soniox')  addLine('soniox',  m.transcript, m.is_final, m.latency_ms);
  if (m.source === 'gladia')  addLine('gladia',  m.transcript, m.is_final, m.latency_ms);
}

// ─── Transcript rendering ─────────────────────────────────────────────────────
function addLine(src, text, fin, latency_ms) {
  if (src === 'aai' && fin) {
    var noteEl = document.getElementById('aaiNote');
    if (noteEl) noteEl.textContent = 'Final turn received';
  }
  if (src === 'voxtral' && fin) {
    var vxEl = document.getElementById('vxNote');
    if (vxEl) vxEl.textContent = 'Flushed';
  }
  if (src === 'soniox' && fin) {
    var sxEl = document.getElementById('soxNote');
    if (sxEl) sxEl.textContent = 'Final received';
  }
  if (src === 'gladia' && fin) {
    var glEl = document.getElementById('glaNote');
    if (glEl) glEl.textContent = 'Final received';
  }

  if (latency_ms != null) updateLatStats(src, latency_ms);

  var p = panels[src];
  if (!p) return;
  var a = document.getElementById(p.aId);
  var ph = a.querySelector('.ph2'); if (ph) ph.remove();

  if (!fin) {
    if (!p.live) {
      p.live = document.createElement('div');
      p.live.className = 'live';
      p.live.innerHTML = '<span class="lbl">LIVE</span><span class="ltx"></span><span class="cur"></span>';
      a.appendChild(p.live);
    }
    p.live.querySelector('.ltx').textContent = text;
    var liveLat = p.live.querySelector('.live-lat');
    if (latency_ms != null) {
      if (!liveLat) {
        liveLat = document.createElement('span');
        liveLat.className = 'live-lat';
        liveLat.style.cssText = 'font-family:"Fira Code",monospace;font-size:.6rem;margin-left:8px;opacity:.75;';
        p.live.querySelector('.lbl').appendChild(liveLat);
      }
      liveLat.textContent = ' ' + latency_ms + 'ms';
      liveLat.style.color = latency_ms < 800 ? 'var(--green)' : latency_ms < 2000 ? 'var(--amber)' : 'var(--red)';
    }
    a.scrollTop = a.scrollHeight;
  } else {
    if (p.live) { p.live.remove(); p.live = null; }
    if (text && text.trim()) {
      var d = document.createElement('div');
      d.className = 'fin';
      d.textContent = text;
      if (latency_ms != null) {
        var badge = document.createElement('span');
        badge.className = latBadgeClass(latency_ms);
        badge.textContent = latency_ms + 'ms';
        d.appendChild(badge);
      }
      a.appendChild(d);
      a.scrollTop = a.scrollHeight;
    }
  }
}

// ─── Panel errors ─────────────────────────────────────────────────────────────
function showPanelErr(src, msg) {
  var aMap = { aai: 'aAAI', voxtral: 'aVX', soniox: 'aSOX', gladia: 'aGLA' };
  var aId = aMap[src];
  if (!aId) return;
  document.getElementById(aId).innerHTML =
    '<div class="panel-err"><strong>Connection failed</strong><br>' +
    '<code style="font-size:.72rem">' + msg + '</code><br><br>' +
    'Check the server terminal and API key.</div>';
}

// ─── VAD meter ────────────────────────────────────────────────────────────────
function meter(rms) {
  var f = document.getElementById('mf'), s = document.getElementById('vs');
  var MAX_RMS = 4000;
  var p = Math.min(rms / MAX_RMS * 100, 100).toFixed(1);
  var c = rms < 200 ? 'var(--text-muted)' : rms < 1000 ? 'var(--amber)' : 'var(--green)';
  f.style.width = p + '%';
  f.style.background = c;
  s.textContent = 'Level ' + rms;
  s.style.color = c;
}

// ─── UI state ─────────────────────────────────────────────────────────────────
function setState(v) {
  recording = v;
  var t = document.getElementById('startBtnTxt');
  var b = document.getElementById('startBtn');
  t.textContent = v ? 'Stop' : 'Start';
  b.className = v ? 'stop' : '';
  document.getElementById('btnEn').disabled = v;
  document.getElementById('btnFr').disabled = v;
  document.getElementById('btnFrBi').disabled = v;
  if (!v) {
    document.getElementById('vs').textContent = 'Not recording';
    document.getElementById('vs').style.color = '';
  }
}

function clear() {
  Object.keys(panels).forEach(function (k) { panels[k].live = null; });
  document.getElementById('aAAI').innerHTML = '<span class="ph2">Transcription appears here live…</span>';
  document.getElementById('aVX').innerHTML  = '<span class="ph2">Transcription appears here live…</span>';
  document.getElementById('aSOX').innerHTML = '<span class="ph2">Transcription appears here live…</span>';
  document.getElementById('aGLA').innerHTML = '<span class="ph2">Transcription appears here live…</span>';
  document.getElementById('aaiNote').textContent = '';
  document.getElementById('vxNote').textContent = '';
  document.getElementById('soxNote').textContent = '';
  document.getElementById('glaNote').textContent = '';
}

function showErr(m) {
  var e = document.getElementById('errEl');
  document.getElementById('errTxt').textContent = m;
  e.classList.add('show');
}
function hideErr() { document.getElementById('errEl').classList.remove('show'); }
