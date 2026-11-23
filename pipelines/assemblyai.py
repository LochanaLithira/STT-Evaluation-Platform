"""AssemblyAI v3 streaming pipeline, parameterized by speech model.

One class drives both models under test:
  - Universal-3.5 Pro Realtime            speech_model=universal-3-5-pro
  - AssemblyAI-STT-3.7-Realtime-Agent     speech_model=assemblyai-stt-3-7-realtime-agent

Protocol (AssemblyAI streaming v3 docs):
  - Endpoint: wss://streaming.assemblyai.com/v3/ws?speech_model=...&sample_rate=16000
  - Auth: "Authorization: <API_KEY>" (raw key, NO "Bearer" prefix)
  - Audio: binary 16 kHz pcm_s16le, 20–1000 ms per chunk (browser sends 80 ms)
  - mode: balanced | min_latency | max_accuracy (omitted = server default preset)
  - Language: both models code-switch natively. language_codes biases output
    toward the listed languages (still allows switching); it must be a
    JSON array in the query string, e.g. language_codes=["fr","en"] — a
    comma list or an unknown code is rejected at connect with close 3006
    (verified against both models). Omitted = no steering.
    language_detection=true only adds language_code to Turn messages.
  - Server events:
      Begin          {id, expires_at, configuration{model, mode, ...}}
      SpeechStarted  {timestamp, confidence}
      Turn           {turn_order, transcript, end_of_turn, words[{start,end(ms)}], language_code?}
                     Each Turn re-transcribes the whole turn → replace, don't append.
      Termination    {audio_duration_seconds, session_duration_seconds}
  - Finish: send {"type":"Terminate"}; the server finalizes the turn in
    progress (one last end_of_turn Turn), sends Termination, then closes.
    Abandoned sessions keep billing, so close() always sends it too.
"""
import json
from urllib.parse import urlencode

import websockets

from .base import Pipeline

STREAMING_URL = "wss://streaming.assemblyai.com/v3/ws"
MODES = ("balanced", "min_latency", "max_accuracy")
# Suppresses background speakers. Both models accept and echo it in Begin
# (verified); an unknown value is rejected at connect with close 3006.
VOICE_FOCUS = ("near-field", "far-field")

# UI language option → language_codes sent to AssemblyAI (None = no steering)
LANGUAGES = {
    "en":           ["en"],
    "fr":           ["fr"],
    "fr-bilingual": ["fr", "en"],
    "auto":         None,
}


class AssemblyAIPipeline(Pipeline):
    def __init__(self, source: str, label: str, speech_model: str, api_key: str,
                 mode: str = "", lang: str = "en", voice_focus: str = ""):
        self.source       = source
        self.label        = label
        self.speech_model = speech_model
        params = {
            "speech_model": speech_model,
            "sample_rate": 16000,
            "encoding": "pcm_s16le",
            "language_detection": "true",
        }
        if mode in MODES:
            params["mode"] = mode
        if voice_focus in VOICE_FOCUS:
            params["voice_focus"] = voice_focus
        codes = LANGUAGES.get(lang, LANGUAGES["en"])
        if codes:
            params["language_codes"] = json.dumps(codes)
        self.url  = STREAMING_URL + "?" + urlencode(params)
        self.auth = {"Authorization": api_key}
        self.ws   = None

    async def connect(self):
        kwargs = dict(open_timeout=15, ping_interval=20, ping_timeout=30)
        try:
            self.ws = await websockets.connect(self.url, additional_headers=self.auth, **kwargs)
        except TypeError:  # websockets < 14
            self.ws = await websockets.connect(self.url, extra_headers=self.auth, **kwargs)
        print(f"[{self.label}] Connected ({self.url.split('?', 1)[1]})")

    async def send_audio(self, data: bytes):
        if self.ws:
            await self.ws.send(data)
            self._mark_sent(data)

    async def receive_loop(self, send_fn):
        try:
            async for raw in self.ws:
                if isinstance(raw, bytes):
                    continue
                msg = json.loads(raw)
                msg_type = msg.get("type", "")

                if msg_type == "Begin":
                    # Echo what the server actually applied so the UI can show it.
                    cfg = msg.get("configuration") or {}
                    print(f"[{self.label}] Session {msg.get('id')} config={cfg}")
                    await send_fn({
                        "source": self.source,
                        "session": {
                            "id": msg.get("id"),
                            "model": cfg.get("model") or cfg.get("speech_model"),
                            "mode": cfg.get("mode"),
                            "voice_focus": cfg.get("voice_focus"),
                        },
                    })

                elif msg_type == "Turn":
                    transcript = (msg.get("transcript") or "").strip()
                    if not transcript:
                        continue
                    # Latency = now − when the audio of the last word was sent.
                    # words[].end is ms from the start of the stream.
                    words = msg.get("words") or []
                    last_end = words[-1].get("end") if words else None
                    latency_ms = (self.latency_for_audio_ms(last_end)
                                  if last_end is not None else self.latency_since_voiced())
                    await send_fn({
                        "source":     self.source,
                        "transcript": transcript,
                        "is_final":   bool(msg.get("end_of_turn")),
                        "latency_ms": latency_ms,
                        "language":   msg.get("language_code"),
                        "turn_order": msg.get("turn_order"),
                    })

                elif msg_type == "Termination":
                    print(f"[{self.label}] Terminated. Audio duration:",
                          msg.get("audio_duration_seconds"), "s")
                    break

                elif msg_type == "Error" or "error" in msg:
                    print(f"[{self.label}] Error:", msg)
                    await send_fn({"source": self.source, "error": str(msg.get("error", msg))})
        except websockets.ConnectionClosed as e:
            # Rejected params / bad model arrive as a close code + reason.
            if e.rcvd and e.rcvd.code not in (1000, 1001):
                reason = f"closed {e.rcvd.code}: {e.rcvd.reason}"
                print(f"[{self.label}] {reason}")
                await send_fn({"source": self.source, "error": reason})
        except Exception as e:
            print(f"[{self.label}] receive_loop error:", e)

    async def finish(self):
        """Ask the server to finalize the current turn and end the session."""
        if self.ws:
            try:
                await self.ws.send(json.dumps({"type": "Terminate"}))
            except Exception:
                pass

    async def close(self):
        if self.ws:
            try:
                await self.ws.send(json.dumps({"type": "Terminate"}))
                await self.ws.close()
            except Exception:
                pass
