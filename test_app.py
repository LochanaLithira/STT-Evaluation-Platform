"""
STT Comparison Test App - Nova-3 vs Flux vs AssemblyAI U3 Pro
=============================================================
Set your API keys in .env:
    DEEPGRAM_API_KEY=your_deepgram_key
    ASSEMBLYAI_API_KEY=your_assemblyai_key

Run:
    pip install fastapi uvicorn websockets python-dotenv
    python test_app.py

Open: http://localhost:8765
"""
import asyncio, json, os, time, uvicorn, base64, struct
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import websockets

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

app  = FastAPI()
HERE = Path(__file__).parent


def get_api_key():
    key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "DEEPGRAM_API_KEY not set. Add it to the .env file next to test_app.py"
        )
    return key


def get_assemblyai_key():
    key = os.environ.get("ASSEMBLYAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "ASSEMBLYAI_API_KEY not set. Add it to the .env file next to test_app.py"
        )
    return key


def get_openai_key():
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "OPENAI_API_KEY not set. Add it to the .env file next to test_app.py"
        )
    return key


def get_mistral_key():
    key = os.environ.get("MISTRAL_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "MISTRAL_API_KEY not set. Add it to the .env file next to test_app.py"
        )
    return key


# ============================================================
# Pipeline: Nova-3  (wss://api.deepgram.com/v1/listen)
# ============================================================
# Key configuration decisions (per Deepgram docs):
#   - endpointing=300  : 300ms silence triggers speech_final.
#                        100ms was too aggressive — caused sentence
#                        fragmentation mid-utterance. 300ms is the
#                        recommended sweet-spot for real-time UI.
#   - utterance_end_ms=1000 : Correct; fires UtteranceEnd after 1s
#                        of post-speech silence as a safety net.
#   - interim_results=true  : Essential for live UI feedback
#   - vad_events=true  : Enables SpeechStarted events so the UI
#                        can react the instant speech begins
#   - punctuate=true   : Adds natural punctuation to transcripts
#   - smart_format=true: Formats numbers, dates, etc.
#   - sample_rate=16000: Browser resamples to 16 kHz before sending.
#                        Deepgram works best at 16 kHz; sending 48kHz
#                        tripled the data size for no accuracy gain.
#   - KeepAlive every 8s : Server closes at 12s of silence.
#   - CloseStream on teardown for clean shutdown.
# ============================================================
class Nova3Pipeline:
    def __init__(self, lang: str, api_key: str):
        # Nova-3 uses the standard 'language' param on /v1/listen.
        # Normalize fr-bilingual -> fr (Nova-3 doesn't have a bilingual mode;
        # it's single-language. Both French modes share the same Nova-3 config.)
        nova_lang = "fr" if lang.startswith("fr") else lang
        self.url = (
            "wss://api.deepgram.com/v1/listen"
            "?model=nova-3"
            f"&language={nova_lang}"
            "&sample_rate=16000&encoding=linear16&channels=1"
            "&smart_format=true&punctuate=true"
            "&interim_results=true&vad_events=true"
            "&endpointing=300&utterance_end_ms=1000"
        )
        self.auth      = {"Authorization": "Token " + api_key}
        self.ws        = None
        self._t_start  = None   # time of first audio chunk sent

    async def connect(self):
        try:
            self.ws = await websockets.connect(
                self.url, additional_headers=self.auth, open_timeout=15
            )
        except TypeError:
            self.ws = await websockets.connect(
                self.url, extra_headers=self.auth, open_timeout=15
            )
        print("[Nova-3] Connected OK")

    async def send_audio(self, data: bytes):
        if self.ws:
            self._t_start = time.monotonic()  # always update: measures last-audio → transcript
            await self.ws.send(data)

    async def keepalive_loop(self):
        """
        Nova-3 server closes the WebSocket after 12 s with no audio.
        Send KeepAlive every 8 s to keep it alive while user is silent.
        Note: ws.open was removed in websockets>=14; use try/except instead.
        """
        try:
            while True:
                await asyncio.sleep(8)
                if self.ws:
                    try:
                        await self.ws.send(json.dumps({"type": "KeepAlive"}))
                    except Exception:
                        break
        except asyncio.CancelledError:
            pass

    async def receive_loop(self, send_fn):
        """
        Nova-3 response format:
          {"type":"Results","is_final":bool,"speech_final":bool,
           "channel":{"alternatives":[{"transcript":"..."}]}}

        We send interim results (is_final=False) immediately for live UI,
        and final results (is_final=True) to commit the line.
        We also handle SpeechStarted events (vad_events=true) to give
        the UI an instant signal that speech has begun.
        """
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                msg_type = msg.get("type", "")
                latency_ms = round((time.monotonic() - self._t_start) * 1000) if self._t_start else None

                # VAD: speech just started — notify UI immediately
                if msg_type == "SpeechStarted":
                    await send_fn({"source": "nova3", "speech_started": True,
                                   "latency_ms": latency_ms})
                    continue

                if msg_type == "Results":
                    alts = msg.get("channel", {}).get("alternatives", [])
                    text = (alts[0].get("transcript", "") if alts else "").strip()
                    is_final     = msg.get("is_final", False)
                    speech_final = msg.get("speech_final", False)
                    if text:
                        await send_fn({
                            "source":       "nova3",
                            "transcript":   text,
                            "is_final":     is_final,
                            "speech_final": speech_final,
                            "latency_ms":   latency_ms,
                        })
                    # latency_ms is now: time-since-last-audio-chunk → transcript
                    # i.e. actual model processing delay after speech ended
                    if speech_final:
                        self._t_start = None  # will be re-set on next audio chunk
        except Exception:
            pass

    async def close(self):
        if self.ws:
            try:
                await self.ws.send(json.dumps({"type": "CloseStream"}))
                await self.ws.close()
            except Exception:
                pass


# ============================================================
# Pipeline: Flux  (wss://api.deepgram.com/v2/listen)
# ============================================================
# Key configuration decisions (per Deepgram docs):
#   - eot_threshold=0.7    : Default per docs (range 0.5–0.9).
#   - eager_eot_threshold=0.4 : Must be <= eot_threshold. Triggers
#                            EagerEndOfTurn so we can show text early.
#   - eot_timeout_ms=3000  : Balanced value; doc default is 5000ms.
#   - sample_rate=16000    : Resampled to 16kHz in the browser.
#   - ping_interval=20     : WS-level keep-alive (no KeepAlive JSON
#                            needed for Flux unlike Nova-3).
#   - Flux does NOT support: channels, smart_format, interim_results,
#                            endpointing, utterance_end_ms, vad_events
#   - Chunk size: 80ms = 1280 samples = 2560 bytes at 16kHz
#                 (docs explicitly say "80ms chunks strongly recommended")
#
# FRENCH LANGUAGE CONFIGURATION (critical per docs):
#   lang="fr"  (monolingual):  model=flux-general-multi
#                              language_hint=fr  ← ONE hint only
#                              Docs: single hint = near-monolingual accuracy
#                              Handles ALL French accents (fr-FR, fr-CA, etc.)
#                              BCP-47 subtags accepted; fall back to base 'fr'
#
#   lang="fr-bilingual":       model=flux-general-multi
#                              language_hint=fr&language_hint=en
#                              Docs: multiple hints = bilingual bias
#                              Use when speaker mixes French and English
#
#   WRONG (old code):          language_hint=fr&language_hint=en for pure FR
#                              This dilutes French bias → lower accent accuracy
#
#   DO NOT use: language=fr — Flux ignores the 'language' param entirely.
#               Only model name + language_hint controls language selection.
# ============================================================
class FluxPipeline:
    def __init__(self, lang: str, api_key: str):
        if lang == "fr":
            # Single hint = maximum French accuracy (monolingual mode)
            # Docs: "Set a single language_hint for best accuracy"
            # Covers all French accents — model treats fr-FR, fr-CA, etc.
            # as base 'fr' automatically
            model       = "flux-general-multi"
            lang_params = "&language_hint=fr"
        elif lang == "fr-bilingual":
            # Two hints = bilingual French+English mode
            # Use when speaker code-switches between French and English
            model       = "flux-general-multi"
            lang_params = "&language_hint=fr&language_hint=en"
        else:
            # English-only: use dedicated English model (no language_hint)
            # Docs: language_hint sent to flux-general-en returns 400 error
            model       = "flux-general-en"
            lang_params = ""
        self.url = (
            "wss://api.deepgram.com/v2/listen"
            "?model=" + model + lang_params +
            "&sample_rate=16000&encoding=linear16"
            "&eot_threshold=0.7&eager_eot_threshold=0.4&eot_timeout_ms=3000"
        )
        self.auth     = {"Authorization": "Token " + api_key}
        self.ws       = None
        self._t_start = None   # time of first audio chunk sent

    async def connect(self):
        try:
            self.ws = await websockets.connect(
                self.url,
                additional_headers=self.auth,
                open_timeout=15,
                ping_interval=20,
                ping_timeout=30,
            )
        except TypeError:
            self.ws = await websockets.connect(
                self.url,
                extra_headers=self.auth,
                open_timeout=15,
                ping_interval=20,
                ping_timeout=30,
            )
        print("[Flux  ] Connected OK")

    async def send_audio(self, data: bytes):
        if self.ws:
            self._t_start = time.monotonic()  # always update: measures last-audio → transcript
            await self.ws.send(data)

    async def receive_loop(self, send_fn):
        """
        Flux response format:
          {"type":"TurnInfo","event":"EndOfTurn"|"EagerEndOfTurn"|"TurnResumed",
           "transcript":"...",
           "languages":["fr"],           <- only on flux-general-multi
           "languages_hinted":["fr"]}    <- only on flux-general-multi

        EagerEndOfTurn  -> show transcript speculatively (is_final=False)
        EndOfTurn       -> commit as final; surface detected language(s)
        TurnResumed     -> user kept talking; clear speculative text
        """
        try:
            async for raw in self.ws:
                if isinstance(raw, bytes):
                    continue
                msg = json.loads(raw)
                if msg.get("type") == "TurnInfo":
                    event      = msg.get("event", "")
                    transcript = msg.get("transcript", "").strip()
                    # languages field: what the model actually detected
                    # (only present on flux-general-multi)
                    detected   = msg.get("languages", [])
                    latency_ms = round((time.monotonic() - self._t_start) * 1000) if self._t_start else None
                    if event == "TurnResumed":
                        await send_fn({
                            "source": "flux", "transcript": "",
                            "is_final": False, "turn_resumed": True,
                            "latency_ms": latency_ms,
                        })
                    elif event == "EagerEndOfTurn" and transcript:
                        # Show speculatively — may be cancelled by TurnResumed
                        await send_fn({
                            "source":     "flux",
                            "transcript": transcript,
                            "is_final":   False,
                            "languages":  detected,
                            "latency_ms": latency_ms,
                        })
                    elif event == "EndOfTurn" and transcript:
                        await send_fn({
                            "source":     "flux",
                            "transcript": transcript,
                            "is_final":   True,
                            "languages":  detected,
                            "latency_ms": latency_ms,
                        })
                        # _t_start will be re-set automatically on next audio chunk
        except Exception:
            pass

    async def close(self):
        if self.ws:
            try:
                await self.ws.send(json.dumps({"type": "CloseStream"}))
                await self.ws.close()
            except Exception:
                pass


# ============================================================
# Pipeline: AssemblyAI Universal-3 Pro Streaming
# ============================================================
# Key configuration decisions (per AssemblyAI docs):
#   - Endpoint: wss://streaming.assemblyai.com/v3/ws?sample_rate=16000&speech_model=u3-rt-pro
#   - Auth: "Authorization: YOUR_API_KEY" (raw key — NO "Bearer" prefix)
#   - Events: Begin | Turn (end_of_turn bool) | Termination
#   - Partials: end_of_turn=False (interim), end_of_turn=True (final)
#   - Turn detection: punctuation-based (not confidence-based like Deepgram)
#     min_turn_silence=100ms (default) — silence before EOT check
#     max_turn_silence=1000ms (default) — force-ends turn after this silence
#   - Early partial at ~800ms of continuous speech (interruption_delay=500 default)
#   - language_code is SILENTLY IGNORED on U3 Pro Streaming.
#     Use prompt query param to steer language: "Transcribe French."
#   - Terminate: send {"type": "Terminate"} — MUST do this or session bills for 3h
#   - Sample rate: 16000 Hz PCM16 mono (same as Deepgram pipelines)
#   - No KeepAlive JSON needed — WS-level ping handles keep-alive
# ============================================================
class AssemblyAIPipeline:
    def __init__(self, lang: str, api_key: str):
        # U3 Pro does not support language_code param — silently ignored.
        # Use ?prompt= to steer language. English needs no special prompt.
        if lang == "fr" or lang == "fr-bilingual":
            # Prepend language directive to the default system prompt
            # Docs: "Transcribe <language>." prepended to default prompt works well
            # fr-bilingual: indicate both languages for code-switching
            if lang == "fr-bilingual":
                prompt = "Transcribe multilingual conversation in French and English."
            else:
                prompt = "Transcribe French."
            lang_param = "&prompt=" + prompt.replace(" ", "%20").replace(",", "%2C")
        else:
            lang_param = ""  # English — default prompt is already English-optimised

        self.url = (
            "wss://streaming.assemblyai.com/v3/ws"
            "?sample_rate=16000"
            "&speech_model=u3-rt-pro"
            + lang_param
        )
        # IMPORTANT: Raw key — no "Bearer" prefix (Voice Agent API is the exception)
        self.auth     = {"Authorization": api_key}
        self.ws       = None
        self._t_start = None

    async def connect(self):
        try:
            self.ws = await websockets.connect(
                self.url,
                additional_headers=self.auth,
                open_timeout=15,
                ping_interval=20,
                ping_timeout=30,
            )
        except TypeError:
            self.ws = await websockets.connect(
                self.url,
                extra_headers=self.auth,
                open_timeout=15,
                ping_interval=20,
                ping_timeout=30,
            )
        print("[AAI U3] Connected OK")

    async def send_audio(self, data: bytes):
        if self.ws:
            self._t_start = time.monotonic()  # always update: measures last-audio → transcript
            await self.ws.send(data)

    async def receive_loop(self, send_fn):
        """
        AssemblyAI U3 Pro response format:
          {"type":"Begin",   "id":"...","expires_at":...}
          {"type":"Turn",    "end_of_turn":bool,"transcript":"...",
                             "words":[...],"turn_order":int}
          {"type":"Termination", "audio_duration_seconds":...,"session_duration_seconds":...}

        end_of_turn=False → partial (interim) transcript
        end_of_turn=True  → final transcript for this turn
        """
        try:
            async for raw in self.ws:
                if isinstance(raw, bytes):
                    continue
                msg = json.loads(raw)
                msg_type = msg.get("type", "")
                latency_ms = round((time.monotonic() - self._t_start) * 1000) if self._t_start else None

                if msg_type == "Begin":
                    print("[AAI U3] Session ID:", msg.get("id"))
                    continue

                if msg_type == "Turn":
                    transcript = msg.get("transcript", "").strip()
                    end_of_turn = msg.get("end_of_turn", False)
                    if transcript:
                        await send_fn({
                            "source":       "aai",
                            "transcript":   transcript,
                            "is_final":     end_of_turn,
                            "latency_ms":   latency_ms,
                        })
                    # _t_start will be re-set automatically on next audio chunk

                elif msg_type == "Termination":
                    print("[AAI U3] Terminated. Audio duration:",
                          msg.get("audio_duration_seconds"), "s")
        except Exception as e:
            print("[AAI U3] receive_loop error:", e)

    async def close(self):
        if self.ws:
            try:
                # MUST send Terminate — abandoned sessions bill for up to 3 hours
                await self.ws.send(json.dumps({"type": "Terminate"}))
                await self.ws.close()
            except Exception:
                pass

# ============================================================
# Helper: upsample PCM16 16 kHz → 24 kHz (linear interpolation)
# ============================================================
def _upsample_16k_to_24k(pcm16_bytes: bytes) -> bytes:
    """
    OpenAI Realtime API requires 24 kHz PCM16; browser sends 16 kHz.
    Ratio: 3/2 — every 2 input samples produce 3 output samples.
    Uses linear interpolation; sufficient quality for STT.
    """
    n_in = len(pcm16_bytes) // 2
    if n_in == 0:
        return b""
    samples_in = struct.unpack(f"<{n_in}h", pcm16_bytes[:n_in * 2])
    n_out = int(n_in * 3 / 2)
    out = []
    for j in range(n_out):
        pos  = j * 2 / 3          # source position in 16k samples
        idx  = int(pos)
        frac = pos - idx
        s0   = samples_in[idx]     if idx     < n_in else 0
        s1   = samples_in[idx + 1] if idx + 1 < n_in else s0
        out.append(max(-32768, min(32767, int(round(s0 + frac * (s1 - s0))))))
    return struct.pack(f"<{n_out}h", *out)


# ============================================================
# Pipeline: OpenAI Realtime Transcription  (wss://api.openai.com/v1/realtime)
# ============================================================
# Key configuration decisions (confirmed from official openai-python SDK source):
#
#   ENDPOINT:
#   - URL: wss://api.openai.com/v1/realtime?intent=transcription
#          (NOT ?model=gpt-4o-transcribe  — that's a REST model, not a WS model)
#          (NOT ?model=gpt-realtime-whisper — does not exist, was hallucinated)
#          The ?intent=transcription parameter selects the transcription-only WS API
#   - Auth: Authorization: Bearer {key}  +  OpenAI-Beta: realtime=v1
#
#   SESSION SETUP:
#   - Wait for "transcription_session.created" (different from "session.created")
#   - Send "transcription_session.update" (not "session.update")
#   - Session shape (from TranscriptionSession SDK model):
#       input_audio_format: "pcm16"   (24kHz — SDK confirms "24kHz sample rate")
#       input_audio_transcription:
#           model:    "gpt-4o-transcribe" | "gpt-4o-mini-transcribe" | "whisper-1"
#           language: ISO-639-1 code (optional, improves accuracy)
#       turn_detection:
#           type: "server_vad"
#           silence_duration_ms: 500
#           threshold: 0.5
#           prefix_padding_ms: 300
#
#   AUDIO:
#   - PCM16 at 24kHz (browser sends 16kHz; we upsample via linear interpolation)
#   - Sent as Base64-encoded JSON: {"type":"input_audio_buffer.append","audio":"<b64>"}
#
#   EVENTS (identical to conversational realtime API):
#   - Interim: conversation.item.input_audio_transcription.delta  -> event.delta
#   - Final:   conversation.item.input_audio_transcription.completed -> event.transcript
#   - VAD:     input_audio_buffer.speech_started -> speech ping in UI
#
#   LANGUAGE (en/fr/fr-bilingual pattern):
#   - en           -> language="en"  (explicit hint -> best accuracy)
#   - fr           -> language="fr"  (monolingual French)
#   - fr-bilingual -> omit language  (auto-detect handles FR/EN code-switching)
# ============================================================
class OpenAIPipeline:
    def __init__(self, lang: str, api_key: str):
        # ?intent=transcription — selects the realtime transcription-only WebSocket API
        # (confirmed from official OpenAI realtime transcription guide)
        self.url  = "wss://api.openai.com/v1/realtime?intent=transcription"
        self.auth = {
            "Authorization": "Bearer " + api_key,
            # NOTE: No "OpenAI-Beta: realtime=v1" header — that was the old beta API.
            # The Realtime API is now GA; sending that header causes beta_api_shape_disabled.
        }
        self.lang     = lang
        self.ws       = None
        self._t_start = None

    async def connect(self):
        try:
            self.ws = await websockets.connect(
                self.url, additional_headers=self.auth, open_timeout=15
            )
        except TypeError:
            self.ws = await websockets.connect(
                self.url, extra_headers=self.auth, open_timeout=15
            )
        print("[OpenAI] Connected OK")
        # Transcription session sends "transcription_session.created" or "session.created"
        async for raw in self.ws:
            msg = json.loads(raw)
            evt = msg.get("type", "")
            if evt in ("transcription_session.created", "session.created"):
                print("[OpenAI] Session ready:", evt)
                break
            if evt == "error":
                raise RuntimeError("[OpenAI] handshake error: " + str(msg))

        # Language config: en->"en", fr->"fr", fr-bilingual->omit (auto-detect)
        transcription_cfg: dict = {"model": "gpt-realtime-whisper"}
        if self.lang == "fr":
            transcription_cfg["language"] = "fr"
        elif self.lang == "en":
            transcription_cfg["language"] = "en"
        # fr-bilingual: omit language -> model auto-detects FR/EN code-switching

        # CONFIRMED valid session.update shape for gpt-realtime-whisper:
        # - type="transcription" is REQUIRED
        # - audio.input.format and audio.input.transcription are accepted
        # - audio.input.turn_detection is NOT supported (model has built-in VAD)
        await self.ws.send(json.dumps({
            "type": "session.update",
            "session": {
                "type": "transcription",
                "audio": {
                    "input": {
                        "format": {"type": "audio/pcm", "rate": 24000},
                        "transcription": transcription_cfg,
                        # NOTE: turn_detection OMITTED - not supported by gpt-realtime-whisper
                        # The model uses built-in VAD (speech_started/stopped fire automatically)
                    }
                },
            }
        }))
        print("[OpenAI] session.update sent - no turn_detection (built-in VAD)", flush=True)

    async def send_audio(self, data: bytes):
        # API requires 24kHz PCM16; browser sends 16kHz -> upsample before encoding
        if self.ws:
            self._t_start = time.monotonic()
            data_24k = _upsample_16k_to_24k(data)
            b64 = base64.b64encode(data_24k).decode("ascii")
            await self.ws.send(json.dumps({
                "type": "input_audio_buffer.append",
                "audio": b64,
            }))

    async def receive_loop(self, send_fn):
        """
        gpt-realtime-whisper (?intent=transcription) actual events observed:
          input_audio_buffer.speech_started  -> VAD ping
          input_audio_buffer.speech_stopped
          input_audio_buffer.committed
          conversation.item.added            -> interim? (check payload)
          conversation.item.done             -> final transcript in item.content[0].transcript
        Also handles classic delta/completed as fallback.
        """
        partials: dict = {}   # item_id -> accumulated partial text
        try:
            async for raw in self.ws:
                if isinstance(raw, bytes):
                    continue
                msg      = json.loads(raw)
                msg_type = msg.get("type", "")
                latency_ms = round((time.monotonic() - self._t_start) * 1000) if self._t_start else None

                # DEBUG: print every non-session event + full payload for item/error events
                if not msg_type.startswith("session"):
                    if msg_type in ("conversation.item.added", "conversation.item.done", "error"):
                        print(f"[OpenAI] {msg_type}: {json.dumps(msg)[:400]}", flush=True)
                    else:
                        print(f"[OpenAI] event: {msg_type}", flush=True)

                if msg_type == "input_audio_buffer.speech_started":
                    await send_fn({"source": "oai", "speech_started": True,
                                   "latency_ms": latency_ms})

                # ── New transcription API: transcript lives in conversation.item.done ──
                elif msg_type == "conversation.item.done":
                    item = msg.get("item", {})
                    item_id = item.get("id", "")
                    content_list = item.get("content", [])
                    # content[0].transcript is the final recognised text
                    transcript_text = ""
                    for c in content_list:
                        t = c.get("transcript", "")
                        if t:
                            transcript_text += t
                    text = transcript_text.strip()
                    partials.pop(item_id, None)
                    if text:
                        await send_fn({
                            "source": "oai", "transcript": text,
                            "is_final": True, "latency_ms": latency_ms,
                        })
                    self._t_start = None

                # ── New transcription API: interim via conversation.item.added? ──
                elif msg_type == "conversation.item.added":
                    item = msg.get("item", {})
                    content_list = item.get("content", [])
                    for c in content_list:
                        t = c.get("transcript", "")
                        if t and t.strip():
                            await send_fn({
                                "source": "oai", "transcript": t.strip(),
                                "is_final": False, "latency_ms": latency_ms,
                            })
                            break

                # ── Fallback: classic delta/completed events (old conversational API) ──
                elif msg_type == "conversation.item.input_audio_transcription.delta":
                    item_id = msg.get("item_id", "")
                    delta   = msg.get("delta", "")
                    partials[item_id] = partials.get(item_id, "") + delta
                    text = partials[item_id].strip()
                    if text:
                        await send_fn({
                            "source": "oai", "transcript": text,
                            "is_final": False, "latency_ms": latency_ms,
                        })

                elif msg_type == "conversation.item.input_audio_transcription.completed":
                    item_id = msg.get("item_id", "")
                    text    = msg.get("transcript", "").strip()
                    partials.pop(item_id, None)
                    if text:
                        await send_fn({
                            "source": "oai", "transcript": text,
                            "is_final": True, "latency_ms": latency_ms,
                        })
                    self._t_start = None

        except Exception as e:
            print(f"[OpenAI] receive_loop error: {e}", flush=True)

    async def close(self):
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass


# ============================================================
# Pipeline: Mistral Voxtral Realtime
# (wss://api.mistral.ai/v1/audio/transcriptions/realtime)
# ============================================================
# Key configuration decisions (protocol reverse-engineered from mistralai
# SDK source — extra/realtime/connection.py on GitHub):
#   - Pure raw WebSocket using websockets lib — NO mistralai SDK needed
#   - URL: wss://api.mistral.ai/v1/audio/transcriptions/realtime?model=...
#   - Auth: Authorization: Bearer {MISTRAL_API_KEY}
#   - Audio: PCM16, 16 kHz mono — matches browser pipeline exactly, no resampling
#   - Audio is Base64-encoded JSON (NOT raw binary):
#     {"type":"input_audio.append","audio":"<b64>"}
#   - Handshake: server sends {"type":"session.created"} immediately on connect
#   - Config: send session.update with audio_format + target_streaming_delay_ms
#   - target_streaming_delay_ms=480: docs recommended balanced latency/accuracy
#   - Interim: {"type":"transcription.text.delta","text":"..."}
#   - Done:    {"type":"transcription.done"} — fires at end of session only
#   - NO per-utterance final event → 1.5 s silence-flush heuristic
#   - Close: send {"type":"input_audio.end"} then close WS
# ============================================================
class VoxtralPipeline:
    def __init__(self, lang: str, api_key: str):
        self.url = (
            "wss://api.mistral.ai/v1/audio/transcriptions/realtime"
            "?model=voxtral-mini-transcribe-realtime-2602"
        )
        self.auth     = {"Authorization": "Bearer " + api_key}
        self.lang     = lang
        self.ws       = None
        self._t_start = None

    async def connect(self):
        try:
            self.ws = await websockets.connect(
                self.url, additional_headers=self.auth,
                open_timeout=15, ping_interval=20, ping_timeout=30,
            )
        except TypeError:
            self.ws = await websockets.connect(
                self.url, extra_headers=self.auth,
                open_timeout=15, ping_interval=20, ping_timeout=30,
            )
        print("[Voxtral] Connected OK")
        # Wait for server's session.created handshake
        async for raw in self.ws:
            text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
            msg  = json.loads(text)
            if msg.get("type") == "session.created":
                print("[Voxtral] Session created:", msg.get("session", {}).get("id", ""))
                break
            if msg.get("type") == "error":
                raise RuntimeError("[Voxtral] handshake error: " + str(msg))
        # Configure: audio format + latency target
        # Language note: Voxtral's session.update ONLY accepts audio_format and
        # target_streaming_delay_ms (confirmed from official mistralai SDK source:
        # RealtimeTranscriptionSessionUpdatePayload has no language field).
        # Voxtral auto-detects language from audio — natively supports 13 languages
        # including French and English, so en/fr/fr-bilingual all work automatically.
        await self.ws.send(json.dumps({
            "type": "session.update",
            "session": {
                "audio_format": {"encoding": "pcm_s16le", "sample_rate": 16000},
                "target_streaming_delay_ms": 480,
            }
        }))

    async def send_audio(self, data: bytes):
        if self.ws:
            self._t_start = time.monotonic()
            b64 = base64.b64encode(data).decode("ascii")
            await self.ws.send(json.dumps({
                "type": "input_audio.append",
                "audio": b64,
            }))

    async def receive_loop(self, send_fn):
        """
        Voxtral has no per-utterance final event.
        Accumulate transcription.text.delta events into a buffer.
        If no delta arrives for SILENCE_TIMEOUT seconds, commit the buffer
        as a final transcript line and reset.
        transcription.done fires only when the full session ends.
        """
        SILENCE_TIMEOUT = 1.5  # seconds
        accumulated = ""
        try:
            while True:
                try:
                    raw = await asyncio.wait_for(self.ws.recv(), timeout=SILENCE_TIMEOUT)
                except asyncio.TimeoutError:
                    # 1.5 s silence → flush as final
                    if accumulated.strip():
                        latency_ms = round((time.monotonic() - self._t_start) * 1000) if self._t_start else None
                        await send_fn({
                            "source": "voxtral", "transcript": accumulated.strip(),
                            "is_final": True, "latency_ms": latency_ms,
                        })
                        accumulated = ""
                        self._t_start = None
                    continue

                text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
                msg      = json.loads(text)
                msg_type = msg.get("type", "")
                latency_ms = round((time.monotonic() - self._t_start) * 1000) if self._t_start else None

                if msg_type == "transcription.text.delta":
                    delta = msg.get("text", "")
                    accumulated += delta
                    if accumulated.strip():
                        await send_fn({
                            "source": "voxtral", "transcript": accumulated,
                            "is_final": False, "latency_ms": latency_ms,
                        })

                elif msg_type == "transcription.done":
                    if accumulated.strip():
                        await send_fn({
                            "source": "voxtral", "transcript": accumulated.strip(),
                            "is_final": True, "latency_ms": latency_ms,
                        })
                    accumulated = ""
                    break

                elif msg_type == "session.updated":
                    print("[Voxtral] Session updated OK")

                elif msg_type == "error":
                    print("[Voxtral] Error:", msg)
                    break
        except Exception:
            pass

    async def close(self):
        if self.ws:
            try:
                await self.ws.send(json.dumps({"type": "input_audio.end"}))
                await self.ws.close()
            except Exception:
                pass


# ============================================================
# FastAPI routes
# ============================================================
@app.get("/")
async def get():
    return HTMLResponse((HERE / "test_ui.html").read_text(encoding="utf-8"))


@app.websocket("/ws/compare")
async def compare_ws(browser: WebSocket, lang: str = "en"):
    """
    Legacy 2-way endpoint (Nova-3 + Flux). Kept for compatibility.
    Audio arrives from the browser already resampled to 16 kHz Int16 PCM.

    lang values:
      "en"           -> Nova-3 language=en  | Flux flux-general-en
      "fr"           -> Nova-3 language=fr  | Flux flux-general-multi + language_hint=fr (monolingual)
      "fr-bilingual" -> Nova-3 language=fr  | Flux flux-general-multi + language_hint=fr&language_hint=en
    """
    await browser.accept()

    try:
        api_key = get_api_key()
    except RuntimeError as e:
        await browser.send_json({"error": str(e)})
        await browser.close()
        return

    nova3 = Nova3Pipeline(lang, api_key)
    flux  = FluxPipeline(lang, api_key)

    print("\n[Nova-3] " + nova3.url)
    print("[Flux  ] " + flux.url + "\n")

    for pipeline, label, src in [(nova3, "Nova-3", "nova3"), (flux, "Flux", "flux")]:
        try:
            await pipeline.connect()
        except Exception as e:
            print("[" + label + "] FAILED: " + str(e))
            try:
                await browser.send_json({"source": src, "error": str(e)})
            except Exception:
                pass

    if not nova3.ws and not flux.ws:
        try:
            await browser.send_json({"error": "Both connections failed. Check server console."})
        except Exception:
            pass
        await browser.close()
        return

    async def send_to_browser(msg: dict):
        try:
            await browser.send_json(msg)
        except Exception:
            pass

    async def audio_relay():
        try:
            while True:
                data = await browser.receive_bytes()
                tasks = []
                if nova3.ws: tasks.append(nova3.send_audio(data))
                if flux.ws:  tasks.append(flux.send_audio(data))
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
        except Exception:
            pass

    tasks = [
        asyncio.create_task(audio_relay()),
        asyncio.create_task(nova3.receive_loop(send_to_browser)),
        asyncio.create_task(nova3.keepalive_loop()),
        asyncio.create_task(flux.receive_loop(send_to_browser)),
    ]

    try:
        await asyncio.gather(*tasks)
    except Exception:
        pass
    finally:
        for t in tasks:
            t.cancel()
        await nova3.close()
        await flux.close()
        try:
            await browser.close()
        except Exception:
            pass


@app.websocket("/ws/compare3")
async def compare3_ws(browser: WebSocket, lang: str = "en"):
    """
    3-way comparison: Nova-3 + Flux + AssemblyAI Universal-3 Pro Streaming.
    Audio arrives from the browser already resampled to 16 kHz Int16 PCM.

    lang values:
      "en"           -> Nova-3 en | Flux flux-general-en      | AAI U3 Pro (English default)
      "fr"           -> Nova-3 fr | Flux flux-general-multi+fr | AAI U3 Pro prompt=Transcribe French.
      "fr-bilingual" -> Nova-3 fr | Flux flux-general-multi+fr+en | AAI U3 Pro multilingual prompt
    """
    await browser.accept()

    try:
        dg_key  = get_api_key()
    except RuntimeError as e:
        await browser.send_json({"error": str(e)})
        await browser.close()
        return

    try:
        aai_key = get_assemblyai_key()
    except RuntimeError as e:
        # AAI key missing — warn but continue with 2-way comparison
        try:
            await browser.send_json({"source": "aai", "error": str(e)})
        except Exception:
            pass
        aai_key = None

    nova3 = Nova3Pipeline(lang, dg_key)
    flux  = FluxPipeline(lang, dg_key)
    aai   = AssemblyAIPipeline(lang, aai_key) if aai_key else None

    print("\n[Nova-3] " + nova3.url)
    print("[Flux  ] " + flux.url)
    if aai:
        print("[AAI U3] " + aai.url)
    print()

    # Connect all three in parallel
    connect_targets = [(nova3, "Nova-3", "nova3"), (flux, "Flux", "flux")]
    if aai:
        connect_targets.append((aai, "AAI U3", "aai"))

    for pipeline, label, src in connect_targets:
        try:
            await pipeline.connect()
        except Exception as e:
            print("[" + label + "] FAILED: " + str(e))
            try:
                await browser.send_json({"source": src, "error": str(e)})
            except Exception:
                pass

    if not nova3.ws and not flux.ws and (not aai or not aai.ws):
        try:
            await browser.send_json({"error": "All connections failed. Check server console."})
        except Exception:
            pass
        await browser.close()
        return

    async def send_to_browser(msg: dict):
        try:
            await browser.send_json(msg)
        except Exception:
            pass

    async def audio_relay():
        try:
            while True:
                data = await browser.receive_bytes()
                tasks = []
                if nova3.ws:        tasks.append(nova3.send_audio(data))
                if flux.ws:         tasks.append(flux.send_audio(data))
                if aai and aai.ws:  tasks.append(aai.send_audio(data))
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
        except Exception:
            pass

    tasks = [
        asyncio.create_task(audio_relay()),
        asyncio.create_task(nova3.receive_loop(send_to_browser)),
        asyncio.create_task(nova3.keepalive_loop()),
        asyncio.create_task(flux.receive_loop(send_to_browser)),
    ]
    if aai:
        tasks.append(asyncio.create_task(aai.receive_loop(send_to_browser)))

    try:
        await asyncio.gather(*tasks)
    except Exception:
        pass
    finally:
        for t in tasks:
            t.cancel()
        await nova3.close()
        await flux.close()
        if aai:
            await aai.close()
        try:
            await browser.close()
        except Exception:
            pass


@app.websocket("/ws/compare5")
async def compare5_ws(browser: WebSocket, lang: str = "en"):
    """
    5-way comparison: Nova-3 + Flux + AssemblyAI U3 Pro + OpenAI gpt-4o-transcribe + Voxtral.
    Audio arrives from the browser resampled to 16 kHz Int16 PCM.
    OpenAI pipeline upsamples 16→24 kHz internally before forwarding.

    lang values:
      "en"           -> Nova-3 en | Flux EN model       | AAI default  | OpenAI language=en | Voxtral auto
      "fr"           -> Nova-3 fr | Flux multi+hint=fr  | AAI prompt   | OpenAI language=fr | Voxtral auto
      "fr-bilingual" -> Nova-3 fr | Flux multi+fr+en   | AAI bi-prompt | OpenAI auto-detect | Voxtral auto
    """
    await browser.accept()

    # ── Deepgram key (required — Nova-3 + Flux both need it) ──────────────────
    try:
        dg_key = get_api_key()
    except RuntimeError as e:
        await browser.send_json({"error": str(e)})
        await browser.close()
        return

    # ── AssemblyAI key (optional — warn + continue without AAI panel) ─────────
    try:
        aai_key = get_assemblyai_key()
    except RuntimeError as e:
        try:
            await browser.send_json({"source": "aai", "error": str(e)})
        except Exception:
            pass
        aai_key = None

    # ── OpenAI key (optional — warn + continue without OpenAI panel) ──────────
    try:
        oai_key = get_openai_key()
    except RuntimeError as e:
        try:
            await browser.send_json({"source": "oai", "error": str(e)})
        except Exception:
            pass
        oai_key = None

    # ── Mistral key (optional — warn + continue without Voxtral panel) ────────
    try:
        mistral_key = get_mistral_key()
    except RuntimeError as e:
        try:
            await browser.send_json({"source": "voxtral", "error": str(e)})
        except Exception:
            pass
        mistral_key = None

    # ── Instantiate all pipelines ─────────────────────────────────────────────
    nova3   = Nova3Pipeline(lang, dg_key)
    flux    = FluxPipeline(lang, dg_key)
    aai     = AssemblyAIPipeline(lang, aai_key)     if aai_key     else None
    oai     = OpenAIPipeline(lang, oai_key)         if oai_key     else None
    voxtral = VoxtralPipeline(lang, mistral_key)    if mistral_key else None

    print("\n[Nova-3 ] " + nova3.url)
    print("[Flux   ] " + flux.url)
    if aai:     print("[AAI U3 ] " + aai.url)
    if oai:     print("[OpenAI ] " + oai.url)
    if voxtral: print("[Voxtral] " + voxtral.url)
    print()

    # ── Connect all pipelines sequentially (errors are non-fatal for optional) ─
    connect_targets = [
        (nova3,   "Nova-3",  "nova3"),
        (flux,    "Flux",    "flux"),
    ]
    if aai:     connect_targets.append((aai,     "AAI U3",  "aai"))
    if oai:     connect_targets.append((oai,     "OpenAI",  "oai"))
    if voxtral: connect_targets.append((voxtral, "Voxtral", "voxtral"))

    for pipeline, label, src in connect_targets:
        try:
            await pipeline.connect()
        except Exception as e:
            print("[" + label + "] FAILED: " + str(e))
            try:
                await browser.send_json({"source": src, "error": str(e)})
            except Exception:
                pass

    # ── Abort if every pipeline failed ────────────────────────────────────────
    all_connected = [
        nova3.ws,
        flux.ws,
        aai     and aai.ws,
        oai     and oai.ws,
        voxtral and voxtral.ws,
    ]
    if not any(all_connected):
        try:
            await browser.send_json({"error": "All connections failed. Check server console."})
        except Exception:
            pass
        await browser.close()
        return

    # ── Helpers ───────────────────────────────────────────────────────────────
    async def send_to_browser(msg: dict):
        try:
            await browser.send_json(msg)
        except Exception:
            pass

    async def audio_relay():
        """
        Receive raw PCM16 16kHz binary frames from the browser and fan out
        to all connected pipelines. OpenAI upsamples to 24kHz internally.
        Voxtral base64-encodes internally. Nova-3/Flux/AAI send raw binary.
        """
        try:
            while True:
                data = await browser.receive_bytes()
                relay_tasks = []
                if nova3.ws:              relay_tasks.append(nova3.send_audio(data))
                if flux.ws:               relay_tasks.append(flux.send_audio(data))
                if aai     and aai.ws:    relay_tasks.append(aai.send_audio(data))
                if oai     and oai.ws:    relay_tasks.append(oai.send_audio(data))
                if voxtral and voxtral.ws: relay_tasks.append(voxtral.send_audio(data))
                if relay_tasks:
                    await asyncio.gather(*relay_tasks, return_exceptions=True)
        except Exception:
            pass

    # ── Task list ─────────────────────────────────────────────────────────────
    tasks = [
        asyncio.create_task(audio_relay()),
        asyncio.create_task(nova3.receive_loop(send_to_browser)),
        asyncio.create_task(nova3.keepalive_loop()),
        asyncio.create_task(flux.receive_loop(send_to_browser)),
    ]
    if aai:     tasks.append(asyncio.create_task(aai.receive_loop(send_to_browser)))
    if oai:     tasks.append(asyncio.create_task(oai.receive_loop(send_to_browser)))
    if voxtral: tasks.append(asyncio.create_task(voxtral.receive_loop(send_to_browser)))

    try:
        await asyncio.gather(*tasks)
    except Exception:
        pass
    finally:
        for t in tasks:
            t.cancel()
        await nova3.close()
        await flux.close()
        if aai:     await aai.close()
        if oai:     await oai.close()
        if voxtral: await voxtral.close()
        try:
            await browser.close()
        except Exception:
            pass


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  STT 5-Way Comparison")
    print("  Nova-3 | Flux | AssemblyAI U3 Pro | OpenAI | Voxtral")
    print("-" * 60)
    try:
        k = get_api_key()
        print("  Deepgram key:    " + k[:8] + "*" * 12)
    except RuntimeError as e:
        print("  WARNING: " + str(e))
    try:
        k2 = get_assemblyai_key()
        print("  AssemblyAI key:  " + k2[:8] + "*" * 12)
    except RuntimeError as e:
        print("  WARNING: " + str(e))
    try:
        k3 = get_openai_key()
        print("  OpenAI key:      " + k3[:8] + "*" * 12)
    except RuntimeError as e:
        print("  WARNING: " + str(e))
    try:
        k4 = get_mistral_key()
        print("  Mistral key:     " + k4[:8] + "*" * 12)
    except RuntimeError as e:
        print("  WARNING: " + str(e))
    print("  Open: http://localhost:8765")
    print("=" * 60 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8765)