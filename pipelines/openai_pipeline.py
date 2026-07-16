import base64, json, struct, time
import websockets

from .base import Pipeline


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
class OpenAIPipeline(Pipeline):
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
