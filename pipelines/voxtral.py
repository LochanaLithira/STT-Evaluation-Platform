import asyncio, base64, json, time
import websockets

from .base import Pipeline

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
class VoxtralPipeline(Pipeline):
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
