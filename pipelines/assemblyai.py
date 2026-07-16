import json, time
import websockets

from .base import Pipeline

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
class AssemblyAIPipeline(Pipeline):
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
