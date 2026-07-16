import asyncio, json, time
import websockets

from .base import Pipeline

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
class Nova3Pipeline(Pipeline):
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
