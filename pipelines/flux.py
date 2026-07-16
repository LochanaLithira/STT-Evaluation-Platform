import json, time
import websockets

from .base import Pipeline

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
class FluxPipeline(Pipeline):
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
