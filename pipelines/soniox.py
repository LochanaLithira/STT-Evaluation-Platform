"""Soniox STT RT v5 — wss://stt-rt.soniox.com/transcribe-websocket"""
import json
import time

import websockets

from .base import Pipeline


class SonioxPipeline(Pipeline):
    def __init__(self, lang: str, api_key: str):
        self.url     = "wss://stt-rt.soniox.com/transcribe-websocket"
        self.api_key = api_key
        self.lang    = lang
        self.ws      = None
        self._t_start = None

    async def connect(self):
        self.ws = await websockets.connect(
            self.url, open_timeout=15, ping_interval=20, ping_timeout=30,
        )
        print("[Soniox] Connected OK")

        if self.lang == "fr":
            hints = ["fr"]
        elif self.lang == "fr-bilingual":
            hints = ["fr", "en"]
        else:
            hints = ["en"]

        await self.ws.send(json.dumps({
            "api_key": self.api_key,
            "model": "stt-rt-v5",
            "audio_format": "s16le",
            "sample_rate": 16000,
            "num_channels": 1,
            "language_hints": hints,
            "enable_endpoint_detection": True,
        }))
        print(f"[Soniox] Config sent (hints={hints})")

    async def send_audio(self, data: bytes):
        if self.ws:
            self._t_start = time.monotonic()
            await self.ws.send(data)

    async def receive_loop(self, send_fn):
        try:
            async for raw in self.ws:
                if isinstance(raw, bytes):
                    continue
                msg = json.loads(raw)

                if msg.get("finished"):
                    print("[Soniox] Session finished")
                    break

                if msg.get("error_code"):
                    print(f"[Soniox] Error: {msg.get('error_message')}")
                    break

                tokens = msg.get("tokens", [])
                if not tokens:
                    continue

                latency_ms = round((time.monotonic() - self._t_start) * 1000) if self._t_start else None

                final_text = ""
                interim_text = ""
                for tok in tokens:
                    if tok.get("is_final"):
                        final_text += tok.get("text", "")
                    else:
                        interim_text += tok.get("text", "")

                if final_text.strip():
                    await send_fn({
                        "source": "soniox",
                        "transcript": final_text.strip(),
                        "is_final": True,
                        "latency_ms": latency_ms,
                    })
                    self._t_start = None
                elif interim_text.strip():
                    await send_fn({
                        "source": "soniox",
                        "transcript": interim_text.strip(),
                        "is_final": False,
                        "latency_ms": latency_ms,
                    })
        except Exception as e:
            print(f"[Soniox] receive_loop error: {e}")

    async def close(self):
        if self.ws:
            try:
                await self.ws.send(b"")
                await self.ws.close()
            except Exception:
                pass
