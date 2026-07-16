"""Gladia Solaria — v2 live API (POST init + WebSocket streaming)."""
import json
import time

import httpx
import websockets

from .base import Pipeline


class GladiaPipeline(Pipeline):
    def __init__(self, lang: str, api_key: str):
        self.api_key  = api_key
        self.lang     = lang
        self.ws       = None
        self._t_start = None
        self._ws_url  = None

    async def connect(self):
        if self.lang == "fr":
            lang_cfg = {"languages": ["fr"], "code_switching": False}
        elif self.lang == "fr-bilingual":
            lang_cfg = {"languages": ["fr", "en"], "code_switching": True}
        else:
            lang_cfg = {"languages": ["en"], "code_switching": False}

        body = {
            "encoding": "wav/pcm",
            "bit_depth": 16,
            "sample_rate": 16000,
            "channels": 1,
            "model": "solaria-1",
            "language_config": lang_cfg,
            "endpointing": 0.3,
            "maximum_duration_without_endpointing": 10,
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.gladia.io/v2/live",
                json=body,
                headers={
                    "x-gladia-key": self.api_key,
                    "Content-Type": "application/json",
                },
                timeout=15,
            )
            if resp.status_code not in (200, 201):
                raise RuntimeError(f"Gladia init failed ({resp.status_code}): {resp.text}")
            data = resp.json()
            self._ws_url = data["url"]

        self.ws = await websockets.connect(
            self._ws_url, open_timeout=15, ping_interval=20, ping_timeout=30,
        )
        print("[Gladia] Connected OK (session URL obtained)")

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
                msg_type = msg.get("type", "")

                if msg_type == "transcript":
                    d = msg.get("data", {})
                    is_final = d.get("is_final", False)
                    utt = d.get("utterance", {})
                    text = utt.get("text", "").strip()
                    latency_ms = round((time.monotonic() - self._t_start) * 1000) if self._t_start else None

                    if text:
                        await send_fn({
                            "source": "gladia",
                            "transcript": text,
                            "is_final": is_final,
                            "latency_ms": latency_ms,
                        })
                        if is_final:
                            self._t_start = None

                elif msg_type == "error":
                    print(f"[Gladia] Error: {msg}")
                    break

        except Exception as e:
            print(f"[Gladia] receive_loop error: {e}")

    async def close(self):
        if self.ws:
            try:
                await self.ws.send(json.dumps({"type": "stop_recording"}))
                await self.ws.close()
            except Exception:
                pass
