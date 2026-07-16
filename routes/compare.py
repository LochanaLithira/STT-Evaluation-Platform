"""
/ws/compare, /ws/compare3, /ws/compare5 — 2/3/5-way STT comparison endpoints.

All three share the same connect -> relay-audio -> receive -> teardown
lifecycle; _run_comparison() implements that once and each route just
decides which pipelines to instantiate before handing off to it.
"""
import asyncio

from fastapi import APIRouter, WebSocket

from config import get_api_key, get_assemblyai_key, get_openai_key, get_mistral_key
from pipelines import (
    Nova3Pipeline, FluxPipeline, AssemblyAIPipeline, OpenAIPipeline, VoxtralPipeline,
)

router = APIRouter()


async def _require_key(browser: WebSocket, getter):
    """Fetch a required key; on failure, notify the browser and close the socket."""
    try:
        return getter()
    except RuntimeError as e:
        await browser.send_json({"error": str(e)})
        await browser.close()
        return None


async def _optional_key(browser: WebSocket, getter, src: str):
    """Fetch an optional key; on failure, warn but let the comparison continue."""
    try:
        return getter()
    except RuntimeError as e:
        try:
            await browser.send_json({"source": src, "error": str(e)})
        except Exception:
            pass
        return None


async def _run_comparison(browser: WebSocket, nova3, flux, extras):
    """
    nova3, flux: required core pipelines (Nova-3 and Flux always run).
    extras: list of (pipeline, label, src) for the optional providers
            that had a usable API key.
    """
    connect_targets = [(nova3, "Nova-3", "nova3"), (flux, "Flux", "flux"), *extras]

    for pipeline, label, src in connect_targets:
        try:
            await pipeline.connect()
        except Exception as e:
            print("[" + label + "] FAILED: " + str(e))
            try:
                await browser.send_json({"source": src, "error": str(e)})
            except Exception:
                pass

    if not any(p.ws for p, _, _ in connect_targets):
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
        """
        Receive raw PCM16 16kHz binary frames from the browser and fan out
        to every connected pipeline. Providers that need a different format
        (e.g. OpenAI's 24kHz upsample, Voxtral's base64 JSON) convert
        internally in their own send_audio().
        """
        try:
            while True:
                data = await browser.receive_bytes()
                relay_tasks = [p.send_audio(data) for p, _, _ in connect_targets if p.ws]
                if relay_tasks:
                    await asyncio.gather(*relay_tasks, return_exceptions=True)
        except Exception:
            pass

    tasks = [asyncio.create_task(audio_relay())]
    tasks.append(asyncio.create_task(nova3.receive_loop(send_to_browser)))
    tasks.append(asyncio.create_task(nova3.keepalive_loop()))
    tasks.append(asyncio.create_task(flux.receive_loop(send_to_browser)))
    for pipeline, _, _ in extras:
        tasks.append(asyncio.create_task(pipeline.receive_loop(send_to_browser)))

    try:
        await asyncio.gather(*tasks)
    except Exception:
        pass
    finally:
        for t in tasks:
            t.cancel()
        for pipeline, _, _ in connect_targets:
            await pipeline.close()
        try:
            await browser.close()
        except Exception:
            pass


@router.websocket("/ws/compare")
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

    dg_key = await _require_key(browser, get_api_key)
    if dg_key is None:
        return

    nova3 = Nova3Pipeline(lang, dg_key)
    flux  = FluxPipeline(lang, dg_key)

    print("\n[Nova-3] " + nova3.url)
    print("[Flux  ] " + flux.url + "\n")

    await _run_comparison(browser, nova3, flux, [])


@router.websocket("/ws/compare3")
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

    dg_key = await _require_key(browser, get_api_key)
    if dg_key is None:
        return

    aai_key = await _optional_key(browser, get_assemblyai_key, "aai")

    nova3 = Nova3Pipeline(lang, dg_key)
    flux  = FluxPipeline(lang, dg_key)

    print("\n[Nova-3] " + nova3.url)
    print("[Flux  ] " + flux.url)

    extras = []
    if aai_key:
        aai = AssemblyAIPipeline(lang, aai_key)
        extras.append((aai, "AAI U3", "aai"))
        print("[AAI U3] " + aai.url)
    print()

    await _run_comparison(browser, nova3, flux, extras)


@router.websocket("/ws/compare5")
async def compare5_ws(browser: WebSocket, lang: str = "en"):
    """
    5-way comparison: Nova-3 + Flux + AssemblyAI U3 Pro + OpenAI gpt-4o-transcribe + Voxtral.
    Audio arrives from the browser resampled to 16 kHz Int16 PCM.
    OpenAI pipeline upsamples 16->24 kHz internally before forwarding.

    lang values:
      "en"           -> Nova-3 en | Flux EN model       | AAI default  | OpenAI language=en | Voxtral auto
      "fr"           -> Nova-3 fr | Flux multi+hint=fr  | AAI prompt   | OpenAI language=fr | Voxtral auto
      "fr-bilingual" -> Nova-3 fr | Flux multi+fr+en   | AAI bi-prompt | OpenAI auto-detect | Voxtral auto
    """
    await browser.accept()

    dg_key = await _require_key(browser, get_api_key)
    if dg_key is None:
        return

    aai_key     = await _optional_key(browser, get_assemblyai_key, "aai")
    oai_key     = await _optional_key(browser, get_openai_key, "oai")
    mistral_key = await _optional_key(browser, get_mistral_key, "voxtral")

    nova3 = Nova3Pipeline(lang, dg_key)
    flux  = FluxPipeline(lang, dg_key)

    print("\n[Nova-3 ] " + nova3.url)
    print("[Flux   ] " + flux.url)

    extras = []
    if aai_key:
        aai = AssemblyAIPipeline(lang, aai_key)
        extras.append((aai, "AAI U3", "aai"))
        print("[AAI U3 ] " + aai.url)
    if oai_key:
        oai = OpenAIPipeline(lang, oai_key)
        extras.append((oai, "OpenAI", "oai"))
        print("[OpenAI ] " + oai.url)
    if mistral_key:
        voxtral = VoxtralPipeline(lang, mistral_key)
        extras.append((voxtral, "Voxtral", "voxtral"))
        print("[Voxtral] " + voxtral.url)
    print()

    await _run_comparison(browser, nova3, flux, extras)
