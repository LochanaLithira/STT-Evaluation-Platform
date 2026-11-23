# STT-Evaluation-Platform

Head-to-head test of two AssemblyAI realtime speech-to-text models, for accuracy and latency. Your mic audio is streamed to both at the same time, with the same settings:

| Panel | Model | `speech_model` |
|---|---|---|
| Left | Universal-3.5 Pro Realtime | `universal-3-5-pro` |
| Right | AssemblyAI-STT-3.7-Realtime-Agent | `assemblyai-stt-3-7-realtime-agent` |

Both connect to `wss://streaming.assemblyai.com/v3/ws` with 16 kHz `pcm_s16le` audio.

## Running a test

1. **Language.** This is the only toggle. It sets `language_codes` for both models: English `["en"]`, French `["fr"]`, French + English bilingual `["fr","en"]`, or Auto (not sent). This biases toward those languages but still allows code-switching.
2. **Reference transcript (optional).** Paste exactly what you will say. When the run ends, each panel shows a word error rate (WER) and a word-by-word diff: red is a wrong word (hover for the expected one), struck-through grey is a missed word, and amber italic is an extra word. Case and punctuation are ignored. Numbers written as digits in one and as words in the other count as errors. Editing the reference after a run re-scores it.
3. **Start / Stop.** Speak into the mic. On Stop, the server sends `Terminate` to both models, so each finalizes the sentence in progress. The run ends once both have answered.

Fixed settings, the same for both models:

- **Mode `balanced`**: AssemblyAI's recommended turn detection, which balances speed and sentence accuracy.
- **Voice focus off**: background-speaker suppression can also drop quiet or distant speech, so it stays off.
- **Raw mic**: browser noise suppression and auto-gain are off, because they can strip detail the models use. Echo cancellation stays on.

To try other values, the endpoint still accepts `mode` and `voice_focus` query parameters (see `routes/compare.py`).

Each panel shows the model, mode and voice focus the server actually applied, taken from the `Begin` message. It turns amber if anything differs from what was requested.

Each panel also shows latency stats (Last / P50 / P90 / Max) for its final transcripts. Each one is the time from when the audio of a sentence's last word left this server to when that sentence's final transcript arrived, using the word timestamps in each `Turn` message. Browser-to-server transit is not included. Latency varies from run to run, so compare several runs rather than a single sentence.

## Structure

- `app.py`: FastAPI entry point (routes, static mount, `uvicorn.run`)
- `config.py`: loads the API key from `.env`
- `pipelines/base.py`: the shared `Pipeline` interface and the audio clock used for latency
- `pipelines/assemblyai.py`: the AssemblyAI v3 streaming client, parameterized by `speech_model`
- `routes/compare.py`: the `/ws/compare?lang=&mode=&voice_focus=` WebSocket endpoint and the list of models under test. The browser sends binary audio, then `{"type":"stop"}`; the server replies `{"type":"done"}` once both models have finalized.
- `static/`: the frontend (`index.html`, `style.css`, `app.js`). WER scoring runs in `app.js`.

To change which models are compared, edit `MODELS` in `routes/compare.py` and the panel markup in `static/index.html`.

## Setup

Add your API key to a `.env` file next to `app.py`:

```
ASSEMBLYAI_API_KEY=your_assemblyai_key
```

## Run

```
pip install -r requirements.txt
python app.py
```

Open http://localhost:8765. Browsers only allow mic access on `localhost` or HTTPS.

To share it, run a Cloudflare quick tunnel in a second terminal. It prints a public HTTPS link, and anyone with that link uses your AssemblyAI key:

```
cloudflared tunnel --url http://localhost:8765
```
