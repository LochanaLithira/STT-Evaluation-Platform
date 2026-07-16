# STT-Evaluation-Platform

Live comparison tool for streaming speech-to-text providers: Deepgram Nova-3, Deepgram Flux, AssemblyAI Universal-3 Pro, OpenAI Realtime transcription, and Mistral Voxtral.

## Structure

- `app.py` — FastAPI entrypoint (routes + static mount + `uvicorn.run`)
- `config.py` — API key loading from `.env`
- `pipelines/` — one module per STT provider, sharing the `Pipeline` interface in `base.py`
- `routes/compare.py` — the `/ws/compare`, `/ws/compare3`, `/ws/compare5` WebSocket endpoints
- `static/` — frontend: `index.html`, `style.css`, `app.js`

## Setup

Add your API keys to a `.env` file next to `app.py`:

```
DEEPGRAM_API_KEY=your_deepgram_key
ASSEMBLYAI_API_KEY=your_assemblyai_key
OPENAI_API_KEY=your_openai_key
MISTRAL_API_KEY=your_mistral_key
```

Only `DEEPGRAM_API_KEY` is required; the others are optional and the corresponding panel is skipped if missing.

## Run

```
pip install -r requirements.txt
python app.py
```

Open http://localhost:8765