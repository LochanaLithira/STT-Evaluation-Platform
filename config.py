"""API key loading from environment / .env."""
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass


def get_api_key():
    key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "DEEPGRAM_API_KEY not set. Add it to the .env file next to app.py"
        )
    return key


def get_assemblyai_key():
    key = os.environ.get("ASSEMBLYAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "ASSEMBLYAI_API_KEY not set. Add it to the .env file next to app.py"
        )
    return key


def get_openai_key():
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "OPENAI_API_KEY not set. Add it to the .env file next to app.py"
        )
    return key


def get_mistral_key():
    key = os.environ.get("MISTRAL_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "MISTRAL_API_KEY not set. Add it to the .env file next to app.py"
        )
    return key
