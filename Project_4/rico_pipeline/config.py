"""Central configuration. Reads env with lab defaults; defines model versions."""
import os

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://rico:rico@postgres:5432/rico")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "rico-raw")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# Model-version identifiers persisted into pipeline_runs each run.
CLIP_ARCH = "ViT-B-32"
CLIP_PRETRAINED = "laion2b_s34b_b79k"
CLIP_MODEL_VERSION = f"open-clip-{CLIP_ARCH}-{CLIP_PRETRAINED.replace('_', '-')}"
CLIP_MODEL_NAME = "open-clip"
SBERT_MODEL_VERSION = "sentence-transformers/all-MiniLM-L6-v2"
SBERT_MODEL_NAME = "sentence-transformers"
PROMPT_VERSION = "v1"

DATASET = "rootsautomation/RICO-Screen2Words"
DEFAULT_LIMIT = 5
