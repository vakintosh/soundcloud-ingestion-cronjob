FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen

COPY main.py stateManager.py ./

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "main.py"]
