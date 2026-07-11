# ===== Stage 1: build venv =====
FROM python:3.12-slim AS builder
ENV PYTHONDONTWRITEBYTECODE=1
RUN pip install --no-cache-dir uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY pipegate/ ./pipegate/
RUN uv sync --frozen --no-dev

# ===== Stage 2: runtime =====
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"
COPY --from=builder /app /app
RUN useradd --create-home appuser && chown -R appuser:appuser /app
WORKDIR /app
USER appuser
EXPOSE 8000
CMD ["uvicorn", "pipegate.server:create_app", "--factory", \
     "--host", "0.0.0.0", "--port", "8000"]
