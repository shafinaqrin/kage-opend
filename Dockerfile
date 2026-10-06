# syntax=docker/dockerfile:1

FROM python:3.12-slim AS runtime

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# The moomoo SDK writes rotating logs next to the container's HOME. Keep them in
# a writable path rather than failing at import time on a read-only HOME.
ENV HOME=/tmp/kage-opend-home
RUN mkdir -p "$HOME"

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=10s --retries=5 --start-period=20s \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=8).status == 200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
