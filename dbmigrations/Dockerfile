FROM python:3.11-slim AS builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

COPY ../requirements-docker.txt .
COPY ../requirements-dev.txt .
RUN pip install --no-cache-dir -r ./requirements-docker.txt

FROM python:3.11-slim AS runner
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/opt/venv/bin:$PATH"

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv

RUN useradd --create-home appuser
USER appuser

COPY . .

ENTRYPOINT ["python", "/app/dbmigration.py"]
