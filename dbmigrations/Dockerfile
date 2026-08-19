FROM python:3.11-alpine AS builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

COPY ../requirements-docker.txt .
COPY ../requirements-dev.txt .
RUN pip install --no-cache-dir -r ./requirements-docker.txt

FROM python:3.11-alpine AS runner
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/opt/venv/bin:$PATH"

RUN apk add --no-cache git

COPY --from=builder /opt/venv /opt/venv

RUN adduser -D appuser
USER appuser

RUN git config --global --add safe.directory '*'

COPY . .

ENTRYPOINT ["python", "/app/dbmigration.py"]
