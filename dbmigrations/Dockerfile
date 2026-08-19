FROM python:3.11-alpine AS builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

COPY ../requirements-docker.txt .
RUN pip install --no-cache-dir -r ./requirements-docker.txt

FROM python:3.11-alpine AS runner
WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

RUN /opt/venv/bin/python -m compileall -q /opt/venv

RUN apk add --no-cache git

COPY dbmigration.py .
COPY dbmigration.toml .
COPY ./translations/ru/LC_MESSAGES/ ./translations/ru/LC_MESSAGES/

RUN /opt/venv/bin/python -m compileall -q /app

RUN adduser -D appuser

USER appuser

RUN /usr/bin/git config --global --add safe.directory '/migrations'

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/opt/venv/bin/python", "/app/dbmigration.py"]
