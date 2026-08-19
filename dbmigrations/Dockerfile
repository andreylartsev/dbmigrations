
#
# builder step
#
FROM python:3.11-alpine AS builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

COPY ../requirements-docker.txt .
RUN pip install --no-cache-dir -r ./requirements-docker.txt

ENV PYTHONDONTWRITEBYTECODE=0
ENV PYTHONUNBUFFERED=0

RUN /opt/venv/bin/python -m compileall -q /opt/venv

COPY dbmigration.py .
COPY dbmigration.example.toml dbmigration.toml
COPY ./translations/ru/LC_MESSAGES/ ./translations/ru/LC_MESSAGES/

RUN /opt/venv/bin/python -m compileall -q /app

#
# runner step
#
FROM python:3.11-alpine AS runner
WORKDIR /app

#
# copy precompiled virtual environment and app
#
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app /app

#
# install git and psql / pg_restore tools
#
RUN apk add --no-cache git postgresql-client

RUN adduser -D appuser

USER appuser

#
# make save point for mounting git repository
#
RUN /usr/bin/git config --global --add safe.directory '/migrations'

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/opt/venv/bin/python", "/app/dbmigration.py"]