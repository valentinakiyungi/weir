# ---- build: produce wheels for the app and all dependencies ----
FROM python:3.12-slim AS build
WORKDIR /src
RUN pip install --no-cache-dir --upgrade pip
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

# ---- runtime ----
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8000 \
    WORKERS=2
RUN useradd --system --uid 10001 --no-create-home weir
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels
WORKDIR /app
# mock provider + example tenants are tiny and handy for local stacks
COPY mock_upstream ./mock_upstream
COPY scripts ./scripts
COPY config ./config
USER weir
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request,os; urllib.request.urlopen(f'http://127.0.0.1:{os.environ[\"PORT\"]}/health', timeout=2)"
CMD ["sh", "-c", "exec uvicorn weir.main:app --host 0.0.0.0 --port ${PORT} --workers ${WORKERS} --no-access-log --backlog 4096 --timeout-keep-alive 75"]
