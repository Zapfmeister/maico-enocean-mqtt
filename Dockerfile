ARG BUILD_FROM=python:3.13-slim

FROM ${BUILD_FROM}

# Install Python if using Alpine-based HA base image
RUN if command -v apk > /dev/null 2>&1; then \
        apk add --no-cache python3 py3-pip bash; \
    fi

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt 2>/dev/null \
    || pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY templates/ ./templates/
COPY run.sh ./run.sh
RUN chmod +x ./run.sh

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python3 -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8080/healthz', timeout=4).status == 200 else 1)"

ENTRYPOINT ["./run.sh"]
