FROM rockylinux:9.3

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN dnf -y update && \
    dnf -y install python3 python3-pip python3-devel gcc make openssl-devel bzip2-devel libffi-devel zlib-devel wget tar && \
    dnf clean all && \
    rm -rf /var/cache/dnf /tmp/*

COPY requirements.txt ./

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir -r requirements.txt gunicorn==23.0.0

COPY . ./

EXPOSE 4001

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:4001/healthz', timeout=3)" || exit 1

CMD ["gunicorn", "--bind", "0.0.0.0:4001", "index:app", "--workers", "2", "--timeout", "120"]