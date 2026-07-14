FROM rockylinux:9.3

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app \
    PATH=/usr/local/bin:${PATH}

WORKDIR /app

RUN dnf -y update && \
    dnf -y install gcc make openssl-devel bzip2-devel libffi-devel zlib-devel wget tar findutils && \
    dnf clean all && \
    rm -rf /var/cache/dnf /tmp/*

WORKDIR /opt
RUN wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz && \
    tar -xzf Python-3.11.9.tgz && \
    cd Python-3.11.9 && \
    ./configure --enable-optimizations --prefix=/usr/local && \
    make -j"$(nproc)" altinstall && \
    rm -rf /opt/Python-3.11.9*

RUN ln -sf /usr/local/bin/python3.11 /usr/local/bin/python3 && \
    ln -sf /usr/local/bin/pip3.11 /usr/local/bin/pip3 && \
    python3 -m ensurepip --upgrade && \
    python3 -m pip install --upgrade pip

WORKDIR /app
COPY requirements.txt ./

RUN python3 -m pip install --no-cache-dir -r requirements.txt gunicorn==23.0.0

COPY . ./

EXPOSE 4001

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:4001/healthz', timeout=3)" || exit 1

CMD ["gunicorn", "--bind", "0.0.0.0:4001", "index:app", "--workers", "2", "--timeout", "120"]