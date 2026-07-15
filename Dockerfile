FROM rockylinux:9.3

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    FLASK_ENV=production

WORKDIR /app

RUN dnf -y update && \
    dnf -y install gcc make openssl-devel bzip2-devel libffi-devel zlib-devel wget tar findutils && \
    dnf clean all && \
    rm -rf /var/cache/dnf /tmp/*

WORKDIR /opt
RUN wget https://www.python.org/ftp/python/3.13.7/Python-3.13.7.tgz && \
    tar -xzf Python-3.13.7.tgz && \
    cd Python-3.13.7 && \
    ./configure --enable-optimizations --prefix=/usr/local && \
    make -j"$(nproc)" altinstall && \
    rm -rf /opt/Python-3.13.7*

RUN ln -sf /usr/local/bin/python3.13 /usr/local/bin/python3 && \
    ln -sf /usr/local/bin/pip3.13 /usr/local/bin/pip3 && \
    python3 -m ensurepip --upgrade && \
    python3 -m pip install --upgrade pip setuptools wheel

WORKDIR /app
COPY requirements.txt ./

RUN python3 -m pip install --no-cache-dir -r requirements.txt gunicorn==23.0.0

COPY . ./

EXPOSE 4001

CMD ["gunicorn", "--bind", "0.0.0.0:4001", "index:app", "--workers", "2", "--timeout", "120"]