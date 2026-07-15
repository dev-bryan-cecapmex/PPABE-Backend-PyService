FROM rockylinux:9.3

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    FLASK_ENV=production

WORKDIR /app

RUN dnf -y update && \
    dnf -y install python3 python3-pip python3-devel gcc make openssl-devel bzip2-devel libffi-devel zlib-devel wget tar findutils && \
    dnf clean all && \
    rm -rf /var/cache/dnf /tmp/*

RUN python3 -m ensurepip --upgrade && \
    python3 -m pip install --upgrade pip setuptools wheel

COPY requirements.txt ./

RUN python3 -m pip install --no-cache-dir -r requirements.txt gunicorn==23.0.0

COPY . ./

EXPOSE 4001

CMD ["gunicorn", "--bind", "0.0.0.0:4001", "index:app", "--workers", "2", "--timeout", "120"]