# Etapa base: Rocky Linux 9.3
FROM rockylinux:9.3

# Variables de entorno para no generar pyc ni buffer
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Actualizar sistema e instalar dependencias de compilación
RUN dnf -y update && \
    dnf -y install gcc make openssl-devel bzip2-devel libffi-devel zlib-devel wget tar && \
    dnf clean all && \
    rm -rf /var/cache/dnf /tmp/*

# Instalar Python 3.13.7 desde fuente
WORKDIR /opt
RUN wget https://www.python.org/ftp/python/3.13.7/Python-3.13.7.tgz && \
    tar -xzf Python-3.13.7.tgz && \
    cd Python-3.13.7 && \
    ./configure --enable-optimizations && \
    make altinstall && \
    rm -rf /opt/Python-3.13.7*

# Crear directorio de la app
WORKDIR /app

# Copiar requirements y dependencias
COPY requirements.txt .

# Instalar dependencias de Python (incluye polars-lts-cpu compatible)
RUN python3.13 -m ensurepip && \
    python3.13 -m pip install --upgrade pip && \
    pip3.13 install --no-cache-dir -r requirements.txt && \
    pip3.13 install --no-cache-dir polars-lts-cpu

# Copiar código fuente
COPY src /app/src
COPY index.py /app
COPY config.py /app

# Variables de entorno para Flask
ENV FLASK_APP=index.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=4001
ENV FLASK_ENV=development
ENV PYTHONPATH=/app/src

# Exponer puerto
EXPOSE 4001

# Comando final
CMD ["flask", "run"]
