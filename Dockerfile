# Etapa base: Rocky Linux 9.3
FROM rockylinux:9.3

# Variables de entorno para no generar pyc ni buffer
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Forzar la ruta global para encontrar gunicorn y pip
ENV PATH="/usr/local/bin:${PATH}"

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

# Instalar dependencias globales con el pip correcto de Python 3.13
RUN python3.13 -m ensurepip && \
    python3.13 -m pip install --upgrade pip && \
    python3.13 -m pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY src /app/src
COPY index.py /app
COPY config.py /app

# Asegurar que el servicio encuentre el paquete src
ENV PYTHONPATH=/app/src

# Exponer puerto
EXPOSE 4001

# Volvemos al ejecutable nativo apuntando a la ruta del PATH corregida
CMD ["gunicorn", "--bind", "0.0.0.0:4001", "index:app", "--workers", "4", "--timeout", "120"]