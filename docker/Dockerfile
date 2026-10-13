FROM apache/airflow:2.7.1

USER root
# 1. Rutas históricas de Debian e instalación de Java
RUN echo "deb http://archive.debian.org/debian/ bullseye main" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian-security bullseye-security main" >> /etc/apt/sources.list && \
    rm -f /etc/apt/sources.list.d/pgdg.list && \
    apt-get update -o Acquire::Check-Valid-Until=false && \
    apt-get install -y default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. HACK MAESTRO: Extraemos el motor 'uv' directamente de su imagen oficial (Cero uso de pip)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# 3. Instalamos las dependencias con permisos de sistema usando uv directamente
COPY requirements.txt /tmp/requirements.txt
RUN uv pip install --system --no-cache -r /tmp/requirements.txt

# 4. Volvemos al usuario seguro de Airflow
USER airflow
