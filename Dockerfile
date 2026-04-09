FROM python:3.10-bullseye

# MegaCMD install karna
RUN apt-get update && apt-get install -y wget \
    && wget https://mega.nz/linux/repo/Debian_11/amd64/megacmd-Debian_11_amd64.deb \
    && apt-get install -y ./megacmd-Debian_11_amd64.deb \
    && rm megacmd-Debian_11_amd64.deb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Koyeb health check port
EXPOSE 8000

CMD ["python", "bot.py"]
