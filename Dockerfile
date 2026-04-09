FROM python:3.10-bullseye

# Direct .deb package se MegaCMD install karna (Bina GPG error ke)
RUN apt-get update && apt-get install -y wget \
    && wget https://mega.nz/linux/repo/Debian_11/amd64/megacmd-Debian_11_amd64.deb \
    && apt-get install -y ./megacmd-Debian_11_amd64.deb \
    && rm megacmd-Debian_11_amd64.deb \
    && rm -rf /var/lib/apt/lists/*

# Working directory set karna
WORKDIR /app

# Requirements copy aur install karna
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pura bot code copy karna
COPY . .

# Koyeb ke health check ke liye Port 8000 expose karna
EXPOSE 8000

# Bot run karne ki command
CMD ["python", "bot.py"]
