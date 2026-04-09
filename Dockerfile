FROM python:3.10-bullseye

# Mega.nz ka official Debian repository add karna aur MegaCMD install karna
RUN apt-get update && apt-get install -y wget curl gnupg2 \
    && curl -fsSL https://mega.nz/keys/MEGA_GPG_KEY.asc | gpg --dearmor -o /usr/share/keyrings/mega-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/mega-archive-keyring.gpg] https://mega.nz/linux/repo/Debian_11/ ./" > /etc/apt/sources.list.d/megacmd.list \
    && apt-get update \
    && apt-get install -y megacmd \
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
