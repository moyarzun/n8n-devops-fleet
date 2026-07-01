# Base idéntica al agile-fleet: Node 22 Alpine con Python, Git, Ruby, Chromium
FROM node:22-alpine

RUN apk add --update --no-cache \
    python3 \
    py3-pip \
    build-base \
    python3-dev \
    libffi-dev \
    openssl-dev \
    curl \
    wget \
    git \
    ruby \
    rust \
    cargo \
    chromium \
    chromium-chromedriver \
    jq

# Vercel CLI + n8n
RUN npm install -g n8n@latest vercel@latest

ENV PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium-browser
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir --break-system-packages \
    -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

RUN mkdir -p /data/scripts /workspace /data/devops_store /home/node/.n8n \
    && chown -R node:node /data/scripts /workspace /data/devops_store /home/node/.n8n

USER node
WORKDIR /home/node

EXPOSE 5679

CMD ["n8n"]
