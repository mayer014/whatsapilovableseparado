# ─────────────────────────────────────────────
#  WhatsHub Engine v2 — Dockerfile de produção
# ─────────────────────────────────────────────
FROM node:20-alpine

# ffmpeg/libc para Baileys (mídia) + tini (signal handling)
RUN apk add --no-cache ffmpeg tini

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund

COPY index.js ./

# Pasta de sessões deve ser mapeada como VOLUME no EasyPanel
RUN mkdir -p /app/sessions
VOLUME ["/app/sessions"]

ENV NODE_ENV=production
ENV SESSIONS_DIR=/app/sessions
ENV PORT=3000

EXPOSE 3000

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "index.js"]
