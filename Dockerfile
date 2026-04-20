FROM node:20-alpine

WORKDIR /app

# Instala git (necessário para algumas dependências do baileys)
RUN apk add --no-cache git

# Instala dependências primeiro (cache de layers)
COPY package.json ./
RUN npm install --omit=dev

# Copia código
COPY index.js ./

# Cria pasta de sessões (volume será montado aqui)
RUN mkdir -p /app/sessions

ENV NODE_OPTIONS="--max-old-space-size=256"
ENV SESSIONS_DIR=/app/sessions
ENV PORT=3000

EXPOSE 3000

CMD ["node", "index.js"]
