# WhatsHub Engine v2.1

Motor WhatsApp (Baileys) unificado com:
- ✅ Persistência de sessão via volume `/app/sessions`
- ✅ Reconexão inteligente (backoff exponencial)
- ✅ Validação do 9º dígito brasileiro (`onWhatsApp`)
- ✅ Download de mídia (`/media/:messageId`)
- ✅ Webhook completo (mensagens recebidas/enviadas)
- ✅ Métricas `/system/metrics`

## Variáveis de ambiente

```
PORT=3000
ADMIN_TOKEN=25896589Ba@23479612
SESSIONS_DIR=/app/sessions
NODE_OPTIONS=--max-old-space-size=256
```

## Volume obrigatório no EasyPanel

- Name: `sessions`
- Mount Path: `/app/sessions`

## Rotas principais

### Admin (header `X-Admin-Token`)
- `POST /instance/create` — cria nova instância
- `GET  /instance/list` — lista todas

### Instância (header `X-Instance-Token`)
- `POST /connect` — gera QR / conecta
- `POST /disconnect` — desconecta e limpa credenciais
- `GET  /status` — status + QR + último motivo de desconexão
- `POST /send` — envia mensagem (valida 9º dígito BR)
- `POST /webhook` — configura URL de webhook
- `GET  /media/:messageId` — baixa mídia de uma mensagem recebida

### Público
- `GET /health`
- `GET /system/metrics`
