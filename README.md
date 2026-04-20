# WhatsHub Engine v2.0 — Motor Baileys resiliente

Substitua o conteúdo do seu repositório da VPS por estes arquivos.

## Por que a v2 resolve as desconexões

| Problema anterior | Solução v2 |
|---|---|
| Sessões perdidas ao reiniciar container | `VOLUME /app/sessions` + `useMultiFileAuthState` |
| WS cortado por proxy após ~2min | `keepAliveIntervalMs: 30_000` |
| Reconnect só em alguns códigos | `decideReconnect()` trata TODOS `DisconnectReason` |
| Não reconectava após crash do processo | `restart: always` + `recoverPersistedSessions()` no boot |
| Não sabia o motivo da queda | `lastDisconnectReason` salvo e retornado pelo `/status` |
| Sem visibilidade de CPU/RAM | Endpoint `/system/metrics` já compatível com o painel |
| Stream conflict reconectava em loop | Detectado (`connectionReplaced 440`) e mantém offline |
| Sessão deslogada tentava reconectar | Detectado (`loggedOut 401`), limpa creds e espera novo QR |

## Variáveis de ambiente obrigatórias

- `WHATSAPI_ADMIN_TOKEN` — mesmo valor configurado no Supabase
- `PORT` — 3000 (padrão)
- `SESSIONS_DIR` — `/app/sessions` (padrão, NÃO altere dentro do container)

## Deploy no EasyPanel

1. Crie/edite o serviço apontando para este repositório.
2. Em **Mounts**, adicione: `Volume` → `whatshub_sessions` → `/app/sessions`.
3. Em **Environment**, adicione `WHATSAPI_ADMIN_TOKEN`.
4. Em **Resources**, defina memória mínima de 2 GB (ideal 8–12 GB conforme quantidade de instâncias).
5. Habilite **Restart Policy: Always**.
6. Faça o deploy.

## Endpoints (usados pelo Supabase Edge Functions)

| Método | Path | Uso |
|---|---|---|
| GET | `/health` | Liveness (sem auth) |
| GET | `/system/metrics` | Métricas reais CPU/RAM (painel WhatsHub) |
| GET | `/instances` | Lista instâncias ativas |
| POST | `/instances` | Cria/inicia (body: `{id, token}`) |
| GET | `/instances/:id/status` | Status + motivo da última queda |
| GET | `/instances/:id/qr` | QR Code (dataURL) |
| POST | `/instances/:id/reconnect` | Força reconexão |
| POST | `/instances/:id/disconnect` | Desconecta sem apagar sessão |
| DELETE | `/instances/:id` | Desloga e apaga sessão do disco |
| POST | `/instances/:id/webhook` | Define URL de webhook (body: `{url}`) |
| POST | `/instances/:id/send` | Envia texto (body: `{phone, message}`) |

Todas exigem header `Authorization: Bearer <WHATSAPI_ADMIN_TOKEN>`.

## Checklist pós-deploy

- [ ] `curl https://sua-vps/health` retorna `{ok:true}`
- [ ] `docker exec whatshub-engine ls /app/sessions` mostra a pasta mapeada
- [ ] Após reiniciar o container, instâncias retornam para `connected` sem novo QR
- [ ] Painel WhatsHub mostra "Dados Reais" (não "Estimativas")
- [ ] Log mostra motivo ao desconectar (ex: `⚠️ desconectado (connectionReplaced)`)
