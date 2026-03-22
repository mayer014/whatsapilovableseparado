# WhatsApp Engine

Motor de conexão WhatsApp usando Baileys para deploy em VPS com EasyPanel.

## Deploy no EasyPanel (via GitHub)

1. **Crie um repositório no GitHub** e faça push deste código
2. **No EasyPanel**, crie um novo serviço → GitHub → selecione o repositório
3. **Configure as variáveis de ambiente**:
   - `PORT` = `3000`
   - `ADMIN_TOKEN` = gere um token seguro (ex: `openssl rand -hex 32`)
4. **Configure volume persistente**: Monte `/app/sessions` para manter as sessões
5. **Deploy!**

## Variáveis de Ambiente

| Variável | Descrição | Obrigatório |
|----------|-----------|-------------|
| `PORT` | Porta do servidor (padrão: 3000) | Não |
| `ADMIN_TOKEN` | Token de admin para criar instâncias | Sim |

## Configuração no Lovable

Após o deploy, configure no Lovable Cloud:

1. **WHATSAPI_URL**: URL da sua VPS (ex: `https://whatsapp.seudominio.com`)
2. **WHATSAPI_ADMIN_TOKEN**: O mesmo ADMIN_TOKEN configurado aqui

## Endpoints da API

### Admin (requer header `admintoken`)
- `POST /instance/init` - Criar nova instância
- `GET /instances` - Listar instâncias
- `DELETE /instance/:id` - Remover instância

### Instância (requer header `token`)
- `POST /instance/connect` - Conectar (gerar QR)
- `GET /instance/status` - Status da conexão
- `POST /instance/disconnect` - Desconectar
- `POST /message/send` - Enviar mensagem
- `POST /sender/simple` - Envio em massa
- `POST /sender/edit` - Controlar campanha (pause/resume/delete)
- `GET /sender/status/:folderId` - Status da campanha

### Health Check
- `GET /health` - Status do servidor
