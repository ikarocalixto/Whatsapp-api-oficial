# Deploy no VPS com `inovetime.com/whats`

## 1. Variaveis no `.env`

Use algo nessa linha:

```env
WHATSAPP_PHONE_NUMBER_ID=...
WHATSAPP_ACCESS_TOKEN=...
WHATSAPP_BUSINESS_ACCOUNT_ID=...
WHATSAPP_WEBHOOK_VERIFY_TOKEN=seu_token_de_verificacao
WHATSAPP_APP_SECRET=seu_app_secret_meta
WHATSAPP_API_VERSION=v17.0
WHATSAPP_OFFICIAL_PORT=3010
WHATSAPP_OFFICIAL_BASE_PATH=/whats
WHATSAPP_MYSQL_HOST=127.0.0.1
WHATSAPP_MYSQL_PORT=3306
WHATSAPP_MYSQL_DATABASE=whatsapp_official
WHATSAPP_MYSQL_USER=whatsapp_user
WHATSAPP_MYSQL_PASSWORD=senha_forte
WHATSAPP_MYSQL_TABLE_PREFIX=wa_
```

## 2. Subir a aplicacao no servidor

Na raiz do projeto:

```bash
npm install
npm run start:whatsapp-official
```

Para manter rodando em producao, prefira `pm2`:

```bash
pm2 start whatsapp-cloud-official/server.js --name whatsapp-cloud-official
pm2 save
```

## 3. Nginx

Exemplo de bloco para publicar em `https://inovetime.com/whats`:

```nginx
location /whats/ {
    proxy_pass http://127.0.0.1:3010/whats/;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}

location = /whats {
    return 301 /whats/;
}
```

Depois:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## 4. URL para cadastrar na Meta

- Painel: `https://inovetime.com/whats/`
- Webhook: `https://inovetime.com/whats/webhook`

No painel da Meta, em Webhooks/WhatsApp:

- `Callback URL`: `https://inovetime.com/whats/webhook`
- `Verify token`: o mesmo valor de `WHATSAPP_WEBHOOK_VERIFY_TOKEN`

## 5. Teste final

1. Abra `https://inovetime.com/whats/`
2. Clique em `Sincronizar tudo`
3. Envie uma mensagem template
4. Veja a area `Rastreamento de entrega`

Se o webhook estiver configurado certo, a mensagem deve sair de `accepted` e depois virar `sent`, `delivered`, `read` ou `failed`.

## 6. Fila automatica na VPS

Com as variaveis de MySQL preenchidas, o `server.js` passa a:

- gravar jobs agendados em banco
- processar automaticamente a fila em background
- persistir status `sent`, `delivered`, `read` e `failed`
- expor estatisticas em `GET /whats/queue/stats`

Se quiser testar manualmente:

```bash
curl -X POST https://inovetime.com/whats/queue/process \
  -H "Content-Type: application/json" \
  -d "{\"limit\":10}"
```
