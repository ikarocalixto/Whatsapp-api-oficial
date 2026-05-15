# WhatsApp Cloud API Oficial

Servidor isolado para usar a API oficial da Meta com:

- painel visual de testes em `/`
- verificacao de webhook (`GET /webhook`)
- recebimento de eventos (`POST /webhook`)
- envio de texto (`POST /send-text`)
- envio de template aprovado (`POST /send-template`)
- submissao de template para aprovacao (`POST /submit-template`)
- consulta de templates (`GET /message-templates`)
- consulta de logs (`GET /logs`)

## Variaveis usadas do `.env`

Ja reaproveita as variaveis existentes na raiz:

- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_ACCESS_TOKEN`

Variaveis recomendadas para adicionar no `.env`:

```env
WHATSAPP_BUSINESS_ACCOUNT_ID=seu_waba_id
WHATSAPP_WEBHOOK_VERIFY_TOKEN=seu_token_de_verificacao
WHATSAPP_APP_SECRET=seu_app_secret_meta
WHATSAPP_API_VERSION=v17.0
WHATSAPP_OFFICIAL_PORT=3010
WHATSAPP_OFFICIAL_BASE_PATH=/whats
```

## Como rodar

```bash
node whatsapp-cloud-official/server.js
```

Ou pela raiz:

```bash
npm run start:whatsapp-official
```

Depois abra:

```text
http://localhost:3010
```

## Endpoints

### Painel visual

`GET /`

Tela HTML para:

- testar health
- enviar texto
- enviar template aprovado com seletor automatico
- submeter template para aprovacao
- listar templates e status
- acompanhar logs do servidor e webhook

### Health

`GET /health`

Retorna se a configuracao minima foi encontrada.

### Verificacao do webhook

`GET /webhook`

Use esta URL no painel da Meta. O token deve ser igual ao `WHATSAPP_WEBHOOK_VERIFY_TOKEN`.

### Recebimento de eventos

`POST /webhook`

Recebe mensagens e status enviados pela Meta e grava no console.

### Enviar texto

`POST /send-text`

Body:

```json
{
  "to": "5511999999999",
  "body": "Ola! Mensagem enviada pela Cloud API oficial."
}
```

### Enviar template aprovado

`POST /send-template`

Body:

```json
{
  "to": "5511999999999",
  "templateName": "nome_do_template_aprovado",
  "languageCode": "pt_BR",
  "bodyParameters": ["Icaro", "Pedido 123"]
}
```

### Submeter template para aprovacao

`POST /submit-template`

Body:

```json
{
  "wabaId": "123456789012345",
  "name": "boas_vindas_cliente",
  "category": "UTILITY",
  "language": "pt_BR",
  "bodyText": "Ola {{1}}, seu pedido {{2}} foi confirmado.",
  "bodyExamples": ["Icaro", "Pedido 123"],
  "footerText": "Equipe Inove"
}
```

### Consultar templates

`GET /message-templates`

Opcionalmente:

```text
/message-templates?wabaId=123456789012345
```

## Observacoes

- Para iniciar conversa fora da janela de 24 horas, use template aprovado.
- O token em `WHATSAPP_ACCESS_TOKEN` precisa ser um token valido para a Cloud API.
- Se `WHATSAPP_APP_SECRET` estiver definido, o servidor valida a assinatura `X-Hub-Signature-256`.
- Para publicar em subpasta no VPS, configure `WHATSAPP_OFFICIAL_BASE_PATH=/whats`.
- Exemplo de callback URL na Meta: `https://inovetime.com/whats/webhook`
