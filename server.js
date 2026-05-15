const crypto = require("crypto");
const express = require("express");
const axios = require("axios");
const cors = require("cors");
const dotenv = require("dotenv");
const path = require("path");

dotenv.config({ path: path.resolve(__dirname, "../.env") });

const app = express();
const port = process.env.WHATSAPP_OFFICIAL_PORT || 3010;
const apiVersion = process.env.WHATSAPP_API_VERSION || "v17.0";
const baseUrl = `https://graph.facebook.com/${apiVersion}`;
const appBasePath = normalizeBasePath(process.env.WHATSAPP_OFFICIAL_BASE_PATH || "/");

const phoneNumberId = process.env.WHATSAPP_PHONE_NUMBER_ID;
const accessToken = process.env.WHATSAPP_ACCESS_TOKEN;
const verifyToken = process.env.WHATSAPP_WEBHOOK_VERIFY_TOKEN || "troque-esse-token";
const appSecret = process.env.WHATSAPP_APP_SECRET;
const businessAccountId = process.env.WHATSAPP_BUSINESS_ACCOUNT_ID;
const logs = [];
const messageTracker = [];

const router = express.Router();

app.use(cors());
app.use(
  express.json({
    verify: (req, _res, buf) => {
      req.rawBody = buf;
    },
  })
);

router.use(express.static(path.resolve(__dirname, "public")));

function normalizeBasePath(value) {
  if (!value || value === "/") return "/";

  const cleaned = `/${String(value).trim().replace(/^\/+|\/+$/g, "")}`;
  return cleaned === "/" ? "/" : cleaned;
}

function buildLocalUrl(routePath = "") {
  const normalizedRoute = routePath.startsWith("/") ? routePath : `/${routePath}`;
  return appBasePath === "/" ? `http://localhost:${port}${normalizedRoute}` : `http://localhost:${port}${appBasePath}${normalizedRoute}`;
}

function addLog(type, message, details = null) {
  logs.unshift({
    id: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
    type,
    message,
    details,
    timestamp: new Date().toISOString(),
  });

  if (logs.length > 200) {
    logs.length = 200;
  }
}

function upsertTrackedMessage(entry) {
  if (!entry?.id) return;

  const existingIndex = messageTracker.findIndex((item) => item.id === entry.id);
  const current = existingIndex >= 0 ? messageTracker[existingIndex] : {};
  const next = {
    ...current,
    ...entry,
    updatedAt: new Date().toISOString(),
  };

  if (existingIndex >= 0) {
    messageTracker.splice(existingIndex, 1);
  }

  messageTracker.unshift(next);

  if (messageTracker.length > 200) {
    messageTracker.length = 200;
  }
}

function validateConfig() {
  const missing = [];

  if (!phoneNumberId) missing.push("WHATSAPP_PHONE_NUMBER_ID");
  if (!accessToken) missing.push("WHATSAPP_ACCESS_TOKEN");

  if (missing.length) {
    throw new Error(`Variaveis ausentes no .env: ${missing.join(", ")}`);
  }
}

function verifyMetaSignature(req) {
  if (!appSecret) return true;

  const signature = req.header("x-hub-signature-256");
  if (!signature || !req.rawBody) return false;

  const expected = `sha256=${crypto
    .createHmac("sha256", appSecret)
    .update(req.rawBody)
    .digest("hex")}`;

  const signatureBuffer = Buffer.from(signature);
  const expectedBuffer = Buffer.from(expected);

  if (signatureBuffer.length !== expectedBuffer.length) {
    return false;
  }

  return crypto.timingSafeEqual(signatureBuffer, expectedBuffer);
}

function extractWebhookMessages(body) {
  const entries = Array.isArray(body.entry) ? body.entry : [];
  const messages = [];
  const statuses = [];

  for (const entry of entries) {
    const changes = Array.isArray(entry.changes) ? entry.changes : [];
    for (const change of changes) {
      const value = change.value || {};
      if (Array.isArray(value.messages)) {
        for (const message of value.messages) {
          messages.push({
            from: message.from,
            id: message.id,
            timestamp: message.timestamp,
            type: message.type,
            text: message.text?.body || null,
            buttonReply: message.interactive?.button_reply?.title || null,
            listReply: message.interactive?.list_reply?.title || null,
          });
        }
      }

      if (Array.isArray(value.statuses)) {
        for (const status of value.statuses) {
          statuses.push({
            id: status.id,
            status: status.status,
            recipientId: status.recipient_id,
            timestamp: status.timestamp,
            conversationId: status.conversation?.id || null,
            pricingCategory: status.pricing?.category || null,
            pricingBillable: status.pricing?.billable ?? null,
            errors: Array.isArray(status.errors)
              ? status.errors.map((error) => ({
                  code: error.code,
                  title: error.title,
                  message: error.message,
                  errorData: error.error_data || null,
                }))
              : [],
          });
        }
      }
    }
  }

  return { messages, statuses };
}

async function sendWhatsAppRequest(payload) {
  const response = await axios.post(`${baseUrl}/${phoneNumberId}/messages`, payload, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
  });

  return response.data;
}

async function fetchPhoneNumberMetadata() {
  const response = await axios.get(`${baseUrl}/${phoneNumberId}`, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
    params: {
      fields: "id,display_phone_number,verified_name",
    },
  });

  return response.data;
}

async function resolveBusinessAccountId(requestWabaId) {
  if (requestWabaId) return requestWabaId;
  if (businessAccountId) return businessAccountId;
  return null;
}

function buildTemplateComponents({ bodyText, footerText, bodyExamples }) {
  const components = [];

  if (bodyText) {
    const bodyComponent = {
      type: "BODY",
      text: bodyText,
    };

    if (Array.isArray(bodyExamples) && bodyExamples.length > 0) {
      bodyComponent.example = {
        body_text: [bodyExamples],
      };
    }

    components.push(bodyComponent);
  }

  if (footerText) {
    components.push({
      type: "FOOTER",
      text: footerText,
    });
  }

  return components;
}

async function createMessageTemplate({
  wabaId,
  name,
  category,
  language,
  bodyText,
  footerText,
  bodyExamples,
  allowCategoryChange,
}) {
  const response = await axios.post(
    `${baseUrl}/${wabaId}/message_templates`,
    {
      name,
      category,
      language,
      allow_category_change: Boolean(allowCategoryChange),
      components: buildTemplateComponents({ bodyText, footerText, bodyExamples }),
    },
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
    }
  );

  return response.data;
}

async function listMessageTemplates(wabaId) {
  const response = await axios.get(`${baseUrl}/${wabaId}/message_templates`, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
    params: {
      limit: 100,
    },
  });

  return response.data;
}

router.get("/health", async (_req, res) => {
  try {
    validateConfig();
    const metadata = await fetchPhoneNumberMetadata().catch(() => null);

    const payload = {
      ok: true,
      mode: "whatsapp-cloud-api",
      apiVersion,
      phoneNumberId,
      basePath: appBasePath,
      displayPhoneNumber: metadata?.display_phone_number || null,
      verifiedName: metadata?.verified_name || null,
      businessAccountId: businessAccountId || null,
      webhookVerifyTokenConfigured: Boolean(process.env.WHATSAPP_WEBHOOK_VERIFY_TOKEN),
      appSecretConfigured: Boolean(appSecret),
    };

    addLog("health", "Consulta de health executada com sucesso.", payload);
    res.json(payload);
  } catch (error) {
    addLog("error", "Falha ao consultar health.", error.message);
    res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

router.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === verifyToken) {
    addLog("webhook", "Webhook verificado pela Meta com sucesso.");
    return res.status(200).send(challenge);
  }

  addLog("error", "Falha na verificacao do webhook.", {
    mode,
    receivedToken: token ? "informado" : "ausente",
  });
  return res.status(403).json({
    success: false,
    error: "Falha na verificacao do webhook.",
  });
});

router.post("/webhook", (req, res) => {
  if (!verifyMetaSignature(req)) {
    addLog("error", "Assinatura do webhook invalida.");
    return res.status(401).json({
      success: false,
      error: "Assinatura do webhook invalida.",
    });
  }

  const { messages, statuses } = extractWebhookMessages(req.body);

  if (messages.length) {
    console.log("Mensagens recebidas da Meta:");
    console.log(JSON.stringify(messages, null, 2));
    addLog("incoming_message", "Mensagens recebidas da Meta.", messages);
  }

  if (statuses.length) {
    console.log("Status de envio/entrega:");
    console.log(JSON.stringify(statuses, null, 2));
    addLog("delivery_status", "Status de envio/entrega recebidos.", statuses);

    for (const status of statuses) {
      upsertTrackedMessage({
        id: status.id,
        latestStatus: status.status,
        recipientId: status.recipientId,
        conversationId: status.conversationId,
        pricingCategory: status.pricingCategory,
        pricingBillable: status.pricingBillable,
        lastWebhookAt: status.timestamp,
        errors: status.errors,
      });
    }
  }

  return res.sendStatus(200);
});

router.post("/send-text", async (req, res) => {
  try {
    validateConfig();

    const { to, body, preview_url } = req.body;
    if (!to || !body) {
      return res.status(400).json({
        success: false,
        error: "`to` e `body` sao obrigatorios.",
      });
    }

    const payload = {
      messaging_product: "whatsapp",
      to,
      type: "text",
      text: {
        body,
        preview_url: Boolean(preview_url),
      },
    };

    const data = await sendWhatsAppRequest(payload);
    const messageId = data.messages?.[0]?.id || null;
    const acceptedStatus = data.messages?.[0]?.message_status || "accepted";

    upsertTrackedMessage({
      id: messageId,
      type: "text",
      to,
      acceptedAt: new Date().toISOString(),
      latestStatus: acceptedStatus,
      apiAccepted: true,
      input: data.contacts?.[0]?.input || to,
      waId: data.contacts?.[0]?.wa_id || null,
    });

    addLog("send_text", `Mensagem de texto enviada para ${to}.`, data);
    return res.json({ success: true, data });
  } catch (error) {
    addLog("error", "Falha ao enviar mensagem de texto.", error.response?.data || error.message);
    return res.status(error.response?.status || 500).json({
      success: false,
      error: error.response?.data || error.message,
    });
  }
});

router.post("/send-template", async (req, res) => {
  try {
    validateConfig();

    const { to, templateName, languageCode = "pt_BR", bodyParameters = [] } = req.body;

    if (!to || !templateName) {
      return res.status(400).json({
        success: false,
        error: "`to` e `templateName` sao obrigatorios.",
      });
    }

    const components = [];

    if (Array.isArray(bodyParameters) && bodyParameters.length > 0) {
      components.push({
        type: "body",
        parameters: bodyParameters.map((text) => ({
          type: "text",
          text: String(text),
        })),
      });
    }

    const payload = {
      messaging_product: "whatsapp",
      to,
      type: "template",
      template: {
        name: templateName,
        language: {
          code: languageCode,
        },
        ...(components.length ? { components } : {}),
      },
    };

    const data = await sendWhatsAppRequest(payload);
    const messageId = data.messages?.[0]?.id || null;
    const acceptedStatus = data.messages?.[0]?.message_status || "accepted";

    upsertTrackedMessage({
      id: messageId,
      type: "template",
      to,
      templateName,
      acceptedAt: new Date().toISOString(),
      latestStatus: acceptedStatus,
      apiAccepted: true,
      input: data.contacts?.[0]?.input || to,
      waId: data.contacts?.[0]?.wa_id || null,
    });

    addLog("send_template", `Template ${templateName} enviado para ${to}.`, data);
    return res.json({ success: true, data });
  } catch (error) {
    addLog("error", "Falha ao enviar template.", error.response?.data || error.message);
    return res.status(error.response?.status || 500).json({
      success: false,
      error: error.response?.data || error.message,
    });
  }
});

router.get("/message-templates", async (req, res) => {
  try {
    validateConfig();

    const wabaId = await resolveBusinessAccountId(req.query.wabaId);
    if (!wabaId) {
      return res.status(400).json({
        success: false,
        error: "Nao foi possivel descobrir o `wabaId`. Informe na query ou configure `WHATSAPP_BUSINESS_ACCOUNT_ID` no .env.",
      });
    }

    const data = await listMessageTemplates(wabaId);
    addLog("template_list", "Lista de templates consultada.", {
      wabaId,
      total: Array.isArray(data.data) ? data.data.length : 0,
    });
    return res.json({ success: true, data });
  } catch (error) {
    addLog("error", "Falha ao listar templates.", error.response?.data || error.message);
    return res.status(error.response?.status || 500).json({
      success: false,
      error: error.response?.data || error.message,
    });
  }
});

router.post("/submit-template", async (req, res) => {
  try {
    validateConfig();

    const {
      wabaId: requestWabaId,
      name,
      category,
      language = "pt_BR",
      bodyText,
      footerText,
      bodyExamples = [],
      allowCategoryChange = true,
    } = req.body;

    const wabaId = await resolveBusinessAccountId(requestWabaId);
    if (!wabaId) {
      return res.status(400).json({
        success: false,
        error: "Nao foi possivel descobrir o `wabaId`. Informe manualmente ou configure `WHATSAPP_BUSINESS_ACCOUNT_ID` no .env.",
      });
    }

    if (!name || !category || !bodyText) {
      return res.status(400).json({
        success: false,
        error: "`name`, `category` e `bodyText` sao obrigatorios.",
      });
    }

    const normalizedExamples = Array.isArray(bodyExamples)
      ? bodyExamples.map((item) => String(item).trim()).filter(Boolean)
      : [];

    const data = await createMessageTemplate({
      wabaId,
      name,
      category,
      language,
      bodyText,
      footerText,
      bodyExamples: normalizedExamples,
      allowCategoryChange,
    });

    addLog("template_submission", `Template ${name} enviado para aprovacao.`, {
      wabaId,
      category,
      language,
      response: data,
    });

    return res.json({ success: true, data });
  } catch (error) {
    addLog("error", "Falha ao enviar template para aprovacao.", error.response?.data || error.message);
    return res.status(error.response?.status || 500).json({
      success: false,
      error: error.response?.data || error.message,
    });
  }
});

router.get("/message-tracker", (_req, res) => {
  res.json({
    success: true,
    messages: messageTracker,
  });
});

router.get("/logs", (_req, res) => {
  res.json({ success: true, logs });
});

app.use(appBasePath, router);

app.listen(port, () => {
  try {
    validateConfig();
    console.log(`Servidor oficial do WhatsApp Cloud API em ${buildLocalUrl("/")}`);
    console.log(`Webhook de verificacao em ${buildLocalUrl("/webhook")}`);
    console.log(`Painel de testes em ${buildLocalUrl("/")}`);
    addLog("startup", `Servidor iniciado na porta ${port}.`, {
      apiVersion,
      phoneNumberId,
      basePath: appBasePath,
    });
  } catch (error) {
    console.error("Falha ao iniciar servidor oficial:", error.message);
    addLog("error", "Falha ao iniciar servidor oficial.", error.message);
  }
});
