const crypto = require("crypto");
const express = require("express");
const axios = require("axios");
const cors = require("cors");
const dotenv = require("dotenv");
const path = require("path");
const fs = require("fs");
let mysql = null;

try {
  mysql = require("mysql2/promise");
} catch (_error) {
  mysql = null;
}

const envCandidates = [
  path.resolve(__dirname, ".env"),
  path.resolve(__dirname, "../.env"),
];
const resolvedEnvPath = envCandidates.find((candidate) => fs.existsSync(candidate));

dotenv.config(resolvedEnvPath ? { path: resolvedEnvPath } : undefined);

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
const mysqlHost = process.env.WHATSAPP_MYSQL_HOST || process.env.MYSQL_HOST || "";
const mysqlPort = Number(process.env.WHATSAPP_MYSQL_PORT || process.env.MYSQL_PORT || 3306);
const mysqlDatabase = process.env.WHATSAPP_MYSQL_DATABASE || process.env.MYSQL_DATABASE || "";
const mysqlUser = process.env.WHATSAPP_MYSQL_USER || process.env.MYSQL_USER || "";
const mysqlPassword = process.env.WHATSAPP_MYSQL_PASSWORD || process.env.MYSQL_PASSWORD || "";
const mysqlTablePrefix = process.env.WHATSAPP_MYSQL_TABLE_PREFIX || "wa_";
const queuePollIntervalMs = Number(process.env.WHATSAPP_QUEUE_POLL_INTERVAL_MS || 15000);
const wordpressAssistantUrl = process.env.WORDPRESS_ASSISTANT_URL || "";
const wordpressAssistantToken = process.env.WORDPRESS_ASSISTANT_TOKEN || "";
const logs = [];
const messageTracker = [];
let dbPool = null;
let queueWorkerHandle = null;

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

  void persistRuntimeLog(type, message, details);
}

function getIncomingMessageText(message) {
  return (
    message?.text ||
    message?.buttonReply ||
    message?.listReply ||
    message?.documentCaption ||
    message?.imageCaption ||
    ""
  );
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

  void persistTrackedMessage(next);
}

function nowMysql() {
  return new Date().toISOString().slice(0, 19).replace("T", " ");
}

function isMySqlQueueEnabled() {
  return Boolean(mysql && mysqlHost && mysqlDatabase && mysqlUser);
}

function getTableName(name) {
  return `${mysqlTablePrefix}${name}`;
}

async function getDbPool() {
  if (!isMySqlQueueEnabled()) {
    return null;
  }

  if (!dbPool) {
    dbPool = mysql.createPool({
      host: mysqlHost,
      port: mysqlPort,
      database: mysqlDatabase,
      user: mysqlUser,
      password: mysqlPassword,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
      charset: "utf8mb4",
    });
  }

  return dbPool;
}

async function ensurePersistenceTables() {
  const pool = await getDbPool();
  if (!pool) {
    return false;
  }

  const queueTable = getTableName("queue_jobs");
  const trackerTable = getTableName("message_tracker");
  const logsTable = getTableName("runtime_logs");

  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${queueTable}\` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      automation_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
      lead_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
      event_name VARCHAR(100) NOT NULL,
      flow_name VARCHAR(191) DEFAULT NULL,
      flow_classification TINYINT NOT NULL DEFAULT 2,
      template_name VARCHAR(191) DEFAULT NULL,
      template_language VARCHAR(20) DEFAULT 'pt_BR',
      template_ref_json LONGTEXT NOT NULL,
      lead_payload_json LONGTEXT NOT NULL,
      context_payload_json LONGTEXT NULL,
      automation_meta_json LONGTEXT NULL,
      scheduled_for DATETIME NOT NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'pending',
      attempts INT NOT NULL DEFAULT 0,
      remote_message_id VARCHAR(191) DEFAULT NULL,
      accepted_status VARCHAR(50) DEFAULT NULL,
      processed_at DATETIME DEFAULT NULL,
      last_error TEXT DEFAULT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_status_schedule (status, scheduled_for),
      KEY idx_flow (flow_name, flow_classification),
      KEY idx_remote_message (remote_message_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${trackerTable}\` (
      id VARCHAR(191) NOT NULL,
      queue_job_id BIGINT UNSIGNED DEFAULT NULL,
      recipient_id VARCHAR(50) DEFAULT NULL,
      latest_status VARCHAR(50) DEFAULT NULL,
      type VARCHAR(40) DEFAULT NULL,
      template_name VARCHAR(191) DEFAULT NULL,
      api_accepted TINYINT(1) NOT NULL DEFAULT 0,
      accepted_at DATETIME DEFAULT NULL,
      delivered_at DATETIME DEFAULT NULL,
      read_at DATETIME DEFAULT NULL,
      failed_at DATETIME DEFAULT NULL,
      last_webhook_at DATETIME DEFAULT NULL,
      conversation_id VARCHAR(191) DEFAULT NULL,
      pricing_category VARCHAR(50) DEFAULT NULL,
      pricing_billable TINYINT(1) DEFAULT NULL,
      wa_id VARCHAR(50) DEFAULT NULL,
      input_phone VARCHAR(50) DEFAULT NULL,
      metadata_json LONGTEXT NULL,
      errors_json LONGTEXT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_status (latest_status),
      KEY idx_queue_job (queue_job_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${logsTable}\` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      type VARCHAR(80) NOT NULL,
      message VARCHAR(255) NOT NULL,
      details_json LONGTEXT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_type_created (type, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  return true;
}

async function persistRuntimeLog(type, message, details = null) {
  const pool = await getDbPool();
  if (!pool) {
    return;
  }

  const logsTable = getTableName("runtime_logs");
  await pool.execute(
    `INSERT INTO \`${logsTable}\` (type, message, details_json, created_at) VALUES (?, ?, ?, ?)`,
    [type, message, details ? JSON.stringify(details) : null, nowMysql()]
  );
}

async function persistTrackedMessage(entry) {
  const pool = await getDbPool();
  if (!pool || !entry?.id) {
    return;
  }

  const trackerTable = getTableName("message_tracker");
  const latestStatus = entry.latestStatus || null;
  const deliveredAt = latestStatus === "delivered" ? nowMysql() : null;
  const readAt = latestStatus === "read" ? nowMysql() : null;
  const failedAt = latestStatus === "failed" ? nowMysql() : null;

  await pool.execute(
    `INSERT INTO \`${trackerTable}\`
      (id, queue_job_id, recipient_id, latest_status, type, template_name, api_accepted, accepted_at, delivered_at, read_at, failed_at, last_webhook_at, conversation_id, pricing_category, pricing_billable, wa_id, input_phone, metadata_json, errors_json)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
      queue_job_id = COALESCE(VALUES(queue_job_id), queue_job_id),
      recipient_id = COALESCE(VALUES(recipient_id), recipient_id),
      latest_status = COALESCE(VALUES(latest_status), latest_status),
      type = COALESCE(VALUES(type), type),
      template_name = COALESCE(VALUES(template_name), template_name),
      api_accepted = VALUES(api_accepted),
      accepted_at = COALESCE(VALUES(accepted_at), accepted_at),
      delivered_at = COALESCE(VALUES(delivered_at), delivered_at),
      read_at = COALESCE(VALUES(read_at), read_at),
      failed_at = COALESCE(VALUES(failed_at), failed_at),
      last_webhook_at = COALESCE(VALUES(last_webhook_at), last_webhook_at),
      conversation_id = COALESCE(VALUES(conversation_id), conversation_id),
      pricing_category = COALESCE(VALUES(pricing_category), pricing_category),
      pricing_billable = COALESCE(VALUES(pricing_billable), pricing_billable),
      wa_id = COALESCE(VALUES(wa_id), wa_id),
      input_phone = COALESCE(VALUES(input_phone), input_phone),
      metadata_json = COALESCE(VALUES(metadata_json), metadata_json),
      errors_json = COALESCE(VALUES(errors_json), errors_json)`,
    [
      entry.id,
      entry.queueJobId || null,
      entry.recipientId || null,
      latestStatus,
      entry.type || null,
      entry.templateName || null,
      entry.apiAccepted ? 1 : 0,
      entry.acceptedAt ? String(entry.acceptedAt).slice(0, 19).replace("T", " ") : null,
      deliveredAt,
      readAt,
      failedAt,
      entry.lastWebhookAt ? String(entry.lastWebhookAt).slice(0, 19).replace("T", " ") : null,
      entry.conversationId || null,
      entry.pricingCategory || null,
      typeof entry.pricingBillable === "boolean" ? (entry.pricingBillable ? 1 : 0) : null,
      entry.waId || null,
      entry.input || null,
      entry.metadata ? JSON.stringify(entry.metadata) : null,
      entry.errors ? JSON.stringify(entry.errors) : null,
    ]
  );
}

async function queueTemplateJob(job) {
  const pool = await getDbPool();
  if (!pool) {
    throw new Error("Fila MySQL indisponivel. Configure mysql2 e as variaveis de banco.");
  }

  const queueTable = getTableName("queue_jobs");
  const [result] = await pool.execute(
    `INSERT INTO \`${queueTable}\`
      (automation_id, lead_id, event_name, flow_name, flow_classification, template_name, template_language, template_ref_json, lead_payload_json, context_payload_json, automation_meta_json, scheduled_for, status, attempts)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0)`,
    [
      Number(job.automationId || 0),
      Number(job.leadId || 0),
      String(job.eventName || ""),
      job.automationMeta?.flow || null,
      Number(job.flowClassification || job.automationMeta?.classification || 2),
      job.templateRef?.name || null,
      job.templateRef?.language || job.templateLanguage || "pt_BR",
      JSON.stringify(job.templateRef || {}),
      JSON.stringify(job.leadPayload || {}),
      JSON.stringify(job.contextPayload || {}),
      JSON.stringify(job.automationMeta || {}),
      job.scheduledFor || nowMysql(),
    ]
  );

  return {
    id: result.insertId,
    scheduledFor: job.scheduledFor || nowMysql(),
    status: "pending",
  };
}

async function markQueueJobProcessing(jobId, attempts) {
  const pool = await getDbPool();
  if (!pool) return;
  const queueTable = getTableName("queue_jobs");
  await pool.execute(
    `UPDATE \`${queueTable}\` SET status = 'processing', attempts = ?, last_error = NULL WHERE id = ?`,
    [attempts, jobId]
  );
}

async function markQueueJobResult(jobId, patch) {
  const pool = await getDbPool();
  if (!pool) return;
  const queueTable = getTableName("queue_jobs");
  await pool.execute(
    `UPDATE \`${queueTable}\`
        SET status = ?, remote_message_id = ?, accepted_status = ?, processed_at = ?, last_error = ?
      WHERE id = ?`,
    [
      patch.status || "sent",
      patch.remoteMessageId || null,
      patch.acceptedStatus || null,
      patch.processedAt || nowMysql(),
      patch.lastError || null,
      jobId,
    ]
  );
}

async function fetchDueQueueJobs(limit = 20) {
  const pool = await getDbPool();
  if (!pool) {
    return [];
  }

  const queueTable = getTableName("queue_jobs");
  const [rows] = await pool.execute(
    `SELECT * FROM \`${queueTable}\`
      WHERE status IN ('pending', 'retry') AND scheduled_for <= NOW()
      ORDER BY scheduled_for ASC, id ASC
      LIMIT ?`,
    [Number(limit)]
  );
  return rows;
}

async function fetchQueueStats() {
  const pool = await getDbPool();
  if (!pool) {
    return {
      pending: 0,
      processing: 0,
      sent: 0,
      failed: 0,
      retry: 0,
      delivered: 0,
      read: 0,
      queued: 0,
    };
  }

  const queueTable = getTableName("queue_jobs");
  const trackerTable = getTableName("message_tracker");
  const [queueRows] = await pool.query(`SELECT status, COUNT(*) total FROM \`${queueTable}\` GROUP BY status`);
  const [trackerRows] = await pool.query(`SELECT latest_status, COUNT(*) total FROM \`${trackerTable}\` WHERE queue_job_id IS NOT NULL GROUP BY latest_status`);

  const stats = {
    pending: 0,
    processing: 0,
    sent: 0,
    failed: 0,
    retry: 0,
    delivered: 0,
    read: 0,
    queued: 0,
  };

  for (const row of queueRows) {
    const status = String(row.status || "");
    if (Object.prototype.hasOwnProperty.call(stats, status)) {
      stats[status] = Number(row.total || 0);
    }
    stats.queued += Number(row.total || 0);
  }

  for (const row of trackerRows) {
    const status = String(row.latest_status || "");
    if (status === "delivered") stats.delivered = Number(row.total || 0);
    if (status === "read") stats.read = Number(row.total || 0);
  }

  return stats;
}

async function processQueueJobs(limit = 20) {
  const jobs = await fetchDueQueueJobs(limit);
  let processed = 0;
  let sent = 0;
  let failed = 0;

  for (const job of jobs) {
    processed += 1;
    const attempts = Number(job.attempts || 0) + 1;
    await markQueueJobProcessing(job.id, attempts);

    try {
      const templateRef = JSON.parse(job.template_ref_json || "{}");
      const leadPayload = JSON.parse(job.lead_payload_json || "{}");
      const templateName = templateRef.name || job.template_name;
      const languageCode = templateRef.language || job.template_language || "pt_BR";
      const bodyParameters = Array.isArray(templateRef.bodyParameters) && templateRef.bodyParameters.length
        ? templateRef.bodyParameters
        : [leadPayload.name || " "];

      const payload = {
        messaging_product: "whatsapp",
        to: leadPayload.phone,
        type: "template",
        template: {
          name: templateName,
          language: {
            code: languageCode,
          },
          components: [
            {
              type: "body",
              parameters: bodyParameters.map((text) => ({
                type: "text",
                text: String(text),
              })),
            },
          ],
        },
      };

      const data = await sendWhatsAppRequest(payload);
      const messageId = data.messages?.[0]?.id || null;
      const acceptedStatus = data.messages?.[0]?.message_status || "accepted";

      upsertTrackedMessage({
        id: messageId,
        queueJobId: job.id,
        type: "template",
        to: leadPayload.phone,
        recipientId: leadPayload.phone,
        templateName,
        acceptedAt: new Date().toISOString(),
        latestStatus: acceptedStatus,
        apiAccepted: true,
        input: data.contacts?.[0]?.input || leadPayload.phone,
        waId: data.contacts?.[0]?.wa_id || null,
      });

      await markQueueJobResult(job.id, {
        status: "sent",
        remoteMessageId: messageId,
        acceptedStatus,
        processedAt: nowMysql(),
      });

      addLog("queue_sent", `Job ${job.id} enviado para ${leadPayload.phone}.`, {
        templateName,
        messageId,
        acceptedStatus,
      });
      sent += 1;
    } catch (error) {
      await markQueueJobResult(job.id, {
        status: "failed",
        processedAt: nowMysql(),
        lastError: error.response?.data ? JSON.stringify(error.response.data) : error.message,
      });
      addLog("queue_error", `Falha ao processar job ${job.id}.`, error.response?.data || error.message);
      failed += 1;
    }
  }

  return {
    processed,
    sent,
    failed,
    pendingFound: jobs.length,
  };
}

function startQueueWorker() {
  if (queueWorkerHandle || !isMySqlQueueEnabled()) {
    return;
  }

  queueWorkerHandle = setInterval(() => {
    processQueueJobs(20).catch((error) => {
      addLog("queue_worker_error", "Falha no worker automatico da fila.", error.message);
    });
  }, Math.max(5000, queuePollIntervalMs));
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
      const contacts = Array.isArray(value.contacts) ? value.contacts : [];
      const contactNames = new Map(
        contacts.map((contact) => [String(contact.wa_id || ""), contact.profile?.name || null])
      );
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
            profileName: contactNames.get(String(message.from || "")) || null,
            audioId: message.audio?.id || null,
            audioMimeType: message.audio?.mime_type || null,
            audioVoice: Boolean(message.audio?.voice),
            imageCaption: message.image?.caption || null,
            documentCaption: message.document?.caption || null,
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

async function downloadWhatsAppMedia(mediaId) {
  const metaResponse = await axios.get(`${baseUrl}/${mediaId}`, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
  });

  const downloadUrl = metaResponse.data?.url;
  if (!downloadUrl) {
    throw new Error("Meta nao retornou URL de download para a midia recebida.");
  }

  const mediaResponse = await axios.get(downloadUrl, {
    responseType: "arraybuffer",
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
  });

  return {
    mimeType: metaResponse.data?.mime_type || mediaResponse.headers["content-type"] || "audio/ogg",
    base64: Buffer.from(mediaResponse.data).toString("base64"),
  };
}

async function requestWordPressAssistantReply(message) {
  if (!wordpressAssistantUrl || !wordpressAssistantToken) {
    return null;
  }

  const payload = {
    from: message.from,
    name: message.profileName || "",
    type: message.type,
    message: getIncomingMessageText(message),
    button_reply: message.buttonReply || "",
    list_reply: message.listReply || "",
  };

  if (message.type === "audio" && message.audioId) {
    const audio = await downloadWhatsAppMedia(message.audioId);
    payload.audio_base64 = audio.base64;
    payload.audio_mime_type = audio.mimeType;
  }

  const response = await axios.post(wordpressAssistantUrl, payload, {
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${wordpressAssistantToken}`,
    },
    timeout: 60000,
  });

  return response.data;
}

async function sendTextMessagesInSequence(to, chunks) {
  for (const chunk of chunks) {
    const body = String(chunk || "").trim();
    if (!body) continue;
    await sendWhatsAppRequest({
      messaging_product: "whatsapp",
      to,
      type: "text",
      text: {
        body,
        preview_url: false,
      },
    });
  }
}

async function handleIncomingMessageWithAssistant(message) {
  if (!wordpressAssistantUrl || !wordpressAssistantToken) {
    return;
  }

  try {
    const assistant = await requestWordPressAssistantReply(message);
    if (!assistant?.chunks?.length && !assistant?.handoff?.alert_message) {
      addLog("assistant_skip", "Assistente nao retornou mensagens para envio.", {
        from: message.from,
        type: message.type,
        mode: assistant?.mode || null,
      });
      return;
    }

    if (Array.isArray(assistant.chunks) && assistant.chunks.length > 0) {
      await sendTextMessagesInSequence(message.from, assistant.chunks);
    }

    if (
      assistant.handoff?.notify_human &&
      assistant.handoff?.number &&
      assistant.handoff?.alert_message &&
      String(assistant.handoff.number) !== String(message.from)
    ) {
      await sendTextMessagesInSequence(assistant.handoff.number, [assistant.handoff.alert_message]);
    }

    addLog("assistant_reply", "Assistente respondeu a mensagem recebida.", {
      from: message.from,
      type: message.type,
      mode: assistant.mode || null,
      chunks: assistant.chunks?.length || 0,
      handoffNumber: assistant.handoff?.number || null,
    });
  } catch (error) {
    addLog("assistant_error", "Falha ao processar resposta automatica com WordPress.", error.response?.data || error.message);
  }
}

async function handleIncomingMessages(messages) {
  for (const message of messages) {
    await handleIncomingMessageWithAssistant(message);
  }
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
      mysqlQueueEnabled: isMySqlQueueEnabled(),
      mysqlConfigured: Boolean(mysqlHost && mysqlDatabase && mysqlUser),
      mysqlDriverInstalled: Boolean(mysql),
      wordpressAssistantConfigured: Boolean(wordpressAssistantUrl && wordpressAssistantToken),
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

  res.sendStatus(200);

  if (messages.length) {
    void handleIncomingMessages(messages);
  }

  return;
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

router.post("/queue/template", async (req, res) => {
  try {
    validateConfig();

    if (!isMySqlQueueEnabled()) {
      return res.status(400).json({
        success: false,
        error: "Fila MySQL nao configurada no servidor.",
      });
    }

    const { templateRef, leadPayload, scheduledFor, eventName } = req.body || {};
    if (!templateRef?.name || !leadPayload?.phone || !scheduledFor || !eventName) {
      return res.status(400).json({
        success: false,
        error: "`templateRef.name`, `leadPayload.phone`, `scheduledFor` e `eventName` sao obrigatorios.",
      });
    }

    const job = await queueTemplateJob(req.body);
    addLog("queue_created", `Job ${job.id} adicionado na fila remota.`, {
      scheduledFor: job.scheduledFor,
      eventName,
      templateName: templateRef.name,
      leadId: req.body.leadId || 0,
    });
    return res.json({ success: true, job });
  } catch (error) {
    addLog("queue_error", "Falha ao criar job remoto.", error.message);
    return res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

router.post("/queue/process", async (req, res) => {
  try {
    validateConfig();
    const limit = Number(req.body?.limit || 20);
    const result = await processQueueJobs(limit);
    const stats = await fetchQueueStats();
    return res.json({ success: true, result, stats });
  } catch (error) {
    addLog("queue_error", "Falha ao processar fila sob demanda.", error.message);
    return res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

router.get("/queue/stats", async (_req, res) => {
  try {
    const stats = await fetchQueueStats();
    return res.json({ success: true, stats });
  } catch (error) {
    addLog("queue_error", "Falha ao consultar estatisticas da fila.", error.message);
    return res.status(500).json({
      success: false,
      error: error.message,
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

app.listen(port, async () => {
  try {
    validateConfig();
    if (isMySqlQueueEnabled()) {
      await ensurePersistenceTables();
      startQueueWorker();
    }

    console.log(`Servidor oficial do WhatsApp Cloud API em ${buildLocalUrl("/")}`);
    console.log(`Webhook de verificacao em ${buildLocalUrl("/webhook")}`);
    console.log(`Painel de testes em ${buildLocalUrl("/")}`);
    if (isMySqlQueueEnabled()) {
      console.log(`Fila MySQL ativa em ${mysqlHost}:${mysqlPort}/${mysqlDatabase}`);
    } else {
      console.log("Fila MySQL desativada: variaveis de banco ou mysql2 ausentes.");
    }
    addLog("startup", `Servidor iniciado na porta ${port}.`, {
      apiVersion,
      phoneNumberId,
      basePath: appBasePath,
      mysqlQueueEnabled: isMySqlQueueEnabled(),
      mysqlConfigured: Boolean(mysqlHost && mysqlDatabase && mysqlUser),
      mysqlDriverInstalled: Boolean(mysql),
    });
  } catch (error) {
    console.error("Falha ao iniciar servidor oficial:", error.message);
    addLog("error", "Falha ao iniciar servidor oficial.", error.message);
  }
});
