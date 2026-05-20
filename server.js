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
const geminiApiKey = process.env.GEMINI_API_KEY || "";
const geminiModel = process.env.GEMINI_MODEL || "gemini-2.0-flash";
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

function toMysqlDateTime(value) {
  if (!value) return null;

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString().slice(0, 19).replace("T", " ");
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    const ms = value > 9999999999 ? value : value * 1000;
    const date = new Date(ms);
    return Number.isNaN(date.getTime()) ? null : date.toISOString().slice(0, 19).replace("T", " ");
  }

  const stringValue = String(value).trim();
  if (!stringValue) return null;

  if (/^\d+$/.test(stringValue)) {
    const numeric = Number(stringValue);
    if (!Number.isFinite(numeric)) return null;
    const ms = stringValue.length >= 13 ? numeric : numeric * 1000;
    const date = new Date(ms);
    return Number.isNaN(date.getTime()) ? null : date.toISOString().slice(0, 19).replace("T", " ");
  }

  const parsed = new Date(stringValue);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString().slice(0, 19).replace("T", " ");
  }

  return null;
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

  const humanAttendanceTable = getTableName("human_attendance");
  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${humanAttendanceTable}\` (
      phone VARCHAR(30) NOT NULL,
      client_id VARCHAR(100) DEFAULT NULL,
      attendant_name VARCHAR(191) DEFAULT NULL,
      taken_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      last_activity DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (phone)
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

  // ── Tabela de estado de conversa (intenção, carrinho, verificação) ──────
  const convStateTable = getTableName("conversation_state");
  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${convStateTable}\` (
      phone VARCHAR(30) NOT NULL,
      client_id VARCHAR(100) NOT NULL,
      verified_email VARCHAR(191) DEFAULT NULL,
      verified_customer_id INT DEFAULT NULL,
      verified_at DATETIME DEFAULT NULL,
      cart_json LONGTEXT DEFAULT NULL,
      awaiting VARCHAR(60) DEFAULT NULL,
      last_order_id INT DEFAULT NULL,
      context_json LONGTEXT DEFAULT NULL,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (phone, client_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  // ── Tabelas do sistema multi-tenant e IA ────────────────────────────────
  const tenantsTable = getTableName("tenants");
  const agentsTable = getTableName("agents");
  const knowledgeTable = getTableName("knowledge_base");
  const conversationsTable = getTableName("conversations");
  const phoneTenantMapTable = getTableName("phone_tenant_map");
  const leadsCacheTable = getTableName("leads_cache");
  const campaignsTable = getTableName("campaigns");
  const campaignRecipientsTable = getTableName("campaign_recipients");

  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${tenantsTable}\` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      client_id VARCHAR(100) NOT NULL,
      wp_url VARCHAR(255) NOT NULL,
      api_key VARCHAR(255) NOT NULL,
      active TINYINT(1) NOT NULL DEFAULT 1,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uniq_client_id (client_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${agentsTable}\` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      name VARCHAR(191) NOT NULL,
      prompt LONGTEXT NOT NULL,
      keywords_json LONGTEXT DEFAULT NULL,
      assigned_domains_json LONGTEXT DEFAULT NULL,
      transfer_keywords_json LONGTEXT DEFAULT NULL,
      transfer_number VARCHAR(30) DEFAULT NULL,
      icon VARCHAR(50) DEFAULT '🤖',
      description VARCHAR(255) DEFAULT NULL,
      active TINYINT(1) NOT NULL DEFAULT 1,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${knowledgeTable}\` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      agent_id BIGINT UNSIGNED DEFAULT NULL,
      title VARCHAR(255) NOT NULL,
      content LONGTEXT NOT NULL,
      keywords_json LONGTEXT DEFAULT NULL,
      active TINYINT(1) NOT NULL DEFAULT 1,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_agent (agent_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${conversationsTable}\` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      client_id VARCHAR(100) NOT NULL,
      phone VARCHAR(30) NOT NULL,
      role VARCHAR(15) NOT NULL,
      message LONGTEXT NOT NULL,
      agent_id BIGINT UNSIGNED DEFAULT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_phone_client (client_id, phone),
      KEY idx_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${phoneTenantMapTable}\` (
      phone VARCHAR(30) NOT NULL,
      client_id VARCHAR(100) NOT NULL,
      lead_name VARCHAR(191) DEFAULT NULL,
      first_seen DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      last_seen DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (phone),
      KEY idx_client (client_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${leadsCacheTable}\` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      client_id VARCHAR(100) NOT NULL,
      phone VARCHAR(30) NOT NULL,
      email VARCHAR(191) DEFAULT NULL,
      name VARCHAR(191) DEFAULT NULL,
      score INT NOT NULL DEFAULT 0,
      stage VARCHAR(50) DEFAULT NULL,
      utm_source VARCHAR(191) DEFAULT NULL,
      utm_medium VARCHAR(191) DEFAULT NULL,
      utm_campaign VARCHAR(191) DEFAULT NULL,
      cart_abandoned TINYINT(1) NOT NULL DEFAULT 0,
      total_orders INT NOT NULL DEFAULT 0,
      last_order_date DATE DEFAULT NULL,
      last_order_product VARCHAR(255) DEFAULT NULL,
      total_spent DECIMAL(10,2) NOT NULL DEFAULT 0,
      days_since_last_purchase INT DEFAULT NULL,
      visited_pages_json LONGTEXT DEFAULT NULL,
      qualification_json LONGTEXT DEFAULT NULL,
      last_synced_at DATETIME DEFAULT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uniq_client_phone (client_id, phone),
      KEY idx_score (score),
      KEY idx_stage (stage),
      KEY idx_last_order (last_order_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${campaignsTable}\` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      name VARCHAR(191) NOT NULL,
      client_id VARCHAR(100) DEFAULT NULL,
      description TEXT DEFAULT NULL,
      filter_params_json LONGTEXT NOT NULL DEFAULT '{}',
      message_type VARCHAR(20) NOT NULL DEFAULT 'text',
      message_text TEXT DEFAULT NULL,
      template_name VARCHAR(191) DEFAULT NULL,
      template_language VARCHAR(20) NOT NULL DEFAULT 'pt_BR',
      status VARCHAR(20) NOT NULL DEFAULT 'draft',
      total_in_list INT NOT NULL DEFAULT 0,
      total_sent INT NOT NULL DEFAULT 0,
      total_delivered INT NOT NULL DEFAULT 0,
      total_read INT NOT NULL DEFAULT 0,
      total_failed INT NOT NULL DEFAULT 0,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_client_status (client_id, status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${campaignRecipientsTable}\` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      campaign_id BIGINT UNSIGNED NOT NULL,
      client_id VARCHAR(100) NOT NULL,
      phone VARCHAR(30) NOT NULL,
      lead_name VARCHAR(191) DEFAULT NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'pending',
      message_id VARCHAR(191) DEFAULT NULL,
      skip_reason VARCHAR(100) DEFAULT NULL,
      sent_at DATETIME DEFAULT NULL,
      delivered_at DATETIME DEFAULT NULL,
      read_at DATETIME DEFAULT NULL,
      failed_at DATETIME DEFAULT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uniq_campaign_phone (campaign_id, phone),
      KEY idx_campaign (campaign_id),
      KEY idx_status (status),
      KEY idx_message_id (message_id)
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
      toMysqlDateTime(entry.acceptedAt),
      deliveredAt,
      readAt,
      failedAt,
      toMysqlDateTime(entry.lastWebhookAt),
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

async function fetchTrackedMessages(limit = 50) {
  const normalizedLimit = Math.max(1, Number(limit || 50));
  const pool = await getDbPool();
  if (!pool) {
    return messageTracker.slice(0, normalizedLimit);
  }

  const trackerTable = getTableName("message_tracker");
  const queueTable = getTableName("queue_jobs");
  const [rows] = await pool.execute(
    `SELECT
        tracker.id,
        tracker.queue_job_id,
        tracker.recipient_id,
        tracker.latest_status,
        tracker.type,
        tracker.template_name,
        tracker.api_accepted,
        tracker.accepted_at,
        tracker.delivered_at,
        tracker.read_at,
        tracker.failed_at,
        tracker.last_webhook_at,
        tracker.conversation_id,
        tracker.pricing_category,
        tracker.pricing_billable,
        tracker.wa_id,
        tracker.input_phone,
        tracker.metadata_json,
        tracker.errors_json,
        tracker.created_at,
        tracker.updated_at,
        queue.automation_id,
        queue.lead_id,
        queue.event_name,
        queue.flow_name,
        queue.flow_classification,
        queue.scheduled_for,
        queue.status AS queue_status,
        queue.attempts,
        queue.remote_message_id,
        queue.accepted_status,
        queue.processed_at,
        queue.last_error
      FROM \`${trackerTable}\` tracker
      LEFT JOIN \`${queueTable}\` queue ON queue.id = tracker.queue_job_id
      ORDER BY COALESCE(tracker.read_at, tracker.delivered_at, tracker.failed_at, tracker.accepted_at, tracker.updated_at, tracker.created_at) DESC
      LIMIT ?`,
    [normalizedLimit]
  );

  return rows.map((row) => ({
    id: row.id,
    queueJobId: row.queue_job_id,
    recipientId: row.recipient_id,
    latestStatus: row.latest_status,
    type: row.type,
    templateName: row.template_name,
    apiAccepted: Boolean(row.api_accepted),
    acceptedAt: row.accepted_at,
    deliveredAt: row.delivered_at,
    readAt: row.read_at,
    failedAt: row.failed_at,
    lastWebhookAt: row.last_webhook_at,
    conversationId: row.conversation_id,
    pricingCategory: row.pricing_category,
    pricingBillable: row.pricing_billable === null ? null : Boolean(row.pricing_billable),
    waId: row.wa_id,
    input: row.input_phone,
    metadata: safeJsonParse(row.metadata_json),
    errors: safeJsonParse(row.errors_json),
    automationId: row.automation_id,
    leadId: row.lead_id,
    eventName: row.event_name,
    flowName: row.flow_name,
    flowClassification: row.flow_classification,
    scheduledFor: row.scheduled_for,
    queueStatus: row.queue_status,
    attempts: row.attempts,
    remoteMessageId: row.remote_message_id,
    acceptedStatus: row.accepted_status,
    processedAt: row.processed_at,
    lastError: row.last_error,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }));
}

function safeJsonParse(value) {
  if (!value || typeof value !== "string") {
    return null;
  }

  try {
    return JSON.parse(value);
  } catch (_error) {
    return null;
  }
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
  // ── Verifica se conversa está em atendimento humano ──────────────────
  if (isMySqlQueueEnabled()) {
    const inHuman = await isPhoneInHumanAttendance(message.from).catch(() => false);
    if (inHuman) {
      addLog("assistant_skip", "Conversa em atendimento humano — IA silenciada.", { from: message.from });
      return;
    }
  }
  // ── MODO 1: IA nativa na VPS (Gemini) ────────────────────────────────
  if (geminiApiKey && isMySqlQueueEnabled()) {
    try {
      const tenant = await resolveTenantForPhone(message.from);
      if (!tenant) {
        addLog("assistant_skip", "Nenhum tenant encontrado para o numero. Tentando fallback WP.", { from: message.from });
        return _handleWithWordPress(message);
      }
      const allAgents = await getAgents();
      const userText = getIncomingMessageText(message);
      const agent = resolveAgentForTenant(tenant.client_id, allAgents, userText);
      if (!agent) {
        addLog("assistant_skip", "Nenhum agente configurado para o tenant.", { clientId: tenant.client_id });
        return _handleWithWordPress(message);
      }
      const leadContext = await fetchLeadContextFromWP(tenant, message.from).catch(() => null);
      const history = await getConversationHistory(tenant.client_id, message.from, 10);
      const knowledgeItems = getRelevantKnowledge(await getKnowledgeItems(agent.id), userText);
      // ── Camada de intenção + ferramentas ──────────────────────────
      const convState  = await getConvState(message.from, tenant.client_id);
      const intent     = detectIntent(userText);
      const tools      = await executeTools(intent, userText, tenant, leadContext, convState);
      addLog("assistant_tools", "Intenção detectada e ferramentas executadas.", {
        from: message.from, intent,
        productsFound: tools.products?.length || 0,
        ordersFound: tools.orders?.length || 0,
        needsEmail: tools.needs_email || false,
      });
      // Salva estado se precisar de e-mail (aguardando verificação)
      if (tools.needs_email && convState.awaiting !== "email_for_orders") {
        await setConvState(message.from, tenant.client_id, { awaiting: "email_for_orders" });
      }
      // Se o cliente está aguardando verificação e a mensagem parece um e-mail
      if (convState.awaiting === "email_for_orders" && /\S+@\S+\.\S+/.test(userText)) {
        const emailMatch = userText.match(/[\w.-]+@[\w.-]+\.[a-z]{2,}/i);
        if (emailMatch) {
          const verif = await verifyCustomer(tenant, message.from, emailMatch[0]).catch(() => null);
          if (verif?.verified) {
            await setConvState(message.from, tenant.client_id, {
              verified_email: emailMatch[0],
              verified_customer_id: verif.customer_id || null,
              verified_at: new Date().toISOString().slice(0, 19).replace("T", " "),
              awaiting: null,
            });
            const orderData = await getOrderTracking(tenant, message.from, emailMatch[0]).catch(() => null);
            if (orderData?.found) tools.orders = orderData.orders;
            else tools.no_orders = true;
            tools.needs_email = false;
          } else {
            tools.needs_email = false;
            tools.email_not_found = true;
          }
        }
      }
      const systemPrompt = buildPersonalizedPrompt(agent, leadContext, knowledgeItems, tools);
      let audioBase64 = null;
      let audioMimeType = null;
      if (message.type === "audio" && message.audioId) {
        const audio = await downloadWhatsAppMedia(message.audioId).catch(() => null);
        if (audio) { audioBase64 = audio.base64; audioMimeType = audio.mimeType; }
      }
      const reply = await callGeminiDirect(geminiApiKey, systemPrompt, history, userText, audioBase64, audioMimeType);
      if (!reply) {
        addLog("assistant_skip", "Gemini nao retornou resposta.", { from: message.from });
        return;
      }
      const storedUserMsg = audioBase64 ? "[áudio]" : (userText || "[mensagem]");
      await saveConversationMessage(tenant.client_id, message.from, "user", storedUserMsg, agent.id);
      await saveConversationMessage(tenant.client_id, message.from, "assistant", reply, agent.id);
      const transferKeywords = safeJsonParse(agent.transfer_keywords_json) || [];
      const shouldTransfer = transferKeywords.some((kw) => (userText || "").toLowerCase().includes(kw.toLowerCase()));
      await sendTextMessagesInSequence(message.from, chunkMessage(reply, 1000));
      if (shouldTransfer && agent.transfer_number && String(agent.transfer_number) !== String(message.from)) {
        await sendTextMessagesInSequence(agent.transfer_number, [
          `🔔 Cliente ${message.from}${leadContext?.name ? ` (${leadContext.name})` : ""} solicitou atendimento humano.\nMensagem: "${userText}"`,
        ]);
      }
      addLog("assistant_reply", "Assistente VPS (Gemini) respondeu.", {
        from: message.from, clientId: tenant.client_id, agent: agent.name,
      });
      return;
    } catch (error) {
      addLog("assistant_error", "Falha no assistente VPS. Tentando fallback WP.", error.response?.data || error.message);
    }
  }
  // ── MODO 2: Fallback WordPress ────────────────────────────────────────
  return _handleWithWordPress(message);
}

async function _handleWithWordPress(message) {
  if (!wordpressAssistantUrl || !wordpressAssistantToken) return;
  try {
    const assistant = await requestWordPressAssistantReply(message);
    if (!assistant?.chunks?.length && !assistant?.handoff?.alert_message) {
      addLog("assistant_skip", "Assistente WP nao retornou mensagens.", { from: message.from, mode: assistant?.mode || null });
      return;
    }
    if (Array.isArray(assistant.chunks) && assistant.chunks.length > 0) {
      await sendTextMessagesInSequence(message.from, assistant.chunks);
    }
    if (
      assistant.handoff?.notify_human && assistant.handoff?.number &&
      assistant.handoff?.alert_message && String(assistant.handoff.number) !== String(message.from)
    ) {
      await sendTextMessagesInSequence(assistant.handoff.number, [assistant.handoff.alert_message]);
    }
    addLog("assistant_reply", "Assistente WP respondeu.", {
      from: message.from, chunks: assistant.chunks?.length || 0, handoffNumber: assistant.handoff?.number || null,
    });
  } catch (error) {
    addLog("assistant_error", "Falha no assistente WP.", error.response?.data || error.message);
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

function buildTemplateComponents({
  bodyText,
  footerText,
  bodyExamples,
  headerFormat,
  headerText,
  headerExampleHandle,
}) {
  const components = [];

  const normalizedHeaderFormat = String(headerFormat || "NONE").trim().toUpperCase();
  if (normalizedHeaderFormat === "TEXT" && headerText) {
    components.push({
      type: "HEADER",
      format: "TEXT",
      text: String(headerText).trim(),
    });
  } else if (["IMAGE", "VIDEO", "DOCUMENT"].includes(normalizedHeaderFormat)) {
    const headerComponent = {
      type: "HEADER",
      format: normalizedHeaderFormat,
    };

    if (headerExampleHandle) {
      headerComponent.example = {
        header_handle: [String(headerExampleHandle).trim()],
      };
    }

    components.push(headerComponent);
  }

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
  headerFormat,
  headerText,
  headerExampleHandle,
  allowCategoryChange,
}) {
  const response = await axios.post(
    `${baseUrl}/${wabaId}/message_templates`,
    {
      name,
      category,
      language,
      allow_category_change: Boolean(allowCategoryChange),
      components: buildTemplateComponents({
        bodyText,
        footerText,
        bodyExamples,
        headerFormat,
        headerText,
        headerExampleHandle,
      }),
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

// =====================================================================
// SEÇÃO: MULTI-TENANT, IA GEMINI E CAMPANHAS
// =====================================================================

// ── Utilitário ────────────────────────────────────────────────────────
function chunkMessage(text, maxLen = 1000) {
  if (!text || text.length <= maxLen) return [String(text)].filter(Boolean);
  const chunks = [];
  let start = 0;
  while (start < text.length) {
    let end = start + maxLen;
    if (end < text.length) {
      const lastNewline = text.lastIndexOf("\n", end);
      const lastPeriod = text.lastIndexOf(". ", end);
      if (lastNewline > start + 100) end = lastNewline + 1;
      else if (lastPeriod > start + 100) end = lastPeriod + 2;
    }
    const part = text.slice(start, Math.min(end, text.length)).trim();
    if (part) chunks.push(part);
    start = end;
  }
  return chunks.filter(Boolean);
}

// ── CRUD: Tenants ──────────────────────────────────────────────────────
async function getTenants() {
  const pool = await getDbPool();
  if (!pool) return [];
  const [rows] = await pool.query(`SELECT * FROM \`${getTableName("tenants")}\` ORDER BY created_at ASC`);
  return rows;
}

async function upsertTenant({ clientId, wpUrl, apiKey, active = 1 }) {
  const pool = await getDbPool();
  if (!pool) throw new Error("DB unavailable");
  await pool.execute(
    `INSERT INTO \`${getTableName("tenants")}\` (client_id, wp_url, api_key, active)
     VALUES (?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE wp_url = VALUES(wp_url), api_key = VALUES(api_key), active = VALUES(active)`,
    [clientId, wpUrl, apiKey, active ? 1 : 0]
  );
}

async function deleteTenant(clientId) {
  const pool = await getDbPool();
  if (!pool) throw new Error("DB unavailable");
  await pool.execute(`DELETE FROM \`${getTableName("tenants")}\` WHERE client_id = ?`, [clientId]);
}

async function getTenantById(clientId) {
  const pool = await getDbPool();
  if (!pool) return null;
  const [rows] = await pool.execute(
    `SELECT * FROM \`${getTableName("tenants")}\` WHERE client_id = ? AND active = 1`,
    [clientId]
  );
  return rows[0] || null;
}

// ── CRUD: Agentes ──────────────────────────────────────────────────────
async function getAgents() {
  const pool = await getDbPool();
  if (!pool) return [];
  const [rows] = await pool.query(`SELECT * FROM \`${getTableName("agents")}\` ORDER BY id ASC`);
  return rows;
}

async function upsertAgent(data) {
  const pool = await getDbPool();
  if (!pool) throw new Error("DB unavailable");
  const { id, name, prompt, keywords, assignedDomains, transferKeywords, transferNumber, icon, description, active } = data;
  if (id) {
    await pool.execute(
      `UPDATE \`${getTableName("agents")}\`
       SET name=?, prompt=?, keywords_json=?, assigned_domains_json=?, transfer_keywords_json=?,
           transfer_number=?, icon=?, description=?, active=?
       WHERE id=?`,
      [
        name, prompt,
        keywords ? JSON.stringify(keywords) : null,
        assignedDomains ? JSON.stringify(assignedDomains) : null,
        transferKeywords ? JSON.stringify(transferKeywords) : null,
        transferNumber || null, icon || "🤖", description || null,
        active !== false ? 1 : 0, id,
      ]
    );
  } else {
    await pool.execute(
      `INSERT INTO \`${getTableName("agents")}\`
       (name, prompt, keywords_json, assigned_domains_json, transfer_keywords_json, transfer_number, icon, description, active)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        name, prompt,
        keywords ? JSON.stringify(keywords) : null,
        assignedDomains ? JSON.stringify(assignedDomains) : null,
        transferKeywords ? JSON.stringify(transferKeywords) : null,
        transferNumber || null, icon || "🤖", description || null,
        active !== false ? 1 : 0,
      ]
    );
  }
}

async function deleteAgent(id) {
  const pool = await getDbPool();
  if (!pool) throw new Error("DB unavailable");
  await pool.execute(`DELETE FROM \`${getTableName("agents")}\` WHERE id = ?`, [id]);
}

// ── CRUD: Knowledge Base ───────────────────────────────────────────────
async function getKnowledgeItems(agentId = null) {
  const pool = await getDbPool();
  if (!pool) return [];
  if (agentId) {
    const [rows] = await pool.execute(
      `SELECT * FROM \`${getTableName("knowledge_base")}\` WHERE (agent_id = ? OR agent_id IS NULL) AND active = 1 ORDER BY id ASC`,
      [agentId]
    );
    return rows;
  }
  const [rows] = await pool.query(`SELECT * FROM \`${getTableName("knowledge_base")}\` ORDER BY id ASC`);
  return rows;
}

async function upsertKnowledgeItem(data) {
  const pool = await getDbPool();
  if (!pool) throw new Error("DB unavailable");
  const { id, agentId, title, content, keywords, active } = data;
  if (id) {
    await pool.execute(
      `UPDATE \`${getTableName("knowledge_base")}\` SET agent_id=?, title=?, content=?, keywords_json=?, active=? WHERE id=?`,
      [agentId || null, title, content, keywords ? JSON.stringify(keywords) : null, active !== false ? 1 : 0, id]
    );
  } else {
    await pool.execute(
      `INSERT INTO \`${getTableName("knowledge_base")}\` (agent_id, title, content, keywords_json, active) VALUES (?, ?, ?, ?, ?)`,
      [agentId || null, title, content, keywords ? JSON.stringify(keywords) : null, active !== false ? 1 : 0]
    );
  }
}

async function deleteKnowledgeItem(id) {
  const pool = await getDbPool();
  if (!pool) throw new Error("DB unavailable");
  await pool.execute(`DELETE FROM \`${getTableName("knowledge_base")}\` WHERE id = ?`, [id]);
}

// ── CRUD: Conversas ────────────────────────────────────────────────────
async function getConversationHistory(clientId, phone, limit = 10) {
  const pool = await getDbPool();
  if (!pool) return [];
  const [rows] = await pool.execute(
    `SELECT role, message, agent_id, created_at FROM \`${getTableName("conversations")}\`
     WHERE client_id = ? AND phone = ?
     ORDER BY created_at DESC LIMIT ?`,
    [clientId, phone, Number(limit)]
  );
  return rows.reverse();
}

async function saveConversationMessage(clientId, phone, role, message, agentId = null) {
  const pool = await getDbPool();
  if (!pool) return;
  await pool.execute(
    `INSERT INTO \`${getTableName("conversations")}\` (client_id, phone, role, message, agent_id, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
    [clientId, phone, role, message, agentId || null, nowMysql()]
  );
}

async function clearConversationHistory(phone, clientId = null) {
  const pool = await getDbPool();
  if (!pool) return;
  if (clientId) {
    await pool.execute(`DELETE FROM \`${getTableName("conversations")}\` WHERE phone = ? AND client_id = ?`, [phone, clientId]);
  } else {
    await pool.execute(`DELETE FROM \`${getTableName("conversations")}\` WHERE phone = ?`, [phone]);
  }
}

// ── IA: Resolução de tenant ────────────────────────────────────────────
async function resolveTenantForPhone(phone) {
  const pool = await getDbPool();
  if (!pool) return null;
  const mapTable = getTableName("phone_tenant_map");
  const [cached] = await pool.execute(`SELECT client_id FROM \`${mapTable}\` WHERE phone = ?`, [phone]);
  if (cached.length > 0) {
    return getTenantById(cached[0].client_id);
  }
  const tenants = await getTenants();
  for (const tenant of tenants.filter((t) => t.active)) {
    try {
      const ctx = await fetchLeadContextFromWP(tenant, phone);
      if (ctx && ctx.found) {
        await pool.execute(
          `INSERT INTO \`${mapTable}\` (phone, client_id, lead_name) VALUES (?, ?, ?)
           ON DUPLICATE KEY UPDATE client_id=VALUES(client_id), lead_name=VALUES(lead_name)`,
          [phone, tenant.client_id, ctx.name || null]
        );
        return tenant;
      }
    } catch (_) { /* continua para o próximo tenant */ }
  }
  return null;
}

// ── IA: Resolução de agente ────────────────────────────────────────────
function resolveAgentForTenant(clientId, allAgents, messageText) {
  const active = allAgents.filter((a) => a.active);
  const tenantSpecific = active.filter((a) => {
    const domains = safeJsonParse(a.assigned_domains_json) || [];
    return domains.length > 0 && domains.includes(clientId);
  });
  const globals = active.filter((a) => {
    const domains = safeJsonParse(a.assigned_domains_json) || [];
    return domains.length === 0;
  });
  const candidates = tenantSpecific.length > 0 ? tenantSpecific : globals;
  if (!candidates.length) return null;
  const msg = (messageText || "").toLowerCase();
  for (const agent of candidates) {
    const keywords = safeJsonParse(agent.keywords_json) || [];
    if (keywords.some((kw) => msg.includes(kw.toLowerCase()))) return agent;
  }
  return candidates[0];
}

// ── IA: Contexto do lead no WordPress ─────────────────────────────────
async function fetchLeadContextFromWP(tenant, phone) {
  if (!tenant?.wp_url || !tenant?.api_key) return null;
  const response = await axios.get(
    `${String(tenant.wp_url).replace(/\/$/, "")}/wp-json/alethe-crm/v1/lead-context`,
    {
      params: { phone },
      headers: { Authorization: `Bearer ${tenant.api_key}` },
      timeout: 10000,
    }
  );
  return response.data || null;
}

// ── IA: Conhecimento relevante ─────────────────────────────────────────
function getRelevantKnowledge(items, messageText, limit = 5) {
  if (!items.length) return [];
  const msg = (messageText || "").toLowerCase();
  return items
    .filter((i) => i.active)
    .map((item) => {
      const keywords = safeJsonParse(item.keywords_json) || [];
      const score = keywords.filter((kw) => msg.includes(kw.toLowerCase())).length;
      return { item, score };
    })
    .sort((a, b) => b.score - a.score)
    .slice(0, limit)
    .map((e) => e.item);
}

// ── IA: Prompt personalizado ───────────────────────────────────────────
function buildPersonalizedPrompt(agent, lead, knowledgeItems, tools = {}) {
  let prompt = agent.prompt || "";

  // ── Perfil do lead (dados do WordPress) ──
  if (lead && lead.found) {
    prompt += "\n\n[PERFIL DO CLIENTE — use estas informações para personalizar TODA a conversa]\n";
    if (lead.name)  prompt += `Nome: ${lead.name}\n`;
    if (lead.score) prompt += `Engajamento: ${lead.score}/100\n`;
    if (lead.stage) prompt += `Estágio no funil: ${lead.stage}\n`;
    if (lead.utm_source) prompt += `Como chegou: ${lead.utm_source}${lead.utm_campaign ? ` / ${lead.utm_campaign}` : ""}\n`;
    if (lead.qualification?.profile) prompt += `Perfil: ${lead.qualification.profile}${lead.qualification.has_store ? " (tem loja)" : ""}\n`;

    prompt += "\n[HISTÓRICO DE COMPRAS]\n";
    if (Number(lead.total_orders) > 0) {
      prompt += `Total de pedidos: ${lead.total_orders}`;
      if (lead.total_spent) prompt += ` | Valor total: R$${lead.total_spent}`;
      prompt += "\n";
      if (lead.last_order_product) prompt += `Último produto comprado: ${lead.last_order_product}\n`;
      if (lead.last_order_date)    prompt += `Data da última compra: ${lead.last_order_date}\n`;
      if (lead.days_since_last_purchase != null) prompt += `Dias desde a última compra: ${lead.days_since_last_purchase} dias\n`;
    } else {
      prompt += "Este cliente ainda NÃO realizou nenhuma compra. Seu objetivo é ajudá-lo a fazer a primeira compra.\n";
    }

    prompt += "\n[COMPORTAMENTO RECENTE]\n";
    if (lead.cart_abandoned)   prompt += "⚠️ Tem carrinho abandonado — iniciou o checkout mas não finalizou.\n";
    if (lead.visited_checkout) prompt += "⚠️ Visitou a página de finalizar compra mas não pagou.\n";
    if (Array.isArray(lead.visited_pages) && lead.visited_pages.length) {
      prompt += `Produtos visitados: ${lead.visited_pages.slice(0, 5).join(", ")}\n`;
    }
    // Timeline completa de navegação (últimas 10 ações)
    if (Array.isArray(lead.timeline) && lead.timeline.length > 0) {
      prompt += "\n[HISTÓRICO DE NAVEGAÇÃO — do mais recente ao mais antigo]\n";
      lead.timeline.slice(0, 10).forEach((item) => {
        prompt += `• ${item.date} — ${item.action}\n`;
      });
    }
  } else {
    prompt += "\n\n[CLIENTE]\nCliente ainda não identificado no sistema. Tente entender o que ele busca.\n";
  }

  // ── Base de conhecimento ──
  if (knowledgeItems.length > 0) {
    prompt += "\n[BASE DE CONHECIMENTO — use como referência para responder com precisão]\n";
    for (const item of knowledgeItems) {
      prompt += `### ${item.title}\n${item.content}\n\n`;
    }
  }

  // ── Resultados das ferramentas (dados em tempo real) ──
  if (tools.products?.length) {
    prompt += "\n[PRODUTOS ENCONTRADOS — dados reais do WooCommerce, use estes e não invente]\n";
    tools.products.forEach((p) => {
      prompt += `• ${p.name} — ${p.price_formatted}`;
      if (p.on_sale && p.regular_price) prompt += ` (de ${p.regular_price})`;
      prompt += p.in_stock ? ` ✅ em estoque` : ` ❌ fora de estoque`;
      if (p.stock_quantity) prompt += ` (${p.stock_quantity} unidades)`;
      prompt += `\n  🔗 ${p.url}\n`;
      if (p.short_description) prompt += `  ${p.short_description.slice(0, 120)}\n`;
      if (p.variations?.length) {
        prompt += `  Variações disponíveis:\n`;
        p.variations.forEach((v) => { prompt += `    - ${v.name}: ${v.price_formatted}${v.in_stock ? "" : " (sem estoque)"}\n`; });
      }
    });
    prompt += "IMPORTANTE: Use exatamente os preços e links acima. Nunca invente preço.\n";
  }

  if (tools.orders?.length) {
    prompt += "\n[PEDIDOS DO CLIENTE — dados reais]\n";
    tools.orders.forEach((o) => {
      prompt += `• Pedido #${o.number} — ${o.status_label} — ${o.total} (${o.date})\n`;
      prompt += `  Itens: ${(o.items || []).join(", ")}\n`;
      prompt += `  Frete: ${o.shipping_method}\n`;
      if (o.tracking_code) {
        prompt += `  🚚 Rastreio: ${o.tracking_code}\n`;
        prompt += `  🔗 Link de rastreio: ${o.tracking_url}\n`;
      } else {
        prompt += `  Rastreio: ainda não disponível\n`;
      }
      if (o.payment_url) prompt += `  💳 Link de pagamento: ${o.payment_url}\n`;
    });
  }

  if (tools.no_orders) {
    prompt += "\n[PEDIDOS] Nenhum pedido encontrado para este cliente.\n";
  }

  if (tools.needs_email) {
    prompt += "\n[AÇÃO NECESSÁRIA] Para consultar pedidos e rastreio, peça o e-mail cadastrado do cliente para verificar a identidade antes de mostrar qualquer dado de pedido.\n";
  }

  if (tools.order_created) {
    prompt += `\n[PEDIDO CRIADO] Pedido #${tools.order_created.order_number} criado com sucesso!\nTotal: ${tools.order_created.total}\nItens: ${(tools.order_created.items || []).join(", ")}\n💳 Link de pagamento: ${tools.order_created.payment_url}\nDiga ao cliente que o pedido foi criado e envie o link para ele pagar.\n`;
  }

  // ── Instrução de contexto de conversa ──
  prompt += "\n[INSTRUÇÃO DE CONTEXTO]\nO histórico das últimas mensagens trocadas com este cliente aparece abaixo (do mais antigo ao mais recente). Leia tudo antes de responder para manter coerência e não repetir informações já ditas.\n";

  return prompt;
}

// ── IA: Chamada direta ao Gemini ───────────────────────────────────────
async function callGeminiDirect(apiKey, systemPrompt, history, userMessage, audioBase64 = null, audioMimeType = null) {
  const contents = [];
  for (const msg of history) {
    contents.push({
      role: msg.role === "assistant" ? "model" : "user",
      parts: [{ text: msg.message }],
    });
  }
  const userParts = [];
  if (audioBase64 && audioMimeType) {
    userParts.push({ inlineData: { mimeType: audioMimeType, data: audioBase64 } });
    userParts.push({ text: "Transcreva e responda ao áudio como se fosse uma mensagem de texto do cliente." });
  } else {
    userParts.push({ text: userMessage || " " });
  }
  contents.push({ role: "user", parts: userParts });
  const response = await axios.post(
    `https://generativelanguage.googleapis.com/v1beta/models/${geminiModel}:generateContent?key=${apiKey}`,
    {
      systemInstruction: { parts: [{ text: systemPrompt }] },
      contents,
      generationConfig: { temperature: 0.7, maxOutputTokens: 1024 },
    },
    { timeout: 30000 }
  );
  return response.data?.candidates?.[0]?.content?.parts?.[0]?.text || null;
}

// ── Atendimento humano ────────────────────────────────────────────────
async function isPhoneInHumanAttendance(phone) {
  const pool = await getDbPool();
  if (!pool) return false;
  const [rows] = await pool.execute(
    `SELECT 1 FROM \`${getTableName("human_attendance")}\` WHERE phone = ?`, [phone]
  );
  return rows.length > 0;
}

async function takeHumanAttendance(phone, clientId, attendantName) {
  const pool = await getDbPool();
  if (!pool) return;
  await pool.execute(
    `INSERT INTO \`${getTableName("human_attendance")}\` (phone, client_id, attendant_name)
     VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE attendant_name=VALUES(attendant_name), last_activity=NOW()`,
    [phone, clientId || null, attendantName || "Atendente"]
  );
}

async function releaseHumanAttendance(phone) {
  const pool = await getDbPool();
  if (!pool) return;
  await pool.execute(`DELETE FROM \`${getTableName("human_attendance")}\` WHERE phone = ?`, [phone]);
}

async function getRecentConversations(clientId = null, limit = 50) {
  const pool = await getDbPool();
  if (!pool) return [];
  const conversationsTable = getTableName("conversations");
  const attendanceTable    = getTableName("human_attendance");
  let sql = `
    SELECT
      c.client_id, c.phone,
      MAX(c.created_at) AS last_message_at,
      COUNT(*) AS total_messages,
      SUM(c.role = 'user') AS user_messages,
      (SELECT LEFT(message,120) FROM \`${conversationsTable}\` c2
       WHERE c2.phone = c.phone AND c2.client_id = c.client_id
       ORDER BY created_at DESC LIMIT 1) AS last_message,
      (SELECT role FROM \`${conversationsTable}\` c3
       WHERE c3.phone = c.phone AND c3.client_id = c.client_id
       ORDER BY created_at DESC LIMIT 1) AS last_role,
      m.lead_name,
      ha.attendant_name,
      IF(ha.phone IS NOT NULL, 1, 0) AS human_mode
    FROM \`${conversationsTable}\` c
    LEFT JOIN \`${getTableName("phone_tenant_map")}\` m ON m.phone = c.phone
    LEFT JOIN \`${attendanceTable}\` ha ON ha.phone = c.phone`;
  const params = [];
  if (clientId) { sql += " WHERE c.client_id = ?"; params.push(clientId); }
  sql += " GROUP BY c.client_id, c.phone ORDER BY last_message_at DESC LIMIT ?";
  params.push(Number(limit));
  const [rows] = await pool.execute(sql, params);
  return rows;
}

// ── Estado de conversa (intenção, carrinho, verificação) ──────────────
async function getConvState(phone, clientId) {
  const pool = await getDbPool();
  if (!pool) return {};
  const [rows] = await pool.execute(
    `SELECT * FROM \`${getTableName("conversation_state")}\` WHERE phone=? AND client_id=?`,
    [phone, clientId]
  );
  const row = rows[0] || {};
  return {
    ...row,
    cart: safeJsonParse(row.cart_json) || [],
    context: safeJsonParse(row.context_json) || {},
  };
}

async function setConvState(phone, clientId, patch) {
  const pool = await getDbPool();
  if (!pool) return;
  const fields = {};
  if ('verified_email' in patch) fields.verified_email = patch.verified_email;
  if ('verified_customer_id' in patch) fields.verified_customer_id = patch.verified_customer_id;
  if ('verified_at' in patch) fields.verified_at = patch.verified_at;
  if ('awaiting' in patch) fields.awaiting = patch.awaiting;
  if ('last_order_id' in patch) fields.last_order_id = patch.last_order_id;
  if ('cart' in patch) fields.cart_json = JSON.stringify(patch.cart);
  if ('context' in patch) fields.context_json = JSON.stringify(patch.context);
  if (Object.keys(fields).length === 0) return;
  const setClauses = Object.keys(fields).map(k => `\`${k}\` = ?`).join(', ');
  const values = [...Object.values(fields), phone, clientId];
  await pool.execute(
    `INSERT INTO \`${getTableName("conversation_state")}\` (phone, client_id, ${Object.keys(fields).join(', ')})
     VALUES (?, ?, ${Object.keys(fields).map(() => '?').join(', ')})
     ON DUPLICATE KEY UPDATE ${setClauses}`,
    [phone, clientId, ...Object.values(fields), ...Object.values(fields)]
  );
}

async function clearConvState(phone, clientId) {
  const pool = await getDbPool();
  if (!pool) return;
  await pool.execute(
    `DELETE FROM \`${getTableName("conversation_state")}\` WHERE phone=? AND client_id=?`,
    [phone, clientId]
  );
}

// ── Detecção de intenção ────────────────────────────────────────────────
function detectIntent(message) {
  const m = (message || "").toLowerCase();
  // Rastreio / pedido
  if (/pedido|rastreio|rastreamento|entrega|chegou|cad[eê]|onde[. ]+est[aá]|status.*pedido|meu.*pedido|acompanhar|prazo.*entrega/i.test(m)) {
    return "order_inquiry";
  }
  // Intenção de compra clara
  if (/quero\s+(comprar|pedir|encomendar)|quero\s+\d|adicionar|finalizar.*compra|checkout|pagar|uni(dade|des)|monte.*pedido|faz.*pedido|coloca.*pedido/i.test(m)) {
    return "purchase_intent";
  }
  // Consulta de produto
  if (/pre[çc]o|quanto\s+custa|valor|tem\s+.{1,30}(disponível|disponivel|em\s+estoque)|estoque|produto|perfume|kit|quanto\s+é|quanto\s+ta|quanto\s+tá|ml\b|envia|link\s+(do|da)|me\s+manda\s+o\s+link/i.test(m)) {
    return "product_inquiry";
  }
  return "conversational";
}

// ── Funções que chamam as ferramentas no WordPress ──────────────────────
async function wpToolCall(tenant, path, method = "GET", body = null) {
  if (!tenant?.wp_url || !tenant?.api_key) return null;
  try {
    const opts = {
      method,
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${tenant.api_key}` },
      timeout: 12000,
    };
    if (body && method !== "GET") opts.data = body;
    const url = `${String(tenant.wp_url).replace(/\/$/, "")}/wp-json/alethe-crm/v1${path}`;
    const response = method === "GET"
      ? await axios.get(url, { headers: opts.headers, timeout: opts.timeout })
      : await axios.post(url, body, { headers: opts.headers, timeout: opts.timeout });
    return response.data;
  } catch (_) { return null; }
}

async function searchProducts(tenant, query) {
  if (!query || query.length < 2) return null;
  return wpToolCall(tenant, `/product-search?q=${encodeURIComponent(query)}&limit=5`);
}

async function getOrderTracking(tenant, phone, email) {
  const params = [];
  if (phone) params.push(`phone=${encodeURIComponent(phone)}`);
  if (email) params.push(`email=${encodeURIComponent(email)}`);
  if (!params.length) return null;
  return wpToolCall(tenant, `/order-tracking?${params.join("&")}`);
}

async function verifyCustomer(tenant, phone, email) {
  return wpToolCall(tenant, "/customer-verify", "POST", { phone, email });
}

async function createOrder(tenant, phone, email, name, items) {
  return wpToolCall(tenant, "/create-order", "POST", { phone, email, name, items });
}

// ── Extrai possíveis nomes de produtos da mensagem (heurística simples) ──
function extractProductQuery(message) {
  const m = (message || "").toLowerCase();
  const cleaned = m
    // Remove intenção
    .replace(/\b(pre[çc]o|quanto\s+custa|quanto\s+[ée]|quanto\s+t[áa]|custa|valor|tem\s+disponível|tem\s+disponivel|quero|me\s+manda|link|enviar?|informa[çc][aã]o|qual|quais|como|sobre|falar\s+sobre|ver|me\s+mostra|me\s+diz|me\s+fala|me\s+passa|me\s+d[áa])\b/gi, "")
    // Remove saudações
    .replace(/\b(oi|ol[áa]|boa\s+tarde|boa\s+noite|bom\s+dia|tudo\s+bem|pfv|pf|por\s+favor|obrigad[oa])\b/gi, "")
    // Remove artigos e preposições
    .replace(/\b(o|a|os|as|um|uma|uns|umas|do|da|de|dos|das|em|no|na|nos|nas|por|para|com|sem|que|[ée]|eu|voc[eê]|me|meu|minha|seu|sua)\b/gi, "")
    // Remove pontuação e espaços extras
    .replace(/[?!.,;:]/g, "")
    .replace(/\s+/g, " ")
    .trim();
  return cleaned.slice(0, 60);
}

// ── Executa ferramentas baseado na intenção ────────────────────────────
async function executeTools(intent, message, tenant, leadContext, convState) {
  const results = { intent };

  if (intent === "product_inquiry" || intent === "purchase_intent") {
    const query = extractProductQuery(message);
    if (query.length >= 2) {
      const data = await searchProducts(tenant, query);
      if (data?.products?.length) results.products = data.products;
    }
  }

  if (intent === "order_inquiry") {
    // Verifica se o cliente já confirmou identidade
    if (convState.verified_email || leadContext?.email) {
      const email = convState.verified_email || leadContext?.email;
      const phone = leadContext?.phone || null;
      const data = await getOrderTracking(tenant, phone, email);
      if (data?.found) {
        results.orders = data.orders;
      } else {
        results.no_orders = true;
      }
    } else {
      // Precisa verificar identidade primeiro
      results.needs_email = true;
    }
  }

  return results;
}

// ── Campanha: Sync de leads do WordPress ──────────────────────────────
async function syncLeadsFromWordPress(tenant) {
  const pool = await getDbPool();
  if (!pool || !tenant?.wp_url) return { synced: 0 };
  let page = 1;
  let total = 0;
  const cacheTable = getTableName("leads_cache");
  while (true) {
    const response = await axios.get(
      `${String(tenant.wp_url).replace(/\/$/, "")}/wp-json/alethe-crm/v1/leads`,
      {
        params: { page, limit: 200 },
        headers: { Authorization: `Bearer ${tenant.api_key}` },
        timeout: 30000,
      }
    );
    const leads = response.data?.leads || (Array.isArray(response.data) ? response.data : []);
    if (!Array.isArray(leads) || leads.length === 0) break;
    for (const lead of leads) {
      if (!lead.phone) continue;
      await pool.execute(
        `INSERT INTO \`${cacheTable}\`
          (client_id, phone, email, name, score, stage, utm_source, utm_medium, utm_campaign,
           cart_abandoned, total_orders, last_order_date, last_order_product, total_spent,
           days_since_last_purchase, visited_pages_json, qualification_json, last_synced_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
          email=VALUES(email), name=VALUES(name), score=VALUES(score), stage=VALUES(stage),
          utm_source=VALUES(utm_source), utm_medium=VALUES(utm_medium), utm_campaign=VALUES(utm_campaign),
          cart_abandoned=VALUES(cart_abandoned), total_orders=VALUES(total_orders),
          last_order_date=VALUES(last_order_date), last_order_product=VALUES(last_order_product),
          total_spent=VALUES(total_spent), days_since_last_purchase=VALUES(days_since_last_purchase),
          visited_pages_json=VALUES(visited_pages_json), qualification_json=VALUES(qualification_json),
          last_synced_at=VALUES(last_synced_at)`,
        [
          tenant.client_id, lead.phone, lead.email || null, lead.name || null,
          lead.score || 0, lead.stage || null, lead.utm_source || null,
          lead.utm_medium || null, lead.utm_campaign || null,
          lead.cart_abandoned ? 1 : 0, lead.total_orders || 0,
          lead.last_order_date || null, lead.last_order_product || null,
          lead.total_spent || 0, lead.days_since_last_purchase || null,
          lead.visited_pages ? JSON.stringify(lead.visited_pages) : null,
          lead.qualification ? JSON.stringify(lead.qualification) : null,
          nowMysql(),
        ]
      );
      total++;
    }
    if (leads.length < 200) break;
    page++;
  }
  return { synced: total };
}

// ── Campanha: Montar lista com filtros ─────────────────────────────────
async function buildCampaignList(campaignId) {
  const pool = await getDbPool();
  if (!pool) throw new Error("DB unavailable");
  const campaignsTable = getTableName("campaigns");
  const cacheTable = getTableName("leads_cache");
  const recipientsTable = getTableName("campaign_recipients");
  const [campaigns] = await pool.execute(`SELECT * FROM \`${campaignsTable}\` WHERE id = ?`, [campaignId]);
  const campaign = campaigns[0];
  if (!campaign) throw new Error("Campaign not found");
  const filters = safeJsonParse(campaign.filter_params_json) || {};
  const conditions = [];
  const params = [];
  if (campaign.client_id) { conditions.push("client_id = ?"); params.push(campaign.client_id); }
  if (filters.inactive_days) { conditions.push("(last_order_date IS NULL OR last_order_date <= DATE_SUB(CURDATE(), INTERVAL ? DAY))"); params.push(Number(filters.inactive_days)); }
  if (filters.abandoned_cart_days) { conditions.push("cart_abandoned = 1"); conditions.push("(last_order_date IS NULL OR last_order_date <= DATE_SUB(CURDATE(), INTERVAL ? DAY))"); params.push(Number(filters.abandoned_cart_days)); }
  if (filters.viewed_product_slug) { conditions.push("visited_pages_json LIKE ?"); params.push(`%${filters.viewed_product_slug}%`); }
  if (filters.never_purchased) conditions.push("total_orders = 0");
  if (filters.score_min != null) { conditions.push("score >= ?"); params.push(Number(filters.score_min)); }
  if (filters.score_max != null) { conditions.push("score <= ?"); params.push(Number(filters.score_max)); }
  if (filters.stage) { conditions.push("stage = ?"); params.push(filters.stage); }
  if (filters.utm_source) { conditions.push("utm_source = ?"); params.push(filters.utm_source); }
  if (filters.min_orders) { conditions.push("total_orders >= ?"); params.push(Number(filters.min_orders)); }
  if (filters.min_spent != null) { conditions.push("total_spent >= ?"); params.push(Number(filters.min_spent)); }
  if (filters.max_spent != null) { conditions.push("total_spent <= ?"); params.push(Number(filters.max_spent)); }
  const where = conditions.length ? " WHERE " + conditions.join(" AND ") : "";
  const [leads] = await pool.execute(`SELECT * FROM \`${cacheTable}\`${where} ORDER BY score DESC LIMIT 10000`, params);
  await pool.execute(`DELETE FROM \`${recipientsTable}\` WHERE campaign_id = ? AND status = 'pending'`, [campaignId]);
  for (const lead of leads) {
    await pool.execute(
      `INSERT IGNORE INTO \`${recipientsTable}\` (campaign_id, client_id, phone, lead_name, status) VALUES (?, ?, ?, ?, 'pending')`,
      [campaignId, lead.client_id, lead.phone, lead.name || null]
    );
  }
  await pool.execute(`UPDATE \`${campaignsTable}\` SET total_in_list = ? WHERE id = ?`, [leads.length, campaignId]);
  return { total: leads.length };
}

// ── Campanha: Enviar lote ──────────────────────────────────────────────
async function sendCampaignBatch(campaignId, limit = 50) {
  const pool = await getDbPool();
  if (!pool) throw new Error("DB unavailable");
  const campaignsTable = getTableName("campaigns");
  const recipientsTable = getTableName("campaign_recipients");
  const [campaigns] = await pool.execute(`SELECT * FROM \`${campaignsTable}\` WHERE id = ?`, [campaignId]);
  const campaign = campaigns[0];
  if (!campaign) throw new Error("Campaign not found");
  const [recipients] = await pool.execute(
    `SELECT * FROM \`${recipientsTable}\` WHERE campaign_id = ? AND status = 'pending' LIMIT ?`,
    [campaignId, Number(limit)]
  );
  let sent = 0;
  let failed = 0;
  for (const recipient of recipients) {
    try {
      let payload;
      if (campaign.message_type === "template" && campaign.template_name) {
        payload = {
          messaging_product: "whatsapp", to: recipient.phone, type: "template",
          template: { name: campaign.template_name, language: { code: campaign.template_language || "pt_BR" } },
        };
      } else if (campaign.message_text) {
        payload = {
          messaging_product: "whatsapp", to: recipient.phone, type: "text",
          text: { body: campaign.message_text, preview_url: false },
        };
      } else { continue; }
      const data = await sendWhatsAppRequest(payload);
      const messageId = data.messages?.[0]?.id || null;
      await pool.execute(
        `UPDATE \`${recipientsTable}\` SET status = 'sent', message_id = ?, sent_at = ? WHERE id = ?`,
        [messageId, nowMysql(), recipient.id]
      );
      if (messageId) {
        upsertTrackedMessage({ id: messageId, type: campaign.message_type === "template" ? "template" : "text",
          to: recipient.phone, templateName: campaign.template_name || null,
          acceptedAt: new Date().toISOString(), latestStatus: "accepted", apiAccepted: true });
      }
      sent++;
    } catch (_err) {
      await pool.execute(`UPDATE \`${recipientsTable}\` SET status = 'failed', failed_at = ? WHERE id = ?`, [nowMysql(), recipient.id]);
      failed++;
    }
  }
  await refreshCampaignTotals(campaignId);
  const remaining = recipients.length - sent - failed;
  return { sent, failed, remaining };
}

async function refreshCampaignTotals(campaignId) {
  const pool = await getDbPool();
  if (!pool) return;
  const recipientsTable = getTableName("campaign_recipients");
  const campaignsTable = getTableName("campaigns");
  const [rows] = await pool.execute(
    `SELECT
       SUM(status IN ('sent','delivered','read')) AS total_sent,
       SUM(status = 'delivered') AS total_delivered,
       SUM(status = 'read') AS total_read,
       SUM(status = 'failed') AS total_failed
     FROM \`${recipientsTable}\` WHERE campaign_id = ?`,
    [campaignId]
  );
  const t = rows[0];
  await pool.execute(
    `UPDATE \`${campaignsTable}\` SET total_sent=?, total_delivered=?, total_read=?, total_failed=? WHERE id=?`,
    [t.total_sent || 0, t.total_delivered || 0, t.total_read || 0, t.total_failed || 0, campaignId]
  );
}

// ── Campanha: Atualizar status por webhook ─────────────────────────────
async function updateCampaignRecipientFromWebhook(messageId, status) {
  const pool = await getDbPool();
  if (!pool || !messageId) return;
  const recipientsTable = getTableName("campaign_recipients");
  const fieldMap = { delivered: "delivered_at", read: "read_at", failed: "failed_at" };
  const field = fieldMap[status];
  if (!field) return;
  const [updated] = await pool.execute(
    `UPDATE \`${recipientsTable}\` SET status = ?, ${field} = ? WHERE message_id = ? AND status != 'read'`,
    [status, nowMysql(), messageId]
  );
  if (updated.affectedRows === 0) return;
  const [rows] = await pool.execute(
    `SELECT DISTINCT campaign_id FROM \`${recipientsTable}\` WHERE message_id = ?`, [messageId]
  );
  for (const row of rows) await refreshCampaignTotals(row.campaign_id);
}

// =====================================================================
// FIM DA SEÇÃO MULTI-TENANT E CAMPANHAS
// =====================================================================

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
      void updateCampaignRecipientFromWebhook(status.id, status.status);
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
      headerFormat = "NONE",
      headerText = "",
      headerExampleHandle = "",
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
      headerFormat,
      headerText,
      headerExampleHandle,
      allowCategoryChange,
    });

    addLog("template_submission", `Template ${name} enviado para aprovacao.`, {
      wabaId,
      category,
      language,
      headerFormat,
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

router.get("/message-tracker", async (req, res) => {
  try {
    const limit = Number(req.query.limit || 50);
    const messages = await fetchTrackedMessages(limit);
    res.json({
      success: true,
      messages,
    });
  } catch (error) {
    addLog("tracker_error", "Falha ao consultar message tracker.", error.message);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

router.get("/logs", (_req, res) => {
  res.json({ success: true, logs });
});

// =====================================================================
// ROTAS: MULTI-TENANT, AGENTES, KNOWLEDGE, CAMPANHAS
// =====================================================================

// ── Tenants ────────────────────────────────────────────────────────────
router.get("/api/tenants", async (_req, res) => {
  try { res.json({ success: true, tenants: await getTenants() }); }
  catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.post("/api/tenants", async (req, res) => {
  try {
    const { clientId, wpUrl, apiKey, active } = req.body || {};
    if (!clientId || !wpUrl || !apiKey) return res.status(400).json({ success: false, error: "clientId, wpUrl e apiKey são obrigatórios." });
    await upsertTenant({ clientId, wpUrl, apiKey, active });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.delete("/api/tenants/:clientId", async (req, res) => {
  try { await deleteTenant(req.params.clientId); res.json({ success: true }); }
  catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// ── Agentes ────────────────────────────────────────────────────────────
router.get("/api/agents", async (_req, res) => {
  try { res.json({ success: true, agents: await getAgents() }); }
  catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.post("/api/agents", async (req, res) => {
  try {
    const { name, prompt } = req.body || {};
    if (!name || !prompt) return res.status(400).json({ success: false, error: "name e prompt são obrigatórios." });
    await upsertAgent(req.body);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.delete("/api/agents/:id", async (req, res) => {
  try { await deleteAgent(req.params.id); res.json({ success: true }); }
  catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// ── Knowledge Base ─────────────────────────────────────────────────────
router.get("/api/knowledge", async (req, res) => {
  try {
    const agentId = req.query.agent_id ? Number(req.query.agent_id) : null;
    res.json({ success: true, items: await getKnowledgeItems(agentId) });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.post("/api/knowledge", async (req, res) => {
  try {
    const { title, content } = req.body || {};
    if (!title || !content) return res.status(400).json({ success: false, error: "title e content são obrigatórios." });
    await upsertKnowledgeItem(req.body);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.delete("/api/knowledge/:id", async (req, res) => {
  try { await deleteKnowledgeItem(req.params.id); res.json({ success: true }); }
  catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// ── Conversas ──────────────────────────────────────────────────────────
router.get("/api/conversations/:phone", async (req, res) => {
  try {
    const clientId = req.query.client_id || "";
    const limit = Number(req.query.limit || 20);
    const history = clientId
      ? await getConversationHistory(clientId, req.params.phone, limit)
      : [];
    res.json({ success: true, history });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.delete("/api/conversations/:phone", async (req, res) => {
  try {
    await clearConversationHistory(req.params.phone, req.query.client_id || null);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// ── Sync de Leads do WordPress ─────────────────────────────────────────
router.post("/api/sync/leads/:clientId", async (req, res) => {
  try {
    const tenant = await getTenantById(req.params.clientId);
    if (!tenant) return res.status(404).json({ success: false, error: "Tenant não encontrado." });
    const result = await syncLeadsFromWordPress(tenant);
    addLog("leads_sync", `Sync de leads do tenant ${req.params.clientId} concluído.`, result);
    res.json({ success: true, ...result });
  } catch (e) {
    addLog("leads_sync_error", "Falha no sync de leads.", e.message);
    res.status(500).json({ success: false, error: e.message });
  }
});

router.get("/api/leads/:clientId", async (req, res) => {
  try {
    const pool = await getDbPool();
    if (!pool) return res.status(503).json({ success: false, error: "DB unavailable" });
    const [rows] = await pool.execute(
      `SELECT * FROM \`${getTableName("leads_cache")}\` WHERE client_id = ? ORDER BY score DESC LIMIT 500`,
      [req.params.clientId]
    );
    res.json({ success: true, leads: rows, total: rows.length });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// ── Campanhas ──────────────────────────────────────────────────────────
router.get("/api/campaigns", async (req, res) => {
  try {
    const pool = await getDbPool();
    if (!pool) return res.status(503).json({ success: false, error: "DB unavailable" });
    const clientId = req.query.client_id;
    let rows;
    if (clientId) {
      [rows] = await pool.execute(
        `SELECT * FROM \`${getTableName("campaigns")}\` WHERE client_id = ? OR client_id IS NULL ORDER BY created_at DESC`,
        [clientId]
      );
    } else {
      [rows] = await pool.query(`SELECT * FROM \`${getTableName("campaigns")}\` ORDER BY created_at DESC`);
    }
    res.json({ success: true, campaigns: rows });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.post("/api/campaigns", async (req, res) => {
  try {
    const pool = await getDbPool();
    if (!pool) return res.status(503).json({ success: false, error: "DB unavailable" });
    const { name, clientId, description, filterParams, messageType, messageText, templateName, templateLanguage } = req.body || {};
    if (!name) return res.status(400).json({ success: false, error: "name é obrigatório." });
    const id = req.body.id;
    if (id) {
      await pool.execute(
        `UPDATE \`${getTableName("campaigns")}\` SET name=?, client_id=?, description=?, filter_params_json=?,
         message_type=?, message_text=?, template_name=?, template_language=? WHERE id=?`,
        [name, clientId || null, description || null,
          JSON.stringify(filterParams || {}), messageType || "text",
          messageText || null, templateName || null, templateLanguage || "pt_BR", id]
      );
    } else {
      const [result] = await pool.execute(
        `INSERT INTO \`${getTableName("campaigns")}\` (name, client_id, description, filter_params_json, message_type, message_text, template_name, template_language)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [name, clientId || null, description || null,
          JSON.stringify(filterParams || {}), messageType || "text",
          messageText || null, templateName || null, templateLanguage || "pt_BR"]
      );
      res.json({ success: true, id: result.insertId });
      return;
    }
    res.json({ success: true });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.get("/api/campaigns/:id", async (req, res) => {
  try {
    const pool = await getDbPool();
    if (!pool) return res.status(503).json({ success: false, error: "DB unavailable" });
    const [rows] = await pool.execute(`SELECT * FROM \`${getTableName("campaigns")}\` WHERE id = ?`, [req.params.id]);
    if (!rows.length) return res.status(404).json({ success: false, error: "Campanha não encontrada." });
    res.json({ success: true, campaign: rows[0] });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.delete("/api/campaigns/:id", async (req, res) => {
  try {
    const pool = await getDbPool();
    if (!pool) return res.status(503).json({ success: false, error: "DB unavailable" });
    await pool.execute(`DELETE FROM \`${getTableName("campaign_recipients")}\` WHERE campaign_id = ?`, [req.params.id]);
    await pool.execute(`DELETE FROM \`${getTableName("campaigns")}\` WHERE id = ?`, [req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.post("/api/campaigns/:id/preview", async (req, res) => {
  try {
    const pool = await getDbPool();
    if (!pool) return res.status(503).json({ success: false, error: "DB unavailable" });
    const [campaigns] = await pool.execute(`SELECT * FROM \`${getTableName("campaigns")}\` WHERE id = ?`, [req.params.id]);
    const campaign = campaigns[0];
    if (!campaign) return res.status(404).json({ success: false, error: "Campanha não encontrada." });
    const filters = safeJsonParse(campaign.filter_params_json) || {};
    const conditions = [];
    const params = [];
    if (campaign.client_id) { conditions.push("client_id = ?"); params.push(campaign.client_id); }
    if (filters.inactive_days) { conditions.push("(last_order_date IS NULL OR last_order_date <= DATE_SUB(CURDATE(), INTERVAL ? DAY))"); params.push(Number(filters.inactive_days)); }
    if (filters.abandoned_cart_days) { conditions.push("cart_abandoned = 1"); }
    if (filters.viewed_product_slug) { conditions.push("visited_pages_json LIKE ?"); params.push(`%${filters.viewed_product_slug}%`); }
    if (filters.never_purchased) conditions.push("total_orders = 0");
    if (filters.score_min != null) { conditions.push("score >= ?"); params.push(Number(filters.score_min)); }
    if (filters.score_max != null) { conditions.push("score <= ?"); params.push(Number(filters.score_max)); }
    if (filters.stage) { conditions.push("stage = ?"); params.push(filters.stage); }
    if (filters.utm_source) { conditions.push("utm_source = ?"); params.push(filters.utm_source); }
    if (filters.min_orders) { conditions.push("total_orders >= ?"); params.push(Number(filters.min_orders)); }
    const where = conditions.length ? " WHERE " + conditions.join(" AND ") : "";
    const [rows] = await pool.execute(
      `SELECT phone, name, score, stage, total_orders, total_spent, last_order_date FROM \`${getTableName("leads_cache")}\`${where} ORDER BY score DESC LIMIT 50`,
      params
    );
    const [[{ total }]] = await pool.execute(`SELECT COUNT(*) AS total FROM \`${getTableName("leads_cache")}\`${where}`, params);
    res.json({ success: true, total, sample: rows });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.post("/api/campaigns/:id/build-list", async (req, res) => {
  try {
    const result = await buildCampaignList(req.params.id);
    res.json({ success: true, ...result });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.post("/api/campaigns/:id/send", async (req, res) => {
  try {
    validateConfig();
    const limit = Number(req.body?.limit || 50);
    const result = await sendCampaignBatch(req.params.id, limit);
    addLog("campaign_send", `Envio de campanha ${req.params.id} executado.`, result);
    res.json({ success: true, ...result });
  } catch (e) {
    addLog("campaign_error", "Falha ao enviar campanha.", e.message);
    res.status(500).json({ success: false, error: e.message });
  }
});

router.get("/api/campaigns/:id/results", async (req, res) => {
  try {
    const pool = await getDbPool();
    if (!pool) return res.status(503).json({ success: false, error: "DB unavailable" });
    const [campaign] = await pool.execute(`SELECT * FROM \`${getTableName("campaigns")}\` WHERE id = ?`, [req.params.id]);
    if (!campaign.length) return res.status(404).json({ success: false, error: "Campanha não encontrada." });
    const limit = Number(req.query.limit || 100);
    const [recipients] = await pool.execute(
      `SELECT id, phone, lead_name, status, message_id, sent_at, delivered_at, read_at, failed_at, skip_reason
       FROM \`${getTableName("campaign_recipients")}\`
       WHERE campaign_id = ? ORDER BY created_at DESC LIMIT ?`,
      [req.params.id, limit]
    );
    res.json({
      success: true,
      campaign: campaign[0],
      recipients,
      stats: {
        total_in_list: campaign[0].total_in_list,
        total_sent: campaign[0].total_sent,
        total_delivered: campaign[0].total_delivered,
        total_read: campaign[0].total_read,
        total_failed: campaign[0].total_failed,
        pending: campaign[0].total_in_list - campaign[0].total_sent - campaign[0].total_failed,
      },
    });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// ── Atendimento humano: listar conversas recentes ─────────────────────
router.get("/api/conversations-recent", async (req, res) => {
  try {
    const clientId = req.query.client_id || null;
    const limit    = Number(req.query.limit || 50);
    const rows     = await getRecentConversations(clientId, limit);
    res.json({ success: true, conversations: rows });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// Listar quem está em atendimento humano
router.get("/api/attendance", async (req, res) => {
  try {
    const pool = await getDbPool();
    if (!pool) return res.json({ success: true, attending: [] });
    const [rows] = await pool.query(`SELECT * FROM \`${getTableName("human_attendance")}\` ORDER BY last_activity DESC`);
    res.json({ success: true, attending: rows });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// Assumir atendimento de um número
router.post("/api/attendance/take", async (req, res) => {
  try {
    const { phone, clientId, attendantName } = req.body || {};
    if (!phone) return res.status(400).json({ success: false, error: "phone obrigatório" });
    await takeHumanAttendance(phone, clientId, attendantName);
    addLog("attendance_take", `Atendimento humano assumido para ${phone}.`, { attendantName });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// Liberar número de volta para a IA
router.post("/api/attendance/release", async (req, res) => {
  try {
    const { phone } = req.body || {};
    if (!phone) return res.status(400).json({ success: false, error: "phone obrigatório" });
    await releaseHumanAttendance(phone);
    addLog("attendance_release", `Conversa ${phone} liberada de volta para a IA.`);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// Enviar mensagem de texto como atendente humano
router.post("/api/attendance/send-text", async (req, res) => {
  try {
    validateConfig();
    const { phone, text } = req.body || {};
    if (!phone || !text) return res.status(400).json({ success: false, error: "phone e text obrigatórios" });
    const data = await sendWhatsAppRequest({
      messaging_product: "whatsapp", to: phone, type: "text",
      text: { body: text, preview_url: false },
    });
    const messageId = data.messages?.[0]?.id || null;
    // Salvar no histórico como "human"
    const pool = await getDbPool();
    if (pool) {
      const [mapped] = await pool.execute(
        `SELECT client_id FROM \`${getTableName("phone_tenant_map")}\` WHERE phone = ?`, [phone]
      );
      const cid = mapped[0]?.client_id || "manual";
      await pool.execute(
        `INSERT INTO \`${getTableName("conversations")}\` (client_id, phone, role, message, created_at) VALUES (?, ?, 'human', ?, ?)`,
        [cid, phone, text, nowMysql()]
      );
      await pool.execute(
        `UPDATE \`${getTableName("human_attendance")}\` SET last_activity = NOW() WHERE phone = ?`, [phone]
      );
    }
    res.json({ success: true, messageId });
  } catch (e) {
    res.status(500).json({ success: false, error: e.response?.data || e.message });
  }
});

// Enviar imagem/vídeo por URL
router.post("/api/attendance/send-media", async (req, res) => {
  try {
    validateConfig();
    const { phone, mediaUrl, mediaType = "image", caption = "" } = req.body || {};
    if (!phone || !mediaUrl) return res.status(400).json({ success: false, error: "phone e mediaUrl obrigatórios" });
    const typeMap = { image: "image", video: "video", audio: "audio", document: "document" };
    const waType = typeMap[mediaType] || "image";
    const mediaObj = { link: mediaUrl };
    if (caption && waType !== "audio") mediaObj.caption = caption;
    const data = await sendWhatsAppRequest({
      messaging_product: "whatsapp", to: phone, type: waType,
      [waType]: mediaObj,
    });
    res.json({ success: true, messageId: data.messages?.[0]?.id || null });
  } catch (e) {
    res.status(500).json({ success: false, error: e.response?.data || e.message });
  }
});

// Diagnóstico: tudo sobre um número de telefone ─────────────────────
router.get("/api/debug/phone/:phone", async (req, res) => {
  const phone = String(req.params.phone).replace(/\D/g, "");
  const result = { phone, checkedAt: new Date().toISOString() };
  try {
    const pool = await getDbPool();
    if (pool) {
      // 1. Mapeamento phone → tenant
      const [mapped] = await pool.execute(
        `SELECT * FROM \`${getTableName("phone_tenant_map")}\` WHERE phone = ?`, [phone]
      );
      result.phoneTenantMap = mapped;

      // 2. Lead no cache local
      const phoneSuffix = phone.slice(-9);
      const [cached] = await pool.execute(
        `SELECT client_id, phone, name, email, score, stage, total_orders, last_order_date, cart_abandoned, last_synced_at
         FROM \`${getTableName("leads_cache")}\` WHERE phone LIKE ?`,
        [`%${phoneSuffix}`]
      );
      result.leadsCache = cached;

      // 3. Últimas conversas
      const [convs] = await pool.execute(
        `SELECT role, LEFT(message,120) AS message_preview, agent_id, created_at
         FROM \`${getTableName("conversations")}\` WHERE phone = ?
         ORDER BY created_at DESC LIMIT 10`,
        [phone]
      );
      result.recentConversations = convs;
      result.conversationCount = convs.length;
    }

    // 4. Resolver tenant em tempo real
    const tenant = await resolveTenantForPhone(phone).catch((e) => ({ error: e.message }));
    result.resolvedTenant = tenant
      ? { client_id: tenant.client_id, wp_url: tenant.wp_url, active: tenant.active }
      : null;

    // 5. Se encontrou tenant, puxa contexto do lead no WordPress
    if (tenant && tenant.wp_url) {
      const ctx = await fetchLeadContextFromWP(tenant, phone).catch((e) => ({ error: e.message }));
      result.leadContextFromWP = ctx;
    }

    // 6. Agentes disponíveis para o tenant
    if (tenant) {
      const allAgents = await getAgents();
      const agent = resolveAgentForTenant(tenant.client_id, allAgents, "");
      result.resolvedAgent = agent
        ? { id: agent.id, name: agent.name, assignedDomains: safeJsonParse(agent.assigned_domains_json) }
        : null;
    }

    res.json({ success: true, ...result });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message, ...result });
  }
});

// =====================================================================
// FIM DAS ROTAS MULTI-TENANT E CAMPANHAS
// =====================================================================

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
