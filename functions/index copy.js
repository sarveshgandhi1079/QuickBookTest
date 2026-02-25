const { onRequest } = require("firebase-functions/v2/https");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const logger = require("firebase-functions/logger");
const { getFirestore, FieldValue, FieldPath } = require("firebase-admin/firestore");
const { SecretManagerServiceClient } = require("@google-cloud/secret-manager");
const axios = require("axios");
const crypto = require("crypto");
const qs = require("qs");
const { initializeApp } = require("firebase-admin/app");

initializeApp();

// ========================================================================================
// PRODUCTION-READY MULTI-TENANT QUICKBOOKS INTEGRATION
// ========================================================================================
// Features:
// ✅ Secret Manager for credentials
// ✅ Firestore-first architecture with auto-generated IDs
// ✅ Comprehensive per-entity logging
// ✅ Per-entity scheduled retry functions
// ✅ Error indexing for easy troubleshooting
// ✅ Production-grade error handling
// ✅ Optimized performance with BulkWriter
// ========================================================================================

// Database configuration
const qbDb = getFirestore("cdptest");


// Secret Manager client
const secretClient = new SecretManagerServiceClient();
const PROJECT_ID = process.env.GCP_PROJECT || process.env.GCLOUD_PROJECT;

// HTTP client with retry logic
const http = axios.create({
    timeout: 30000,
    validateStatus: (s) => s >= 200 && s < 300,
});

// ========================================================================================
// CONSTANTS & CONFIGURATION
// ========================================================================================

const LOCAL_STATUS = {
    PENDING_CREATE: "PENDING_CREATE",
    PENDING_UPDATE: "PENDING_UPDATE",
    PENDING_DELETE: "PENDING_DELETE",
    SYNCED: "SYNCED",
    ERROR_CREATE: "ERROR_CREATE",
    ERROR_UPDATE: "ERROR_UPDATE",
    ERROR_DELETE: "ERROR_DELETE",
    ERROR_LOCAL_SAVE: "ERROR_LOCAL_SAVE",
};

const LOG_STAGES = {
    START: "START",
    FS_OK: "FS_OK",
    FS_FAIL: "FS_FAIL",
    QBO_REQUEST: "QBO_REQUEST",
    QBO_OK: "QBO_OK",
    QBO_FAIL: "QBO_FAIL",
    END: "END",
    FATAL: "FATAL",
};

const OPERATIONS = {
    CREATE: "CREATE",
    UPDATE: "UPDATE",
    DELETE: "DELETE",
    WEBHOOK: "WEBHOOK",
    BULK_LOAD: "BULK_LOAD",
    RETRY: "RETRY",
};

const ENTITY_CONFIG = {
    Customer: { collection: "customers", getPath: (id, mv) => `/customer/${id}?minorversion=${mv}` },
    Vendor: { collection: "vendors", getPath: (id, mv) => `/vendor/${id}?minorversion=${mv}` },
    Account: { collection: "accounts", getPath: (id, mv) => `/account/${id}?minorversion=${mv}` },
    Bill: { collection: "bills", getPath: (id, mv) => `/bill/${id}?minorversion=${mv}` },
    Invoice: { collection: "invoices", getPath: (id, mv) => `/invoice/${id}?minorversion=${mv}` },
    Payment: { collection: "payments", getPath: (id, mv) => `/payment/${id}?minorversion=${mv}` },
    Item: { collection: "items", getPath: (id, mv) => `/item/${id}?minorversion=${mv}` },
    TaxCode: { collection: "taxCodes", getPath: (id, mv) => `/taxcode/${id}?minorversion=${mv}` },
    TaxRate: { collection: "taxRates", getPath: (id, mv) => `/taxrate/${id}?minorversion=${mv}` },
    Term: { collection: "terms", getPath: (id, mv) => `/term/${id}?minorversion=${mv}` },
    SalesReceipt: { collection: "salesReceipts", getPath: (id, mv) => `/salesreceipt/${id}?minorversion=${mv}` },
    PaymentMethod: { collection: "paymentMethods", getPath: (id, mv) => `/paymentmethod/${id}?minorversion=${mv}` },
    Attachable: { collection: "attachables", getPath: (id, mv) => `/attachable/${id}?minorversion=${mv}` },
};

// ========================================================================================
// UTILITY FUNCTIONS
// ========================================================================================

function utcNowISO() {
    return new Date().toISOString();
}

function tsId() {
    return new Date().toISOString().replace(/[:.]/g, "-");
}

function trimObject(obj, maxLen = 6000) {
    if (!obj) return null;
    try {
        const s = JSON.stringify(obj);
        if (s.length <= maxLen) return obj;
        return { _trimmed: true, preview: s.slice(0, maxLen) };
    } catch {
        return { _unserializable: true };
    }
}

function pickErrorDetails(err) {
    return {
        message: err?.message || String(err),
        code: err?.code || null,
        status: err?.response?.status || err?.statusCode || null,
        details: trimObject(err?.response?.data || null),
    };
}

function cleanUndefined(input) {
    if (input === undefined || input === null) return null;
    if (Array.isArray(input)) {
        return input.map((v) => cleanUndefined(v)).filter((v) => v !== undefined && v !== null);
    }
    if (typeof input === "object") {
        const out = {};
        for (const [k, v] of Object.entries(input)) {
            if (v === undefined) continue;
            const cleaned = cleanUndefined(v);
            if (cleaned !== undefined && cleaned !== null) out[k] = cleaned;
        }
        return out;
    }
    return input;
}

function safeIdPart(v, maxLen = 80) {
    const s = String(v ?? "").trim();
    if (!s) return "NA";
    return s
        .replace(/\s+/g, "_")
        .replace(/[^a-zA-Z0-9_-]/g, "_")
        .replace(/_+/g, "_")
        .replace(/^_+|_+$/g, "")
        .slice(0, maxLen) || "NA";
}

function parseBool(v) {
    if (v === undefined || v === null || v === "") return null;
    const s = String(v).toLowerCase().trim();
    if (s === "true" || s === "1" || s === "yes") return true;
    if (s === "false" || s === "0" || s === "no") return false;
    return null;
}

function parseIntSafe(v, def) {
    const n = parseInt(String(v), 10);
    return Number.isFinite(n) ? n : def;
}

function parseISODate(v) {
    if (!v) return null;
    const d = new Date(String(v));
    if (Number.isNaN(d.getTime())) return null;
    // You are storing ISO strings in Firestore; comparing strings works if ISO format is consistent.
    // If you store Firestore Timestamp, then use Timestamp.fromDate(d) instead.
    return d.toISOString();
}

async function getTotalCountSafe(query) {
    // Firestore Admin SDK supports aggregate count in recent versions:
    // query.count().get() -> snapshot.data().count
    try {
        if (typeof query.count === "function") {
            const agg = await query.count().get();
            const data = agg.data();
            const c = data?.count;
            return typeof c === "number" ? c : null;
        }
    } catch (e) {
        logger.warn("Count aggregation failed (falling back to null):", e?.message || e);
    }
    return null; // avoid expensive full-scan fallback by default
}

// ========================================================================================
// SECRET MANAGER OPERATIONS
// ========================================================================================

async function storeSecret(tenantId, secretType, secretValue) {
    const secretId = `qb-${tenantId}-${secretType}`;
    const parent = `projects/${PROJECT_ID}`;
    const secretPath = `${parent}/secrets/${secretId}`;

    try {
        let secretExists = false;
        try {
            await secretClient.getSecret({ name: secretPath });
            secretExists = true;
        } catch (error) {
            if (error.code !== 5) throw error;
        }

        if (secretExists) {
            try {
                const [currentVersion] = await secretClient.accessSecretVersion({
                    name: `${secretPath}/versions/latest`,
                });
                const currentValue = currentVersion.payload.data.toString('utf8');

                if (currentValue === secretValue) {
                    logger.info(`⏭️  Secret unchanged, skipping: ${secretId}`);
                    return;
                }

                await secretClient.addSecretVersion({
                    parent: secretPath,
                    payload: { data: Buffer.from(secretValue, 'utf8') },
                });

                await secretClient.disableSecretVersion({ name: currentVersion.name });
                logger.info(`✅ Updated secret (value changed): ${secretId}`);
            } catch (accessError) {
                logger.warn(`Could not compare secret values for ${secretId}, updating anyway`);
                await secretClient.addSecretVersion({
                    parent: secretPath,
                    payload: { data: Buffer.from(secretValue, 'utf8') },
                });
            }
        } else {
            const [secret] = await secretClient.createSecret({
                parent,
                secretId,
                secret: { replication: { automatic: {} } },
            });
            await secretClient.addSecretVersion({
                parent: secret.name,
                payload: { data: Buffer.from(secretValue, 'utf8') },
            });
            logger.info(`✅ Created secret: ${secretId}`);
        }
    } catch (error) {
        logger.error(`Failed to store secret ${secretId}:`, error.message);
        throw new Error(`Failed to store secret: ${error.message}`);
    }
}

async function getSecret(tenantId, secretType) {
    const secretId = `qb-${tenantId}-${secretType}`;
    const name = `projects/${PROJECT_ID}/secrets/${secretId}/versions/latest`;

    try {
        const [version] = await secretClient.accessSecretVersion({ name });
        return version.payload.data.toString('utf8');
    } catch (error) {
        if (error.code === 5) {
            throw new Error(`Secret not found: ${secretId}. Please register the tenant first.`);
        }
        logger.error(`Failed to retrieve secret ${secretId}:`, error.message);
        throw new Error(`Failed to retrieve secret: ${error.message}`);
    }
}

// ========================================================================================
// TENANT CONFIGURATION
// ========================================================================================

async function getTenantConfig(tenantId) {
    if (!tenantId) throw new Error("tenantId is required");

    const tenantRef = qbDb.collection("qb_tenants").doc(tenantId);
    const tenantDoc = await tenantRef.get();

    if (!tenantDoc.exists) {
        throw new Error(`Tenant ${tenantId} not found. Please register this QuickBooks company first.`);
    }

    const data = tenantDoc.data();
    if (data.isActive === false) {
        throw new Error(`Tenant ${tenantId} is inactive`);
    }

    const [clientId, clientSecret, refreshToken, webhookVerifier] = await Promise.all([
        getSecret(tenantId, 'clientId'),
        getSecret(tenantId, 'clientSecret'),
        getSecret(tenantId, 'refreshToken'),
        getSecret(tenantId, 'webhookVerifier'),
    ]);

    return {
        clientId,
        clientSecret,
        refreshToken,
        realmId: data.realmId,
        webhookVerifier,
        baseUrl: data.baseUrl || "https://quickbooks.api.intuit.com/v3/company",
        minorVersion: data.minorVersion || "65",
        environment: data.environment || "production",
        isActive: data.isActive !== false,
        metadata: data.metadata || {},
    };
}

async function updateTenantRefreshToken(tenantId, newRefreshToken) {
    const secretId = `qb-${tenantId}-refreshToken`;
    const secretPath = `projects/${PROJECT_ID}/secrets/${secretId}`;

    try {
        const [currentVersion] = await secretClient.accessSecretVersion({
            name: `${secretPath}/versions/latest`,
        });
        const currentValue = currentVersion.payload.data.toString('utf8');

        if (currentValue === newRefreshToken) {
            logger.info(`⏭️  [${tenantId}] Refresh token unchanged, skipping update`);
            return;
        }

        await storeSecret(tenantId, 'refreshToken', newRefreshToken);
        await qbDb.collection("qb_tenants").doc(tenantId).update({
            lastTokenRefresh: utcNowISO(),
            updatedAt: utcNowISO(),
        });

        logger.info(`🔄 [${tenantId}] Refresh token rotated successfully`);
    } catch (error) {
        if (error.code === 5) {
            await storeSecret(tenantId, 'refreshToken', newRefreshToken);
            await qbDb.collection("qb_tenants").doc(tenantId).update({
                lastTokenRefresh: utcNowISO(),
                updatedAt: utcNowISO(),
            });
        } else {
            throw error;
        }
    }
}

// ========================================================================================
// LOGGING SYSTEM (PER-ENTITY + ERROR INDEX)
// ========================================================================================

function getEntityLogDocRef({ tenantId, entityName, docId }) {
    return qbDb
        .collection("tenants")
        .doc(tenantId)
        .collection("logs")
        .doc(docId);
}

async function appendLogStep({
    tenantId,
    entityName,
    collectionName,
    docId,
    qbId = null,
    op,
    stage,
    msg,
    req = null,
    res = null,
    err = null,
    maxSteps = 50,
}) {
    if (!tenantId || !entityName || !docId) return;

    const now = utcNowISO();
    const ref = getEntityLogDocRef({ tenantId, entityName, docId });

    const step = cleanUndefined({
        ts: now,
        op,
        stage,
        msg,
        req: trimObject(req),
        res: trimObject(res),
        err: err ? trimObject(err) : null,
    });

    await qbDb.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        const prev = snap.exists ? snap.data() : {};
        const steps = Array.isArray(prev.steps) ? prev.steps : [];

        steps.push(step);
        const trimmedSteps = steps.slice(Math.max(0, steps.length - maxSteps));

        tx.set(
            ref,
            cleanUndefined({
                tenantId,
                entityName,
                collectionName,
                docId,
                qbId: qbId ?? prev.qbId ?? null,
                createdAt: prev.createdAt || now,
                updatedAt: now,
                lastOp: op,
                lastStage: stage,
                lastMessage: msg,
                lastError: err || null,
                steps: trimmedSteps,
            }),
            { merge: true }
        );
    });
}

function buildTenantErrorDocId({ entityName, op, stage, recordDocId, qbId, logDocId }) {
    const e = safeIdPart(entityName, 40);
    const o = safeIdPart(op, 20);
    const st = safeIdPart(stage, 20);

    if (recordDocId) return `${e}__${o}__${st}__doc_${safeIdPart(recordDocId, 80)}`;
    if (qbId) return `${e}__${o}__${st}__qb_${safeIdPart(qbId, 80)}`;
    if (logDocId) return `${e}__${o}__${st}__log_${safeIdPart(logDocId, 80)}`;
    return `${e}__${o}__${st}__unknown`;
}

async function upsertTenantErrorIndex({
    tenantId,
    entityName,
    collectionName = null,
    op,
    stage,
    message,
    recordDocId = null,
    qbId = null,
    logDocId = null,
    request = null,
    response = null,
    error = null,
}) {
    if (!tenantId || !entityName || !op || !stage) return;

    const now = utcNowISO();
    const errorDocId = buildTenantErrorDocId({ entityName, op, stage, recordDocId, qbId, logDocId });
    const ref = qbDb.collection("tenants").doc(tenantId).collection("error_index").doc(errorDocId);

    const patch = cleanUndefined({
        errorDocId,
        tenantId,
        entityName,
        collectionName,
        op,
        stage,
        message: message || null,
        recordDocId: recordDocId || null,
        qbId: qbId || null,
        logDocId: logDocId || null,
        lastError: error ? trimObject(error) : null,
        lastRequest: request ? trimObject(request) : null,
        lastResponse: response ? trimObject(response) : null,
        lastSeenAt: now,
        updatedAt: now,
    });

    await qbDb.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        if (!snap.exists) {
            tx.set(ref, cleanUndefined({ ...patch, count: 1, firstSeenAt: now, createdAt: now }), { merge: true });
        } else {
            tx.set(ref, cleanUndefined({ ...patch, count: FieldValue.increment(1) }), { merge: true });
        }
    });
}

// Convenience wrappers
async function logStart(p) { return appendLogStep({ ...p, stage: LOG_STAGES.START }); }
async function logEnd(p) { return appendLogStep({ ...p, stage: LOG_STAGES.END }); }
async function logFsOk(p) { return appendLogStep({ ...p, stage: LOG_STAGES.FS_OK }); }
async function logQboRequest(p) { return appendLogStep({ ...p, stage: LOG_STAGES.QBO_REQUEST }); }
async function logQboOk(p) { return appendLogStep({ ...p, stage: LOG_STAGES.QBO_OK }); }

async function logFsFail(p) {
    await appendLogStep({ ...p, stage: LOG_STAGES.FS_FAIL });
    await upsertTenantErrorIndex({
        tenantId: p.tenantId,
        entityName: p.entityName,
        collectionName: p.collectionName,
        op: p.op,
        stage: "FS_FAIL",
        message: p.msg,
        recordDocId: p.recordDocId,
        qbId: p.qbId,
        logDocId: p.docId,
        request: p.req,
        response: p.res,
        error: p.err,
    }).catch(e => logger.error("Failed to upsert error_index (FS_FAIL):", e?.message));
}

async function logQboFail(p) {
    await appendLogStep({ ...p, stage: LOG_STAGES.QBO_FAIL });
    await upsertTenantErrorIndex({
        tenantId: p.tenantId,
        entityName: p.entityName,
        collectionName: p.collectionName,
        op: p.op,
        stage: "QBO_FAIL",
        message: p.msg,
        recordDocId: p.recordDocId,
        qbId: p.qbId,
        logDocId: p.docId,
        request: p.req,
        response: p.res,
        error: p.err,
    }).catch(e => logger.error("Failed to upsert error_index (QBO_FAIL):", e?.message));
}

async function logFatal(p) {
    await appendLogStep({ ...p, stage: LOG_STAGES.FATAL });
    await upsertTenantErrorIndex({
        tenantId: p.tenantId,
        entityName: p.entityName,
        collectionName: p.collectionName,
        op: p.op,
        stage: "FATAL",
        message: p.msg,
        recordDocId: p.recordDocId,
        qbId: p.qbId,
        logDocId: p.docId,
        request: p.req,
        response: p.res,
        error: p.err,
    }).catch(e => logger.error("Failed to upsert error_index (FATAL):", e?.message));
}

// ========================================================================================
// QUICKBOOKS AUTHENTICATION
// ========================================================================================

async function getAccessToken(tenantId) {
    const config = await getTenantConfig(tenantId);
    const authHeader = Buffer.from(`${config.clientId}:${config.clientSecret}`).toString("base64");

    try {
        const tokenRes = await http.post(
            "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
            qs.stringify({ grant_type: "refresh_token", refresh_token: config.refreshToken }),
            {
                headers: {
                    Authorization: `Basic ${authHeader}`,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            }
        );

        const accessToken = tokenRes.data?.access_token;
        const newRefreshToken = tokenRes.data?.refresh_token;

        if (!accessToken) throw new Error("Failed to obtain QBO access_token");
        if (newRefreshToken && newRefreshToken !== config.refreshToken) {
            await updateTenantRefreshToken(tenantId, newRefreshToken);
        }

        return accessToken;
    } catch (err) {
        logger.error(`QBO token refresh failed for tenant ${tenantId}:`, err.response?.status, err.response?.data || err.message);
        throw err;
    }
}

async function withQboClient(tenantId, fn) {
    const config = await getTenantConfig(tenantId);
    if (!config.isActive) throw new Error(`Tenant ${tenantId} is inactive`);

    const accessToken = await getAccessToken(tenantId);
    const baseUrl = `${config.baseUrl.replace(/\/$/, "")}/${config.realmId}`;

    return fn({ accessToken, baseUrl, minorVersion: config.minorVersion, tenantId });
}

// ========================================================================================
// QUICKBOOKS API OPERATIONS
// ========================================================================================

async function qboGet(path, client) {
    const url = `${client.baseUrl}${path}`;
    return http.get(url, {
        headers: {
            Authorization: `Bearer ${client.accessToken}`,
            Accept: "application/json",
        },
    });
}

async function qboPost(path, payload, client) {
    const url = `${client.baseUrl}${path}`;
    return http.post(url, payload, {
        headers: {
            Authorization: `Bearer ${client.accessToken}`,
            Accept: "application/json",
            "Content-Type": "application/json",
        },
    });
}

async function qboDelete(entityName, id, syncToken, client) {
    const path = `/${entityName.toLowerCase()}?operation=delete&minorversion=${client.minorVersion}`;
    const payload = { Id: id.toString(), SyncToken: syncToken.toString() };
    return qboPost(path, payload, client);
}

async function qboQuery(selectSql, client) {
    const path = `/query?query=${encodeURIComponent(selectSql)}&minorversion=${client.minorVersion}`;
    return qboGet(path, client);
}

// ========================================================================================
// FIRESTORE OPERATIONS
// ========================================================================================

function getTenantCollection(tenantId, entityCollection) {
    return qbDb.collection("tenants").doc(tenantId).collection(entityCollection);
}

async function findDocRefByQbId(tenantId, collectionName, qbId) {
    const tenantCol = getTenantCollection(tenantId, collectionName);
    const snap = await tenantCol
        .where("qbId", "==", qbId.toString())
        .where("_tenantId", "==", tenantId)
        .limit(1)
        .get();

    return snap.empty ? null : snap.docs[0].ref;
}

async function upsertFromQbo(tenantId, entityName, entityObj, collectionName) {
    if (!entityObj?.Id) return;

    const qbId = entityObj.Id.toString();
    const nowUtc = utcNowISO();
    const tenantCol = getTenantCollection(tenantId, collectionName);

    const existingRef = await findDocRefByQbId(tenantId, collectionName, qbId);
    const ref = existingRef || tenantCol.doc();

    const safe = cleanUndefined(entityObj);

    await ref.set(
        cleanUndefined({
            ...safe,
            qbId,
            isDeleted: false,
            localStatus: LOCAL_STATUS.SYNCED,
            updatedAt: nowUtc,
            _tenantId: tenantId,
            _entityType: entityName,
        }),
        { merge: true }
    );

    logger.info(`💾 [${tenantId}] ${entityName} qbId=${qbId} upserted docId=${ref.id}`);
    return ref;
}

async function softDeleteInFirestore(tenantId, entityName, qbId, collectionName) {
    const nowUtc = utcNowISO();
    const tenantCol = getTenantCollection(tenantId, collectionName);

    const ref = await findDocRefByQbId(tenantId, collectionName, qbId);
    const docRef = ref || tenantCol.doc();

    await docRef.set(
        {
            qbId: qbId.toString(),
            Id: qbId.toString(),
            isDeleted: true,
            deletedAt: nowUtc,
            updatedAt: nowUtc,
            _tenantId: tenantId,
            _entityType: entityName,
        },
        { merge: true }
    );

    logger.info(`🗑️ [${tenantId}] ${entityName} qbId=${qbId} soft-deleted docId=${docRef.id}`);
    return docRef;
}

async function saveListToFirestore(tenantId, collectionName, items, entityName) {
    if (!Array.isArray(items) || items.length === 0) return 0;

    const tenantCol = getTenantCollection(tenantId, collectionName);
    const valid = items.filter((x) => x && x.Id);
    if (valid.length === 0) return 0;

    const qbIds = Array.from(new Set(valid.map((x) => x.Id.toString())));
    const chunk = (arr, size) => {
        const out = [];
        for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
        return out;
    };

    const qbIdToDocRef = new Map();
    for (const qbIdChunk of chunk(qbIds, 30)) {
        const snap = await tenantCol.where("qbId", "in", qbIdChunk).get();
        snap.forEach((doc) => {
            const d = doc.data() || {};
            if (d.qbId) qbIdToDocRef.set(d.qbId.toString(), doc.ref);
        });
    }

    const writer = qbDb.bulkWriter();
    writer.onWriteError((err) => {
        const code = err?.code || err?.status;
        if (err.failedAttempts < 5 && (code === 4 || code === 10 || code === 14)) return true;
        logger.error("BulkWriter write failed:", err);
        return false;
    });

    const nowUtc = utcNowISO();
    let saved = 0;

    try {
        for (const item of valid) {
            const qbId = item.Id.toString();
            const existingRef = qbIdToDocRef.get(qbId);
            const ref = existingRef || tenantCol.doc();

            const baseData = {
                ...item,
                qbId,
                isDeleted: false,
                localStatus: LOCAL_STATUS.SYNCED,
                updatedAt: nowUtc,
                _tenantId: tenantId,
                _entityType: entityName,
            };

            const data = existingRef ? baseData : { ...baseData, createdAt: nowUtc };
            writer.set(ref, data, { merge: true });
            saved++;
        }
    } finally {
        await writer.close();
    }

    logger.info(`✅ [${tenantId}] BulkWriter saved ${saved} ${entityName}`);
    return saved;
}

async function readDocSafe(ref) {
    try {
        const snap = await ref.get();
        return snap.exists ? { docId: snap.id, ...snap.data() } : null;
    } catch {
        return null;
    }
}

async function getEntityDocByDocId(tenantId, collectionName, docId) {
    const ref = getTenantCollection(tenantId, collectionName).doc(docId);
    const snap = await ref.get();
    if (!snap.exists) throw new Error(`Entity doc not found: ${collectionName}/${docId}`);
    return { ref, data: snap.data() || {} };
}

// ========================================================================================
// SCHEDULED RETRY MECHANISM (PER-ENTITY)
// ========================================================================================

function makeRetryHandler(entityName) {
    const cfg = ENTITY_CONFIG[entityName];

    return onSchedule(
        {
            schedule: "every 6 hours",
            timeoutSeconds: 540,
            memory: "1GiB",
            region: "asia-south1",
        },
        async (event) => {
            logger.info(`🔄 [RETRY] Starting for ${entityName}`);

            const results = {
                entityType: entityName,
                processed: 0,
                succeeded: 0,
                failed: 0,
                skipped: 0,
                startTime: utcNowISO(),
            };

            try {
                const tenantsSnapshot = await qbDb.collection("qb_tenants")
                    .where("isActive", "==", true)
                    .get();

                for (const tenantDoc of tenantsSnapshot.docs) {
                    const tenantId = tenantDoc.id;
                    const tenantCol = getTenantCollection(tenantId, cfg.collection);

                    // Query failed records
                    const failedSnapshot = await tenantCol
                        .where("localStatus", "in", [
                            LOCAL_STATUS.ERROR_CREATE,
                            LOCAL_STATUS.ERROR_UPDATE,
                            LOCAL_STATUS.ERROR_DELETE,
                        ])
                        .limit(100)
                        .get();

                    logger.info(`📦 [RETRY] Found ${failedSnapshot.size} failed ${entityName} for tenant ${tenantId}`);

                    for (const doc of failedSnapshot.docs) {
                        results.processed++;
                        const data = doc.data();
                        const attemptCount = data._attemptCount || 0;

                        if (attemptCount >= 5) {
                            results.skipped++;
                            continue;
                        }

                        try {
                            await retryEntityOperation(tenantId, entityName, doc.ref, data);
                            results.succeeded++;
                        } catch (error) {
                            results.failed++;
                            logger.error(`❌ [RETRY] Failed ${entityName} ${doc.id}:`, error.message);
                        }

                        await new Promise(resolve => setTimeout(resolve, 1000));
                    }
                }

                results.endTime = utcNowISO();
                logger.info(`🏁 [RETRY] ${entityName} completed:`, results);

                await qbDb.collection(`qb_${entityName.toLowerCase()}_retry_logs`).add(results);
            } catch (error) {
                logger.error(`❌ [RETRY] ${entityName} job failed:`, error);
                results.error = error.message;
                results.endTime = utcNowISO();
                await qbDb.collection(`qb_${entityName.toLowerCase()}_retry_logs`).add(results);
                throw error;
            }
        }
    );
}

async function retryEntityOperation(tenantId, entityName, docRef, data) {
    const cfg = ENTITY_CONFIG[entityName];
    const localStatus = data.localStatus;
    const retryPayload = data.retryPayload;

    if (localStatus === LOCAL_STATUS.ERROR_CREATE && retryPayload) {
        // Retry create
        const created = await withQboClient(tenantId, async (client) => {
            const path = `/${entityName.toLowerCase()}?minorversion=${client.minorVersion}`;
            const qbRes = await qboPost(path, retryPayload, client);
            return qbRes.data?.[entityName];
        });

        if (created?.Id) {
            const qbId = created.Id.toString();
            await docRef.set(
                cleanUndefined({
                    ...created,
                    qbId,
                    localStatus: LOCAL_STATUS.SYNCED,
                    lastError: null,
                    retryPayload: null,
                    updatedAt: utcNowISO(),
                }),
                { merge: true }
            );
            logger.info(`✅ [RETRY] ${entityName} ${docRef.id} create succeeded`);
        }
    } else if (localStatus === LOCAL_STATUS.ERROR_UPDATE && retryPayload) {
        // Retry update
        const updated = await withQboClient(tenantId, async (client) => {
            const path = `/${entityName.toLowerCase()}?minorversion=${client.minorVersion}`;
            const qbRes = await qboPost(path, retryPayload, client);
            return qbRes.data?.[entityName];
        });

        if (updated?.Id) {
            await docRef.set(
                cleanUndefined({
                    ...updated,
                    localStatus: LOCAL_STATUS.SYNCED,
                    lastError: null,
                    retryPayload: null,
                    updatedAt: utcNowISO(),
                }),
                { merge: true }
            );
            logger.info(`✅ [RETRY] ${entityName} ${docRef.id} update succeeded`);
        }
    } else if (localStatus === LOCAL_STATUS.ERROR_DELETE && retryPayload) {
        // Retry delete
        const { Id, SyncToken } = retryPayload;
        await withQboClient(tenantId, async (client) => {
            await qboDelete(entityName, Id, SyncToken, client);
        });

        await docRef.set(
            cleanUndefined({
                localStatus: LOCAL_STATUS.SYNCED,
                lastError: null,
                retryPayload: null,
                updatedAt: utcNowISO(),
            }),
            { merge: true }
        );
        logger.info(`✅ [RETRY] ${entityName} ${docRef.id} delete succeeded`);
    }
}

// ========================================================================================
// WEBHOOK HANDLER
// ========================================================================================

function verifyWebhook(req, webhookVerifier) {
    const signature = req.headers["intuit-signature"];
    const raw = req.rawBody;

    if (!raw || !Buffer.isBuffer(raw)) {
        throw new Error("Missing rawBody (cannot verify webhook)");
    }

    const hmac = crypto.createHmac("sha256", webhookVerifier);
    hmac.update(raw);
    const hash = hmac.digest("base64");

    if (!signature || hash !== signature) {
        const err = new Error("Invalid webhook signature");
        err.statusCode = 401;
        throw err;
    }
}

// exports.quickBooksWebhook = onRequest(
//     {
//         region: "asia-south1",
//         timeoutSeconds: 120,
//         memory: "512MiB",
//         maxInstances: 10,
//     },
//     async (req, res) => {
//         let tenantId = null;

//         try {
//             if (req.method !== "POST") return res.status(405).send("Only POST allowed");

//             tenantId = req.query.tenantId;
//             if (!tenantId) {
//                 return res.status(400).json({ error: "tenantId query parameter is required" });
//             }

//             const webhookVerifier = await getSecret(tenantId, "webhookVerifier");
//             verifyWebhook(req, webhookVerifier);
//             logger.info(`✅ [${tenantId}] Webhook verified`);

//             const entities =
//                 req.body?.eventNotifications?.flatMap((n) => n?.dataChangeEvent?.entities || []) || [];

//             if (!entities.length) return res.status(200).send("No entities");

//             await withQboClient(tenantId, async (client) => {
//                 for (const e of entities) {
//                     const { name, operation, id } = e || {};
//                     if (!name || !operation || !id) continue;

//                     const cfg = ENTITY_CONFIG[name];
//                     if (!cfg) {
//                         logger.info(`ℹ️ [${tenantId}] Unsupported entity: ${name}`);
//                         continue;
//                     }

//                     const qbId = id.toString();
//                     const logDocId = `${name}_WEBHOOK_${operation}_${tsId()}_qb_${qbId}`;
//                     const existingRef = await findDocRefByQbId(tenantId, cfg.collection, qbId);

//                     await logStart({
//                         tenantId,
//                         entityName: name,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.WEBHOOK,
//                         msg: `Webhook received: ${name} ${operation}`,
//                         req: { name, operation, qbId },
//                     });

//                     const opNorm = String(operation || "").toLowerCase();
//                     if (opNorm === "delete") {
//                         try {
//                             await softDeleteInFirestore(tenantId, name, qbId, cfg.collection);
//                             await logFsOk({
//                                 tenantId,
//                                 entityName: name,
//                                 collectionName: cfg.collection,
//                                 docId: logDocId,
//                                 qbId,
//                                 op: OPERATIONS.WEBHOOK,
//                                 msg: "Soft-deleted in Firestore",
//                                 res: { qbId, isDeleted: true },
//                             });
//                             await logEnd({
//                                 tenantId,
//                                 entityName: name,
//                                 collectionName: cfg.collection,
//                                 docId: logDocId,
//                                 qbId,
//                                 op: OPERATIONS.WEBHOOK,
//                                 msg: "Webhook processed (Delete)",
//                             });
//                         } catch (fsErr) {
//                             const errObj = pickErrorDetails(fsErr);
//                             await logFsFail({
//                                 tenantId,
//                                 entityName: name,
//                                 collectionName: cfg.collection,
//                                 docId: logDocId,
//                                 recordDocId: existingRef?.id || null,
//                                 qbId,
//                                 op: OPERATIONS.WEBHOOK,
//                                 msg: "Firestore soft delete failed",
//                                 err: errObj,
//                             });
//                         }
//                         continue;
//                     }

//                     const getPathForLog = cfg.getPath(qbId, client.minorVersion);
//                     await logQboRequest({
//                         tenantId,
//                         entityName: name,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.WEBHOOK,
//                         msg: "Fetching latest entity from QBO",
//                         req: { path: getPathForLog },
//                     });

//                     let entityObj = null;
//                     try {
//                         const getRes = await qboGet(cfg.getPath(qbId, client.minorVersion), client);
//                         entityObj = getRes.data?.[name];

//                         if (!entityObj?.Id) {
//                             const e2 = { message: `QBO returned empty ${name} for id ${qbId}` };
//                             await logQboFail({
//                                 tenantId,
//                                 entityName: name,
//                                 collectionName: cfg.collection,
//                                 docId: logDocId,
//                                 recordDocId: existingRef?.id || null,
//                                 qbId,
//                                 op: OPERATIONS.WEBHOOK,
//                                 msg: "QBO fetch returned empty entity",
//                                 err: e2,
//                                 res: getRes.data,
//                             });
//                             continue;
//                         }

//                         await logQboOk({
//                             tenantId,
//                             entityName: name,
//                             collectionName: cfg.collection,
//                             docId: logDocId,
//                             qbId,
//                             op: OPERATIONS.WEBHOOK,
//                             msg: "QBO fetch OK",
//                             res: entityObj,
//                         });
//                     } catch (qboErr) {
//                         const errObj = pickErrorDetails(qboErr);
//                         await logQboFail({
//                             tenantId,
//                             entityName: name,
//                             collectionName: cfg.collection,
//                             docId: logDocId,
//                             recordDocId: existingRef?.id || null,
//                             qbId,
//                             op: OPERATIONS.WEBHOOK,
//                             msg: "Failed to fetch entity from QBO",
//                             err: errObj,
//                         });
//                         continue;
//                     }

//                     try {
//                         const safeEntityObj = cleanUndefined(entityObj);
//                         await upsertFromQbo(tenantId, name, safeEntityObj, cfg.collection);
//                         await logFsOk({
//                             tenantId,
//                             entityName: name,
//                             collectionName: cfg.collection,
//                             docId: logDocId,
//                             qbId,
//                             op: OPERATIONS.WEBHOOK,
//                             msg: "Upserted into Firestore successfully",
//                             res: { qbId, operation },
//                         });
//                         await logEnd({
//                             tenantId,
//                             entityName: name,
//                             collectionName: cfg.collection,
//                             docId: logDocId,
//                             qbId,
//                             op: OPERATIONS.WEBHOOK,
//                             msg: "Webhook processed",
//                         });
//                     } catch (fsErr2) {
//                         const errObj = pickErrorDetails(fsErr2);
//                         await logFsFail({
//                             tenantId,
//                             entityName: name,
//                             collectionName: cfg.collection,
//                             docId: logDocId,
//                             qbId,
//                             recordDocId: existingRef?.id || null,
//                             op: OPERATIONS.WEBHOOK,
//                             msg: "Firestore upsert failed",
//                             err: errObj,
//                             req: entityObj,
//                         });
//                     }
//                 }
//             });

//             return res.status(200).send("Webhook processed");
//         } catch (err) {
//             const code = err.statusCode || 500;
//             logger.error("❌ Webhook error:", err.message);
//             return res.status(code).send(err.message || "Webhook error");
//         }
//     }
//);



// ========================================================================================
// CRUD HANDLERS (CREATE/UPDATE/DELETE)
// ========================================================================================

// function makeCreateHandler(entityName) {
//     const cfg = ENTITY_CONFIG[entityName];

//     return onRequest(
//         {
//             region: "asia-south1",
//             timeoutSeconds: 120,
//             memory: "512MiB",
//             maxInstances: 10,
//         },
//         async (req, res) => {
//             let tenantId = null;
//             let docRef = null;
//             let qbId = null;
//             let payload = null;
//             let logDocId = null;

//             try {
//                 if (req.method !== "POST") {
//                     return res.status(405).json({ error: "Only POST allowed" });
//                 }

//                 tenantId = req.query.tenantId || req.body?.tenantId;
//                 if (!tenantId) {
//                     return res.status(400).json({ error: "tenantId is required" });
//                 }

//                 const input = req.body;
//                 if (!input || typeof input !== "object") {
//                     return res.status(400).json({ error: "Request body must be a JSON object" });
//                 }

//                 const { tenantId: _t, ...rest } = input;
//                 payload = rest;

//                 const tenantCol = getTenantCollection(tenantId, cfg.collection);
//                 docRef = tenantCol.doc();
//                 const nowUtc = utcNowISO();
//                 logDocId = `${entityName}_CREATE_${tsId()}_${docRef.id}`;

//                 await logStart({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     op: OPERATIONS.CREATE,
//                     msg: "Create request received",
//                     req: payload,
//                 });

//                 try {
//                     await docRef.set(
//                         cleanUndefined({
//                             ...payload,
//                             qbId: null,
//                             isDeleted: false,
//                             localStatus: LOCAL_STATUS.PENDING_CREATE,
//                             createdAt: nowUtc,
//                             updatedAt: nowUtc,
//                             _tenantId: tenantId,
//                             _entityType: entityName,
//                         }),
//                         { merge: true }
//                     );

//                     await logFsOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         op: OPERATIONS.CREATE,
//                         msg: "Firestore created (PENDING_CREATE)",
//                         res: { localStatus: LOCAL_STATUS.PENDING_CREATE },
//                     });
//                 } catch (fsErr) {
//                     const e = pickErrorDetails(fsErr);
//                     await logFsFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docRef.id,
//                         op: OPERATIONS.CREATE,
//                         msg: "Firestore create failed (before QBO call)",
//                         err: e,
//                         req: payload,
//                     });
//                     throw fsErr;
//                 }

//                 const qboPathForLog = `/${entityName.toLowerCase()}?minorversion=<minorVersion>`;
//                 await logQboRequest({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     op: OPERATIONS.CREATE,
//                     msg: "Calling QuickBooks create endpoint",
//                     req: { path: qboPathForLog, body: payload },
//                 });

//                 let created = null;
//                 try {
//                     created = await withQboClient(tenantId, async (client) => {
//                         const path = `/${entityName.toLowerCase()}?minorversion=${client.minorVersion}`;
//                         const qbRes = await qboPost(path, payload, client);
//                         return qbRes.data?.[entityName];
//                     });
//                 } catch (qboErr) {
//                     const e = pickErrorDetails(qboErr);

//                     await docRef.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_CREATE,
//                             lastError: e,
//                             retryPayload: payload,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     await logQboFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docRef.id,
//                         op: OPERATIONS.CREATE,
//                         msg: "QuickBooks create failed (saved for retry)",
//                         err: e,
//                         req: { body: payload },
//                     });

//                     return res.status(500).json({
//                         error: "QuickBooks create failed",
//                         details: e,
//                         docId: logDocId,
//                     });
//                 }

//                 if (!created?.Id) {
//                     const e = { message: `QBO did not return valid ${entityName} (missing Id)` };

//                     await docRef.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_CREATE,
//                             lastError: e,
//                             retryPayload: payload,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     await logQboFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docRef.id,
//                         op: OPERATIONS.CREATE,
//                         msg: "QuickBooks create returned invalid response (missing Id)",
//                         err: e,
//                         res: created,
//                     });

//                     return res.status(500).json({
//                         error: e.message,
//                         docId: logDocId,
//                     });
//                 }

//                 qbId = created.Id.toString();

//                 await logQboOk({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     qbId,
//                     op: OPERATIONS.CREATE,
//                     msg: "QuickBooks create succeeded",
//                     res: created,
//                 });

//                 try {
//                     await docRef.set(
//                         cleanUndefined({
//                             ...created,
//                             qbId,
//                             localStatus: LOCAL_STATUS.SYNCED,
//                             lastError: null,
//                             retryPayload: null,
//                             updatedAt: utcNowISO(),
//                             _tenantId: tenantId,
//                             _entityType: entityName,
//                         }),
//                         { merge: true }
//                     );

//                     await logFsOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.CREATE,
//                         msg: "Firestore updated with QBO response (SYNCED)",
//                         res: { qbId, localStatus: LOCAL_STATUS.SYNCED },
//                     });
//                 } catch (fsErr2) {
//                     const e = pickErrorDetails(fsErr2);

//                     await docRef.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_LOCAL_SAVE,
//                             lastError: e,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     await logFsFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docRef.id,
//                         qbId,
//                         op: OPERATIONS.CREATE,
//                         msg: "Firestore failed saving QBO response",
//                         err: e,
//                         res: created,
//                     });

//                     return res.status(200).json({
//                         message: `${entityName} created in QBO, but Firestore update failed`,
//                         tenantId,
//                         docId: logDocId,
//                         qbId,
//                         warning: e,
//                     });
//                 }

//                 await logEnd({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     qbId,
//                     op: OPERATIONS.CREATE,
//                     msg: "Create finished",
//                 });

//                 return res.status(200).json({
//                     message: `${entityName} created`,
//                     tenantId,
//                     docId: logDocId,
//                     qbId,
//                 });
//             } catch (err) {
//                 const e = pickErrorDetails(err);

//                 try {
//                     if (tenantId && cfg?.collection && docRef?.id) {
//                         await logFatal({
//                             tenantId,
//                             entityName,
//                             collectionName: cfg.collection,
//                             docId: logDocId,
//                             recordDocId: docRef.id,
//                             qbId,
//                             op: OPERATIONS.CREATE,
//                             msg: "Create crashed (unexpected)",
//                             err: e,
//                             req: payload,
//                         });

//                         await docRef.set(
//                             cleanUndefined({
//                                 localStatus: LOCAL_STATUS.ERROR_CREATE,
//                                 lastError: e,
//                                 retryPayload: payload || null,
//                                 updatedAt: utcNowISO(),
//                             }),
//                             { merge: true }
//                         );
//                     }
//                 } catch (inner) {
//                     logger.error("❌ Logging/marking ERROR_CREATE failed:", inner?.message || inner);
//                 }

//                 logger.error(`❌ create${entityName} error:`, err.message, err.response?.data);
//                 return res.status(500).json({ error: err.message, details: err.response?.data || null });
//             }
//         }
//     );
// }

// function makeCreateHandler(entityName) {
//     const cfg = ENTITY_CONFIG[entityName];

//     return onRequest(
//         {
//             region: "asia-south1",
//             timeoutSeconds: 120,
//             memory: "512MiB",
//             maxInstances: 10,
//         },
//         async (req, res) => {
//             let tenantId = null;
//             let docRef = null;
//             let qbId = null;
//             let payload = null;
//             let logDocId = null;

//             // ✅ keep these across branches
//             let fsAfterPending = null;

//             try {
//                 if (req.method !== "POST") {
//                     return res.status(405).json({ error: "Only POST allowed" });
//                 }

//                 tenantId = req.query.tenantId || req.body?.tenantId;
//                 if (!tenantId) {
//                     return res.status(400).json({ error: "tenantId is required" });
//                 }

//                 const input = req.body;
//                 if (!input || typeof input !== "object") {
//                     return res.status(400).json({ error: "Request body must be a JSON object" });
//                 }

//                 // Remove tenantId from QBO payload
//                 const { tenantId: _t, ...rest } = input;
//                 payload = rest;

//                 // 1) Create Firestore record first (auto-id)
//                 const tenantCol = getTenantCollection(tenantId, cfg.collection);
//                 docRef = tenantCol.doc();
//                 const nowUtc = utcNowISO();
//                 logDocId = `${entityName}_CREATE_${tsId()}_${docRef.id}`;

//                 await logStart({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     op: OPERATIONS.CREATE,
//                     msg: "Create request received",
//                     req: payload,
//                 });

//                 // 2) Save PENDING_CREATE locally
//                 try {
//                     await docRef.set(
//                         cleanUndefined({
//                             ...payload,
//                             qbId: null,
//                             isDeleted: false,
//                             localStatus: LOCAL_STATUS.PENDING_CREATE,
//                             createdAt: nowUtc,
//                             updatedAt: nowUtc,
//                             _tenantId: tenantId,
//                             _entityType: entityName,
//                         }),
//                         { merge: true }
//                     );

//                     // ✅ keep FS state to return on error later
//                     fsAfterPending = await readDocSafe(docRef);

//                     await logFsOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         op: OPERATIONS.CREATE,
//                         msg: "Firestore created (PENDING_CREATE)",
//                         res: { localStatus: LOCAL_STATUS.PENDING_CREATE },
//                     });
//                 } catch (fsErr) {
//                     const e = pickErrorDetails(fsErr);

//                     await logFsFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docRef.id,
//                         op: OPERATIONS.CREATE,
//                         msg: "Firestore create failed (before QBO call)",
//                         err: e,
//                         req: payload,
//                     });

//                     throw fsErr;
//                 }

//                 // 3) Call QBO create
//                 const qboPathForLog = `/${entityName.toLowerCase()}?minorversion=<minorVersion>`;
//                 await logQboRequest({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     op: OPERATIONS.CREATE,
//                     msg: "Calling QuickBooks create endpoint",
//                     req: { path: qboPathForLog, body: payload },
//                 });

//                 let created = null;
//                 try {
//                     created = await withQboClient(tenantId, async (client) => {
//                         const path = `/${entityName.toLowerCase()}?minorversion=${client.minorVersion}`;
//                         const qbRes = await qboPost(path, payload, client);
//                         return qbRes.data?.[entityName];
//                     });
//                 } catch (qboErr) {
//                     const e = pickErrorDetails(qboErr);

//                     await docRef.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_CREATE,
//                             lastError: e,
//                             retryPayload: payload,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     const fsAfterError = await readDocSafe(docRef);

//                     await logQboFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docRef.id,
//                         op: OPERATIONS.CREATE,
//                         msg: "QuickBooks create failed (saved for retry)",
//                         err: e,
//                         req: { body: payload },
//                     });

//                     // ✅ return error + firestore docId + firestore data
//                     return res.status(500).json({
//                         ok: false,
//                         error: "QuickBooks create failed",
//                         details: e,
//                         tenantId,
//                         docId: docRef.id,     // ✅ Firestore doc id for update handler
//                         logId: logDocId,
//                         firestore: fsAfterError || fsAfterPending || null,
//                     });
//                 }

//                 if (!created?.Id) {
//                     const e = { message: `QBO did not return valid ${entityName} (missing Id)` };

//                     await docRef.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_CREATE,
//                             lastError: e,
//                             retryPayload: payload,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     const fsAfterError = await readDocSafe(docRef);

//                     await logQboFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docRef.id,
//                         op: OPERATIONS.CREATE,
//                         msg: "QuickBooks create returned invalid response (missing Id)",
//                         err: e,
//                         res: created,
//                     });

//                     return res.status(500).json({
//                         ok: false,
//                         error: e.message,
//                         tenantId,
//                         docId: docRef.id,
//                         logId: logDocId,
//                         firestore: fsAfterError || fsAfterPending || null,
//                     });
//                 }

//                 qbId = created.Id.toString();

//                 await logQboOk({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     qbId,
//                     op: OPERATIONS.CREATE,
//                     msg: "QuickBooks create succeeded",
//                     res: created,
//                 });

//                 // 4) Save QBO response to Firestore
//                 try {
//                     await docRef.set(
//                         cleanUndefined({
//                             ...created,
//                             qbId,
//                             localStatus: LOCAL_STATUS.SYNCED,
//                             lastError: null,
//                             retryPayload: null,
//                             updatedAt: utcNowISO(),
//                             _tenantId: tenantId,
//                             _entityType: entityName,
//                         }),
//                         { merge: true }
//                     );

//                     const fsAfterSynced = await readDocSafe(docRef);

//                     await logFsOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.CREATE,
//                         msg: "Firestore updated with QBO response (SYNCED)",
//                         res: { qbId, localStatus: LOCAL_STATUS.SYNCED },
//                     });

//                     await logEnd({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.CREATE,
//                         msg: "Create finished",
//                     });

//                     // ✅ return firestore docId + data
//                     return res.status(200).json({
//                         ok: true,
//                         message: `${entityName} created`,
//                         tenantId,
//                         docId: docRef.id,
//                         qbId,
//                         logId: logDocId,
//                         firestore: fsAfterSynced || null,
//                     });
//                 } catch (fsErr2) {
//                     const e = pickErrorDetails(fsErr2);

//                     await docRef.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_LOCAL_SAVE,
//                             lastError: e,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     const fsAfterErrorLocalSave = await readDocSafe(docRef);

//                     await logFsFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docRef.id,
//                         qbId,
//                         op: OPERATIONS.CREATE,
//                         msg: "Firestore failed saving QBO response (entity created in QBO but local save failed)",
//                         err: e,
//                         res: created,
//                     });

//                     // ✅ IMPORTANT: return Firestore docId (docRef.id), not log id
//                     return res.status(200).json({
//                         ok: false,
//                         message: `${entityName} created in QBO, but Firestore update failed`,
//                         tenantId,
//                         docId: docRef.id,   // ✅ Firestore doc id for update handler
//                         qbId,
//                         logId: logDocId,
//                         warning: e,
//                         firestore: fsAfterErrorLocalSave || fsAfterPending || null,
//                     });
//                 }
//             } catch (err) {
//                 const e = pickErrorDetails(err);

//                 // Best-effort log + mark local status
//                 try {
//                     if (tenantId && cfg?.collection && docRef?.id) {
//                         await logFatal({
//                             tenantId,
//                             entityName,
//                             collectionName: cfg.collection,
//                             docId: logDocId,
//                             recordDocId: docRef.id,
//                             qbId,
//                             op: OPERATIONS.CREATE,
//                             msg: "Create crashed (unexpected)",
//                             err: e,
//                             req: payload,
//                         });

//                         await docRef.set(
//                             cleanUndefined({
//                                 localStatus: LOCAL_STATUS.ERROR_CREATE,
//                                 lastError: e,
//                                 retryPayload: payload || null,
//                                 updatedAt: utcNowISO(),
//                             }),
//                             { merge: true }
//                         );
//                     }
//                 } catch (inner) {
//                     logger.error("❌ Logging/marking ERROR_CREATE failed:", inner?.message || inner);
//                 }

//                 logger.error(`❌ create${entityName} error:`, err.message, err.response?.data);

//                 // ✅ return docId + firestore state if we have it
//                 let fsLatest = null;
//                 try {
//                     if (docRef) fsLatest = await readDocSafe(docRef);
//                 } catch { }

//                 return res.status(500).json({
//                     ok: false,
//                     error: err.message,
//                     details: err.response?.data || null,
//                     tenantId,
//                     docId: docRef?.id || null,
//                     logId: logDocId,
//                     firestore: fsLatest || fsAfterPending || null,
//                 });
//             }
//         }
//     );
// }

// function makeUpdateHandler(entityName) {
//     const cfg = ENTITY_CONFIG[entityName];

//     return onRequest(
//         {
//             region: "asia-south1",
//             timeoutSeconds: 120,
//             memory: "512MiB",
//             maxInstances: 10,
//         },
//         async (req, res) => {
//             let tenantId = null;
//             let docId = null;
//             let qbId = null;
//             let patch = null;
//             let logDocId = null;

//             try {
//                 if (req.method !== "POST") {
//                     return res.status(405).json({ error: "Only POST allowed" });
//                 }

//                 tenantId = req.query.tenantId || req.body?.tenantId;
//                 if (!tenantId) {
//                     return res.status(400).json({ error: "tenantId is required" });
//                 }

//                 const input = req.body;
//                 if (!input || typeof input !== "object") {
//                     return res.status(400).json({ error: "Request body must be a JSON object" });
//                 }

//                 const { docId: dId, tenantId: _t, ...rest } = input;
//                 if (!dId) {
//                     return res.status(400).json({ error: "docId (Firestore document id) is required" });
//                 }

//                 docId = dId;
//                 logDocId = `${entityName}_UPDATE_${tsId()}_${docId}`;
//                 patch = rest;

//                 await logStart({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     op: OPERATIONS.UPDATE,
//                     msg: "Update request received",
//                     req: patch,
//                 });

//                 // 1) Load Firestore record
//                 const { ref, data } = await getEntityDocByDocId(tenantId, cfg.collection, docId);
//                 qbId = (data.qbId || data.Id || "").toString();
//                 const syncToken = (data.SyncToken || "").toString();

//                 if (!qbId || !syncToken) {
//                     const e = { message: "Missing qbId or SyncToken on Firestore record" };

//                     await logFatal({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "Cannot update – missing qbId or SyncToken",
//                         err: e,
//                     });

//                     return res.status(400).json(e);
//                 }

//                 // 2) Update Firestore first
//                 try {
//                     await ref.set(
//                         cleanUndefined({
//                             ...patch,
//                             localStatus: LOCAL_STATUS.PENDING_UPDATE, // ✅ changed
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     await logFsOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "Firestore updated (PENDING_UPDATE)",
//                         res: { localStatus: LOCAL_STATUS.PENDING_UPDATE }, // ✅ changed
//                     });
//                 } catch (fsErr) {
//                     const e = pickErrorDetails(fsErr);

//                     await logFsFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "Firestore update failed (before QBO call)",
//                         err: e,
//                         req: patch,
//                     });

//                     throw fsErr;
//                 }

//                 // 3) Call QBO update
//                 const qboPayload = cleanUndefined({
//                     sparse: true,
//                     Id: qbId,
//                     SyncToken: syncToken,
//                     ...patch,
//                 });

//                 await logQboRequest({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     qbId,
//                     op: OPERATIONS.UPDATE,
//                     msg: "Calling QuickBooks update endpoint",
//                     req: qboPayload,
//                 });

//                 let updated = null;
//                 try {
//                     updated = await withQboClient(tenantId, async (client) => {
//                         const path = `/${entityName.toLowerCase()}?minorversion=${client.minorVersion}`;
//                         const qbRes = await qboPost(path, qboPayload, client);
//                         return qbRes.data?.[entityName];
//                     });
//                 } catch (qboErr) {
//                     const e = pickErrorDetails(qboErr);

//                     // store payload for retry
//                     await ref.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_UPDATE, // ✅ changed
//                             lastError: e,
//                             retryPayload: qboPayload,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     await logQboFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "QuickBooks update failed (saved for retry)",
//                         err: e,
//                         req: qboPayload,
//                     });

//                     return res.status(500).json({
//                         error: "QuickBooks update failed",
//                         details: e,
//                         docId,
//                         qbId,
//                     });
//                 }

//                 if (!updated?.Id) {
//                     const e = { message: "QBO did not return updated entity" };

//                     await ref.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_UPDATE, // ✅ changed
//                             lastError: e,
//                             retryPayload: qboPayload,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     await logQboFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "QuickBooks update returned invalid response",
//                         err: e,
//                         res: updated,
//                     });

//                     return res.status(500).json({
//                         error: e.message,
//                         docId: logDocId,
//                         qbId,
//                     });
//                 }

//                 // 4) Save QBO response to Firestore
//                 try {
//                     qbId = updated.Id.toString();

//                     await ref.set(
//                         cleanUndefined({
//                             ...updated,
//                             qbId,
//                             localStatus: LOCAL_STATUS.SYNCED, // ✅ changed
//                             lastError: null,
//                             retryPayload: null,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     await logQboOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "QuickBooks update succeeded",
//                         res: updated,
//                     });

//                     await logFsOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "Firestore updated with QBO response (SYNCED)",
//                         res: { localStatus: LOCAL_STATUS.SYNCED }, // ✅ changed
//                     });
//                 } catch (fsErr2) {
//                     const e = pickErrorDetails(fsErr2);

//                     await ref.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_LOCAL_SAVE, // ✅ changed
//                             lastError: e,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     await logFsFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "Firestore failed saving QBO response",
//                         err: e,
//                         res: updated,
//                     });

//                     return res.status(200).json({
//                         message: `${entityName} updated in QBO, but Firestore save failed`,
//                         tenantId,
//                         docId: logDocId,
//                         qbId,
//                         warning: e,
//                     });
//                 }

//                 await logEnd({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     qbId,
//                     op: OPERATIONS.UPDATE,
//                     msg: "Update finished",
//                 });

//                 return res.status(200).json({
//                     message: `${entityName} updated`,
//                     tenantId,
//                     docId: logDocId,
//                     qbId,
//                 });
//             } catch (err) {
//                 const e = pickErrorDetails(err);

//                 try {
//                     if (tenantId && cfg?.collection && docId) {
//                         await logFatal({
//                             tenantId,
//                             entityName,
//                             collectionName: cfg.collection,
//                             docId: logDocId,
//                             recordDocId: docId,
//                             qbId,
//                             op: OPERATIONS.UPDATE,
//                             msg: "Update crashed (unexpected)",
//                             err: e,
//                             req: patch,
//                         });

//                         const ref = getTenantCollection(tenantId, cfg.collection).doc(docId);
//                         await ref.set(
//                             cleanUndefined({
//                                 localStatus: LOCAL_STATUS.ERROR_UPDATE, // ✅ changed
//                                 lastError: e,
//                                 retryPayload: patch || null,
//                                 updatedAt: utcNowISO(),
//                             }),
//                             { merge: true }
//                         );
//                     }
//                 } catch (inner) {
//                     logger.error("❌ Logging ERROR_UPDATE failed:", inner?.message || inner);
//                 }

//                 logger.error(`❌ update${entityName} error:`, err.message, err.response?.data);
//                 return res.status(500).json({ error: err.message, details: err.response?.data || null });
//             }
//         }
//     );
// }

// function makeUpdateHandler(entityName) {
//     const cfg = ENTITY_CONFIG[entityName];

//     return onRequest(
//         {
//             region: "asia-south1",
//             timeoutSeconds: 120,
//             memory: "512MiB",
//             maxInstances: 10,
//         },
//         async (req, res) => {
//             let tenantId = null;
//             let docId = null;
//             let qbId = null;
//             let patch = null;
//             let logDocId = null;

//             // ✅ keep FS snapshots to return
//             let fsAfterPending = null;

//             try {
//                 if (req.method !== "POST") {
//                     return res.status(405).json({ error: "Only POST allowed" });
//                 }

//                 tenantId = req.query.tenantId || req.body?.tenantId;
//                 if (!tenantId) {
//                     return res.status(400).json({ error: "tenantId is required" });
//                 }

//                 const input = req.body;
//                 if (!input || typeof input !== "object") {
//                     return res.status(400).json({ error: "Request body must be a JSON object" });
//                 }

//                 const { docId: dId, tenantId: _t, ...rest } = input;
//                 if (!dId) {
//                     return res.status(400).json({ error: "docId (Firestore document id) is required" });
//                 }

//                 docId = dId;
//                 patch = rest;

//                 logDocId = `${entityName}_UPDATE_${tsId()}_${docId}`;

//                 await logStart({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     op: OPERATIONS.UPDATE,
//                     msg: "Update request received",
//                     req: patch,
//                 });

//                 // 1) Load Firestore record
//                 const { ref, data } = await getEntityDocByDocId(tenantId, cfg.collection, docId);
//                 qbId = (data.qbId || data.Id || "").toString();
//                 const syncToken = (data.SyncToken || "").toString();

//                 if (!qbId || !syncToken) {
//                     const e = { message: "Missing qbId or SyncToken on Firestore record" };

//                     await logFatal({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "Cannot update – missing qbId or SyncToken",
//                         err: e,
//                     });

//                     const fsNow = await readDocSafe(ref).catch(() => null);

//                     return res.status(400).json({
//                         ok: false,
//                         error: e.message,
//                         tenantId,
//                         docId,
//                         qbId: qbId || null,
//                         logId: logDocId,
//                         firestore: fsNow || null,
//                     });
//                 }

//                 // 2) Update Firestore first (PENDING_UPDATE)
//                 try {
//                     await ref.set(
//                         cleanUndefined({
//                             ...patch,
//                             localStatus: LOCAL_STATUS.PENDING_UPDATE,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     fsAfterPending = await readDocSafe(ref);

//                     await logFsOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "Firestore updated (PENDING_UPDATE)",
//                         res: { localStatus: LOCAL_STATUS.PENDING_UPDATE },
//                     });
//                 } catch (fsErr) {
//                     const e = pickErrorDetails(fsErr);

//                     await logFsFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "Firestore update failed (before QBO call)",
//                         err: e,
//                         req: patch,
//                     });

//                     throw fsErr;
//                 }

//                 // 3) Call QBO update
//                 const qboPayload = cleanUndefined({
//                     sparse: true,
//                     Id: qbId,
//                     SyncToken: syncToken,
//                     ...patch,
//                 });

//                 await logQboRequest({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     qbId,
//                     op: OPERATIONS.UPDATE,
//                     msg: "Calling QuickBooks update endpoint",
//                     req: qboPayload,
//                 });

//                 let updated = null;
//                 try {
//                     updated = await withQboClient(tenantId, async (client) => {
//                         const path = `/${entityName.toLowerCase()}?minorversion=${client.minorVersion}`;
//                         const qbRes = await qboPost(path, qboPayload, client);
//                         return qbRes.data?.[entityName];
//                     });
//                 } catch (qboErr) {
//                     const e = pickErrorDetails(qboErr);

//                     await ref.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_UPDATE,
//                             lastError: e,
//                             retryPayload: qboPayload,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     const fsAfterError = await readDocSafe(ref);

//                     await logQboFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "QuickBooks update failed (saved for retry)",
//                         err: e,
//                         req: qboPayload,
//                     });

//                     return res.status(500).json({
//                         ok: false,
//                         error: "QuickBooks update failed",
//                         details: e,
//                         tenantId,
//                         docId, // ✅ Firestore doc id
//                         qbId,
//                         logId: logDocId,
//                         firestore: fsAfterError || fsAfterPending || null,
//                     });
//                 }

//                 if (!updated?.Id) {
//                     const e = { message: "QBO did not return updated entity (missing Id)" };

//                     await ref.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_UPDATE,
//                             lastError: e,
//                             retryPayload: qboPayload,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     const fsAfterError = await readDocSafe(ref);

//                     await logQboFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "QuickBooks update returned invalid response",
//                         err: e,
//                         res: updated,
//                     });

//                     return res.status(500).json({
//                         ok: false,
//                         error: e.message,
//                         tenantId,
//                         docId,
//                         qbId,
//                         logId: logDocId,
//                         firestore: fsAfterError || fsAfterPending || null,
//                     });
//                 }

//                 // 4) Save QBO response to Firestore (SYNCED)
//                 try {
//                     qbId = updated.Id.toString();

//                     await ref.set(
//                         cleanUndefined({
//                             ...updated,
//                             qbId,
//                             localStatus: LOCAL_STATUS.SYNCED,
//                             lastError: null,
//                             retryPayload: null,
//                             updatedAt: utcNowISO(),
//                             _tenantId: tenantId,
//                             _entityType: entityName,
//                         }),
//                         { merge: true }
//                     );

//                     const fsAfterSynced = await readDocSafe(ref);

//                     await logQboOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "QuickBooks update succeeded",
//                         res: updated,
//                     });

//                     await logFsOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "Firestore updated with QBO response (SYNCED)",
//                         res: { localStatus: LOCAL_STATUS.SYNCED },
//                     });

//                     await logEnd({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "Update finished",
//                     });

//                     return res.status(200).json({
//                         ok: true,
//                         message: `${entityName} updated`,
//                         tenantId,
//                         docId,
//                         qbId,
//                         logId: logDocId,
//                         firestore: fsAfterSynced || null,
//                     });
//                 } catch (fsErr2) {
//                     const e = pickErrorDetails(fsErr2);

//                     await ref.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_LOCAL_SAVE,
//                             lastError: e,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     const fsAfterErrorLocalSave = await readDocSafe(ref);

//                     await logFsFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.UPDATE,
//                         msg: "Firestore failed saving QBO response (QBO updated but local save failed)",
//                         err: e,
//                         res: updated,
//                     });

//                     // ✅ still return docId + firestore state
//                     return res.status(200).json({
//                         ok: false,
//                         message: `${entityName} updated in QBO, but Firestore save failed`,
//                         tenantId,
//                         docId,
//                         qbId,
//                         logId: logDocId,
//                         warning: e,
//                         firestore: fsAfterErrorLocalSave || fsAfterPending || null,
//                     });
//                 }
//             } catch (err) {
//                 const e = pickErrorDetails(err);

//                 // best-effort log + mark local status (and return docId + firestore if possible)
//                 try {
//                     if (tenantId && cfg?.collection && docId) {
//                         await logFatal({
//                             tenantId,
//                             entityName,
//                             collectionName: cfg.collection,
//                             docId: logDocId,
//                             recordDocId: docId,
//                             qbId,
//                             op: OPERATIONS.UPDATE,
//                             msg: "Update crashed (unexpected)",
//                             err: e,
//                             req: patch,
//                         });

//                         const ref2 = getTenantCollection(tenantId, cfg.collection).doc(docId);
//                         await ref2.set(
//                             cleanUndefined({
//                                 localStatus: LOCAL_STATUS.ERROR_UPDATE,
//                                 lastError: e,
//                                 retryPayload: patch || null,
//                                 updatedAt: utcNowISO(),
//                             }),
//                             { merge: true }
//                         );
//                     }
//                 } catch (inner) {
//                     logger.error("❌ Logging ERROR_UPDATE failed:", inner?.message || inner);
//                 }

//                 logger.error(`❌ update${entityName} error:`, err.message, err.response?.data);

//                 let fsLatest = null;
//                 try {
//                     if (tenantId && docId) {
//                         const ref3 = getTenantCollection(tenantId, cfg.collection).doc(docId);
//                         fsLatest = await readDocSafe(ref3);
//                     }
//                 } catch { }

//                 return res.status(500).json({
//                     ok: false,
//                     error: err.message,
//                     details: err.response?.data || null,
//                     tenantId,
//                     docId: docId || null,
//                     qbId: qbId || null,
//                     logId: logDocId,
//                     firestore: fsLatest || fsAfterPending || null,
//                 });
//             }
//         }
//     );
// }

// function makeDeleteHandler(entityName) {
//     const cfg = ENTITY_CONFIG[entityName];

//     return onRequest(
//         {
//             region: "asia-south1",
//             timeoutSeconds: 120,
//             memory: "512MiB",
//             maxInstances: 10,
//         },
//         async (req, res) => {
//             let tenantId = null;
//             let docId = null;
//             let qbId = null;
//             let logDocId = null;

//             try {
//                 if (req.method !== "POST") {
//                     return res.status(405).json({ error: "Only POST allowed" });
//                 }

//                 tenantId = req.query.tenantId || req.body?.tenantId;
//                 if (!tenantId) {
//                     return res.status(400).json({ error: "tenantId is required" });
//                 }

//                 const input = req.body;
//                 if (!input || typeof input !== "object") {
//                     return res.status(400).json({ error: "Request body must be a JSON object" });
//                 }

//                 docId = input.docId;
//                 if (!docId) {
//                     return res.status(400).json({ error: "docId (Firestore document id) is required" });
//                 }

//                 logDocId = `${entityName}_DELETE_${tsId()}_${docId}`;

//                 await logStart({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     op: OPERATIONS.DELETE,
//                     msg: "Delete request received",
//                     req: { docId },
//                 });

//                 // 1) Load Firestore record
//                 const { ref, data } = await getEntityDocByDocId(tenantId, cfg.collection, docId);
//                 qbId = (data.qbId || data.Id || "").toString();
//                 const syncToken = (data.SyncToken || "").toString();

//                 if (!qbId || !syncToken) {
//                     const e = { message: "Missing qbId or SyncToken on Firestore record" };

//                     await logFatal({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         op: OPERATIONS.DELETE,
//                         msg: "Cannot delete – missing qbId or SyncToken",
//                         err: e,
//                     });

//                     return res.status(400).json(e);
//                 }

//                 // 2) Mark deleted locally first
//                 try {
//                     await ref.set(
//                         cleanUndefined({
//                             isDeleted: true,
//                             localStatus: LOCAL_STATUS.PENDING_DELETE, // ✅ changed
//                             deletedAt: utcNowISO(),
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     await logFsOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.DELETE,
//                         msg: "Firestore marked deleted (PENDING_DELETE)",
//                         res: { localStatus: LOCAL_STATUS.PENDING_DELETE, isDeleted: true }, // ✅ changed
//                     });
//                 } catch (fsErr) {
//                     const e = pickErrorDetails(fsErr);

//                     await logFsFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.DELETE,
//                         msg: "Firestore delete-mark failed (before QBO call)",
//                         err: e,
//                     });

//                     throw fsErr;
//                 }

//                 // 3) Call QBO delete
//                 const qboPayload = { Id: qbId, SyncToken: syncToken };

//                 await logQboRequest({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     qbId,
//                     op: OPERATIONS.DELETE,
//                     msg: "Calling QuickBooks delete endpoint",
//                     req: qboPayload,
//                 });

//                 let deletedRes = null;
//                 try {
//                     deletedRes = await withQboClient(tenantId, async (client) => {
//                         const qbRes = await qboDelete(entityName, qbId, syncToken, client);
//                         return qbRes.data?.[entityName] || qbRes.data || null;
//                     });
//                 } catch (qboErr) {
//                     const e = pickErrorDetails(qboErr);

//                     // store payload for retry
//                     await ref.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_DELETE, // ✅ changed
//                             lastError: e,
//                             retryPayload: qboPayload,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     await logQboFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.DELETE,
//                         msg: "QuickBooks delete failed (saved for retry)",
//                         err: e,
//                         req: qboPayload,
//                     });

//                     return res.status(500).json({
//                         error: "QuickBooks delete failed",
//                         details: e,
//                         docId: logDocId,
//                         qbId,
//                     });
//                 }

//                 await logQboOk({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     qbId,
//                     op: OPERATIONS.DELETE,
//                     msg: "QuickBooks delete succeeded",
//                     res: deletedRes,
//                 });

//                 // 4) Final Firestore update
//                 try {
//                     await ref.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.SYNCED, // ✅ changed
//                             lastError: null,
//                             retryPayload: null,
//                             updatedAt: utcNowISO(),
//                             qbDeleteResponse: deletedRes,
//                         }),
//                         { merge: true }
//                     );

//                     await logFsOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.DELETE,
//                         msg: "Firestore updated after QBO delete (SYNCED)",
//                         res: { localStatus: LOCAL_STATUS.SYNCED }, // ✅ changed
//                     });
//                 } catch (fsErr2) {
//                     const e = pickErrorDetails(fsErr2);

//                     await ref.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_LOCAL_SAVE, // ✅ changed
//                             lastError: e,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     await logFsFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.DELETE,
//                         msg: "Firestore failed saving delete result (QBO delete succeeded)",
//                         err: e,
//                         res: deletedRes,
//                     });

//                     return res.status(200).json({
//                         message: `${entityName} deleted in QBO, but Firestore save failed`,
//                         tenantId,
//                         docId: logDocId,
//                         qbId,
//                         warning: e,
//                     });
//                 }

//                 await logEnd({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     qbId,
//                     op: OPERATIONS.DELETE,
//                     msg: "Delete finished",
//                 });

//                 return res.status(200).json({
//                     message: `${entityName} deleted`,
//                     tenantId,
//                     docId: logDocId,
//                     qbId,
//                 });
//             } catch (err) {
//                 const e = pickErrorDetails(err);

//                 try {
//                     if (tenantId && cfg?.collection && docId) {
//                         await logFatal({
//                             tenantId,
//                             entityName,
//                             collectionName: cfg.collection,
//                             docId: logDocId,
//                             recordDocId: docId,
//                             qbId,
//                             op: OPERATIONS.DELETE,
//                             msg: "Delete crashed (unexpected)",
//                             err: e,
//                         });

//                         const ref = getTenantCollection(tenantId, cfg.collection).doc(docId);
//                         await ref.set(
//                             cleanUndefined({
//                                 localStatus: LOCAL_STATUS.ERROR_DELETE, // ✅ changed
//                                 lastError: e,
//                                 retryPayload: qbId ? { Id: qbId } : null,
//                                 updatedAt: utcNowISO(),
//                             }),
//                             { merge: true }
//                         );
//                     }
//                 } catch (inner) {
//                     logger.error("❌ Logging ERROR_DELETE failed:", inner?.message || inner);
//                 }

//                 logger.error(`❌ delete${entityName} error:`, err.message, err.response?.data);
//                 return res.status(500).json({ error: err.message, details: err.response?.data || null });
//             }
//         }
//     );
// }

// function makeDeleteHandler(entityName) {
//     const cfg = ENTITY_CONFIG[entityName];

//     return onRequest(
//         {
//             region: "asia-south1",
//             timeoutSeconds: 120,
//             memory: "512MiB",
//             maxInstances: 10,
//         },
//         async (req, res) => {
//             let tenantId = null;
//             let docId = null;
//             let qbId = null;
//             let logDocId = null;

//             // ✅ keep FS snapshots to return
//             let fsAfterPending = null;

//             try {
//                 if (req.method !== "POST") {
//                     return res.status(405).json({ error: "Only POST allowed" });
//                 }

//                 tenantId = req.query.tenantId || req.body?.tenantId;
//                 if (!tenantId) {
//                     return res.status(400).json({ error: "tenantId is required" });
//                 }

//                 const input = req.body;
//                 if (!input || typeof input !== "object") {
//                     return res.status(400).json({ error: "Request body must be a JSON object" });
//                 }

//                 docId = input.docId;
//                 if (!docId) {
//                     return res.status(400).json({ error: "docId (Firestore document id) is required" });
//                 }

//                 logDocId = `${entityName}_DELETE_${tsId()}_${docId}`;

//                 await logStart({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     op: OPERATIONS.DELETE,
//                     msg: "Delete request received",
//                     req: { docId },
//                 });

//                 // 1) Load Firestore record
//                 const { ref, data } = await getEntityDocByDocId(tenantId, cfg.collection, docId);
//                 qbId = (data.qbId || data.Id || "").toString();
//                 const syncToken = (data.SyncToken || "").toString();

//                 if (!qbId || !syncToken) {
//                     const e = { message: "Missing qbId or SyncToken on Firestore record" };

//                     await logFatal({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId: qbId || null,
//                         op: OPERATIONS.DELETE,
//                         msg: "Cannot delete – missing qbId or SyncToken",
//                         err: e,
//                     });

//                     const fsNow = await readDocSafe(ref).catch(() => null);

//                     return res.status(400).json({
//                         ok: false,
//                         error: e.message,
//                         tenantId,
//                         docId,
//                         qbId: qbId || null,
//                         logId: logDocId,
//                         firestore: fsNow || null,
//                     });
//                 }

//                 // 2) Mark deleted locally first (PENDING_DELETE)
//                 try {
//                     await ref.set(
//                         cleanUndefined({
//                             isDeleted: true,
//                             localStatus: LOCAL_STATUS.PENDING_DELETE,
//                             deletedAt: utcNowISO(),
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     fsAfterPending = await readDocSafe(ref);

//                     await logFsOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.DELETE,
//                         msg: "Firestore marked deleted (PENDING_DELETE)",
//                         res: { localStatus: LOCAL_STATUS.PENDING_DELETE, isDeleted: true },
//                     });
//                 } catch (fsErr) {
//                     const e = pickErrorDetails(fsErr);

//                     await logFsFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.DELETE,
//                         msg: "Firestore delete-mark failed (before QBO call)",
//                         err: e,
//                     });

//                     throw fsErr;
//                 }

//                 // 3) Call QBO delete
//                 const qboPayload = { Id: qbId, SyncToken: syncToken };

//                 await logQboRequest({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     qbId,
//                     op: OPERATIONS.DELETE,
//                     msg: "Calling QuickBooks delete endpoint",
//                     req: qboPayload,
//                 });

//                 let deletedRes = null;
//                 try {
//                     deletedRes = await withQboClient(tenantId, async (client) => {
//                         const qbRes = await qboDelete(entityName, qbId, syncToken, client);
//                         // QB delete responses vary; keep whole response for debugging
//                         return qbRes.data?.[entityName] || qbRes.data || null;
//                     });
//                 } catch (qboErr) {
//                     const e = pickErrorDetails(qboErr);

//                     await ref.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_DELETE,
//                             lastError: e,
//                             retryPayload: qboPayload,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     const fsAfterError = await readDocSafe(ref);

//                     await logQboFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.DELETE,
//                         msg: "QuickBooks delete failed (saved for retry)",
//                         err: e,
//                         req: qboPayload,
//                     });

//                     return res.status(500).json({
//                         ok: false,
//                         error: "QuickBooks delete failed",
//                         details: e,
//                         tenantId,
//                         docId, // ✅ Firestore doc id
//                         qbId,
//                         logId: logDocId,
//                         firestore: fsAfterError || fsAfterPending || null,
//                     });
//                 }

//                 await logQboOk({
//                     tenantId,
//                     entityName,
//                     collectionName: cfg.collection,
//                     docId: logDocId,
//                     qbId,
//                     op: OPERATIONS.DELETE,
//                     msg: "QuickBooks delete succeeded",
//                     res: deletedRes,
//                 });

//                 // 4) Final Firestore update (SYNCED)
//                 try {
//                     await ref.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.SYNCED,
//                             lastError: null,
//                             retryPayload: null,
//                             updatedAt: utcNowISO(),
//                             qbDeleteResponse: deletedRes,
//                         }),
//                         { merge: true }
//                     );

//                     const fsAfterSynced = await readDocSafe(ref);

//                     await logFsOk({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.DELETE,
//                         msg: "Firestore updated after QBO delete (SYNCED)",
//                         res: { localStatus: LOCAL_STATUS.SYNCED },
//                     });

//                     await logEnd({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         qbId,
//                         op: OPERATIONS.DELETE,
//                         msg: "Delete finished",
//                     });

//                     return res.status(200).json({
//                         ok: true,
//                         message: `${entityName} deleted`,
//                         tenantId,
//                         docId,
//                         qbId,
//                         logId: logDocId,
//                         firestore: fsAfterSynced || null,
//                     });
//                 } catch (fsErr2) {
//                     const e = pickErrorDetails(fsErr2);

//                     await ref.set(
//                         cleanUndefined({
//                             localStatus: LOCAL_STATUS.ERROR_LOCAL_SAVE,
//                             lastError: e,
//                             updatedAt: utcNowISO(),
//                         }),
//                         { merge: true }
//                     );

//                     const fsAfterErrorLocalSave = await readDocSafe(ref);

//                     await logFsFail({
//                         tenantId,
//                         entityName,
//                         collectionName: cfg.collection,
//                         docId: logDocId,
//                         recordDocId: docId,
//                         qbId,
//                         op: OPERATIONS.DELETE,
//                         msg: "Firestore failed saving delete result (QBO delete succeeded)",
//                         err: e,
//                         res: deletedRes,
//                     });

//                     // ✅ still return docId + firestore state
//                     return res.status(200).json({
//                         ok: false,
//                         message: `${entityName} deleted in QBO, but Firestore save failed`,
//                         tenantId,
//                         docId,
//                         qbId,
//                         logId: logDocId,
//                         warning: e,
//                         firestore: fsAfterErrorLocalSave || fsAfterPending || null,
//                     });
//                 }
//             } catch (err) {
//                 const e = pickErrorDetails(err);

//                 // best-effort log + mark local status (and return docId + firestore if possible)
//                 try {
//                     if (tenantId && cfg?.collection && docId) {
//                         await logFatal({
//                             tenantId,
//                             entityName,
//                             collectionName: cfg.collection,
//                             docId: logDocId,
//                             recordDocId: docId,
//                             qbId,
//                             op: OPERATIONS.DELETE,
//                             msg: "Delete crashed (unexpected)",
//                             err: e,
//                         });

//                         const ref2 = getTenantCollection(tenantId, cfg.collection).doc(docId);
//                         await ref2.set(
//                             cleanUndefined({
//                                 localStatus: LOCAL_STATUS.ERROR_DELETE,
//                                 lastError: e,
//                                 retryPayload: qbId ? { Id: qbId } : null,
//                                 updatedAt: utcNowISO(),
//                             }),
//                             { merge: true }
//                         );
//                     }
//                 } catch (inner) {
//                     logger.error("❌ Logging ERROR_DELETE failed:", inner?.message || inner);
//                 }

//                 logger.error(`❌ delete${entityName} error:`, err.message, err.response?.data);

//                 let fsLatest = null;
//                 try {
//                     if (tenantId && docId) {
//                         const ref3 = getTenantCollection(tenantId, cfg.collection).doc(docId);
//                         fsLatest = await readDocSafe(ref3);
//                     }
//                 } catch { }

//                 return res.status(500).json({
//                     ok: false,
//                     error: err.message,
//                     details: err.response?.data || null,
//                     tenantId,
//                     docId: docId || null,
//                     qbId: qbId || null,
//                     logId: logDocId,
//                     firestore: fsLatest || fsAfterPending || null,
//                 });
//             }
//         }
//     );
// }

// ========================================================================================
// BULK LOAD HANDLER
// ========================================================================================

// async function loadEntityFromQboToFirestore(tenantId, { entityName, collectionName, pageSize = 500 }) {
//     const runId = `${entityName}_BULK_LOAD_${tsId()}_run`;

//     await logStart({
//         tenantId,
//         entityName,
//         collectionName,
//         docId: runId,
//         op: OPERATIONS.BULK_LOAD,
//         msg: `Bulk load started (pageSize=${pageSize})`,
//         req: { entityName, collectionName, pageSize },
//     });

//     try {
//         const total = await withQboClient(tenantId, async (client) => {
//             let startPosition = 1;
//             let totalSaved = 0;

//             while (true) {
//                 const sql = `SELECT * FROM ${entityName} STARTPOSITION ${startPosition} MAXRESULTS ${pageSize}`;

//                 await logQboRequest({
//                     tenantId,
//                     entityName,
//                     collectionName,
//                     docId: runId,
//                     op: OPERATIONS.BULK_LOAD,
//                     msg: "Querying QBO",
//                     req: { sql, startPosition, pageSize },
//                 });

//                 let qbRes;
//                 try {
//                     qbRes = await qboQuery(sql, client);
//                 } catch (qboErr) {
//                     const e = pickErrorDetails(qboErr);
//                     await logQboFail({
//                         tenantId,
//                         entityName,
//                         collectionName,
//                         docId: runId,
//                         op: OPERATIONS.BULK_LOAD,
//                         msg: "QBO query failed",
//                         err: e,
//                         req: { sql },
//                     });
//                     throw qboErr;
//                 }

//                 const list = qbRes.data?.QueryResponse?.[entityName] || [];
//                 const got = list.length;

//                 await logQboOk({
//                     tenantId,
//                     entityName,
//                     collectionName,
//                     docId: runId,
//                     op: OPERATIONS.BULK_LOAD,
//                     msg: `QBO query ok (fetched ${got})`,
//                     res: { startPosition, fetched: got },
//                 });

//                 if (!got) break;

//                 let savedThisPage = 0;
//                 try {
//                     const safeList = list.map((x) => cleanUndefined(x));
//                     savedThisPage = await saveListToFirestore(tenantId, collectionName, safeList, entityName);
//                 } catch (fsErr) {
//                     const e = pickErrorDetails(fsErr);
//                     await logFsFail({
//                         tenantId,
//                         entityName,
//                         collectionName,
//                         docId: runId,
//                         op: OPERATIONS.BULK_LOAD,
//                         msg: `Firestore save failed (startPosition=${startPosition})`,
//                         err: e,
//                         req: { startPosition, fetched: got },
//                     });
//                     throw fsErr;
//                 }

//                 totalSaved += savedThisPage;

//                 await logFsOk({
//                     tenantId,
//                     entityName,
//                     collectionName,
//                     docId: runId,
//                     op: OPERATIONS.BULK_LOAD,
//                     msg: `Saved page (saved=${savedThisPage}, total=${totalSaved})`,
//                     res: { startPosition, savedThisPage, totalSaved },
//                 });

//                 if (got < pageSize) break;
//                 startPosition += pageSize;
//             }

//             return totalSaved;
//         });

//         await logEnd({
//             tenantId,
//             entityName,
//             collectionName,
//             docId: runId,
//             op: OPERATIONS.BULK_LOAD,
//             msg: `Bulk load finished (totalSaved=${total})`,
//             res: { totalSaved: total },
//         });

//         return total;
//     } catch (err) {
//         const e = pickErrorDetails(err);
//         await logFatal({
//             tenantId,
//             entityName,
//             collectionName,
//             docId: runId,
//             op: OPERATIONS.BULK_LOAD,
//             msg: "Bulk load crashed",
//             err: e,
//         });
//         throw err;
//     }
// }

// function makeBulkLoadHandler(entityName, collectionName, defaultPageSize = 500) {
//     return onRequest(
//         {
//             region: "asia-south1",
//             timeoutSeconds: 540,
//             memory: "1GiB",
//             maxInstances: 3,
//         },
//         async (req, res) => {
//             try {
//                 if (req.method !== "GET") {
//                     return res.status(405).send("Only GET allowed");
//                 }

//                 const tenantId = req.query.tenantId;
//                 if (!tenantId) {
//                     return res.status(400).json({ error: "tenantId query parameter is required" });
//                 }

//                 const total = await loadEntityFromQboToFirestore(tenantId, {
//                     entityName,
//                     collectionName,
//                     pageSize: defaultPageSize,
//                 });

//                 return res.status(200).json({
//                     message: `${entityName} loaded from QBO into Firestore`,
//                     tenantId,
//                     count: total,
//                 });
//             } catch (err) {
//                 logger.error(`❌ load${entityName} error:`, err.message, err.response?.data);
//                 return res.status(500).json({
//                     error: err.message,
//                     details: err.response?.data || null,
//                 });
//             }
//         }
//     );
// }

// function makeListHandler(entityName) {
//     const cfg = ENTITY_CONFIG[entityName];
//     if (!cfg?.collection) {
//         throw new Error(`ENTITY_CONFIG missing for ${entityName}`);
//     }

//     return onRequest(
//         {
//             region: "asia-south1",
//             timeoutSeconds: 120,
//             memory: "512MiB",
//             maxInstances: 10,
//         },
//         async (req, res) => {
//             try {
//                 if (req.method !== "GET") {
//                     return res.status(405).json({ error: "Only GET allowed" });
//                 }

//                 const tenantId = req.query.tenantId;
//                 if (!tenantId) {
//                     return res.status(400).json({ error: "tenantId query parameter is required" });
//                 }

//                 const qbId = req.query.qbId ? String(req.query.qbId) : null;
//                 const status = req.query.status ? String(req.query.status) : null;
//                 const isDeleted = parseBool(req.query.isDeleted);

//                 const includeTotalCount = parseBool(req.query.includeTotalCount) === true;

//                 const orderByRaw = req.query.orderBy ? String(req.query.orderBy) : "updatedAt";
//                 const orderBy = ["updatedAt", "createdAt"].includes(orderByRaw) ? orderByRaw : "updatedAt";

//                 const orderDirRaw = req.query.orderDir ? String(req.query.orderDir).toLowerCase() : "desc";
//                 const orderDir = orderDirRaw === "asc" ? "asc" : "desc";

//                 // Optional date filters (ISO strings)
//                 const updatedFrom = parseISODate(req.query.updatedFrom);
//                 const updatedTo = parseISODate(req.query.updatedTo);
//                 const createdFrom = parseISODate(req.query.createdFrom);
//                 const createdTo = parseISODate(req.query.createdTo);

//                 // Pagination is OPTIONAL:
//                 // - if page/pageSize absent -> return ALL
//                 const hasPage = req.query.page !== undefined && String(req.query.page) !== "";
//                 const hasPageSize = req.query.pageSize !== undefined && String(req.query.pageSize) !== "";
//                 const usePaging = hasPage || hasPageSize;

//                 const page = usePaging ? Math.max(parseIntSafe(req.query.page, 1), 1) : null;
//                 const pageSize = usePaging
//                     ? Math.min(Math.max(parseIntSafe(req.query.pageSize, 50), 1), 200)
//                     : null;

//                 const col = getTenantCollection(tenantId, cfg.collection);

//                 // Build query
//                 let q = col/*.where("_tenantId", "==", tenantId)*/;

//                 if (qbId) q = q.where("qbId", "==", qbId);
//                 if (status) q = q.where("localStatus", "==", status);
//                 if (isDeleted !== null) q = q.where("isDeleted", "==", isDeleted);

//                 if (updatedFrom) q = q.where("updatedAt", ">=", updatedFrom);
//                 if (updatedTo) q = q.where("updatedAt", "<=", updatedTo);
//                 if (createdFrom) q = q.where("createdAt", ">=", createdFrom);
//                 if (createdTo) q = q.where("createdAt", "<=", createdTo);

//                 // Stable ordering
//                 q = q.orderBy(orderBy, orderDir).orderBy(FieldPath.documentId(), orderDir);

//                 // Total count (optional)
//                 let totalCount = null;
//                 if (includeTotalCount) {
//                     totalCount = await getTotalCountSafe(q);
//                 }

//                 // Fetch
//                 let snap;
//                 let offset = 0;

//                 if (!usePaging) {
//                     snap = await q.get(); // ✅ ALL
//                 } else {
//                     offset = (page - 1) * pageSize;
//                     snap = await q.offset(offset).limit(pageSize).get();
//                 }

//                 const records = snap.docs.map((d) => ({
//                     docId: d.id, // ✅ include Firestore docId
//                     ...(d.data() || {}),
//                 }));

//                 return res.status(200).json({
//                     ok: true,
//                     entityName,
//                     collection: cfg.collection,
//                     tenantId,
//                     filters: {
//                         qbId: qbId || null,
//                         status: status || null,
//                         isDeleted,
//                         updatedFrom,
//                         updatedTo,
//                         createdFrom,
//                         createdTo,
//                     },
//                     order: { orderBy, orderDir },
//                     pagination: usePaging
//                         ? { page, pageSize, offset, returned: records.length }
//                         : { all: true, returned: records.length },
//                     totalCount, // null if not requested or if aggregation not supported
//                     records,
//                 });
//             } catch (err) {
//                 logger.error(`❌ list${entityName} error:`, err?.message || err);
//                 return res.status(500).json({ ok: false, error: err?.message || String(err) });
//             }
//         }
//     );
// }

// ========================================================================================
// TENANT MANAGEMENT ENDPOINTS
// ========================================================================================

exports.registerTenant = onRequest(
    {
        region: "asia-south1",
        timeoutSeconds: 120,
        memory: "256MiB"
    },
    async (req, res) => {
        try {
            if (req.method !== "POST") {
                return res.status(405).json({ error: "Only POST allowed" });
            }

            const {
                tenantId,
                clientId,
                clientSecret,
                refreshToken,
                realmId,
                webhookVerifier,
                baseUrl,
                minorVersion,
                environment,
                metadata,
            } = req.body;

            if (!tenantId || !clientId || !clientSecret || !refreshToken || !realmId || !webhookVerifier) {
                return res.status(400).json({
                    error: "Missing required fields",
                    required: ["tenantId", "clientId", "clientSecret", "refreshToken", "realmId", "webhookVerifier"],
                });
            }

            // Store sensitive credentials in Secret Manager
            await Promise.all([
                storeSecret(tenantId, 'clientId', clientId),
                storeSecret(tenantId, 'clientSecret', clientSecret),
                storeSecret(tenantId, 'refreshToken', refreshToken),
                storeSecret(tenantId, 'webhookVerifier', webhookVerifier),
            ]);

            // Store non-sensitive metadata in Firestore
            const tenantData = {
                tenantId,
                realmId,
                baseUrl: baseUrl || "https://quickbooks.api.intuit.com/v3/company",
                minorVersion: minorVersion || "65",
                environment: environment || "production",
                isActive: true,
                metadata: metadata || {},
                createdAt: utcNowISO(),
                updatedAt: utcNowISO(),
                // Do NOT store credentials here
            };

            await qbDb.collection("qb_tenants").doc(tenantId).set(tenantData, { merge: true });

            logger.info(`✅ Tenant ${tenantId} registered successfully`);

            return res.status(200).json({
                message: "Tenant registered successfully (credentials stored securely in Secret Manager)",
                tenantId,
            });
        } catch (err) {
            logger.error("❌ registerTenant error:", err.message);
            return res.status(500).json({ error: err.message });
        }
    }
);

exports.updateTenant = onRequest(
    {
        region: "asia-south1",
        timeoutSeconds: 120,
        memory: "256MiB"
    },
    async (req, res) => {
        try {
            if (req.method !== "POST") {
                return res.status(405).json({ error: "Only POST allowed" });
            }

            const {
                tenantId,
                clientId,
                clientSecret,
                refreshToken,
                webhookVerifier,
                ...firestoreUpdates
            } = req.body;

            if (!tenantId) {
                return res.status(400).json({ error: "tenantId is required" });
            }

            // Update secrets if provided
            const secretUpdates = [];
            if (clientId) secretUpdates.push(storeSecret(tenantId, 'clientId', clientId));
            if (clientSecret) secretUpdates.push(storeSecret(tenantId, 'clientSecret', clientSecret));
            if (refreshToken) secretUpdates.push(storeSecret(tenantId, 'refreshToken', refreshToken));
            if (webhookVerifier) secretUpdates.push(storeSecret(tenantId, 'webhookVerifier', webhookVerifier));

            if (secretUpdates.length > 0) {
                await Promise.all(secretUpdates);
            }

            // Update Firestore metadata
            delete firestoreUpdates.createdAt;
            delete firestoreUpdates.tenantId;
            firestoreUpdates.updatedAt = utcNowISO();

            if (Object.keys(firestoreUpdates).length > 1) { // More than just updatedAt
                await qbDb.collection("qb_tenants").doc(tenantId).update(firestoreUpdates);
            }

            logger.info(`✅ Tenant ${tenantId} updated successfully`);

            return res.status(200).json({
                message: "Tenant updated successfully",
                tenantId,
            });
        } catch (err) {
            logger.error("❌ updateTenant error:", err.message);
            return res.status(500).json({ error: err.message });
        }
    }
);

exports.deactivateTenant = onRequest(
    {
        region: "asia-south1",
        timeoutSeconds: 60,
        memory: "256MiB"
    },
    async (req, res) => {
        try {
            if (req.method !== "POST") {
                return res.status(405).json({ error: "Only POST allowed" });
            }

            const { tenantId } = req.body;

            if (!tenantId) {
                return res.status(400).json({ error: "tenantId is required" });
            }

            await qbDb.collection("qb_tenants").doc(tenantId).update({
                isActive: false,
                deactivatedAt: utcNowISO(),
                updatedAt: utcNowISO(),
            });

            logger.info(`✅ Tenant ${tenantId} deactivated`);

            return res.status(200).json({
                message: "Tenant deactivated successfully (credentials retained in Secret Manager)",
                tenantId,
            });
        } catch (err) {
            logger.error("❌ deactivateTenant error:", err.message);
            return res.status(500).json({ error: err.message });
        }
    }
);

exports.deleteTenant = onRequest(
    {
        region: "asia-south1",
        timeoutSeconds: 120,
        memory: "256MiB"
    },
    async (req, res) => {
        try {
            if (req.method !== "POST") {
                return res.status(405).json({ error: "Only POST allowed" });
            }

            const { tenantId, confirmDelete } = req.body;

            if (!tenantId) {
                return res.status(400).json({ error: "tenantId is required" });
            }

            if (confirmDelete !== true) {
                return res.status(400).json({
                    error: "Must set confirmDelete=true to permanently delete tenant"
                });
            }

            // Delete secrets from Secret Manager
            await deleteTenantSecrets(tenantId);

            // Delete Firestore metadata
            await qbDb.collection("qb_tenants").doc(tenantId).delete();

            logger.info(`✅ Tenant ${tenantId} permanently deleted`);

            return res.status(200).json({
                message: "Tenant permanently deleted (including all secrets)",
                tenantId,
            });
        } catch (err) {
            logger.error("❌ deleteTenant error:", err.message);
            return res.status(500).json({ error: err.message });
        }
    }
);

// ========================================================================================
// EXPORTED ENDPOINTS
// ========================================================================================

// CRUD Operations
exports.createBill = makeCreateHandler("Bill");
exports.updateBill = makeUpdateHandler("Bill");
// exports.deleteBill = makeDeleteHandler("Bill");

// exports.createVendor = makeCreateHandler("Vendor");
// exports.updateVendor = makeUpdateHandler("Vendor");
// exports.deleteVendor = makeDeleteHandler("Vendor");

// exports.createAccount = makeCreateHandler("Account");
// exports.updateAccount = makeUpdateHandler("Account");
// exports.deleteAccount = makeDeleteHandler("Account");

// exports.createCustomer = makeCreateHandler("Customer");
// exports.updateCustomer = makeUpdateHandler("Customer");
// exports.deleteCustomer = makeDeleteHandler("Customer");

// exports.createInvoice = makeCreateHandler("Invoice");
// exports.updateInvoice = makeUpdateHandler("Invoice");
// exports.deleteInvoice = makeDeleteHandler("Invoice");

// exports.createPayment = makeCreateHandler("Payment");
// exports.updatePayment = makeUpdateHandler("Payment");
// exports.deletePayment = makeDeleteHandler("Payment");

// exports.createItem = makeCreateHandler("Item");
// exports.updateItem = makeUpdateHandler("Item");
// exports.deleteItem = makeDeleteHandler("Item");

exports.createSalesReceipt = makeCreateHandler("SalesReceipt");
exports.updateSalesReceipt = makeUpdateHandler("SalesReceipt");
// exports.deleteSalesReceipt = makeDeleteHandler("SalesReceipt");

//Bulk Load Operations
exports.loadVendors = makeBulkLoadHandler("Vendor", "vendors", 500);
exports.loadAccounts = makeBulkLoadHandler("Account", "accounts", 500);
exports.loadBills = makeBulkLoadHandler("Bill", "bills", 200);
exports.loadCustomers = makeBulkLoadHandler("Customer", "customers", 500);
exports.loadTaxRates = makeBulkLoadHandler("TaxRate", "taxRates", 500);
exports.loadTaxCodes = makeBulkLoadHandler("TaxCode", "taxCodes", 500);
exports.loadTerms = makeBulkLoadHandler("Term", "terms", 500);
exports.loadItems = makeBulkLoadHandler("Item", "items", 500);
exports.loadSalesReceipts = makeBulkLoadHandler("SalesReceipt", "salesReceipts", 500);
exports.loadPaymentMethods = makeBulkLoadHandler("PaymentMethod", "paymentMethods", 500);
exports.loadAttachables = makeBulkLoadHandler("Attachable", "attachables", 500);
// exports.loadInvoices = makeBulkLoadHandler("Invoice", "invoices", 200);
// exports.loadPayments = makeBulkLoadHandler("Payment", "payments", 300);


// Scheduled Retry Functions (Per-Entity)
exports.retryFailedBills = makeRetryHandler("Bill");
exports.retryFailedSalesReceipts = makeRetryHandler("SalesReceipt");
// exports.retryFailedVendors = makeRetryHandler("Vendor");
// exports.retryFailedAccounts = makeRetryHandler("Account");
// exports.retryFailedCustomers = makeRetryHandler("Customer");
// exports.retryFailedInvoices = makeRetryHandler("Invoice");
// exports.retryFailedPayments = makeRetryHandler("Payment");
// exports.retryFailedItems = makeRetryHandler("Item");

// List Entities
exports.listBills = makeListHandler("Bill");
exports.listAccounts = makeListHandler("Account");
exports.listVendors = makeListHandler("Vendor");
exports.listCustomers = makeListHandler("Customer");
exports.listInvoices = makeListHandler("Invoice");
exports.listTaxRates = makeListHandler("TaxRate");
exports.listTaxCodes = makeListHandler("TaxCode");
exports.listTerms = makeListHandler("Term");
exports.listSalesReceipts = makeListHandler("SalesReceipt");
exports.listPaymentMethods = makeListHandler("PaymentMethod");
exports.listAttachables = makeListHandler("Attachable");
//exports.listPayments = makeListHandler("Payment");
exports.listItems = makeListHandler("Item");



// ========================================================================================
// STORAGE CONFIGURATION
// ========================================================================================

const STORAGE_CONFIG = {
    BUCKET_NAME: 'mbs-app-3ffa0.firebasestorage.app',
    BASE_PATH: 'cdpTest',
    MAX_FILE_SIZE: 20 * 1024 * 1024, // 20MB
    MAX_FILES: 10,
    ALLOWED_EXTENSIONS: [
        '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.webp',
        '.doc', '.docx', '.xls', '.xlsx', '.csv',
        '.txt', '.zip', '.rar'
    ],
    SIGNED_URL_EXPIRY_DAYS: 7,
};

/**
 * Generate storage path for entity attachment
 */
function getStoragePath(tenantId, entityName, entityDocId, filename) {
    const fileExt = path.extname(filename);
    const safeFileName = `${uuidv4()}${fileExt}`;
    return `${STORAGE_CONFIG.BASE_PATH}/${tenantId}/${entityName}/${entityDocId}/${safeFileName}`;
}

/**
 * Get public URL for storage file
 */
function getPublicUrl(storagePath) {
    return `https://storage.googleapis.com/${STORAGE_CONFIG.BUCKET_NAME}/${storagePath}`;
}

/**
 * Validate file before upload
 */
function validateFile(file) {
    const errors = [];

    // Check file size
    if (file.size > STORAGE_CONFIG.MAX_FILE_SIZE) {
        errors.push(`File ${file.filename} exceeds maximum size of ${STORAGE_CONFIG.MAX_FILE_SIZE / (1024 * 1024)}MB`);
    }

    // Check file extension
    const fileExt = path.extname(file.filename).toLowerCase();
    if (fileExt && !STORAGE_CONFIG.ALLOWED_EXTENSIONS.includes(fileExt)) {
        errors.push(`File type ${fileExt} is not allowed for ${file.filename}`);
    }

    // Check if filename is valid
    if (!file.filename || file.filename.trim() === '') {
        errors.push('Filename cannot be empty');
    }

    return {
        isValid: errors.length === 0,
        errors,
    };
}

// ========================================================================================
// ATTACHMENT HANDLING UTILITIES
// ========================================================================================

const Busboy = require('busboy');
const { getStorage } = require('firebase-admin/storage');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

// Storage bucket reference
const bucket = getStorage().bucket(STORAGE_CONFIG.BUCKET_NAME);

/**
 * Parse multipart form data with Busboy - GENERIC VERSION
 */
function parseMultipartForm(req) {
    return new Promise((resolve, reject) => {
        const busboy = Busboy({
            headers: req.headers,
            limits: {
                fileSize: STORAGE_CONFIG.MAX_FILE_SIZE,
                files: STORAGE_CONFIG.MAX_FILES,
                fields: 50,
            }
        });

        const fields = {};
        const files = [];

        busboy.on('field', (fieldname, val) => {
            fields[fieldname] = val;
        });

        busboy.on('file', (fieldname, fileStream, info) => {
            const { filename, encoding, mimeType } = info;

            if (!filename) {
                fileStream.resume();
                return;
            }

            const chunks = [];

            fileStream.on('data', (chunk) => {
                chunks.push(chunk);
            });

            fileStream.on('end', () => {
                const buffer = Buffer.concat(chunks);

                if (buffer.length === 0) {
                    logger.warn(`Empty file detected: ${filename}`);
                    return;
                }

                files.push({
                    fieldname,
                    filename,
                    encoding,
                    mimeType,
                    buffer,
                    size: buffer.length,
                });

                logger.info(`📎 File received: ${filename} (${buffer.length} bytes)`);
            });

            fileStream.on('error', (err) => {
                logger.error(`File stream error for ${filename}:`, err);
                reject(err);
            });
        });

        busboy.on('finish', () => {
            logger.info(`✅ Form parsing complete: ${Object.keys(fields).length} fields, ${files.length} files`);
            resolve({ fields, files });
        });

        busboy.on('error', (err) => {
            logger.error('❌ Busboy error:', err);
            reject(err);
        });

        if (req.rawBody) {
            busboy.end(req.rawBody);
        } else {
            req.pipe(busboy);
        }
    });
}

/**
 * Smart JSON parser
 */
function tryParseJSON(str) {
    if (typeof str !== 'string') return str;

    const trimmed = str.trim();

    if (
        (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
        (trimmed.startsWith('[') && trimmed.endsWith(']'))
    ) {
        try {
            return JSON.parse(trimmed);
        } catch (e) {
            return str;
        }
    }

    if (trimmed === 'true') return true;
    if (trimmed === 'false') return false;
    if (trimmed === 'null') return null;

    if (!isNaN(trimmed) && trimmed !== '') {
        const num = Number(trimmed);
        if (Number.isFinite(num)) return num;
    }

    return str;
}

/**
 * Process parsed form fields into usable payload
 */
function processFormFields(fields) {
    if (fields.body) {
        try {
            const parsed = tryParseJSON(fields.body);
            if (typeof parsed === 'object' && parsed !== null) {
                logger.info(`📦 Using 'body' field as complete payload`);
                return parsed;
            }
        } catch (e) {
            logger.warn(`'body' field exists but is not valid JSON`);
        }
    }

    const result = {};
    for (const [key, value] of Object.entries(fields)) {
        result[key] = tryParseJSON(value);
    }

    logger.info(`📦 Processed ${Object.keys(result).length} individual fields`);
    return result;
}

/**
 * Upload file to Cloud Storage
 * Returns { storagePath, downloadUrl, metadata }
 */
async function uploadFileToStorage({
    tenantId,
    entityName,
    entityDocId,
    file,
    attachableDocId,
}) {
    // Validate file
    const validation = validateFile(file);
    if (!validation.isValid) {
        throw new Error(`File validation failed: ${validation.errors.join(', ')}`);
    }

    // Generate storage path using helper
    const storagePath = getStoragePath(tenantId, entityName, entityDocId, file.filename);
    const fileRef = bucket.file(storagePath);

    await fileRef.save(file.buffer, {
        metadata: {
            contentType: file.mimeType,
            metadata: {
                originalName: file.filename,
                uploadedAt: utcNowISO(),
                tenantId,
                entityName,
                entityDocId,
                attachableDocId,
            },
        },
        resumable: false,
    });

    // Make file publicly accessible
    await fileRef.makePublic();

    // Get public URL using helper
    const downloadUrl = getPublicUrl(storagePath);

    logger.info(`📤 Uploaded: ${storagePath}`);

    return {
        storagePath,
        downloadUrl,
        metadata: {
            originalName: file.filename,
            mimeType: file.mimeType,
            size: file.size,
            uploadedAt: utcNowISO(),
        },
    };
}

/**
 * Delete file from Cloud Storage
 */
async function deleteFileFromStorage(storagePath) {
    try {
        const fileRef = bucket.file(storagePath);
        await fileRef.delete();
        logger.info(`🗑️ Deleted: ${storagePath}`);
        return true;
    } catch (err) {
        logger.error(`❌ Delete failed: ${storagePath}`, err.message);
        return false;
    }
}

/**
 * Create Attachable record in Firestore
 */
async function createAttachableInFirestore({
    tenantId,
    entityName,
    entityDocId,
    entityQbId,
    file,
    storageInfo,
}) {
    const tenantCol = getTenantCollection(tenantId, "attachables");
    const attachableRef = tenantCol.doc();
    const nowUtc = utcNowISO();

    const attachableData = cleanUndefined({
        qbId: null,
        localStatus: LOCAL_STATUS.PENDING_CREATE,
        isDeleted: false,
        createdAt: nowUtc,
        updatedAt: nowUtc,
        _tenantId: tenantId,
        _entityType: "Attachable",

        // Entity reference
        attachedToEntity: entityName,
        attachedToDocId: entityDocId,
        attachedToQbId: entityQbId || null,

        // File metadata
        fileName: file.filename,
        originalFileName: file.filename,
        mimeType: file.mimeType,
        fileSize: file.size,

        // Storage info
        storagePath: storageInfo.storagePath,
        downloadUrl: storageInfo.downloadUrl,
        storageUploadedAt: storageInfo.metadata.uploadedAt,
    });

    await attachableRef.set(attachableData, { merge: true });

    logger.info(`📎 [${tenantId}] Created Attachable docId=${attachableRef.id} for ${entityName}/${entityDocId}`);

    return {
        ref: attachableRef,
        docId: attachableRef.id,
        data: attachableData,
    };
}

/**
 * Upload Attachable to QuickBooks
 */
async function uploadAttachableToQBO({
    tenantId,
    entityName,
    entityQbId,
    file,
    attachableDocId,
    storageDownloadUrl,
}) {
    return withQboClient(tenantId, async (client) => {
        // Download file from storage
        const fileResponse = await http.get(storageDownloadUrl, {
            responseType: 'arraybuffer',
            timeout: 60000, // 60 second timeout for large files
        });
        const fileBuffer = Buffer.from(fileResponse.data);

        // Build QBO Attachable payload
        const attachablePayload = {
            AttachableRef: [
                {
                    EntityRef: {
                        type: entityName,
                        value: entityQbId.toString(),
                    },
                },
            ],
            FileName: file.filename,
            ContentType: file.mimeType,
        };

        // QBO multipart upload
        const boundary = `----WebKitFormBoundary${uuidv4().replace(/-/g, '')}`;
        const CRLF = '\r\n';

        let body = '';

        // Part 1: JSON metadata
        body += `--${boundary}${CRLF}`;
        body += `Content-Disposition: form-data; name="file_metadata_01"${CRLF}`;
        body += `Content-Type: application/json${CRLF}${CRLF}`;
        body += JSON.stringify(attachablePayload) + CRLF;

        // Part 2: File content
        body += `--${boundary}${CRLF}`;
        body += `Content-Disposition: form-data; name="file_content_01"; filename="${file.filename}"${CRLF}`;
        body += `Content-Type: ${file.mimeType}${CRLF}${CRLF}`;

        const bodyBuffer = Buffer.concat([
            Buffer.from(body, 'utf8'),
            fileBuffer,
            Buffer.from(`${CRLF}--${boundary}--${CRLF}`, 'utf8'),
        ]);

        const uploadUrl = `${client.baseUrl}/upload?minorversion=${client.minorVersion}`;

        const response = await http.post(uploadUrl, bodyBuffer, {
            headers: {
                'Authorization': `Bearer ${client.accessToken}`,
                'Content-Type': `multipart/form-data; boundary=${boundary}`,
                'Accept': 'application/json',
            },
            maxBodyLength: Infinity,
            maxContentLength: Infinity,
            timeout: 120000, // 2 minute timeout
        });

        return response.data?.AttachableResponse?.[0]?.Attachable;
    });
}

/**
 * Delete Attachable from QuickBooks
 */
async function deleteAttachableFromQBO(tenantId, qbId, syncToken) {
    return withQboClient(tenantId, async (client) => {
        await qboDelete("Attachable", qbId, syncToken, client);
    });
}

/**
 * Process attachments for entity (create/update/delete)
 */
async function processEntityAttachments({
    tenantId,
    entityName,
    entityDocId,
    entityQbId,
    filesToAdd = [],
    attachmentIdsToRemove = [],
    logDocId,
}) {
    const results = {
        added: [],
        removed: [],
        errors: [],
    };

    // Validate all files first
    const validationErrors = [];
    for (const file of filesToAdd) {
        const validation = validateFile(file);
        if (!validation.isValid) {
            validationErrors.push({
                file: file.filename,
                errors: validation.errors,
            });
        }
    }

    if (validationErrors.length > 0) {
        results.errors.push(...validationErrors);
        logger.warn(`⚠️ ${validationErrors.length} file(s) failed validation`);
    }

    // Filter out invalid files
    const validFiles = filesToAdd.filter(file => {
        const validation = validateFile(file);
        return validation.isValid;
    });

    // ========== REMOVE ATTACHMENTS ==========
    for (const attachableDocId of attachmentIdsToRemove) {
        try {
            const { ref, data } = await getEntityDocByDocId(tenantId, "attachables", attachableDocId);
            const qbId = data.qbId;
            const syncToken = data.SyncToken;
            const storagePath = data.storagePath;

            await logStart({
                tenantId,
                entityName: "Attachable",
                collectionName: "attachables",
                docId: `${logDocId}_ATT_DELETE_${attachableDocId}`,
                qbId,
                op: OPERATIONS.DELETE,
                msg: `Removing attachment ${attachableDocId}`,
            });

            // Delete from QBO if synced
            if (qbId && syncToken) {
                try {
                    await deleteAttachableFromQBO(tenantId, qbId, syncToken);

                    await logQboOk({
                        tenantId,
                        entityName: "Attachable",
                        collectionName: "attachables",
                        docId: `${logDocId}_ATT_DELETE_${attachableDocId}`,
                        qbId,
                        op: OPERATIONS.DELETE,
                        msg: "QBO delete succeeded",
                    });
                } catch (qboErr) {
                    const e = pickErrorDetails(qboErr);
                    await logQboFail({
                        tenantId,
                        entityName: "Attachable",
                        collectionName: "attachables",
                        docId: `${logDocId}_ATT_DELETE_${attachableDocId}`,
                        qbId,
                        op: OPERATIONS.DELETE,
                        msg: "QBO delete failed",
                        err: e,
                    });
                    results.errors.push({ attachableDocId, error: e });
                    continue;
                }
            }

            // Delete from storage
            if (storagePath) {
                await deleteFileFromStorage(storagePath);
            }

            // Mark deleted in Firestore
            await ref.set({
                isDeleted: true,
                localStatus: LOCAL_STATUS.SYNCED,
                deletedAt: utcNowISO(),
                updatedAt: utcNowISO(),
            }, { merge: true });

            await logEnd({
                tenantId,
                entityName: "Attachable",
                collectionName: "attachables",
                docId: `${logDocId}_ATT_DELETE_${attachableDocId}`,
                qbId,
                op: OPERATIONS.DELETE,
                msg: "Attachment removed",
            });

            results.removed.push({ attachableDocId, qbId });
        } catch (err) {
            const e = pickErrorDetails(err);
            results.errors.push({ attachableDocId, error: e });
            logger.error(`❌ Failed to remove attachment ${attachableDocId}:`, e);
        }
    }

    // ========== ADD ATTACHMENTS ==========
    for (const file of validFiles) {
        let attachableDocId = null;
        let qbId = null;

        try {
            // 1. Create Firestore record (PENDING)
            const { ref: attachableRef, docId, data: attachableData } = await createAttachableInFirestore({
                tenantId,
                entityName,
                entityDocId,
                entityQbId,
                file,
                storageInfo: { storagePath: '', downloadUrl: '', metadata: { uploadedAt: utcNowISO() } },
            });

            attachableDocId = docId;

            await logStart({
                tenantId,
                entityName: "Attachable",
                collectionName: "attachables",
                docId: `${logDocId}_ATT_CREATE_${attachableDocId}`,
                op: OPERATIONS.CREATE,
                msg: `Adding attachment ${file.filename}`,
            });

            // 2. Upload to Cloud Storage
            let storageInfo;
            try {
                storageInfo = await uploadFileToStorage({
                    tenantId,
                    entityName,
                    entityDocId,
                    file,
                    attachableDocId,
                });

                await attachableRef.set({
                    storagePath: storageInfo.storagePath,
                    downloadUrl: storageInfo.downloadUrl,
                    storageUploadedAt: storageInfo.metadata.uploadedAt,
                    updatedAt: utcNowISO(),
                }, { merge: true });

                await logFsOk({
                    tenantId,
                    entityName: "Attachable",
                    collectionName: "attachables",
                    docId: `${logDocId}_ATT_CREATE_${attachableDocId}`,
                    op: OPERATIONS.CREATE,
                    msg: "Uploaded to Cloud Storage",
                    res: { storagePath: storageInfo.storagePath },
                });
            } catch (storageErr) {
                const e = pickErrorDetails(storageErr);

                await attachableRef.set({
                    localStatus: LOCAL_STATUS.ERROR_CREATE,
                    lastError: e,
                    updatedAt: utcNowISO(),
                }, { merge: true });

                await logFsFail({
                    tenantId,
                    entityName: "Attachable",
                    collectionName: "attachables",
                    docId: `${logDocId}_ATT_CREATE_${attachableDocId}`,
                    op: OPERATIONS.CREATE,
                    msg: "Cloud Storage upload failed",
                    err: e,
                });

                results.errors.push({ file: file.filename, attachableDocId, error: e });
                continue;
            }

            // 3. Upload to QuickBooks (only if entityQbId exists)
            if (!entityQbId) {
                results.added.push({
                    file: file.filename,
                    attachableDocId,
                    qbId: null,
                    status: 'PENDING_ENTITY_SYNC',
                });
                continue;
            }

            try {
                await logQboRequest({
                    tenantId,
                    entityName: "Attachable",
                    collectionName: "attachables",
                    docId: `${logDocId}_ATT_CREATE_${attachableDocId}`,
                    op: OPERATIONS.CREATE,
                    msg: "Uploading to QuickBooks",
                });

                const qboAttachable = await uploadAttachableToQBO({
                    tenantId,
                    entityName,
                    entityQbId,
                    file,
                    attachableDocId,
                    storageDownloadUrl: storageInfo.downloadUrl,
                });

                if (!qboAttachable?.Id) {
                    throw new Error("QBO upload did not return valid Attachable");
                }

                qbId = qboAttachable.Id.toString();

                await logQboOk({
                    tenantId,
                    entityName: "Attachable",
                    collectionName: "attachables",
                    docId: `${logDocId}_ATT_CREATE_${attachableDocId}`,
                    qbId,
                    op: OPERATIONS.CREATE,
                    msg: "QBO upload succeeded",
                    res: qboAttachable,
                });

                // 4. Update Firestore with QBO response
                await attachableRef.set(cleanUndefined({
                    ...qboAttachable,
                    qbId,
                    localStatus: LOCAL_STATUS.SYNCED,
                    lastError: null,
                    updatedAt: utcNowISO(),
                }), { merge: true });

                await logFsOk({
                    tenantId,
                    entityName: "Attachable",
                    collectionName: "attachables",
                    docId: `${logDocId}_ATT_CREATE_${attachableDocId}`,
                    qbId,
                    op: OPERATIONS.CREATE,
                    msg: "Firestore updated with QBO response (SYNCED)",
                });

                await logEnd({
                    tenantId,
                    entityName: "Attachable",
                    collectionName: "attachables",
                    docId: `${logDocId}_ATT_CREATE_${attachableDocId}`,
                    qbId,
                    op: OPERATIONS.CREATE,
                    msg: "Attachment added successfully",
                });

                results.added.push({ file: file.filename, attachableDocId, qbId });
            } catch (qboErr) {
                const e = pickErrorDetails(qboErr);

                await attachableRef.set({
                    localStatus: LOCAL_STATUS.ERROR_CREATE,
                    lastError: e,
                    retryPayload: {
                        entityName,
                        entityQbId,
                        fileName: file.filename,
                    },
                    updatedAt: utcNowISO(),
                }, { merge: true });

                await logQboFail({
                    tenantId,
                    entityName: "Attachable",
                    collectionName: "attachables",
                    docId: `${logDocId}_ATT_CREATE_${attachableDocId}`,
                    op: OPERATIONS.CREATE,
                    msg: "QBO upload failed (file in storage, marked for retry)",
                    err: e,
                });

                results.errors.push({ file: file.filename, attachableDocId, error: e });
            }
        } catch (err) {
            const e = pickErrorDetails(err);
            results.errors.push({ file: file.filename, attachableDocId, error: e });
            logger.error(`❌ Failed to add attachment ${file.filename}:`, e);
        }
    }

    return results;
}

/**
 * Get attachments for entity
 */
async function getEntityAttachments(tenantId, entityName, entityDocId) {
    try {
        const tenantCol = getTenantCollection(tenantId, "attachables");

        logger.info(`📎 Fetching attachments: tenantId=${tenantId}, entityName=${entityName}, entityDocId=${entityDocId}`);

        const snap = await tenantCol
            .where("attachedToEntity", "==", entityName)
            .where("attachedToDocId", "==", entityDocId)
            .where("isDeleted", "==", false)
            .orderBy("createdAt", "desc")
            .get();

        logger.info(`📎 Found ${snap.size} attachments for ${entityName}/${entityDocId}`);

        const attachments = snap.docs.map(doc => ({
            attachableDocId: doc.id,
            ...doc.data(),
        }));

        return attachments;
    } catch (err) {
        logger.error(`❌ Error fetching attachments for ${entityName}/${entityDocId}:`, err.message);
        logger.error('Error details:', err);
        return []; // Return empty array on error instead of throwing
    }
}

// ========================================================================================
// WEBHOOK ATTACHMENT SYNC UTILITIES
// ========================================================================================

/**
 * Sync attachments from QuickBooks to Firestore and Cloud Storage
 * Downloads new files and marks deleted files
 */
async function syncAttachmentsFromQBO({
    tenantId,
    entityName,
    entityQbId,
    entityDocId,
    client,
    logDocId,
}) {
    logger.info(`📎 [${tenantId}] Syncing attachments for ${entityName} ${entityQbId}`);

    // 1. Query QBO for attachables linked to this entity
    const qboAttachablesQuery = `SELECT * FROM Attachable WHERE AttachableRef.EntityRef.Type = '${entityName}' AND AttachableRef.EntityRef.Value = '${entityQbId}'`;

    let qboAttachables = [];
    try {
        const qboAttachablesRes = await qboQuery(qboAttachablesQuery, client);
        qboAttachables = qboAttachablesRes.data?.QueryResponse?.Attachable || [];

        logger.info(`📎 [${tenantId}] Found ${qboAttachables.length} attachments in QBO for ${entityName} ${entityQbId}`);
    } catch (err) {
        logger.error(`❌ Failed to query QBO attachables:`, err.message);
        return;
    }

    // 2. Get local attachments (including deleted ones for comparison)
    const localAttachments = await getAllEntityAttachments(tenantId, entityName, entityDocId);
    const localQbIds = new Set(localAttachments.map(att => att.qbId?.toString()));
    const qboQbIds = new Set(qboAttachables.map(att => att.Id?.toString()));

    logger.info(`📎 [${tenantId}] Local attachments: ${localAttachments.length}, QBO attachments: ${qboAttachables.length}`);

    // 3. Find NEW attachments (in QBO but not in local)
    const newQboAttachables = qboAttachables.filter(att => !localQbIds.has(att.Id?.toString()));

    // 4. Find DELETED attachments (in local but not in QBO, and not already marked deleted)
    const activeLocalAttachments = localAttachments.filter(att => !att.isDeleted);
    const deletedAttachments = activeLocalAttachments.filter(att =>
        att.qbId && !qboQbIds.has(att.qbId.toString())
    );

    logger.info(`📎 [${tenantId}] To sync: ${newQboAttachables.length} new, ${deletedAttachments.length} deleted`);

    // 5. Download and save NEW attachments
    for (const qboAtt of newQboAttachables) {
        try {
            await downloadAndSaveQBOAttachment({
                tenantId,
                entityName,
                entityDocId,
                entityQbId,
                qboAttachable: qboAtt,
                client,
                logDocId,
            });
        } catch (err) {
            logger.error(`❌ [${tenantId}] Failed to sync attachment ${qboAtt.Id}:`, err.message);
        }
    }

    // 6. Mark DELETED attachments
    for (const localAtt of deletedAttachments) {
        try {
            await markAttachmentAsDeletedFromQBO({
                tenantId,
                entityName,
                entityDocId,
                attachableDocId: localAtt.attachableDocId,
                qbId: localAtt.qbId,
                logDocId,
            });
        } catch (err) {
            logger.error(`❌ [${tenantId}] Failed to mark attachment ${localAtt.attachableDocId} as deleted:`, err.message);
        }
    }

    logger.info(`✅ [${tenantId}] Attachment sync complete for ${entityName} ${entityQbId}`);
}

/**
 * Get ALL entity attachments including deleted ones (for sync comparison)
 */
async function getAllEntityAttachments(tenantId, entityName, entityDocId) {
    try {
        const tenantCol = getTenantCollection(tenantId, "attachables");

        // Query WITHOUT isDeleted filter to get all attachments
        const snap = await tenantCol
            .where("attachedToEntity", "==", entityName)
            .where("attachedToDocId", "==", entityDocId)
            .get();

        const attachments = snap.docs.map(doc => ({
            attachableDocId: doc.id,
            ...doc.data(),
        }));

        return attachments;
    } catch (err) {
        logger.error(`❌ Error fetching all attachments for ${entityName}/${entityDocId}:`, err.message);
        return [];
    }
}

/**
 * Download attachment from QuickBooks and save to Cloud Storage + Firestore
 */
async function downloadAndSaveQBOAttachment({
    tenantId,
    entityName,
    entityDocId,
    entityQbId,
    qboAttachable,
    client,
    logDocId,
}) {
    const qbId = qboAttachable.Id.toString();
    const fileName = qboAttachable.FileName || `attachment_${qbId}`;
    const mimeType = qboAttachable.ContentType || 'application/octet-stream';

    logger.info(`📎 [${tenantId}] Downloading attachment ${qbId}: ${fileName}`);

    // 1. Download file from QuickBooks
    let fileBuffer;
    try {
        // QuickBooks Download URL
        const downloadUrl = `${client.baseUrl}/download/${qbId}?minorversion=${client.minorVersion}`;

        const downloadRes = await http.get(downloadUrl, {
            headers: {
                Authorization: `Bearer ${client.accessToken}`,
                Accept: '*/*',
            },
            responseType: 'arraybuffer',
            timeout: 60000, // 60 second timeout for large files
        });

        fileBuffer = Buffer.from(downloadRes.data);

        logger.info(`📥 [${tenantId}] Downloaded ${fileBuffer.length} bytes for ${fileName}`);
    } catch (downloadErr) {
        logger.error(`❌ Failed to download attachment ${qbId}:`, downloadErr.message);

        // Create Firestore record even if download fails (for tracking)
        await createAttachableRecordFromQBO({
            tenantId,
            entityName,
            entityDocId,
            entityQbId,
            qboAttachable,
            downloadFailed: true,
        });

        return;
    }

    // 2. Upload to Cloud Storage
    const storagePath = getStoragePath(tenantId, entityName, entityDocId, fileName);
    const fileRef = bucket.file(storagePath);

    try {
        await fileRef.save(fileBuffer, {
            metadata: {
                contentType: mimeType,
                metadata: {
                    originalName: fileName,
                    uploadedAt: utcNowISO(),
                    tenantId,
                    entityName,
                    entityDocId,
                    qbId,
                    syncedFromQBO: 'true',
                },
            },
            resumable: false,
        });

        await fileRef.makePublic();
        const downloadUrl = getPublicUrl(storagePath);

        logger.info(`📤 [${tenantId}] Uploaded to storage: ${storagePath}`);

        // 3. Create Firestore record
        await createAttachableRecordFromQBO({
            tenantId,
            entityName,
            entityDocId,
            entityQbId,
            qboAttachable,
            storageInfo: {
                storagePath,
                downloadUrl,
                fileSize: fileBuffer.length,
            },
        });

        logger.info(`✅ [${tenantId}] Synced attachment ${qbId}: ${fileName}`);
    } catch (storageErr) {
        logger.error(`❌ Failed to save attachment ${qbId} to storage:`, storageErr.message);
        throw storageErr;
    }
}

/**
 * Create Attachable record in Firestore from QBO data
 */
async function createAttachableRecordFromQBO({
    tenantId,
    entityName,
    entityDocId,
    entityQbId,
    qboAttachable,
    storageInfo = null,
    downloadFailed = false,
}) {
    const tenantCol = getTenantCollection(tenantId, "attachables");

    // Check if already exists by QBO ID
    const existingRef = await findDocRefByQbId(tenantId, "attachables", qboAttachable.Id.toString());
    const attachableRef = existingRef || tenantCol.doc();

    const nowUtc = utcNowISO();
    const qbId = qboAttachable.Id.toString();

    const attachableData = cleanUndefined({
        // QBO data
        ...qboAttachable,
        qbId,

        // Local metadata
        localStatus: downloadFailed ? LOCAL_STATUS.ERROR_CREATE : LOCAL_STATUS.SYNCED,
        isDeleted: false,
        createdAt: existingRef ? undefined : nowUtc,
        updatedAt: nowUtc,
        _tenantId: tenantId,
        _entityType: "Attachable",

        // Entity reference
        attachedToEntity: entityName,
        attachedToDocId: entityDocId,
        attachedToQbId: entityQbId,

        // File metadata
        fileName: qboAttachable.FileName,
        originalFileName: qboAttachable.FileName,
        mimeType: qboAttachable.ContentType,
        fileSize: storageInfo?.fileSize || qboAttachable.Size || 0,

        // Storage info (if download succeeded)
        storagePath: storageInfo?.storagePath || null,
        downloadUrl: storageInfo?.downloadUrl || null,
        storageUploadedAt: storageInfo ? nowUtc : null,

        // Sync metadata
        syncedFromQBO: true,
        lastSyncedAt: nowUtc,
        downloadFailed: downloadFailed || false,
    });

    await attachableRef.set(attachableData, { merge: true });

    logger.info(`💾 [${tenantId}] Saved Attachable record: ${attachableRef.id} (qbId: ${qbId})`);

    return {
        ref: attachableRef,
        docId: attachableRef.id,
        data: attachableData,
    };
}

/**
 * Mark attachment as deleted when removed from QuickBooks
 */
async function markAttachmentAsDeletedFromQBO({
    tenantId,
    entityName,
    entityDocId,
    attachableDocId,
    qbId,
    logDocId,
}) {
    logger.info(`🗑️ [${tenantId}] Marking attachment ${attachableDocId} (qbId: ${qbId}) as deleted (removed from QBO)`);

    const tenantCol = getTenantCollection(tenantId, "attachables");
    const attachableRef = tenantCol.doc(attachableDocId);

    try {
        const doc = await attachableRef.get();
        if (!doc.exists) {
            logger.warn(`⚠️ Attachment ${attachableDocId} not found in Firestore`);
            return;
        }

        const data = doc.data();

        // Mark as deleted in Firestore
        await attachableRef.set({
            isDeleted: true,
            deletedAt: utcNowISO(),
            deletedFromQBO: true, // Flag to indicate it was deleted from QBO UI
            deletedReason: 'Removed from QuickBooks',
            localStatus: LOCAL_STATUS.SYNCED,
            updatedAt: utcNowISO(),
        }, { merge: true });

        logger.info(`✅ [${tenantId}] Marked attachment ${attachableDocId} as deleted in Firestore`);

        // NOTE: We keep files in Cloud Storage for audit purposes
        // You can optionally delete them here by uncommenting:
        /*
        const storagePath = data.storagePath;
        if (storagePath) {
            await deleteFileFromStorage(storagePath);
            logger.info(`🗑️ [${tenantId}] Deleted file from storage: ${storagePath}`);
        }
        */

        // Log the deletion
        if (logDocId) {
            await appendLogStep({
                tenantId,
                entityName: "Attachable",
                collectionName: "attachables",
                docId: logDocId,
                qbId,
                op: OPERATIONS.WEBHOOK,
                stage: LOG_STAGES.FS_OK,
                msg: `Marked attachment as deleted (removed from QBO)`,
                res: { attachableDocId, qbId },
            });
        }

    } catch (err) {
        logger.error(`❌ Failed to mark attachment ${attachableDocId} as deleted:`, err.message);
        throw err;
    }
}



function makeCreateHandler(entityName) {
    const cfg = ENTITY_CONFIG[entityName];

    return onRequest(
        {
            region: "asia-south1",
            timeoutSeconds: 120,
            memory: "512MiB",
            maxInstances: 10,
        },
        async (req, res) => {
            let tenantId = null;
            let docRef = null;
            let qbId = null;
            let payload = null;
            let logDocId = null;
            let fsAfterPending = null;
            let attachmentResults = null;

            try {
                if (req.method !== "POST") {
                    return res.status(405).json({ error: "Only POST allowed" });
                }

                // ========== PARSE REQUEST (MULTIPART OR JSON) ==========
                const contentType = req.headers['content-type'] || '';
                const isMultipart = contentType.includes('multipart/form-data');

                let input;
                let files = [];

                if (isMultipart) {
                    const parsed = await parseMultipartForm(req);

                    // Check if there's a 'body' field with JSON string
                    if (parsed.fields.body) {
                        try {
                            // parsed.fields.body is already a string from Busboy
                            const bodyStr = typeof parsed.fields.body === 'string'
                                ? parsed.fields.body
                                : JSON.stringify(parsed.fields.body);

                            input = JSON.parse(bodyStr);
                            logger.info(`📦 Parsed body field as JSON`);
                        } catch (parseErr) {
                            logger.error('❌ Failed to parse body field:', parseErr.message);
                            logger.error('Body content:', parsed.fields.body);
                            return res.status(400).json({
                                error: "Invalid JSON in 'body' field",
                                details: parseErr.message,
                                received: typeof parsed.fields.body === 'string'
                                    ? parsed.fields.body.substring(0, 200)
                                    : String(parsed.fields.body).substring(0, 200)
                            });
                        }
                    } else {
                        // Use all fields as input
                        input = parsed.fields;
                    }

                    files = parsed.files;
                    logger.info(`📎 Received ${files.length} file(s) for ${entityName}`);
                } else {
                    // Regular JSON body
                    input = req.body;
                }

                // ========== VALIDATE INPUT ==========
                tenantId = req.query.tenantId || input?.tenantId;
                if (!tenantId) {
                    return res.status(400).json({ error: "tenantId is required" });
                }

                if (!input || typeof input !== "object") {
                    return res.status(400).json({ error: "Request body must be a JSON object" });
                }

                // Remove tenantId from QBO payload
                const { tenantId: _t, ...rest } = input;
                payload = rest;

                // ========== CREATE FIRESTORE RECORD ==========
                const tenantCol = getTenantCollection(tenantId, cfg.collection);
                docRef = tenantCol.doc();
                const nowUtc = utcNowISO();
                logDocId = `${entityName}_CREATE_${tsId()}_${docRef.id}`;

                await logStart({
                    tenantId,
                    entityName,
                    collectionName: cfg.collection,
                    docId: logDocId,
                    op: OPERATIONS.CREATE,
                    msg: `Create request received (${files.length} attachments)`,
                    req: { ...payload, attachmentCount: files.length },
                });

                // Save PENDING_CREATE locally
                try {
                    await docRef.set(
                        cleanUndefined({
                            ...payload,
                            qbId: null,
                            isDeleted: false,
                            localStatus: LOCAL_STATUS.PENDING_CREATE,
                            createdAt: nowUtc,
                            updatedAt: nowUtc,
                            _tenantId: tenantId,
                            _entityType: entityName,
                        }),
                        { merge: true }
                    );

                    fsAfterPending = await readDocSafe(docRef);

                    await logFsOk({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        op: OPERATIONS.CREATE,
                        msg: "Firestore created (PENDING_CREATE)",
                        res: { localStatus: LOCAL_STATUS.PENDING_CREATE, docId: docRef.id },
                    });
                } catch (fsErr) {
                    const e = pickErrorDetails(fsErr);

                    await logFsFail({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        recordDocId: docRef.id,
                        op: OPERATIONS.CREATE,
                        msg: "Firestore create failed (before QBO call)",
                        err: e,
                        req: payload,
                    });

                    throw fsErr;
                }

                // ========== CALL QBO CREATE ==========
                const qboPathForLog = `/${entityName.toLowerCase()}?minorversion=<minorVersion>`;
                await logQboRequest({
                    tenantId,
                    entityName,
                    collectionName: cfg.collection,
                    docId: logDocId,
                    op: OPERATIONS.CREATE,
                    msg: "Calling QuickBooks create endpoint",
                    req: { path: qboPathForLog, body: payload },
                });

                let created = null;
                try {
                    created = await withQboClient(tenantId, async (client) => {
                        const path = `/${entityName.toLowerCase()}?minorversion=${client.minorVersion}`;
                        const qbRes = await qboPost(path, payload, client);
                        return qbRes.data?.[entityName];
                    });
                } catch (qboErr) {
                    const e = pickErrorDetails(qboErr);

                    await docRef.set(
                        cleanUndefined({
                            localStatus: LOCAL_STATUS.ERROR_CREATE,
                            lastError: e,
                            retryPayload: payload,
                            updatedAt: utcNowISO(),
                        }),
                        { merge: true }
                    );

                    const fsAfterError = await readDocSafe(docRef);

                    await logQboFail({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        recordDocId: docRef.id,
                        op: OPERATIONS.CREATE,
                        msg: "QuickBooks create failed (saved for retry)",
                        err: e,
                        req: { body: payload },
                    });

                    return res.status(500).json({
                        ok: false,
                        error: "QuickBooks create failed",
                        details: e,
                        tenantId,
                        docId: docRef.id,
                        logId: logDocId,
                        firestore: fsAfterError || fsAfterPending || null,
                        attachments: null,
                    });
                }

                if (!created?.Id) {
                    const e = { message: `QBO did not return valid ${entityName} (missing Id)` };

                    await docRef.set(
                        cleanUndefined({
                            localStatus: LOCAL_STATUS.ERROR_CREATE,
                            lastError: e,
                            retryPayload: payload,
                            updatedAt: utcNowISO(),
                        }),
                        { merge: true }
                    );

                    const fsAfterError = await readDocSafe(docRef);

                    await logQboFail({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        recordDocId: docRef.id,
                        op: OPERATIONS.CREATE,
                        msg: "QuickBooks create returned invalid response (missing Id)",
                        err: e,
                        res: created,
                    });

                    return res.status(500).json({
                        ok: false,
                        error: e.message,
                        tenantId,
                        docId: docRef.id,
                        logId: logDocId,
                        firestore: fsAfterError || fsAfterPending || null,
                        attachments: null,
                    });
                }

                qbId = created.Id.toString();

                await logQboOk({
                    tenantId,
                    entityName,
                    collectionName: cfg.collection,
                    docId: logDocId,
                    qbId,
                    op: OPERATIONS.CREATE,
                    msg: "QuickBooks create succeeded",
                    res: created,
                });

                // ========== SAVE QBO RESPONSE TO FIRESTORE ==========
                try {
                    await docRef.set(
                        cleanUndefined({
                            ...created,
                            qbId,
                            localStatus: LOCAL_STATUS.SYNCED,
                            lastError: null,
                            retryPayload: null,
                            updatedAt: utcNowISO(),
                            _tenantId: tenantId,
                            _entityType: entityName,
                        }),
                        { merge: true }
                    );

                    const fsAfterSynced = await readDocSafe(docRef);

                    await logFsOk({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        qbId,
                        op: OPERATIONS.CREATE,
                        msg: "Firestore updated with QBO response (SYNCED)",
                        res: { qbId, localStatus: LOCAL_STATUS.SYNCED },
                    });

                    // ========== PROCESS ATTACHMENTS ==========
                    if (files.length > 0) {
                        logger.info(`📎 Processing ${files.length} attachment(s) for ${entityName} ${qbId}`);

                        try {
                            attachmentResults = await processEntityAttachments({
                                tenantId,
                                entityName,
                                entityDocId: docRef.id,
                                entityQbId: qbId,
                                filesToAdd: files,
                                attachmentIdsToRemove: [],
                                logDocId,
                            });

                            logger.info(`✅ Attachment processing complete:`, {
                                added: attachmentResults.added.length,
                                errors: attachmentResults.errors.length,
                            });

                            // Log attachment results
                            await appendLogStep({
                                tenantId,
                                entityName,
                                collectionName: cfg.collection,
                                docId: logDocId,
                                qbId,
                                op: OPERATIONS.CREATE,
                                stage: LOG_STAGES.END,
                                msg: `Processed ${files.length} attachments`,
                                res: {
                                    added: attachmentResults.added.length,
                                    errors: attachmentResults.errors.length,
                                },
                            });
                        } catch (attachErr) {
                            logger.error(`❌ Attachment processing failed:`, attachErr.message);
                            attachmentResults = {
                                added: [],
                                removed: [],
                                errors: [{ error: pickErrorDetails(attachErr) }],
                            };
                        }
                    }

                    await logEnd({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        qbId,
                        op: OPERATIONS.CREATE,
                        msg: "Create finished",
                    });

                    return res.status(200).json({
                        ok: true,
                        message: `${entityName} created`,
                        tenantId,
                        docId: docRef.id,
                        qbId,
                        logId: logDocId,
                        firestore: fsAfterSynced || null,
                        attachments: attachmentResults || { added: [], removed: [], errors: [] },
                    });
                } catch (fsErr2) {
                    const e = pickErrorDetails(fsErr2);

                    await docRef.set(
                        cleanUndefined({
                            localStatus: LOCAL_STATUS.ERROR_LOCAL_SAVE,
                            lastError: e,
                            updatedAt: utcNowISO(),
                        }),
                        { merge: true }
                    );

                    const fsAfterErrorLocalSave = await readDocSafe(docRef);

                    await logFsFail({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        recordDocId: docRef.id,
                        qbId,
                        op: OPERATIONS.CREATE,
                        msg: "Firestore failed saving QBO response (entity created in QBO but local save failed)",
                        err: e,
                        res: created,
                    });

                    return res.status(200).json({
                        ok: false,
                        message: `${entityName} created in QBO, but Firestore update failed`,
                        tenantId,
                        docId: docRef.id,
                        qbId,
                        logId: logDocId,
                        warning: e,
                        firestore: fsAfterErrorLocalSave || fsAfterPending || null,
                        attachments: attachmentResults || null,
                    });
                }
            } catch (err) {
                const e = pickErrorDetails(err);

                // Best-effort log + mark local status
                try {
                    if (tenantId && cfg?.collection && docRef?.id) {
                        await logFatal({
                            tenantId,
                            entityName,
                            collectionName: cfg.collection,
                            docId: logDocId,
                            recordDocId: docRef.id,
                            qbId,
                            op: OPERATIONS.CREATE,
                            msg: "Create crashed (unexpected)",
                            err: e,
                            req: payload,
                        });

                        await docRef.set(
                            cleanUndefined({
                                localStatus: LOCAL_STATUS.ERROR_CREATE,
                                lastError: e,
                                retryPayload: payload || null,
                                updatedAt: utcNowISO(),
                            }),
                            { merge: true }
                        );
                    }
                } catch (inner) {
                    logger.error("❌ Logging/marking ERROR_CREATE failed:", inner?.message || inner);
                }

                logger.error(`❌ create${entityName} error:`, err.message, err.response?.data);

                let fsLatest = null;
                try {
                    if (docRef) fsLatest = await readDocSafe(docRef);
                } catch { }

                return res.status(500).json({
                    ok: false,
                    error: err.message,
                    details: err.response?.data || null,
                    tenantId,
                    docId: docRef?.id || null,
                    logId: logDocId,
                    firestore: fsLatest || fsAfterPending || null,
                    attachments: attachmentResults || null,
                });
            }
        }
    );
}

function makeUpdateHandler(entityName) {
    const cfg = ENTITY_CONFIG[entityName];

    return onRequest(
        {
            region: "asia-south1",
            timeoutSeconds: 120,
            memory: "512MiB",
            maxInstances: 10,
        },
        async (req, res) => {
            let tenantId = null;
            let docId = null;
            let qbId = null;
            let patch = null;
            let logDocId = null;
            let fsAfterPending = null;
            let attachmentResults = null;

            try {
                if (req.method !== "POST") {
                    return res.status(405).json({ error: "Only POST allowed" });
                }

                // ========== PARSE REQUEST (MULTIPART OR JSON) ==========
                const contentType = req.headers['content-type'] || '';
                const isMultipart = contentType.includes('multipart/form-data');

                let input;
                let files = [];
                let attachmentIdsToRemove = [];

                if (isMultipart) {
                    const parsed = await parseMultipartForm(req);
                    input = processFormFields(parsed.fields);
                    files = parsed.files;

                    // Extract attachmentIdsToRemove if provided
                    if (input.attachmentIdsToRemove) {
                        attachmentIdsToRemove = Array.isArray(input.attachmentIdsToRemove)
                            ? input.attachmentIdsToRemove
                            : [input.attachmentIdsToRemove];
                        delete input.attachmentIdsToRemove; // Remove from entity payload
                    }

                    logger.info(`📦 Processed multipart update: ${Object.keys(input).length} fields, ${files.length} files to add, ${attachmentIdsToRemove.length} to remove`);
                } else {
                    input = req.body;

                    // Extract attachmentIdsToRemove from JSON body
                    if (input.attachmentIdsToRemove) {
                        attachmentIdsToRemove = Array.isArray(input.attachmentIdsToRemove)
                            ? input.attachmentIdsToRemove
                            : [input.attachmentIdsToRemove];
                        delete input.attachmentIdsToRemove;
                    }
                }

                // ========== VALIDATE INPUT ==========
                tenantId = req.query.tenantId || input?.tenantId;
                if (!tenantId) {
                    return res.status(400).json({ error: "tenantId is required" });
                }

                if (!input || typeof input !== "object") {
                    return res.status(400).json({ error: "Request body must be a JSON object" });
                }

                const { docId: dId, tenantId: _t, ...rest } = input;
                if (!dId) {
                    return res.status(400).json({ error: "docId (Firestore document id) is required" });
                }

                docId = dId;
                patch = rest;

                logDocId = `${entityName}_UPDATE_${tsId()}_${docId}`;

                await logStart({
                    tenantId,
                    entityName,
                    collectionName: cfg.collection,
                    docId: logDocId,
                    op: OPERATIONS.UPDATE,
                    msg: `Update request received (${files.length} files to add, ${attachmentIdsToRemove.length} to remove)`,
                    req: { ...patch, attachmentChanges: { toAdd: files.length, toRemove: attachmentIdsToRemove.length } },
                });

                // ========== LOAD FIRESTORE RECORD ==========
                const { ref, data } = await getEntityDocByDocId(tenantId, cfg.collection, docId);
                qbId = (data.qbId || data.Id || "").toString();
                const syncToken = (data.SyncToken || "").toString();

                if (!qbId || !syncToken) {
                    const e = { message: "Missing qbId or SyncToken on Firestore record" };

                    await logFatal({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        recordDocId: docId,
                        qbId: qbId || null,
                        op: OPERATIONS.UPDATE,
                        msg: "Cannot update – missing qbId or SyncToken",
                        err: e,
                    });

                    const fsNow = await readDocSafe(ref).catch(() => null);

                    return res.status(400).json({
                        ok: false,
                        error: e.message,
                        tenantId,
                        docId,
                        qbId: qbId || null,
                        logId: logDocId,
                        firestore: fsNow || null,
                    });
                }

                // ========== UPDATE FIRESTORE FIRST (PENDING_UPDATE) ==========
                try {
                    await ref.set(
                        cleanUndefined({
                            ...patch,
                            localStatus: LOCAL_STATUS.PENDING_UPDATE,
                            updatedAt: utcNowISO(),
                        }),
                        { merge: true }
                    );

                    fsAfterPending = await readDocSafe(ref);

                    await logFsOk({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        qbId,
                        op: OPERATIONS.UPDATE,
                        msg: "Firestore updated (PENDING_UPDATE)",
                        res: { localStatus: LOCAL_STATUS.PENDING_UPDATE },
                    });
                } catch (fsErr) {
                    const e = pickErrorDetails(fsErr);

                    await logFsFail({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        recordDocId: docId,
                        qbId,
                        op: OPERATIONS.UPDATE,
                        msg: "Firestore update failed (before QBO call)",
                        err: e,
                        req: patch,
                    });

                    throw fsErr;
                }

                // ========== CALL QBO UPDATE ==========
                const qboPayload = cleanUndefined({
                    sparse: true,
                    Id: qbId,
                    SyncToken: syncToken,
                    ...patch,
                });

                await logQboRequest({
                    tenantId,
                    entityName,
                    collectionName: cfg.collection,
                    docId: logDocId,
                    qbId,
                    op: OPERATIONS.UPDATE,
                    msg: "Calling QuickBooks update endpoint",
                    req: qboPayload,
                });

                let updated = null;
                try {
                    updated = await withQboClient(tenantId, async (client) => {
                        const path = `/${entityName.toLowerCase()}?minorversion=${client.minorVersion}`;
                        const qbRes = await qboPost(path, qboPayload, client);
                        return qbRes.data?.[entityName];
                    });
                } catch (qboErr) {
                    const e = pickErrorDetails(qboErr);

                    await ref.set(
                        cleanUndefined({
                            localStatus: LOCAL_STATUS.ERROR_UPDATE,
                            lastError: e,
                            retryPayload: qboPayload,
                            updatedAt: utcNowISO(),
                        }),
                        { merge: true }
                    );

                    const fsAfterError = await readDocSafe(ref);

                    await logQboFail({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        recordDocId: docId,
                        qbId,
                        op: OPERATIONS.UPDATE,
                        msg: "QuickBooks update failed (saved for retry)",
                        err: e,
                        req: qboPayload,
                    });

                    return res.status(500).json({
                        ok: false,
                        error: "QuickBooks update failed",
                        details: e,
                        tenantId,
                        docId,
                        qbId,
                        logId: logDocId,
                        firestore: fsAfterError || fsAfterPending || null,
                        attachments: null,
                    });
                }

                if (!updated?.Id) {
                    const e = { message: "QBO did not return updated entity (missing Id)" };

                    await ref.set(
                        cleanUndefined({
                            localStatus: LOCAL_STATUS.ERROR_UPDATE,
                            lastError: e,
                            retryPayload: qboPayload,
                            updatedAt: utcNowISO(),
                        }),
                        { merge: true }
                    );

                    const fsAfterError = await readDocSafe(ref);

                    await logQboFail({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        recordDocId: docId,
                        qbId,
                        op: OPERATIONS.UPDATE,
                        msg: "QuickBooks update returned invalid response",
                        err: e,
                        res: updated,
                    });

                    return res.status(500).json({
                        ok: false,
                        error: e.message,
                        tenantId,
                        docId,
                        qbId,
                        logId: logDocId,
                        firestore: fsAfterError || fsAfterPending || null,
                        attachments: null,
                    });
                }

                // ========== SAVE QBO RESPONSE TO FIRESTORE (SYNCED) ==========
                try {
                    qbId = updated.Id.toString();

                    await ref.set(
                        cleanUndefined({
                            ...updated,
                            qbId,
                            localStatus: LOCAL_STATUS.SYNCED,
                            lastError: null,
                            retryPayload: null,
                            updatedAt: utcNowISO(),
                            _tenantId: tenantId,
                            _entityType: entityName,
                        }),
                        { merge: true }
                    );

                    const fsAfterSynced = await readDocSafe(ref);

                    await logQboOk({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        qbId,
                        op: OPERATIONS.UPDATE,
                        msg: "QuickBooks update succeeded",
                        res: updated,
                    });

                    await logFsOk({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        qbId,
                        op: OPERATIONS.UPDATE,
                        msg: "Firestore updated with QBO response (SYNCED)",
                        res: { localStatus: LOCAL_STATUS.SYNCED },
                    });

                    // ========== PROCESS ATTACHMENTS ==========
                    if (files.length > 0 || attachmentIdsToRemove.length > 0) {
                        logger.info(`📎 Processing attachments: ${files.length} to add, ${attachmentIdsToRemove.length} to remove`);

                        try {
                            attachmentResults = await processEntityAttachments({
                                tenantId,
                                entityName,
                                entityDocId: docId,
                                entityQbId: qbId,
                                filesToAdd: files,
                                attachmentIdsToRemove,
                                logDocId,
                            });

                            logger.info(`✅ Attachment processing complete:`, {
                                added: attachmentResults.added.length,
                                removed: attachmentResults.removed.length,
                                errors: attachmentResults.errors.length,
                            });

                            await appendLogStep({
                                tenantId,
                                entityName,
                                collectionName: cfg.collection,
                                docId: logDocId,
                                qbId,
                                op: OPERATIONS.UPDATE,
                                stage: LOG_STAGES.END,
                                msg: `Processed attachments`,
                                res: {
                                    added: attachmentResults.added.length,
                                    removed: attachmentResults.removed.length,
                                    errors: attachmentResults.errors.length,
                                },
                            });
                        } catch (attachErr) {
                            logger.error(`❌ Attachment processing failed:`, attachErr.message);
                            attachmentResults = {
                                added: [],
                                removed: [],
                                errors: [{ error: pickErrorDetails(attachErr) }],
                            };
                        }
                    }

                    await logEnd({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        qbId,
                        op: OPERATIONS.UPDATE,
                        msg: "Update finished",
                    });

                    return res.status(200).json({
                        ok: true,
                        message: `${entityName} updated`,
                        tenantId,
                        docId,
                        qbId,
                        logId: logDocId,
                        firestore: fsAfterSynced || null,
                        attachments: attachmentResults || { added: [], removed: [], errors: [] },
                    });
                } catch (fsErr2) {
                    const e = pickErrorDetails(fsErr2);

                    await ref.set(
                        cleanUndefined({
                            localStatus: LOCAL_STATUS.ERROR_LOCAL_SAVE,
                            lastError: e,
                            updatedAt: utcNowISO(),
                        }),
                        { merge: true }
                    );

                    const fsAfterErrorLocalSave = await readDocSafe(ref);

                    await logFsFail({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        recordDocId: docId,
                        qbId,
                        op: OPERATIONS.UPDATE,
                        msg: "Firestore failed saving QBO response (QBO updated but local save failed)",
                        err: e,
                        res: updated,
                    });

                    return res.status(200).json({
                        ok: false,
                        message: `${entityName} updated in QBO, but Firestore save failed`,
                        tenantId,
                        docId,
                        qbId,
                        logId: logDocId,
                        warning: e,
                        firestore: fsAfterErrorLocalSave || fsAfterPending || null,
                        attachments: attachmentResults || null,
                    });
                }
            } catch (err) {
                const e = pickErrorDetails(err);

                try {
                    if (tenantId && cfg?.collection && docId) {
                        await logFatal({
                            tenantId,
                            entityName,
                            collectionName: cfg.collection,
                            docId: logDocId,
                            recordDocId: docId,
                            qbId,
                            op: OPERATIONS.UPDATE,
                            msg: "Update crashed (unexpected)",
                            err: e,
                            req: patch,
                        });

                        const ref2 = getTenantCollection(tenantId, cfg.collection).doc(docId);
                        await ref2.set(
                            cleanUndefined({
                                localStatus: LOCAL_STATUS.ERROR_UPDATE,
                                lastError: e,
                                retryPayload: patch || null,
                                updatedAt: utcNowISO(),
                            }),
                            { merge: true }
                        );
                    }
                } catch (inner) {
                    logger.error("❌ Logging ERROR_UPDATE failed:", inner?.message || inner);
                }

                logger.error(`❌ update${entityName} error:`, err.message, err.response?.data);

                let fsLatest = null;
                try {
                    if (tenantId && docId) {
                        const ref3 = getTenantCollection(tenantId, cfg.collection).doc(docId);
                        fsLatest = await readDocSafe(ref3);
                    }
                } catch { }

                return res.status(500).json({
                    ok: false,
                    error: err.message,
                    details: err.response?.data || null,
                    tenantId,
                    docId: docId || null,
                    qbId: qbId || null,
                    logId: logDocId,
                    firestore: fsLatest || fsAfterPending || null,
                    attachments: attachmentResults || null,
                });
            }
        }
    );
}

function makeDeleteHandler(entityName) {
    const cfg = ENTITY_CONFIG[entityName];

    return onRequest(
        {
            region: "asia-south1",
            timeoutSeconds: 120,
            memory: "512MiB",
            maxInstances: 10,
        },
        async (req, res) => {
            let tenantId = null;
            let docId = null;
            let qbId = null;
            let logDocId = null;
            let fsAfterPending = null;
            let attachmentResults = null;

            try {
                if (req.method !== "POST") {
                    return res.status(405).json({ error: "Only POST allowed" });
                }

                tenantId = req.query.tenantId || req.body?.tenantId;
                if (!tenantId) {
                    return res.status(400).json({ error: "tenantId is required" });
                }

                const input = req.body;
                if (!input || typeof input !== "object") {
                    return res.status(400).json({ error: "Request body must be a JSON object" });
                }

                docId = input.docId;
                if (!docId) {
                    return res.status(400).json({ error: "docId (Firestore document id) is required" });
                }

                const deleteAttachments = parseBool(input.deleteAttachments) !== false; // Default true

                logDocId = `${entityName}_DELETE_${tsId()}_${docId}`;

                await logStart({
                    tenantId,
                    entityName,
                    collectionName: cfg.collection,
                    docId: logDocId,
                    op: OPERATIONS.DELETE,
                    msg: `Delete request received (deleteAttachments: ${deleteAttachments})`,
                    req: { docId, deleteAttachments },
                });

                // ========== LOAD FIRESTORE RECORD ==========
                const { ref, data } = await getEntityDocByDocId(tenantId, cfg.collection, docId);
                qbId = (data.qbId || data.Id || "").toString();
                const syncToken = (data.SyncToken || "").toString();

                if (!qbId || !syncToken) {
                    const e = { message: "Missing qbId or SyncToken on Firestore record" };

                    await logFatal({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        recordDocId: docId,
                        qbId: qbId || null,
                        op: OPERATIONS.DELETE,
                        msg: "Cannot delete – missing qbId or SyncToken",
                        err: e,
                    });

                    const fsNow = await readDocSafe(ref).catch(() => null);

                    return res.status(400).json({
                        ok: false,
                        error: e.message,
                        tenantId,
                        docId,
                        qbId: qbId || null,
                        logId: logDocId,
                        firestore: fsNow || null,
                    });
                }

                // ========== GET ENTITY ATTACHMENTS (IF DELETING) ==========
                let attachmentIdsToRemove = [];
                if (deleteAttachments) {
                    try {
                        const attachments = await getEntityAttachments(tenantId, entityName, docId);
                        attachmentIdsToRemove = attachments.map(att => att.attachableDocId);
                        logger.info(`📎 Found ${attachmentIdsToRemove.length} attachments to delete`);
                    } catch (err) {
                        logger.warn(`Failed to fetch attachments for deletion:`, err.message);
                    }
                }

                // ========== MARK DELETED LOCALLY FIRST (PENDING_DELETE) ==========
                try {
                    await ref.set(
                        cleanUndefined({
                            isDeleted: true,
                            localStatus: LOCAL_STATUS.PENDING_DELETE,
                            deletedAt: utcNowISO(),
                            updatedAt: utcNowISO(),
                        }),
                        { merge: true }
                    );

                    fsAfterPending = await readDocSafe(ref);

                    await logFsOk({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        qbId,
                        op: OPERATIONS.DELETE,
                        msg: "Firestore marked deleted (PENDING_DELETE)",
                        res: { localStatus: LOCAL_STATUS.PENDING_DELETE, isDeleted: true },
                    });
                } catch (fsErr) {
                    const e = pickErrorDetails(fsErr);

                    await logFsFail({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        recordDocId: docId,
                        qbId,
                        op: OPERATIONS.DELETE,
                        msg: "Firestore delete-mark failed (before QBO call)",
                        err: e,
                    });

                    throw fsErr;
                }

                // ========== CALL QBO DELETE ==========
                const qboPayload = { Id: qbId, SyncToken: syncToken };

                await logQboRequest({
                    tenantId,
                    entityName,
                    collectionName: cfg.collection,
                    docId: logDocId,
                    qbId,
                    op: OPERATIONS.DELETE,
                    msg: "Calling QuickBooks delete endpoint",
                    req: qboPayload,
                });

                let deletedRes = null;
                try {
                    deletedRes = await withQboClient(tenantId, async (client) => {
                        const qbRes = await qboDelete(entityName, qbId, syncToken, client);
                        return qbRes.data?.[entityName] || qbRes.data || null;
                    });
                } catch (qboErr) {
                    const e = pickErrorDetails(qboErr);

                    await ref.set(
                        cleanUndefined({
                            localStatus: LOCAL_STATUS.ERROR_DELETE,
                            lastError: e,
                            retryPayload: qboPayload,
                            updatedAt: utcNowISO(),
                        }),
                        { merge: true }
                    );

                    const fsAfterError = await readDocSafe(ref);

                    await logQboFail({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        recordDocId: docId,
                        qbId,
                        op: OPERATIONS.DELETE,
                        msg: "QuickBooks delete failed (saved for retry)",
                        err: e,
                        req: qboPayload,
                    });

                    return res.status(500).json({
                        ok: false,
                        error: "QuickBooks delete failed",
                        details: e,
                        tenantId,
                        docId,
                        qbId,
                        logId: logDocId,
                        firestore: fsAfterError || fsAfterPending || null,
                        attachments: null,
                    });
                }

                await logQboOk({
                    tenantId,
                    entityName,
                    collectionName: cfg.collection,
                    docId: logDocId,
                    qbId,
                    op: OPERATIONS.DELETE,
                    msg: "QuickBooks delete succeeded",
                    res: deletedRes,
                });

                // ========== DELETE ATTACHMENTS (IF REQUESTED) ==========
                if (deleteAttachments && attachmentIdsToRemove.length > 0) {
                    logger.info(`📎 Deleting ${attachmentIdsToRemove.length} attachments`);

                    try {
                        attachmentResults = await processEntityAttachments({
                            tenantId,
                            entityName,
                            entityDocId: docId,
                            entityQbId: qbId,
                            filesToAdd: [],
                            attachmentIdsToRemove,
                            logDocId,
                        });

                        logger.info(`✅ Attachment deletion complete:`, {
                            removed: attachmentResults.removed.length,
                            errors: attachmentResults.errors.length,
                        });

                        await appendLogStep({
                            tenantId,
                            entityName,
                            collectionName: cfg.collection,
                            docId: logDocId,
                            qbId,
                            op: OPERATIONS.DELETE,
                            stage: LOG_STAGES.END,
                            msg: `Deleted ${attachmentResults.removed.length} attachments`,
                            res: {
                                removed: attachmentResults.removed.length,
                                errors: attachmentResults.errors.length,
                            },
                        });
                    } catch (attachErr) {
                        logger.error(`❌ Attachment deletion failed:`, attachErr.message);
                        attachmentResults = {
                            added: [],
                            removed: [],
                            errors: [{ error: pickErrorDetails(attachErr) }],
                        };
                    }
                }

                // ========== FINAL FIRESTORE UPDATE (SYNCED) ==========
                try {
                    await ref.set(
                        cleanUndefined({
                            localStatus: LOCAL_STATUS.SYNCED,
                            lastError: null,
                            retryPayload: null,
                            updatedAt: utcNowISO(),
                            qbDeleteResponse: deletedRes,
                        }),
                        { merge: true }
                    );

                    const fsAfterSynced = await readDocSafe(ref);

                    await logFsOk({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        qbId,
                        op: OPERATIONS.DELETE,
                        msg: "Firestore updated after QBO delete (SYNCED)",
                        res: { localStatus: LOCAL_STATUS.SYNCED },
                    });

                    await logEnd({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        qbId,
                        op: OPERATIONS.DELETE,
                        msg: "Delete finished",
                    });

                    return res.status(200).json({
                        ok: true,
                        message: `${entityName} deleted`,
                        tenantId,
                        docId,
                        qbId,
                        logId: logDocId,
                        firestore: fsAfterSynced || null,
                        attachments: attachmentResults || { added: [], removed: [], errors: [] },
                    });
                } catch (fsErr2) {
                    const e = pickErrorDetails(fsErr2);

                    await ref.set(
                        cleanUndefined({
                            localStatus: LOCAL_STATUS.ERROR_LOCAL_SAVE,
                            lastError: e,
                            updatedAt: utcNowISO(),
                        }),
                        { merge: true }
                    );

                    const fsAfterErrorLocalSave = await readDocSafe(ref);

                    await logFsFail({
                        tenantId,
                        entityName,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        recordDocId: docId,
                        qbId,
                        op: OPERATIONS.DELETE,
                        msg: "Firestore failed saving delete result (QBO delete succeeded)",
                        err: e,
                        res: deletedRes,
                    });

                    return res.status(200).json({
                        ok: false,
                        message: `${entityName} deleted in QBO, but Firestore save failed`,
                        tenantId,
                        docId,
                        qbId,
                        logId: logDocId,
                        warning: e,
                        firestore: fsAfterErrorLocalSave || fsAfterPending || null,
                        attachments: attachmentResults || null,
                    });
                }
            } catch (err) {
                const e = pickErrorDetails(err);

                try {
                    if (tenantId && cfg?.collection && docId) {
                        await logFatal({
                            tenantId,
                            entityName,
                            collectionName: cfg.collection,
                            docId: logDocId,
                            recordDocId: docId,
                            qbId,
                            op: OPERATIONS.DELETE,
                            msg: "Delete crashed (unexpected)",
                            err: e,
                        });

                        const ref2 = getTenantCollection(tenantId, cfg.collection).doc(docId);
                        await ref2.set(
                            cleanUndefined({
                                localStatus: LOCAL_STATUS.ERROR_DELETE,
                                lastError: e,
                                retryPayload: qbId ? { Id: qbId } : null,
                                updatedAt: utcNowISO(),
                            }),
                            { merge: true }
                        );
                    }
                } catch (inner) {
                    logger.error("❌ Logging ERROR_DELETE failed:", inner?.message || inner);
                }

                logger.error(`❌ delete${entityName} error:`, err.message, err.response?.data);

                let fsLatest = null;
                try {
                    if (tenantId && docId) {
                        const ref3 = getTenantCollection(tenantId, cfg.collection).doc(docId);
                        fsLatest = await readDocSafe(ref3);
                    }
                } catch { }

                return res.status(500).json({
                    ok: false,
                    error: err.message,
                    details: err.response?.data || null,
                    tenantId,
                    docId: docId || null,
                    qbId: qbId || null,
                    logId: logDocId,
                    firestore: fsLatest || fsAfterPending || null,
                    attachments: attachmentResults || null,
                });
            }
        }
    );
}

function makeListHandler(entityName) {
    const cfg = ENTITY_CONFIG[entityName];
    if (!cfg?.collection) {
        throw new Error(`ENTITY_CONFIG missing for ${entityName}`);
    }

    return onRequest(
        {
            region: "asia-south1",
            timeoutSeconds: 120,
            memory: "512MiB",
            maxInstances: 10,
        },
        async (req, res) => {
            try {
                if (req.method !== "GET") {
                    return res.status(405).json({ error: "Only GET allowed" });
                }

                const tenantId = req.query.tenantId;
                if (!tenantId) {
                    return res.status(400).json({ error: "tenantId query parameter is required" });
                }

                // ========== QUERY PARAMETERS ==========
                const qbId = req.query.qbId ? String(req.query.qbId) : null;
                const status = req.query.status ? String(req.query.status) : null;
                const isDeleted = parseBool(req.query.isDeleted);

                const includeTotalCount = parseBool(req.query.includeTotalCount) === true;
                const includeAttachments = parseBool(req.query.includeAttachments) === true;

                const orderByRaw = req.query.orderBy ? String(req.query.orderBy) : "updatedAt";
                const orderBy = ["updatedAt", "createdAt"].includes(orderByRaw) ? orderByRaw : "updatedAt";

                const orderDirRaw = req.query.orderDir ? String(req.query.orderDir).toLowerCase() : "desc";
                const orderDir = orderDirRaw === "asc" ? "asc" : "desc";

                // Optional date filters (ISO strings)
                const updatedFrom = parseISODate(req.query.updatedFrom);
                const updatedTo = parseISODate(req.query.updatedTo);
                const createdFrom = parseISODate(req.query.createdFrom);
                const createdTo = parseISODate(req.query.createdTo);

                // Pagination is OPTIONAL
                const hasPage = req.query.page !== undefined && String(req.query.page) !== "";
                const hasPageSize = req.query.pageSize !== undefined && String(req.query.pageSize) !== "";
                const usePaging = hasPage || hasPageSize;

                const page = usePaging ? Math.max(parseIntSafe(req.query.page, 1), 1) : null;
                const pageSize = usePaging
                    ? Math.min(Math.max(parseIntSafe(req.query.pageSize, 50), 1), 200)
                    : null;

                // ========== BUILD QUERY ==========
                const col = getTenantCollection(tenantId, cfg.collection);
                let q = col;

                if (qbId) q = q.where("qbId", "==", qbId);
                if (status) q = q.where("localStatus", "==", status);
                if (isDeleted !== null) q = q.where("isDeleted", "==", isDeleted);

                if (updatedFrom) q = q.where("updatedAt", ">=", updatedFrom);
                if (updatedTo) q = q.where("updatedAt", "<=", updatedTo);
                if (createdFrom) q = q.where("createdAt", ">=", createdFrom);
                if (createdTo) q = q.where("createdAt", "<=", createdTo);

                // Stable ordering
                q = q.orderBy(orderBy, orderDir).orderBy(FieldPath.documentId(), orderDir);

                // ========== TOTAL COUNT (OPTIONAL) ==========
                let totalCount = null;
                if (includeTotalCount) {
                    totalCount = await getTotalCountSafe(q);
                }

                // ========== FETCH RECORDS ==========
                let snap;
                let offset = 0;

                if (!usePaging) {
                    snap = await q.get(); // ALL records
                } else {
                    offset = (page - 1) * pageSize;
                    snap = await q.offset(offset).limit(pageSize).get();
                }

                const records = snap.docs.map((d) => ({
                    docId: d.id,
                    ...(d.data() || {}),
                }));

                // ========== FETCH ATTACHMENTS (OPTIONAL) ==========
                let attachmentsMap = {};

                if (includeAttachments && records.length > 0) {
                    logger.info(`📎 Fetching attachments for ${records.length} ${entityName} records`);

                    try {
                        // Batch fetch attachments for all records
                        const attachmentPromises = records.map(async (record) => {
                            try {
                                const attachments = await getEntityAttachments(
                                    tenantId,
                                    entityName,
                                    record.docId
                                );
                                return {
                                    docId: record.docId,
                                    attachments,
                                };
                            } catch (err) {
                                logger.error(`Failed to fetch attachments for ${record.docId}:`, err.message);
                                return {
                                    docId: record.docId,
                                    attachments: [],
                                    error: err.message,
                                };
                            }
                        });

                        const attachmentResults = await Promise.all(attachmentPromises);

                        // Build map of docId -> attachments
                        attachmentResults.forEach((result) => {
                            attachmentsMap[result.docId] = result.attachments || [];
                        });

                        logger.info(`✅ Fetched attachments for ${Object.keys(attachmentsMap).length} records`);
                    } catch (attachErr) {
                        logger.error(`❌ Failed to fetch attachments:`, attachErr.message);
                        // Continue without attachments on error
                    }
                }

                // ========== ENRICH RECORDS WITH ATTACHMENTS ==========
                const enrichedRecords = records.map((record) => {
                    if (includeAttachments) {
                        return {
                            ...record,
                            attachments: attachmentsMap[record.docId] || [],
                            attachmentCount: (attachmentsMap[record.docId] || []).length,
                        };
                    }
                    return record;
                });

                // ========== RETURN RESPONSE ==========
                return res.status(200).json({
                    ok: true,
                    entityName,
                    collection: cfg.collection,
                    tenantId,
                    filters: {
                        qbId: qbId || null,
                        status: status || null,
                        isDeleted,
                        updatedFrom,
                        updatedTo,
                        createdFrom,
                        createdTo,
                    },
                    order: { orderBy, orderDir },
                    pagination: usePaging
                        ? { page, pageSize, offset, returned: enrichedRecords.length }
                        : { all: true, returned: enrichedRecords.length },
                    totalCount,
                    includeAttachments,
                    records: enrichedRecords,
                });
            } catch (err) {
                logger.error(`❌ list${entityName} error:`, err?.message || err);
                return res.status(500).json({
                    ok: false,
                    error: err?.message || String(err),
                    details: err?.stack || null,
                });
            }
        }
    );
}

exports.quickBooksWebhook = onRequest(
    {
        region: "asia-south1",
        timeoutSeconds: 120,
        memory: "512MiB",
        maxInstances: 10,
    },
    async (req, res) => {
        let tenantId = null;

        try {
            if (req.method !== "POST") return res.status(405).send("Only POST allowed");

            tenantId = req.query.tenantId;
            if (!tenantId) {
                return res.status(400).json({ error: "tenantId query parameter is required" });
            }

            const webhookVerifier = await getSecret(tenantId, "webhookVerifier");
            verifyWebhook(req, webhookVerifier);
            logger.info(`✅ [${tenantId}] Webhook verified`);

            const entities =
                req.body?.eventNotifications?.flatMap((n) => n?.dataChangeEvent?.entities || []) || [];

            if (!entities.length) return res.status(200).send("No entities");

            await withQboClient(tenantId, async (client) => {
                for (const e of entities) {
                    const { name, operation, id } = e || {};
                    if (!name || !operation || !id) continue;

                    const cfg = ENTITY_CONFIG[name];
                    if (!cfg) {
                        logger.info(`ℹ️ [${tenantId}] Unsupported entity: ${name}`);
                        continue;
                    }

                    const qbId = id.toString();
                    const logDocId = `${name}_WEBHOOK_${operation}_${tsId()}_qb_${qbId}`;
                    const existingRef = await findDocRefByQbId(tenantId, cfg.collection, qbId);

                    await logStart({
                        tenantId,
                        entityName: name,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        qbId,
                        op: OPERATIONS.WEBHOOK,
                        msg: `Webhook received: ${name} ${operation}`,
                        req: { name, operation, qbId },
                    });

                    const opNorm = String(operation || "").toLowerCase();

                    // ========== HANDLE DELETE OPERATION ==========
                    if (opNorm === "delete") {
                        try {
                            await softDeleteInFirestore(tenantId, name, qbId, cfg.collection);

                            await logFsOk({
                                tenantId,
                                entityName: name,
                                collectionName: cfg.collection,
                                docId: logDocId,
                                qbId,
                                op: OPERATIONS.WEBHOOK,
                                msg: "Soft-deleted in Firestore",
                                res: { qbId, isDeleted: true },
                            });

                            // Delete associated attachments
                            if (existingRef) {
                                const entityDocId = existingRef.id;

                                try {
                                    const attachments = await getEntityAttachments(tenantId, name, entityDocId);
                                    const attachmentIdsToRemove = attachments.map(att => att.attachableDocId);

                                    if (attachmentIdsToRemove.length > 0) {
                                        logger.info(`📎 [${tenantId}] Deleting ${attachmentIdsToRemove.length} attachments for deleted ${name} ${qbId}`);

                                        const attachmentResults = await processEntityAttachments({
                                            tenantId,
                                            entityName: name,
                                            entityDocId,
                                            entityQbId: qbId,
                                            filesToAdd: [],
                                            attachmentIdsToRemove,
                                            logDocId,
                                        });

                                        logger.info(`✅ [${tenantId}] Deleted ${attachmentResults.removed.length} attachments`);
                                    }
                                } catch (attachErr) {
                                    logger.error(`❌ [${tenantId}] Failed to delete attachments:`, attachErr.message);
                                }
                            }

                            await logEnd({
                                tenantId,
                                entityName: name,
                                collectionName: cfg.collection,
                                docId: logDocId,
                                qbId,
                                op: OPERATIONS.WEBHOOK,
                                msg: "Webhook processed (Delete)",
                            });
                        } catch (fsErr) {
                            const errObj = pickErrorDetails(fsErr);
                            await logFsFail({
                                tenantId,
                                entityName: name,
                                collectionName: cfg.collection,
                                docId: logDocId,
                                recordDocId: existingRef?.id || null,
                                qbId,
                                op: OPERATIONS.WEBHOOK,
                                msg: "Firestore soft delete failed",
                                err: errObj,
                            });
                        }
                        continue;
                    }

                    // ========== HANDLE CREATE/UPDATE/MERGE OPERATIONS ==========
                    const getPathForLog = cfg.getPath(qbId, client.minorVersion);
                    await logQboRequest({
                        tenantId,
                        entityName: name,
                        collectionName: cfg.collection,
                        docId: logDocId,
                        qbId,
                        op: OPERATIONS.WEBHOOK,
                        msg: "Fetching latest entity from QBO",
                        req: { path: getPathForLog },
                    });

                    let entityObj = null;
                    try {
                        const getRes = await qboGet(cfg.getPath(qbId, client.minorVersion), client);
                        entityObj = getRes.data?.[name];

                        if (!entityObj?.Id) {
                            const e2 = { message: `QBO returned empty ${name} for id ${qbId}` };
                            await logQboFail({
                                tenantId,
                                entityName: name,
                                collectionName: cfg.collection,
                                docId: logDocId,
                                recordDocId: existingRef?.id || null,
                                qbId,
                                op: OPERATIONS.WEBHOOK,
                                msg: "QBO fetch returned empty entity",
                                err: e2,
                                res: getRes.data,
                            });
                            continue;
                        }

                        await logQboOk({
                            tenantId,
                            entityName: name,
                            collectionName: cfg.collection,
                            docId: logDocId,
                            qbId,
                            op: OPERATIONS.WEBHOOK,
                            msg: "QBO fetch OK",
                            res: entityObj,
                        });
                    } catch (qboErr) {
                        const errObj = pickErrorDetails(qboErr);
                        await logQboFail({
                            tenantId,
                            entityName: name,
                            collectionName: cfg.collection,
                            docId: logDocId,
                            recordDocId: existingRef?.id || null,
                            qbId,
                            op: OPERATIONS.WEBHOOK,
                            msg: "Failed to fetch entity from QBO",
                            err: errObj,
                        });
                        continue;
                    }

                    try {
                        const safeEntityObj = cleanUndefined(entityObj);
                        const upsertedRef = await upsertFromQbo(tenantId, name, safeEntityObj, cfg.collection);
                        const entityDocId = upsertedRef.id;

                        await logFsOk({
                            tenantId,
                            entityName: name,
                            collectionName: cfg.collection,
                            docId: logDocId,
                            qbId,
                            op: OPERATIONS.WEBHOOK,
                            msg: "Upserted into Firestore successfully",
                            res: { qbId, operation, entityDocId },
                        });

                        // ========== SYNC ATTACHMENTS FROM QBO ==========
                        try {
                            await syncAttachmentsFromQBO({
                                tenantId,
                                entityName: name,
                                entityQbId: qbId,
                                entityDocId,
                                client,
                                logDocId,
                            });
                        } catch (syncErr) {
                            logger.error(`❌ [${tenantId}] Attachment sync failed for ${name} ${qbId}:`, syncErr.message);
                            // Don't fail webhook - entity sync succeeded
                        }

                        await logEnd({
                            tenantId,
                            entityName: name,
                            collectionName: cfg.collection,
                            docId: logDocId,
                            qbId,
                            op: OPERATIONS.WEBHOOK,
                            msg: "Webhook processed (with attachment sync)",
                        });
                    } catch (fsErr2) {
                        const errObj = pickErrorDetails(fsErr2);
                        await logFsFail({
                            tenantId,
                            entityName: name,
                            collectionName: cfg.collection,
                            docId: logDocId,
                            qbId,
                            recordDocId: existingRef?.id || null,
                            op: OPERATIONS.WEBHOOK,
                            msg: "Firestore upsert failed",
                            err: errObj,
                            req: entityObj,
                        });
                    }
                }
            });

            return res.status(200).send("Webhook processed");
        } catch (err) {
            const code = err.statusCode || 500;
            logger.error("❌ Webhook error:", err.message);
            return res.status(code).send(err.message || "Webhook error");
        }
    }
);


/**
 * Cleanup endpoint to permanently delete old soft-deleted attachments
 * Can be run as a scheduled job or manually
 */
exports.cleanupDeletedAttachments = onRequest(
    {
        region: "asia-south1",
        timeoutSeconds: 300,
        memory: "512MiB",
    },
    async (req, res) => {
        try {
            const tenantId = req.query.tenantId;
            const daysOld = parseInt(req.query.daysOld || '30', 10); // Default 30 days
            const dryRun = parseBool(req.query.dryRun) !== false; // Default true (safe)
            const deleteFromStorage = parseBool(req.query.deleteFromStorage) === true; // Default false

            if (!tenantId) {
                return res.status(400).json({ error: "tenantId query parameter is required" });
            }

            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - daysOld);
            const cutoffISO = cutoffDate.toISOString();

            logger.info(`🗑️ [${tenantId}] Cleaning up attachments deleted before ${cutoffISO} (dryRun: ${dryRun})`);

            const tenantCol = getTenantCollection(tenantId, "attachables");

            const snap = await tenantCol
                .where("isDeleted", "==", true)
                .where("deletedAt", "<=", cutoffISO)
                .get();

            logger.info(`📊 [${tenantId}] Found ${snap.size} deleted attachments older than ${daysOld} days`);

            const results = {
                total: snap.size,
                deleted: 0,
                failed: 0,
                dryRun,
                deleteFromStorage,
                attachments: [],
            };

            for (const doc of snap.docs) {
                const data = doc.data();

                results.attachments.push({
                    attachableDocId: doc.id,
                    fileName: data.fileName,
                    qbId: data.qbId,
                    deletedAt: data.deletedAt,
                    storagePath: data.storagePath,
                });

                if (!dryRun) {
                    try {
                        // Delete from storage if requested
                        if (deleteFromStorage && data.storagePath) {
                            try {
                                await deleteFileFromStorage(data.storagePath);
                                logger.info(`🗑️ Deleted file from storage: ${data.storagePath}`);
                            } catch (storageErr) {
                                logger.error(`❌ Failed to delete file from storage:`, storageErr.message);
                            }
                        }

                        // Delete Firestore document
                        await doc.ref.delete();
                        results.deleted++;
                        logger.info(`✅ Permanently deleted attachment ${doc.id}`);
                    } catch (err) {
                        logger.error(`❌ Failed to delete ${doc.id}:`, err.message);
                        results.failed++;
                    }
                }
            }

            logger.info(`✅ [${tenantId}] Cleanup complete: ${results.deleted} deleted, ${results.failed} failed`);

            return res.status(200).json({
                ok: true,
                tenantId,
                daysOld,
                cutoffDate: cutoffISO,
                ...results,
            });

        } catch (err) {
            logger.error('❌ Cleanup error:', err);
            return res.status(500).json({
                ok: false,
                error: err.message,
            });
        }
    }
);


async function loadEntityFromQboToFirestore(tenantId, {
    entityName,
    collectionName,
    pageSize = 500,
    syncAttachments = false,
    client = null,
}) {
    const runId = `${entityName}_BULK_LOAD_${tsId()}_run`;

    await logStart({
        tenantId,
        entityName,
        collectionName,
        docId: runId,
        op: OPERATIONS.BULK_LOAD,
        msg: `Bulk load started (pageSize=${pageSize}, syncAttachments=${syncAttachments})`,
        req: { entityName, collectionName, pageSize, syncAttachments },
    });

    const stats = {
        totalFetched: 0,
        totalSaved: 0,
        attachmentsSynced: 0,
        attachmentErrors: 0,
    };

    try {
        const clientProvided = !!client;

        const processLoad = async (qboClient) => {
            let startPosition = 1;

            while (true) {
                const sql = `SELECT * FROM ${entityName} STARTPOSITION ${startPosition} MAXRESULTS ${pageSize}`;

                await logQboRequest({
                    tenantId,
                    entityName,
                    collectionName,
                    docId: runId,
                    op: OPERATIONS.BULK_LOAD,
                    msg: "Querying QBO",
                    req: { sql, startPosition, pageSize },
                });

                let qbRes;
                try {
                    qbRes = await qboQuery(sql, qboClient);
                } catch (qboErr) {
                    const e = pickErrorDetails(qboErr);
                    await logQboFail({
                        tenantId,
                        entityName,
                        collectionName,
                        docId: runId,
                        op: OPERATIONS.BULK_LOAD,
                        msg: "QBO query failed",
                        err: e,
                        req: { sql },
                    });
                    throw qboErr;
                }

                const list = qbRes.data?.QueryResponse?.[entityName] || [];
                const got = list.length;
                stats.totalFetched += got;

                await logQboOk({
                    tenantId,
                    entityName,
                    collectionName,
                    docId: runId,
                    op: OPERATIONS.BULK_LOAD,
                    msg: `QBO query ok (fetched ${got}, total: ${stats.totalFetched})`,
                    res: { startPosition, fetched: got, totalFetched: stats.totalFetched },
                });

                if (!got) break;

                let savedThisPage = 0;
                let entityDocIds = [];

                try {
                    const safeList = list.map((x) => cleanUndefined(x));

                    // Save entities and get doc IDs
                    const saveResult = await saveListToFirestoreWithIds(
                        tenantId,
                        collectionName,
                        safeList,
                        entityName
                    );

                    savedThisPage = saveResult.saved;
                    entityDocIds = saveResult.docIds;
                } catch (fsErr) {
                    const e = pickErrorDetails(fsErr);
                    await logFsFail({
                        tenantId,
                        entityName,
                        collectionName,
                        docId: runId,
                        op: OPERATIONS.BULK_LOAD,
                        msg: `Firestore save failed (startPosition=${startPosition})`,
                        err: e,
                        req: { startPosition, fetched: got },
                    });
                    throw fsErr;
                }

                stats.totalSaved += savedThisPage;

                await logFsOk({
                    tenantId,
                    entityName,
                    collectionName,
                    docId: runId,
                    op: OPERATIONS.BULK_LOAD,
                    msg: `Saved page (saved=${savedThisPage}, total=${stats.totalSaved})`,
                    res: { startPosition, savedThisPage, totalSaved: stats.totalSaved },
                });

                // ========== SYNC ATTACHMENTS IF REQUESTED ==========
                if (syncAttachments && entityDocIds.length > 0) {
                    logger.info(`📎 [${tenantId}] Syncing attachments for ${entityDocIds.length} entities (page ${Math.ceil(startPosition / pageSize)})`);

                    for (let i = 0; i < entityDocIds.length; i++) {
                        const { entityDocId, entityQbId } = entityDocIds[i];

                        try {
                            await syncAttachmentsFromQBO({
                                tenantId,
                                entityName,
                                entityQbId,
                                entityDocId,
                                client: qboClient,
                                logDocId: runId,
                            });

                            stats.attachmentsSynced++;

                            // Log progress every 10 entities
                            if ((stats.attachmentsSynced) % 10 === 0) {
                                logger.info(`📎 [${tenantId}] Attachment sync progress: ${stats.attachmentsSynced} entities processed, ${stats.attachmentErrors} errors`);
                            }
                        } catch (attachErr) {
                            logger.error(`❌ [${tenantId}] Attachment sync failed for ${entityName} ${entityQbId}:`, attachErr.message);
                            stats.attachmentErrors++;
                        }
                    }

                    await logFsOk({
                        tenantId,
                        entityName,
                        collectionName,
                        docId: runId,
                        op: OPERATIONS.BULK_LOAD,
                        msg: `Attachments synced for page`,
                        res: {
                            pageAttachmentsSynced: entityDocIds.length,
                            totalAttachmentsSynced: stats.attachmentsSynced,
                            totalAttachmentErrors: stats.attachmentErrors
                        },
                    });
                }

                if (got < pageSize) break;
                startPosition += pageSize;

                // Brief pause between batches to avoid rate limits
                await new Promise(resolve => setTimeout(resolve, 500));
            }

            return stats;
        };

        // Use provided client or create new one
        if (clientProvided) {
            await processLoad(client);
        } else {
            await withQboClient(tenantId, processLoad);
        }

        await logEnd({
            tenantId,
            entityName,
            collectionName,
            docId: runId,
            op: OPERATIONS.BULK_LOAD,
            msg: `Bulk load finished`,
            res: stats,
        });

        return stats;
    } catch (err) {
        const e = pickErrorDetails(err);
        await logFatal({
            tenantId,
            entityName,
            collectionName,
            docId: runId,
            op: OPERATIONS.BULK_LOAD,
            msg: "Bulk load crashed",
            err: e,
        });
        throw err;
    }
}

function makeBulkLoadHandler(entityName, collectionName, defaultPageSize = 500) {
    return onRequest(
        {
            region: "asia-south1",
            timeoutSeconds: 540,
            memory: "1GiB",
            maxInstances: 3,
        },
        async (req, res) => {
            try {
                if (req.method !== "GET" && req.method !== "POST") {
                    return res.status(405).send("Only GET and POST allowed");
                }

                const tenantId = req.query.tenantId || req.body?.tenantId;
                if (!tenantId) {
                    return res.status(400).json({ error: "tenantId query parameter is required" });
                }

                // Allow customization via query params or body
                const pageSize = req.method === "POST"
                    ? parseIntSafe(req.body?.pageSize, defaultPageSize)
                    : parseIntSafe(req.query.pageSize, defaultPageSize);

                const syncAttachments = req.method === "POST"
                    ? parseBool(req.body?.syncAttachments) === true
                    : parseBool(req.query.syncAttachments) === true;

                const stats = await loadEntityFromQboToFirestore(tenantId, {
                    entityName,
                    collectionName,
                    pageSize,
                    syncAttachments,
                });

                return res.status(200).json({
                    ok: true,
                    message: `${entityName} loaded from QBO into Firestore`,
                    tenantId,
                    entityName,
                    stats: {
                        fetched: stats.totalFetched,
                        saved: stats.totalSaved,
                        attachmentsSynced: stats.attachmentsSynced,
                        attachmentErrors: stats.attachmentErrors,
                    },
                });
            } catch (err) {
                logger.error(`❌ load${entityName} error:`, err.message, err.response?.data);
                return res.status(500).json({
                    ok: false,
                    error: err.message,
                    details: err.response?.data || null,
                });
            }
        }
    );
}

/**
 * Save list to Firestore using BulkWriter for performance
 * Returns count of saved items (backward compatible)
 */
async function saveListToFirestore(tenantId, collectionName, items, entityName) {
    const result = await saveListToFirestoreWithIds(tenantId, collectionName, items, entityName);
    return result.saved;
}

/**
 * Enhanced version that saves to Firestore AND returns document IDs for attachment sync
 * Uses BulkWriter for optimal performance
 */
async function saveListToFirestoreWithIds(tenantId, collectionName, items, entityName) {
    if (!Array.isArray(items) || items.length === 0) {
        return { saved: 0, docIds: [] };
    }

    const tenantCol = getTenantCollection(tenantId, collectionName);
    const valid = items.filter((x) => x && x.Id);
    if (valid.length === 0) {
        return { saved: 0, docIds: [] };
    }

    const qbIds = Array.from(new Set(valid.map((x) => x.Id.toString())));
    const chunk = (arr, size) => {
        const out = [];
        for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
        return out;
    };

    // Check for existing documents
    const qbIdToDocRef = new Map();
    for (const qbIdChunk of chunk(qbIds, 30)) {
        const snap = await tenantCol.where("qbId", "in", qbIdChunk).get();
        snap.forEach((doc) => {
            const d = doc.data() || {};
            if (d.qbId) qbIdToDocRef.set(d.qbId.toString(), doc.ref);
        });
    }

    const writer = qbDb.bulkWriter();
    writer.onWriteError((err) => {
        const code = err?.code || err?.status;
        if (err.failedAttempts < 5 && (code === 4 || code === 10 || code === 14)) return true;
        logger.error("BulkWriter write failed:", err);
        return false;
    });

    const nowUtc = utcNowISO();
    let saved = 0;
    const docIds = []; // Track document IDs for attachment sync

    try {
        for (const item of valid) {
            const qbId = item.Id.toString();
            const existingRef = qbIdToDocRef.get(qbId);
            const ref = existingRef || tenantCol.doc();

            const baseData = {
                ...item,
                qbId,
                isDeleted: false,
                localStatus: LOCAL_STATUS.SYNCED,
                updatedAt: nowUtc,
                _tenantId: tenantId,
                _entityType: entityName,
            };

            const data = existingRef ? baseData : { ...baseData, createdAt: nowUtc };
            writer.set(ref, data, { merge: true });

            // Track doc ID and QB ID for attachment sync
            docIds.push({
                entityDocId: ref.id,
                entityQbId: qbId,
            });

            saved++;
        }
    } finally {
        await writer.close();
    }

    logger.info(`✅ [${tenantId}] BulkWriter saved ${saved} ${entityName}`);

    return { saved, docIds };
}