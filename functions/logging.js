// ========================================================================================
// LOGGING SYSTEM (PER-ENTITY + ERROR INDEX)
// Extracted for testability — dependencies injected via factory
// ========================================================================================

/**
 * Factory that returns all logging helpers.
 *
 * @param {object} deps
 * @param {object}   deps.qbDb          - Firestore database instance
 * @param {object}   deps.FieldValue    - Firestore FieldValue (for increment)
 * @param {Function} deps.utcNowISO     - () => ISO timestamp string
 * @param {Function} deps.cleanUndefined
 * @param {Function} deps.trimObject
 * @param {Function} deps.safeIdPart
 * @param {object}   deps.logger        - {info, warn, error}
 * @param {object}   deps.LOG_STAGES    - Stage constants
 */
function createLogging({ qbDb, FieldValue, utcNowISO, cleanUndefined, trimObject, safeIdPart, logger, LOG_STAGES }) {

    // ── Internal helpers ──────────────────────────────────────────────────────

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

    // ── Simple stage wrappers ─────────────────────────────────────────────────

    async function logStart(p) { return appendLogStep({ ...p, stage: LOG_STAGES.START }); }
    async function logEnd(p) { return appendLogStep({ ...p, stage: LOG_STAGES.END }); }
    async function logFsOk(p) { return appendLogStep({ ...p, stage: LOG_STAGES.FS_OK }); }
    async function logQboRequest(p) { return appendLogStep({ ...p, stage: LOG_STAGES.QBO_REQUEST }); }
    async function logQboOk(p) { return appendLogStep({ ...p, stage: LOG_STAGES.QBO_OK }); }

    // ── Error wrappers (log + error index) ───────────────────────────────────

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

    return {
        getEntityLogDocRef,
        appendLogStep,
        buildTenantErrorDocId,
        upsertTenantErrorIndex,
        logStart,
        logEnd,
        logFsOk,
        logQboRequest,
        logQboOk,
        logFsFail,
        logQboFail,
        logFatal,
    };
}

module.exports = { createLogging };
