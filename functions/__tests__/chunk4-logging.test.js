/**
 * CHUNK 4 — Logging System
 * File under test: functions/logging.js
 *
 * Mocks: qbDb (runTransaction, collection chain), FieldValue, logger
 * Real deps: utcNowISO, cleanUndefined, trimObject, safeIdPart (from utils — already 100% tested)
 * Goal: 100% line, branch, function, and statement coverage for logging.js
 */

const { createLogging } = require("../logging");
const { utcNowISO, cleanUndefined, trimObject, safeIdPart } = require("../utils");

// ─────────────────────────────────────────────────────────────────────────────
// Constants (mirrors index.js)
// ─────────────────────────────────────────────────────────────────────────────
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

// ─────────────────────────────────────────────────────────────────────────────
// Mock factories
// ─────────────────────────────────────────────────────────────────────────────

/** Build a Firestore doc-ref mock that records .collection().doc() chains */
function makeDocRefMock() {
    const mock = { id: "mockDocId" };
    return mock;
}

/**
 * Build a qbDb mock where runTransaction executes the callback synchronously
 * using a fake tx with get/set.
 *
 * @param {object} snapData   - { exists, data } returned by tx.get()
 */
function makeQbDb(snapData = { exists: false, data: () => ({}) }) {
    const txSet = jest.fn();
    const txGet = jest.fn().mockResolvedValue(snapData);
    const tx = { get: txGet, set: txSet };

    const docMock = { id: "logDocId" };
    const deepDocMock = { id: "errDocId" };

    // Supports:
    //   qbDb.collection("tenants").doc(id).collection("logs").doc(docId)
    //   qbDb.collection("tenants").doc(id).collection("error_index").doc(errId)
    const innerCollection = jest.fn().mockReturnValue({ doc: jest.fn().mockReturnValue(deepDocMock) });
    const tenantDoc = jest.fn().mockReturnValue({ collection: innerCollection });
    const outerCollection = jest.fn().mockReturnValue({ doc: tenantDoc });

    return {
        collection: outerCollection,
        runTransaction: jest.fn().mockImplementation(async (cb) => cb(tx)),
        _tx: tx,
        _txSet: txSet,
        _txGet: txGet,
    };
}

function makeFieldValue() {
    return { increment: jest.fn().mockReturnValue({ _increment: true }) };
}

function makeLogger() {
    return { info: jest.fn(), warn: jest.fn(), error: jest.fn() };
}

/** Standard base params shared across many tests */
const BASE_PARAMS = {
    tenantId: "tenant1",
    entityName: "Bill",
    collectionName: "bills",
    docId: "log-doc-123",
    qbId: "qb-456",
    op: "CREATE",
    msg: "test message",
};

// ─────────────────────────────────────────────────────────────────────────────
// buildTenantErrorDocId  (pure — no external deps needed)
// ─────────────────────────────────────────────────────────────────────────────
describe("buildTenantErrorDocId", () => {
    let buildTenantErrorDocId;

    beforeEach(() => {
        const { buildTenantErrorDocId: fn } = createLogging({
            qbDb: makeQbDb(), FieldValue: makeFieldValue(),
            utcNowISO, cleanUndefined, trimObject, safeIdPart,
            logger: makeLogger(), LOG_STAGES,
        });
        buildTenantErrorDocId = fn;
    });

    it("uses doc_ prefix when recordDocId is provided", () => {
        const result = buildTenantErrorDocId({
            entityName: "Bill", op: "CREATE", stage: "FS_FAIL", recordDocId: "doc123",
        });
        expect(result).toMatch(/^Bill__CREATE__FS_FAIL__doc_doc123$/);
    });

    it("uses qb_ prefix when qbId is provided (no recordDocId)", () => {
        const result = buildTenantErrorDocId({
            entityName: "Bill", op: "CREATE", stage: "FS_FAIL", qbId: "qb999",
        });
        expect(result).toMatch(/^Bill__CREATE__FS_FAIL__qb_qb999$/);
    });

    it("uses log_ prefix when logDocId is provided (no recordDocId, no qbId)", () => {
        const result = buildTenantErrorDocId({
            entityName: "Bill", op: "CREATE", stage: "FS_FAIL", logDocId: "logABC",
        });
        expect(result).toMatch(/^Bill__CREATE__FS_FAIL__log_logABC$/);
    });

    it("uses __unknown suffix when none of the optional ids are provided", () => {
        const result = buildTenantErrorDocId({
            entityName: "Bill", op: "CREATE", stage: "FS_FAIL",
        });
        expect(result).toBe("Bill__CREATE__FS_FAIL__unknown");
    });

    it("prefers recordDocId over qbId", () => {
        const result = buildTenantErrorDocId({
            entityName: "Bill", op: "CREATE", stage: "FS_FAIL",
            recordDocId: "docFirst", qbId: "qbSecond",
        });
        expect(result).toContain("doc_docFirst");
        expect(result).not.toContain("qb_");
    });

    it("prefers qbId over logDocId", () => {
        const result = buildTenantErrorDocId({
            entityName: "Bill", op: "CREATE", stage: "FS_FAIL",
            qbId: "qbFirst", logDocId: "logSecond",
        });
        expect(result).toContain("qb_qbFirst");
        expect(result).not.toContain("log_");
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// getEntityLogDocRef
// ─────────────────────────────────────────────────────────────────────────────
describe("getEntityLogDocRef", () => {
    it("navigates the correct Firestore path: tenants/{tenantId}/logs/{docId}", () => {
        const qbDb = makeQbDb();
        const { getEntityLogDocRef } = createLogging({
            qbDb, FieldValue: makeFieldValue(), utcNowISO,
            cleanUndefined, trimObject, safeIdPart, logger: makeLogger(), LOG_STAGES,
        });

        getEntityLogDocRef({ tenantId: "t1", entityName: "Bill", docId: "d1" });

        // collection("tenants") was called
        expect(qbDb.collection).toHaveBeenCalledWith("tenants");
        // .doc("t1") was called
        const tenantDocFn = qbDb.collection.mock.results[0].value.doc;
        expect(tenantDocFn).toHaveBeenCalledWith("t1");
        // Then .collection("logs")
        const innerColFn = tenantDocFn.mock.results[0].value.collection;
        expect(innerColFn).toHaveBeenCalledWith("logs");
        // Then .doc("d1")
        const deepDocFn = innerColFn.mock.results[0].value.doc;
        expect(deepDocFn).toHaveBeenCalledWith("d1");
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// appendLogStep
// ─────────────────────────────────────────────────────────────────────────────
describe("appendLogStep", () => {
    let qbDb, logger, appendLogStep;

    function build(snapData) {
        qbDb = makeQbDb(snapData);
        logger = makeLogger();
        ({ appendLogStep } = createLogging({
            qbDb, FieldValue: makeFieldValue(), utcNowISO,
            cleanUndefined, trimObject, safeIdPart, logger, LOG_STAGES,
        }));
    }

    // ── Guard: missing required fields ───────────────────────────────────────
    it("returns early (no transaction) when tenantId is missing", async () => {
        build();
        await appendLogStep({ ...BASE_PARAMS, tenantId: null });
        expect(qbDb.runTransaction).not.toHaveBeenCalled();
    });

    it("returns early when entityName is missing", async () => {
        build();
        await appendLogStep({ ...BASE_PARAMS, entityName: "" });
        expect(qbDb.runTransaction).not.toHaveBeenCalled();
    });

    it("returns early when docId is missing", async () => {
        build();
        await appendLogStep({ ...BASE_PARAMS, docId: undefined });
        expect(qbDb.runTransaction).not.toHaveBeenCalled();
    });

    // ── Transaction: doc does NOT exist yet ──────────────────────────────────
    describe("when the log doc does not yet exist", () => {
        beforeEach(() => build({ exists: false, data: () => ({}) }));

        it("calls runTransaction", async () => {
            await appendLogStep({ ...BASE_PARAMS, stage: "START" });
            expect(qbDb.runTransaction).toHaveBeenCalledTimes(1);
        });

        it("calls tx.set with createdAt equal to updatedAt (new doc)", async () => {
            await appendLogStep({ ...BASE_PARAMS, stage: "START" });
            const setCall = qbDb._txSet.mock.calls[0][1];
            expect(setCall.createdAt).toBe(setCall.updatedAt);
        });

        it("sets steps array with one entry", async () => {
            await appendLogStep({ ...BASE_PARAMS, stage: "START", msg: "hello" });
            const setCall = qbDb._txSet.mock.calls[0][1];
            expect(Array.isArray(setCall.steps)).toBe(true);
            expect(setCall.steps.length).toBe(1);
            expect(setCall.steps[0].msg).toBe("hello");
        });

        it("sets lastError to null when err is not provided", async () => {
            await appendLogStep({ ...BASE_PARAMS, stage: "START" });
            const setCall = qbDb._txSet.mock.calls[0][1];
            // cleanUndefined removes null keys, so lastError might be absent
            expect(setCall.lastError == null).toBe(true);
        });
    });

    // ── Transaction: doc EXISTS with previous steps ──────────────────────────
    describe("when the log doc already exists", () => {
        const prevStep = { ts: "2026-01-01T00:00:00.000Z", msg: "previous" };

        beforeEach(() => build({
            exists: true,
            data: () => ({
                steps: [prevStep],
                createdAt: "2026-01-01T00:00:00.000Z",
                qbId: "prev-qb-id",
            }),
        }));

        it("appends the new step to existing steps", async () => {
            await appendLogStep({ ...BASE_PARAMS, stage: "END", msg: "new step" });
            const setCall = qbDb._txSet.mock.calls[0][1];
            expect(setCall.steps.length).toBe(2);
            expect(setCall.steps[1].msg).toBe("new step");
        });

        it("preserves the original createdAt", async () => {
            await appendLogStep({ ...BASE_PARAMS, stage: "END" });
            const setCall = qbDb._txSet.mock.calls[0][1];
            expect(setCall.createdAt).toBe("2026-01-01T00:00:00.000Z");
        });

        it("uses prev.qbId when qbId param is null", async () => {
            await appendLogStep({ ...BASE_PARAMS, stage: "END", qbId: null });
            const setCall = qbDb._txSet.mock.calls[0][1];
            expect(setCall.qbId).toBe("prev-qb-id");
        });
    });

    // ── qbId default param branch (line 36) ──────────────────────────────────
    it("uses null for qbId when neither qbId param nor prev.qbId are provided", async () => {
        // snap has no qbId → prev.qbId is undefined; we also don't pass qbId → hits default=null then ?? null
        build({ exists: true, data: () => ({ steps: [], createdAt: "2026-01-01T00:00:00.000Z" }) });
        // Omit qbId entirely to hit the default-param branch (qbId = null)
        const { tenantId, entityName, collectionName, docId, op, msg } = BASE_PARAMS;
        await appendLogStep({ tenantId, entityName, collectionName, docId, op, msg, stage: "START" });
        const setCall = qbDb._txSet.mock.calls[0][1];
        // qbId should be absent (cleanUndefined removes null values) or explicitly null
        expect(setCall.qbId == null).toBe(true);
    });

    // ── Transaction: maxSteps trimming ───────────────────────────────────────
    it("trims steps to maxSteps when exceeded", async () => {
        const manySteps = Array.from({ length: 5 }, (_, i) => ({ msg: `step${i}` }));
        build({
            exists: true,
            data: () => ({ steps: manySteps, createdAt: "2026-01-01T00:00:00.000Z" }),
        });

        // maxSteps = 3 → only last 3 of 6 total (5 prev + 1 new) survive
        await appendLogStep({ ...BASE_PARAMS, stage: "START", maxSteps: 3 });
        const setCall = qbDb._txSet.mock.calls[0][1];
        expect(setCall.steps.length).toBe(3);
    });

    // ── err field handling ────────────────────────────────────────────────────
    it("includes err in the step when err is provided", async () => {
        build({ exists: false, data: () => ({}) });
        const errObj = { message: "oops", code: 500 };
        await appendLogStep({ ...BASE_PARAMS, stage: "FS_FAIL", err: errObj });
        const setCall = qbDb._txSet.mock.calls[0][1];
        // step.err comes from trimObject(errObj) which returns the obj as-is (small)
        expect(setCall.steps[0].err).toBeDefined();
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// upsertTenantErrorIndex
// ─────────────────────────────────────────────────────────────────────────────
describe("upsertTenantErrorIndex", () => {
    let FieldValue, logger, upsertTenantErrorIndex;

    function build(snapData) {
        const qbDb = makeQbDb(snapData);
        FieldValue = makeFieldValue();
        logger = makeLogger();
        ({ upsertTenantErrorIndex } = createLogging({
            qbDb, FieldValue, utcNowISO, cleanUndefined, trimObject, safeIdPart, logger, LOG_STAGES,
        }));
        return qbDb;
    }

    const BASE = {
        tenantId: "t1", entityName: "Bill", op: "CREATE", stage: "FS_FAIL",
        message: "something failed",
    };

    // ── Guard: missing required fields ───────────────────────────────────────
    it("returns early when tenantId is missing", async () => {
        const qbDb = build();
        await upsertTenantErrorIndex({ ...BASE, tenantId: null });
        expect(qbDb.runTransaction).not.toHaveBeenCalled();
    });

    it("returns early when entityName is missing", async () => {
        const qbDb = build();
        await upsertTenantErrorIndex({ ...BASE, entityName: "" });
        expect(qbDb.runTransaction).not.toHaveBeenCalled();
    });

    it("returns early when op is missing", async () => {
        const qbDb = build();
        await upsertTenantErrorIndex({ ...BASE, op: null });
        expect(qbDb.runTransaction).not.toHaveBeenCalled();
    });

    it("returns early when stage is missing", async () => {
        const qbDb = build();
        await upsertTenantErrorIndex({ ...BASE, stage: "" });
        expect(qbDb.runTransaction).not.toHaveBeenCalled();
    });

    // ── New error doc (does NOT exist) ───────────────────────────────────────
    describe("when the error doc does not exist", () => {
        it("creates doc with count=1 and firstSeenAt", async () => {
            const qbDb = build({ exists: false, data: () => ({}) });
            await upsertTenantErrorIndex({ ...BASE });
            const setCall = qbDb._txSet.mock.calls[0][1];
            expect(setCall.count).toBe(1);
            expect(setCall.firstSeenAt).toBeDefined();
            expect(setCall.createdAt).toBeDefined();
        });

        it("does NOT call FieldValue.increment", async () => {
            build({ exists: false, data: () => ({}) });
            await upsertTenantErrorIndex({ ...BASE });
            expect(FieldValue.increment).not.toHaveBeenCalled();
        });
    });

    // ── Existing error doc ────────────────────────────────────────────────────
    describe("when the error doc already exists", () => {
        it("calls FieldValue.increment(1) for count", async () => {
            const qbDb = build({ exists: true, data: () => ({ count: 3 }) });
            await upsertTenantErrorIndex({ ...BASE });
            expect(FieldValue.increment).toHaveBeenCalledWith(1);
            const setCall = qbDb._txSet.mock.calls[0][1];
            expect(setCall.count).toEqual({ _increment: true });
        });

        it("does NOT set firstSeenAt or createdAt on update", async () => {
            const qbDb = build({ exists: true, data: () => ({ count: 1 }) });
            await upsertTenantErrorIndex({ ...BASE });
            const setCall = qbDb._txSet.mock.calls[0][1];
            expect(setCall.firstSeenAt).toBeUndefined();
            expect(setCall.createdAt).toBeUndefined();
        });
    });

    // ── Optional fields ───────────────────────────────────────────────────────
    it("includes lastError when error is provided", async () => {
        const qbDb = build({ exists: false, data: () => ({}) });
        await upsertTenantErrorIndex({ ...BASE, error: { message: "boom" } });
        const setCall = qbDb._txSet.mock.calls[0][1];
        expect(setCall.lastError).toBeDefined();
    });

    it("omits lastError when error is null", async () => {
        const qbDb = build({ exists: false, data: () => ({}) });
        await upsertTenantErrorIndex({ ...BASE, error: null });
        const setCall = qbDb._txSet.mock.calls[0][1];
        // cleanUndefined removes null values
        expect(setCall.lastError == null).toBe(true);
    });

    it("handles missing message (falsy-message branch)", async () => {
        const qbDb = build({ exists: false, data: () => ({}) });
        await upsertTenantErrorIndex({ ...BASE, message: null });
        // transaction should still run — no error
        expect(qbDb.runTransaction).toHaveBeenCalled();
    });

    it("sets recordDocId in patch when recordDocId is provided", async () => {
        const qbDb = build({ exists: false, data: () => ({}) });
        await upsertTenantErrorIndex({ ...BASE, recordDocId: "docABC" });
        // patch is passed to tx.set — verify runTransaction was called (recordDocId truthy branch)
        expect(qbDb.runTransaction).toHaveBeenCalled();
        const setCall = qbDb._txSet.mock.calls[0][1];
        expect(setCall.recordDocId).toBe("docABC");
    });

    it("includes lastRequest when request is provided", async () => {
        const qbDb = build({ exists: false, data: () => ({}) });
        await upsertTenantErrorIndex({ ...BASE, request: { url: "/api" } });
        const setCall = qbDb._txSet.mock.calls[0][1];
        expect(setCall.lastRequest).toBeDefined();
    });

    it("includes lastResponse when response is provided", async () => {
        const qbDb = build({ exists: false, data: () => ({}) });
        await upsertTenantErrorIndex({ ...BASE, response: { status: 200 } });
        const setCall = qbDb._txSet.mock.calls[0][1];
        expect(setCall.lastResponse).toBeDefined();
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// Simple stage wrappers: logStart, logEnd, logFsOk, logQboRequest, logQboOk
// ─────────────────────────────────────────────────────────────────────────────
describe("simple stage wrappers", () => {
    let qbDb, wrappers;

    beforeEach(() => {
        qbDb = makeQbDb({ exists: false, data: () => ({}) });
        wrappers = createLogging({
            qbDb, FieldValue: makeFieldValue(), utcNowISO,
            cleanUndefined, trimObject, safeIdPart, logger: makeLogger(), LOG_STAGES,
        });
    });

    const cases = [
        ["logStart",      "START"],
        ["logEnd",        "END"],
        ["logFsOk",       "FS_OK"],
        ["logQboRequest", "QBO_REQUEST"],
        ["logQboOk",      "QBO_OK"],
    ];

    it.each(cases)("%s passes stage '%s' to appendLogStep", async (fnName, expectedStage) => {
        await wrappers[fnName]({ ...BASE_PARAMS });
        const setCall = qbDb._txSet.mock.calls[0][1];
        expect(setCall.lastStage).toBe(expectedStage);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// Error wrappers: logFsFail, logQboFail, logFatal
// ─────────────────────────────────────────────────────────────────────────────
describe("error stage wrappers", () => {
    const errorCases = [
        ["logFsFail",  "FS_FAIL",  "FS_FAIL"],
        ["logQboFail", "QBO_FAIL", "QBO_FAIL"],
        ["logFatal",   "FATAL",    "FATAL"],
    ];

    describe.each(errorCases)("%s", (fnName, expectedLogStage, expectedErrorStage) => {
        let qbDb, logger, fn;

        beforeEach(() => {
            qbDb = makeQbDb({ exists: false, data: () => ({}) });
            logger = makeLogger();
            const fns = createLogging({
                qbDb, FieldValue: makeFieldValue(), utcNowISO,
                cleanUndefined, trimObject, safeIdPart, logger, LOG_STAGES,
            });
            fn = fns[fnName];
        });

        it(`writes stage '${expectedLogStage}' to the log doc`, async () => {
            await fn({ ...BASE_PARAMS });
            const setCall = qbDb._txSet.mock.calls[0][1];
            expect(setCall.lastStage).toBe(expectedLogStage);
        });

        it(`also calls runTransaction a second time for upsertTenantErrorIndex`, async () => {
            await fn({ ...BASE_PARAMS });
            // First call = appendLogStep, second = upsertTenantErrorIndex
            expect(qbDb.runTransaction).toHaveBeenCalledTimes(2);
        });

        it("swallows upsertTenantErrorIndex errors (via .catch) and logs them", async () => {
            // Make the second runTransaction call throw
            let callCount = 0;
            qbDb.runTransaction.mockImplementation(async (cb) => {
                callCount++;
                if (callCount === 2) throw new Error("index write failed");
                return cb(qbDb._tx);
            });

            // Should NOT throw
            await expect(fn({ ...BASE_PARAMS })).resolves.toBeUndefined();
            expect(logger.error).toHaveBeenCalledWith(
                expect.stringContaining("Failed to upsert error_index"),
                expect.any(String)
            );
        });
    });
});
