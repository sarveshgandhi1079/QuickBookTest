/**
 * CHUNK 5 — Firestore Helper Utilities
 * File under test: functions/firestore.js
 *
 * Mocks: logger
 * Goal: 100% line, branch, function, and statement coverage for firestore.js
 */

const { createFirestoreHelpers } = require("../firestore");

// ─────────────────────────────────────────────────────────────────────────────
// Mock factory helpers
// ─────────────────────────────────────────────────────────────────────────────

function makeLogger() {
    return { info: jest.fn(), warn: jest.fn(), error: jest.fn() };
}

// ─────────────────────────────────────────────────────────────────────────────
// getTotalCountSafe
// ─────────────────────────────────────────────────────────────────────────────
describe("getTotalCountSafe", () => {
    let logger, getTotalCountSafe;

    beforeEach(() => {
        logger = makeLogger();
        ({ getTotalCountSafe } = createFirestoreHelpers({ logger }));
    });

    it("returns the numeric count when query.count() succeeds", async () => {
        const query = {
            count: jest.fn().mockReturnValue({
                get: jest.fn().mockResolvedValue({ data: () => ({ count: 42 }) }),
            }),
        };
        const result = await getTotalCountSafe(query);
        expect(result).toBe(42);
        expect(logger.warn).not.toHaveBeenCalled();
    });

    it("returns null when data.count is not a number (e.g. undefined)", async () => {
        const query = {
            count: jest.fn().mockReturnValue({
                get: jest.fn().mockResolvedValue({ data: () => ({ count: undefined }) }),
            }),
        };
        const result = await getTotalCountSafe(query);
        expect(result).toBeNull();
    });

    it("returns null when data.count is a string (non-number)", async () => {
        const query = {
            count: jest.fn().mockReturnValue({
                get: jest.fn().mockResolvedValue({ data: () => ({ count: "42" }) }),
            }),
        };
        const result = await getTotalCountSafe(query);
        expect(result).toBeNull();
    });

    it("warns and returns null when query.count().get() throws", async () => {
        const query = {
            count: jest.fn().mockReturnValue({
                get: jest.fn().mockRejectedValue(new Error("aggregation unavailable")),
            }),
        };
        const result = await getTotalCountSafe(query);
        expect(result).toBeNull();
        expect(logger.warn).toHaveBeenCalledWith(
            "Count aggregation failed (falling back to null):",
            "aggregation unavailable"
        );
    });

    it("returns null and logs message when thrown error has no .message", async () => {
        const query = {
            count: jest.fn().mockReturnValue({
                get: jest.fn().mockRejectedValue("raw string error"),
            }),
        };
        const result = await getTotalCountSafe(query);
        expect(result).toBeNull();
        expect(logger.warn).toHaveBeenCalledWith(
            "Count aggregation failed (falling back to null):",
            "raw string error"
        );
    });

    it("returns null when query.count is not a function (no count API)", async () => {
        const query = { count: undefined };
        const result = await getTotalCountSafe(query);
        expect(result).toBeNull();
        expect(logger.warn).not.toHaveBeenCalled();
    });

    it("returns null when query has no count property at all", async () => {
        const result = await getTotalCountSafe({});
        expect(result).toBeNull();
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// readDocSafe
// ─────────────────────────────────────────────────────────────────────────────
describe("readDocSafe", () => {
    let readDocSafe;

    beforeEach(() => {
        ({ readDocSafe } = createFirestoreHelpers({ logger: makeLogger() }));
    });

    it("returns { docId, ...data } when the doc exists", async () => {
        const ref = {
            get: jest.fn().mockResolvedValue({
                exists: true,
                id: "doc-abc",
                data: () => ({ name: "Alice", amount: 100 }),
            }),
        };
        const result = await readDocSafe(ref);
        expect(result).toEqual({ docId: "doc-abc", name: "Alice", amount: 100 });
    });

    it("returns null when the doc does not exist", async () => {
        const ref = {
            get: jest.fn().mockResolvedValue({ exists: false }),
        };
        const result = await readDocSafe(ref);
        expect(result).toBeNull();
    });

    it("returns null when ref.get() throws (swallows error silently)", async () => {
        const ref = {
            get: jest.fn().mockRejectedValue(new Error("Firestore unavailable")),
        };
        const result = await readDocSafe(ref);
        expect(result).toBeNull();
    });
});
