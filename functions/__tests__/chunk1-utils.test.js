/**
 * CHUNK 1 — Pure Utility Functions
 * File under test: functions/utils.js
 *
 * All functions here have zero external dependencies, so no mocks are needed.
 * Goal: 100% line, branch, function, and statement coverage for utils.js
 */

const {
    utcNowISO,
    tsId,
    trimObject,
    pickErrorDetails,
    cleanUndefined,
    safeIdPart,
    parseBool,
    parseIntSafe,
    parseISODate,
    tryParseJSON,
} = require("../utils");

// ─────────────────────────────────────────────────────────────────────────────
// utcNowISO
// ─────────────────────────────────────────────────────────────────────────────
describe("utcNowISO", () => {
    it("returns a valid ISO 8601 string", () => {
        const result = utcNowISO();
        expect(typeof result).toBe("string");
        expect(new Date(result).toISOString()).toBe(result);
    });

    it("ends with Z (UTC marker)", () => {
        expect(utcNowISO().endsWith("Z")).toBe(true);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// tsId
// ─────────────────────────────────────────────────────────────────────────────
describe("tsId", () => {
    it("returns a string with no colons or dots", () => {
        const result = tsId();
        expect(result).not.toMatch(/[:.]/);
    });

    it("replaces colons and dots with dashes", () => {
        // e.g. "2026-02-10T14-55-30-456Z"
        expect(tsId()).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z$/);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// trimObject
// ─────────────────────────────────────────────────────────────────────────────
describe("trimObject", () => {
    it("returns null for null input", () => {
        expect(trimObject(null)).toBeNull();
    });

    it("returns null for undefined input", () => {
        expect(trimObject(undefined)).toBeNull();
    });

    it("returns the original object when JSON is within maxLen", () => {
        const obj = { a: 1, b: "hello" };
        const result = trimObject(obj, 6000);
        expect(result).toBe(obj); // same reference — not trimmed
    });

    it("returns a trimmed preview when JSON exceeds maxLen", () => {
        const bigObj = { data: "x".repeat(200) };
        const result = trimObject(bigObj, 10);
        expect(result._trimmed).toBe(true);
        expect(typeof result.preview).toBe("string");
        expect(result.preview.length).toBe(10);
    });

    it("returns { _unserializable: true } for a circular object", () => {
        const circular = {};
        circular.self = circular;
        const result = trimObject(circular);
        expect(result).toEqual({ _unserializable: true });
    });

    it("uses default maxLen of 6000 when not provided", () => {
        const smallObj = { hello: "world" };
        expect(trimObject(smallObj)).toBe(smallObj);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// pickErrorDetails
// ─────────────────────────────────────────────────────────────────────────────
describe("pickErrorDetails", () => {
    it("picks message, code, status, and details from a full error object", () => {
        const err = {
            message: "Not Found",
            code: "ERR_404",
            response: { status: 404, data: { reason: "missing" } },
        };
        const result = pickErrorDetails(err);
        expect(result.message).toBe("Not Found");
        expect(result.code).toBe("ERR_404");
        expect(result.status).toBe(404);
        expect(result.details).toEqual({ reason: "missing" });
    });

    it("falls back to statusCode when response.status is absent", () => {
        const err = { message: "Timeout", statusCode: 503 };
        expect(pickErrorDetails(err).status).toBe(503);
    });

    it("uses String(err) as message when err.message is absent", () => {
        const err = "plain string error";
        expect(pickErrorDetails(err).message).toBe("plain string error");
    });

    it("returns null for code and status when not present", () => {
        const err = new Error("oops");
        const result = pickErrorDetails(err);
        expect(result.code).toBeNull();
        expect(result.status).toBeNull();
    });

    it("returns null for details when response.data is absent", () => {
        const err = { message: "err" };
        expect(pickErrorDetails(err).details).toBeNull();
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// cleanUndefined
// ─────────────────────────────────────────────────────────────────────────────
describe("cleanUndefined", () => {
    it("returns null for null input", () => {
        expect(cleanUndefined(null)).toBeNull();
    });

    it("returns null for undefined input", () => {
        expect(cleanUndefined(undefined)).toBeNull();
    });

    it("returns the same primitive (string)", () => {
        expect(cleanUndefined("hello")).toBe("hello");
    });

    it("returns the same primitive (number)", () => {
        expect(cleanUndefined(42)).toBe(42);
    });

    it("returns the same primitive (boolean)", () => {
        expect(cleanUndefined(false)).toBe(false);
    });

    it("filters null and undefined values out of arrays", () => {
        const result = cleanUndefined([1, null, undefined, 2, "a"]);
        expect(result).toEqual([1, 2, "a"]);
    });

    it("removes undefined keys from an object", () => {
        const result = cleanUndefined({ a: 1, b: undefined, c: "x" });
        expect(result).toEqual({ a: 1, c: "x" });
    });

    it("removes null values from an object", () => {
        const result = cleanUndefined({ a: 1, b: null });
        expect(result).toEqual({ a: 1 });
    });

    it("recursively cleans nested objects", () => {
        const result = cleanUndefined({ a: { b: undefined, c: 2 }, d: 3 });
        expect(result).toEqual({ a: { c: 2 }, d: 3 });
    });

    it("recursively cleans arrays inside objects", () => {
        const result = cleanUndefined({ arr: [1, null, undefined, 2] });
        expect(result).toEqual({ arr: [1, 2] });
    });

    it("returns empty object when all keys are undefined", () => {
        expect(cleanUndefined({ a: undefined })).toEqual({});
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// safeIdPart
// ─────────────────────────────────────────────────────────────────────────────
describe("safeIdPart", () => {
    it("returns 'NA' for empty string", () => {
        expect(safeIdPart("")).toBe("NA");
    });

    it("returns 'NA' for null", () => {
        expect(safeIdPart(null)).toBe("NA");
    });

    it("returns 'NA' for undefined", () => {
        expect(safeIdPart(undefined)).toBe("NA");
    });

    it("replaces spaces with underscores", () => {
        expect(safeIdPart("hello world")).toBe("hello_world");
    });

    it("replaces special characters with underscores", () => {
        expect(safeIdPart("foo@bar!baz")).toBe("foo_bar_baz");
    });

    it("collapses multiple underscores into one", () => {
        expect(safeIdPart("a  b   c")).toBe("a_b_c");
    });

    it("strips leading and trailing underscores", () => {
        expect(safeIdPart("@hello@")).toBe("hello");
    });

    it("truncates to maxLen", () => {
        const long = "a".repeat(200);
        expect(safeIdPart(long, 80).length).toBe(80);
    });

    it("allows alphanumerics, underscores, and dashes", () => {
        expect(safeIdPart("hello-world_123")).toBe("hello-world_123");
    });

    it("returns 'NA' when result after sanitization is empty", () => {
        // input is only special chars that become underscores — then stripped
        expect(safeIdPart("@@@")).toBe("NA");
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// parseBool
// ─────────────────────────────────────────────────────────────────────────────
describe("parseBool", () => {
    it("returns null for undefined", () => {
        expect(parseBool(undefined)).toBeNull();
    });

    it("returns null for null", () => {
        expect(parseBool(null)).toBeNull();
    });

    it("returns null for empty string", () => {
        expect(parseBool("")).toBeNull();
    });

    it.each(["true", "TRUE", "True", "1", "yes", "YES"])(
        "returns true for truthy string '%s'",
        (v) => {
            expect(parseBool(v)).toBe(true);
        }
    );

    it.each(["false", "FALSE", "False", "0", "no", "NO"])(
        "returns false for falsy string '%s'",
        (v) => {
            expect(parseBool(v)).toBe(false);
        }
    );

    it("returns null for unrecognized values", () => {
        expect(parseBool("maybe")).toBeNull();
        expect(parseBool("2")).toBeNull();
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// parseIntSafe
// ─────────────────────────────────────────────────────────────────────────────
describe("parseIntSafe", () => {
    it("parses a valid integer string", () => {
        expect(parseIntSafe("42", 0)).toBe(42);
    });

    it("parses a numeric value", () => {
        expect(parseIntSafe(7, 0)).toBe(7);
    });

    it("returns default for non-numeric string", () => {
        expect(parseIntSafe("abc", 99)).toBe(99);
    });

    it("returns default for undefined", () => {
        expect(parseIntSafe(undefined, -1)).toBe(-1);
    });

    it("returns default for null", () => {
        expect(parseIntSafe(null, 5)).toBe(5);
    });

    it("truncates floats to integer", () => {
        expect(parseIntSafe("3.9", 0)).toBe(3);
    });

    it("parses negative integers", () => {
        expect(parseIntSafe("-10", 0)).toBe(-10);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// parseISODate
// ─────────────────────────────────────────────────────────────────────────────
describe("parseISODate", () => {
    it("returns null for null", () => {
        expect(parseISODate(null)).toBeNull();
    });

    it("returns null for undefined", () => {
        expect(parseISODate(undefined)).toBeNull();
    });

    it("returns null for empty string", () => {
        expect(parseISODate("")).toBeNull();
    });

    it("returns null for an invalid date string", () => {
        expect(parseISODate("not-a-date")).toBeNull();
    });

    it("parses a valid ISO date string and returns ISO string", () => {
        const result = parseISODate("2026-02-25T10:00:00.000Z");
        expect(result).toBe("2026-02-25T10:00:00.000Z");
    });

    it("parses a plain date string and returns ISO string", () => {
        const result = parseISODate("2026-02-25");
        expect(typeof result).toBe("string");
        expect(result).toMatch(/2026-02-25/);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// tryParseJSON
// ─────────────────────────────────────────────────────────────────────────────
describe("tryParseJSON", () => {
    it("returns non-string values as-is", () => {
        expect(tryParseJSON(123)).toBe(123);
        expect(tryParseJSON(null)).toBeNull();
        expect(tryParseJSON({ a: 1 })).toEqual({ a: 1 });
    });

    it("parses a valid JSON object string", () => {
        expect(tryParseJSON('{"key":"value"}')).toEqual({ key: "value" });
    });

    it("parses a valid JSON array string", () => {
        expect(tryParseJSON('[1,2,3]')).toEqual([1, 2, 3]);
    });

    it("returns the original string for an invalid JSON object string", () => {
        const bad = "{not valid json}";
        expect(tryParseJSON(bad)).toBe(bad);
    });

    it("returns the original string for an invalid JSON array string", () => {
        const bad = "[not valid";
        expect(tryParseJSON(bad)).toBe(bad);
    });

    it("returns true for string 'true'", () => {
        expect(tryParseJSON("true")).toBe(true);
    });

    it("returns false for string 'false'", () => {
        expect(tryParseJSON("false")).toBe(false);
    });

    it("returns null for string 'null'", () => {
        expect(tryParseJSON("null")).toBeNull();
    });

    it("returns a number for a numeric string", () => {
        expect(tryParseJSON("42")).toBe(42);
        expect(tryParseJSON("-3.14")).toBe(-3.14);
    });

    it("returns the original string for a plain non-numeric string", () => {
        expect(tryParseJSON("hello world")).toBe("hello world");
    });

    it("handles whitespace-padded numeric strings", () => {
        expect(tryParseJSON("  7  ")).toBe(7);
    });

    it("returns the original string for 'Infinity' (non-finite number)", () => {
        // "Infinity" passes !isNaN but Number.isFinite(Infinity) === false
        expect(tryParseJSON("Infinity")).toBe("Infinity");
    });
});
