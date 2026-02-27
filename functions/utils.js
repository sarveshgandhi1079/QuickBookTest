// ========================================================================================
// PURE UTILITY FUNCTIONS
// Extracted for testability — no external dependencies
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
    if (input === null) return null;
    if (input === undefined) return null;
    if (Array.isArray(input)) {
        return input
            .filter((v) => v !== undefined)             // strip undefined only
            .map((v) => (v === null ? null : cleanUndefined(v))); // preserve null, recurse others
    }
    if (typeof input === "object") {
        const out = {};
        for (const [k, v] of Object.entries(input)) {
            if (v === undefined) continue;              // skip undefined keys only
            out[k] = v === null ? null : cleanUndefined(v); // preserve null values
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
    return d.toISOString();
}

function tryParseJSON(str) {
    if (typeof str !== "string") return str;

    const trimmed = str.trim();

    if (
        (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
        (trimmed.startsWith("[") && trimmed.endsWith("]"))
    ) {
        try {
            return JSON.parse(trimmed);
        } catch (e) {
            return str;
        }
    }

    if (trimmed === "true") return true;
    if (trimmed === "false") return false;
    if (trimmed === "null") return null;

    if (!isNaN(trimmed) && trimmed !== "") {
        const num = Number(trimmed);
        if (Number.isFinite(num)) return num;
    }

    return str;
}

module.exports = {
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
};
