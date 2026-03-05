// ========================================================================================
// FIRESTORE HELPER UTILITIES
// Extracted for testability — dependencies injected via factory
// ========================================================================================

/**
 * Factory that returns Firestore helper utilities.
 *
 * @param {object} deps
 * @param {object} deps.logger - {info, warn, error}
 */
function createFirestoreHelpers({ logger }) {
    /**
     * Safely retrieves the count of documents matching a Firestore query.
     * Returns null if the aggregation API isn't available or throws.
     *
     * @param {object} query - Firestore Query object
     * @returns {Promise<number|null>}
     */
    async function getTotalCountSafe(query) {
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
        return null;
    }

    /**
     * Safely reads a Firestore document by ref.
     * Returns a plain object { docId, ...data } if the doc exists, or null otherwise.
     * Returns null (instead of throwing) on any error.
     *
     * @param {object} ref - Firestore DocumentReference
     * @returns {Promise<object|null>}
     */
    async function readDocSafe(ref) {
        try {
            const snap = await ref.get();
            return snap.exists ? { docId: snap.id, ...snap.data() } : null;
        } catch {
            return null;
        }
    }

    return { getTotalCountSafe, readDocSafe };
}

module.exports = { createFirestoreHelpers };
