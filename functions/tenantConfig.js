// ========================================================================================
// TENANT CONFIGURATION
// Extracted for testability — dependencies injected via factory
// ========================================================================================

/**
 * Factory that returns getTenantConfig and updateTenantRefreshToken.
 *
 * @param {object} deps
 * @param {object}   deps.qbDb          - Firestore database instance
 * @param {object}   deps.secretClient  - SecretManagerServiceClient instance
 * @param {Function} deps.storeSecret   - storeSecret(tenantId, type, value)
 * @param {Function} deps.getSecret     - getSecret(tenantId, type)
 * @param {Function} deps.utcNowISO     - returns current UTC ISO string
 * @param {string}   deps.PROJECT_ID    - GCP project ID
 * @param {object}   deps.logger        - {info, warn, error}
 */
function createTenantConfig({ qbDb, secretClient, storeSecret, getSecret, utcNowISO, PROJECT_ID, logger }) {

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
            getSecret(tenantId, "clientId"),
            getSecret(tenantId, "clientSecret"),
            getSecret(tenantId, "refreshToken"),
            getSecret(tenantId, "webhookVerifier"),
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
            const currentValue = currentVersion.payload.data.toString("utf8");

            if (currentValue === newRefreshToken) {
                logger.info(`⏭️  [${tenantId}] Refresh token unchanged, skipping update`);
                return;
            }

            await storeSecret(tenantId, "refreshToken", newRefreshToken);
            await qbDb.collection("qb_tenants").doc(tenantId).update({
                lastTokenRefresh: utcNowISO(),
                updatedAt: utcNowISO(),
            });

            logger.info(`🔄 [${tenantId}] Refresh token rotated successfully`);
        } catch (error) {
            if (error.code === 5) {
                await storeSecret(tenantId, "refreshToken", newRefreshToken);
                await qbDb.collection("qb_tenants").doc(tenantId).update({
                    lastTokenRefresh: utcNowISO(),
                    updatedAt: utcNowISO(),
                });
            } else {
                throw error;
            }
        }
    }

    return { getTenantConfig, updateTenantRefreshToken };
}

module.exports = { createTenantConfig };
