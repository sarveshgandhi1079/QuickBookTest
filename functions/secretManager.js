// ========================================================================================
// SECRET MANAGER OPERATIONS
// Extracted for testability — dependencies injected via factory
// ========================================================================================

/**
 * Factory that returns storeSecret and getSecret bound to the given dependencies.
 *
 * @param {object} deps
 * @param {import('@google-cloud/secret-manager').SecretManagerServiceClient} deps.secretClient
 * @param {string}   deps.PROJECT_ID
 * @param {object}   deps.logger        - firebase-functions logger (or any {info,warn,error})
 */
function createSecretManager({ secretClient, PROJECT_ID, logger }) {

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
                // code 5 = NOT_FOUND → secretExists stays false
            }

            if (secretExists) {
                try {
                    const [currentVersion] = await secretClient.accessSecretVersion({
                        name: `${secretPath}/versions/latest`,
                    });
                    const currentValue = currentVersion.payload.data.toString("utf8");

                    if (currentValue === secretValue) {
                        logger.info(`⏭️  Secret unchanged, skipping: ${secretId}`);
                        return;
                    }

                    await secretClient.addSecretVersion({
                        parent: secretPath,
                        payload: { data: Buffer.from(secretValue, "utf8") },
                    });

                    await secretClient.disableSecretVersion({ name: currentVersion.name });
                    logger.info(`✅ Updated secret (value changed): ${secretId}`);
                } catch (accessError) {
                    logger.warn(`Could not compare secret values for ${secretId}, updating anyway`);
                    await secretClient.addSecretVersion({
                        parent: secretPath,
                        payload: { data: Buffer.from(secretValue, "utf8") },
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
                    payload: { data: Buffer.from(secretValue, "utf8") },
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
            return version.payload.data.toString("utf8");
        } catch (error) {
            if (error.code === 5) {
                throw new Error(`Secret not found: ${secretId}. Please register the tenant first.`);
            }
            logger.error(`Failed to retrieve secret ${secretId}:`, error.message);
            throw new Error(`Failed to retrieve secret: ${error.message}`);
        }
    }

    /**
     * Delete all Secret Manager secrets associated with a tenant.
     * Secret types deleted: clientId, clientSecret, refreshToken, webhookVerifier.
     * Silently skips secrets that do not exist (NOT_FOUND / code 5).
     *
     * @param {string} tenantId
     */
    async function deleteTenantSecrets(tenantId) {
        const secretTypes = ['clientId', 'clientSecret', 'refreshToken', 'webhookVerifier'];
        const parent = `projects/${PROJECT_ID}`;

        await Promise.all(
            secretTypes.map(async (secretType) => {
                const secretId = `qb-${tenantId}-${secretType}`;
                const secretName = `${parent}/secrets/${secretId}`;
                try {
                    await secretClient.deleteSecret({ name: secretName });
                    logger.info(`✅ Deleted secret: ${secretId}`);
                } catch (err) {
                    if (err.code === 5) {
                        // NOT_FOUND — secret does not exist, skip silently
                        logger.warn(`⚠️ Secret not found, skipping: ${secretId}`);
                    } else {
                        logger.error(`❌ Failed to delete secret ${secretId}:`, err.message);
                        throw new Error(`Failed to delete secret ${secretId}: ${err.message}`);
                    }
                }
            })
        );
    }

    return { storeSecret, getSecret, deleteTenantSecrets };
}

module.exports = { createSecretManager };
