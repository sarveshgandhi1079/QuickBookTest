/**
 * CHUNK 2 — Secret Manager Operations
 * File under test: functions/secretManager.js
 *
 * All external calls (SecretManagerServiceClient) are mocked with jest.fn().
 * Goal: 100% line, branch, function, and statement coverage for secretManager.js
 */

const { createSecretManager } = require("../secretManager");

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

/** Build a fake secretClient with all methods as jest.fn() */
function makeSecretClient() {
    return {
        getSecret: jest.fn(),
        accessSecretVersion: jest.fn(),
        addSecretVersion: jest.fn(),
        disableSecretVersion: jest.fn(),
        createSecret: jest.fn(),
    };
}

/** Build a fake logger */
function makeLogger() {
    return {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
    };
}

const PROJECT_ID = "test-project";

// Helper: encode a string exactly as the real Secret Manager payload does
function encodePayload(value) {
    return { payload: { data: Buffer.from(value, "utf8") } };
}

// ─────────────────────────────────────────────────────────────────────────────
// storeSecret
// ─────────────────────────────────────────────────────────────────────────────
describe("storeSecret", () => {
    let secretClient, logger, storeSecret;

    beforeEach(() => {
        secretClient = makeSecretClient();
        logger = makeLogger();
        ({ storeSecret } = createSecretManager({ secretClient, PROJECT_ID, logger }));
    });

    // ── Branch 1: Secret does NOT yet exist (getSecret throws code 5) ────────
    describe("when the secret does not exist yet", () => {
        beforeEach(() => {
            // getSecret → throws NOT_FOUND (code 5) → secretExists stays false
            secretClient.getSecret.mockRejectedValue({ code: 5 });

            // createSecret returns an object whose [0].name is the secret resource name
            secretClient.createSecret.mockResolvedValue([
                { name: "projects/test-project/secrets/qb-tenant1-clientId" },
            ]);

            // addSecretVersion succeeds
            secretClient.addSecretVersion.mockResolvedValue([{}]);
        });

        it("calls createSecret with correct args", async () => {
            await storeSecret("tenant1", "clientId", "secret-value");

            expect(secretClient.createSecret).toHaveBeenCalledWith({
                parent: "projects/test-project",
                secretId: "qb-tenant1-clientId",
                secret: { replication: { automatic: {} } },
            });
        });

        it("calls addSecretVersion with the encoded value on the new secret's name", async () => {
            await storeSecret("tenant1", "clientId", "secret-value");

            expect(secretClient.addSecretVersion).toHaveBeenCalledWith({
                parent: "projects/test-project/secrets/qb-tenant1-clientId",
                payload: { data: Buffer.from("secret-value", "utf8") },
            });
        });

        it("logs a created message", async () => {
            await storeSecret("tenant1", "clientId", "secret-value");
            expect(logger.info).toHaveBeenCalledWith(
                expect.stringContaining("Created secret")
            );
        });

        it("does NOT call disableSecretVersion", async () => {
            await storeSecret("tenant1", "clientId", "secret-value");
            expect(secretClient.disableSecretVersion).not.toHaveBeenCalled();
        });
    });

    // ── Branch 2: Secret exists, value is UNCHANGED ──────────────────────────
    describe("when the secret exists and the value is unchanged", () => {
        const EXISTING_VALUE = "same-value";

        beforeEach(() => {
            secretClient.getSecret.mockResolvedValue([{}]); // exists
            secretClient.accessSecretVersion.mockResolvedValue([
                {
                    name: "projects/test-project/secrets/qb-tenant1-clientId/versions/3",
                    ...encodePayload(EXISTING_VALUE),
                },
            ]);
        });

        it("returns without calling addSecretVersion or disableSecretVersion", async () => {
            await storeSecret("tenant1", "clientId", EXISTING_VALUE);

            expect(secretClient.addSecretVersion).not.toHaveBeenCalled();
            expect(secretClient.disableSecretVersion).not.toHaveBeenCalled();
        });

        it("logs a skipped message", async () => {
            await storeSecret("tenant1", "clientId", EXISTING_VALUE);
            expect(logger.info).toHaveBeenCalledWith(
                expect.stringContaining("unchanged")
            );
        });
    });

    // ── Branch 3: Secret exists, value HAS changed ───────────────────────────
    describe("when the secret exists and the value has changed", () => {
        const OLD_VALUE = "old-value";
        const NEW_VALUE = "new-value";
        const VERSION_NAME =
            "projects/test-project/secrets/qb-tenant1-clientId/versions/3";

        beforeEach(() => {
            secretClient.getSecret.mockResolvedValue([{}]); // exists
            secretClient.accessSecretVersion.mockResolvedValue([
                { name: VERSION_NAME, ...encodePayload(OLD_VALUE) },
            ]);
            secretClient.addSecretVersion.mockResolvedValue([{}]);
            secretClient.disableSecretVersion.mockResolvedValue([{}]);
        });

        it("calls addSecretVersion with the new value", async () => {
            await storeSecret("tenant1", "clientId", NEW_VALUE);

            expect(secretClient.addSecretVersion).toHaveBeenCalledWith({
                parent: "projects/test-project/secrets/qb-tenant1-clientId",
                payload: { data: Buffer.from(NEW_VALUE, "utf8") },
            });
        });

        it("calls disableSecretVersion on the old version", async () => {
            await storeSecret("tenant1", "clientId", NEW_VALUE);

            expect(secretClient.disableSecretVersion).toHaveBeenCalledWith({
                name: VERSION_NAME,
            });
        });

        it("logs an updated message", async () => {
            await storeSecret("tenant1", "clientId", NEW_VALUE);
            expect(logger.info).toHaveBeenCalledWith(
                expect.stringContaining("Updated secret")
            );
        });
    });

    // ── Branch 4: Secret exists, but accessSecretVersion throws ─────────────
    describe("when accessSecretVersion throws (cannot compare values)", () => {
        beforeEach(() => {
            secretClient.getSecret.mockResolvedValue([{}]); // exists
            secretClient.accessSecretVersion.mockRejectedValue(
                new Error("Permission denied to access version")
            );
            secretClient.addSecretVersion.mockResolvedValue([{}]);
        });

        it("still calls addSecretVersion (fallback write)", async () => {
            await storeSecret("tenant1", "clientId", "some-value");

            expect(secretClient.addSecretVersion).toHaveBeenCalledWith({
                parent: "projects/test-project/secrets/qb-tenant1-clientId",
                payload: { data: Buffer.from("some-value", "utf8") },
            });
        });

        it("does NOT call disableSecretVersion", async () => {
            await storeSecret("tenant1", "clientId", "some-value");
            expect(secretClient.disableSecretVersion).not.toHaveBeenCalled();
        });

        it("warns about the comparison failure", async () => {
            await storeSecret("tenant1", "clientId", "some-value");
            expect(logger.warn).toHaveBeenCalledWith(
                expect.stringContaining("Could not compare")
            );
        });
    });

    // ── Branch 5: getSecret throws with code !== 5 (unexpected error) ────────
    describe("when getSecret throws an unexpected error (code !== 5)", () => {
        beforeEach(() => {
            const err = new Error("Internal GCP error");
            err.code = 13; // INTERNAL
            secretClient.getSecret.mockRejectedValue(err);
        });

        it("throws a wrapped error with 'Failed to store secret'", async () => {
            await expect(
                storeSecret("tenant1", "clientId", "value")
            ).rejects.toThrow("Failed to store secret: Internal GCP error");
        });

        it("logs the error", async () => {
            await storeSecret("tenant1", "clientId", "value").catch(() => {});
            expect(logger.error).toHaveBeenCalledWith(
                expect.stringContaining("Failed to store secret"),
                expect.any(String)
            );
        });
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// getSecret
// ─────────────────────────────────────────────────────────────────────────────
describe("getSecret", () => {
    let secretClient, logger, getSecret;

    beforeEach(() => {
        secretClient = makeSecretClient();
        logger = makeLogger();
        ({ getSecret } = createSecretManager({ secretClient, PROJECT_ID, logger }));
    });

    // ── Branch 1: Success ────────────────────────────────────────────────────
    it("returns the decoded secret value on success", async () => {
        secretClient.accessSecretVersion.mockResolvedValue([
            encodePayload("my-secret-token"),
        ]);

        const result = await getSecret("tenant1", "refreshToken");
        expect(result).toBe("my-secret-token");
    });

    it("calls accessSecretVersion with the correct resource name", async () => {
        secretClient.accessSecretVersion.mockResolvedValue([
            encodePayload("val"),
        ]);

        await getSecret("tenant1", "refreshToken");

        expect(secretClient.accessSecretVersion).toHaveBeenCalledWith({
            name: "projects/test-project/secrets/qb-tenant1-refreshToken/versions/latest",
        });
    });

    // ── Branch 2: Throws code 5 (NOT_FOUND) ──────────────────────────────────
    it("throws 'Secret not found' when error code is 5", async () => {
        const err = new Error("not found");
        err.code = 5;
        secretClient.accessSecretVersion.mockRejectedValue(err);

        await expect(getSecret("tenant1", "clientId")).rejects.toThrow(
            "Secret not found: qb-tenant1-clientId. Please register the tenant first."
        );
    });

    it("does NOT call logger.error when error code is 5", async () => {
        const err = new Error("not found");
        err.code = 5;
        secretClient.accessSecretVersion.mockRejectedValue(err);

        await getSecret("tenant1", "clientId").catch(() => {});
        expect(logger.error).not.toHaveBeenCalled();
    });

    // ── Branch 3: Throws a different error ───────────────────────────────────
    it("throws 'Failed to retrieve secret' for non-5 errors", async () => {
        const err = new Error("network timeout");
        err.code = 14; // UNAVAILABLE
        secretClient.accessSecretVersion.mockRejectedValue(err);

        await expect(getSecret("tenant1", "clientId")).rejects.toThrow(
            "Failed to retrieve secret: network timeout"
        );
    });

    it("logs the error for non-5 errors", async () => {
        const err = new Error("network timeout");
        err.code = 14;
        secretClient.accessSecretVersion.mockRejectedValue(err);

        await getSecret("tenant1", "clientId").catch(() => {});
        expect(logger.error).toHaveBeenCalledWith(
            expect.stringContaining("Failed to retrieve secret"),
            expect.any(String)
        );
    });
});
