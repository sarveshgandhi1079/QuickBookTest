/**
 * CHUNK 3 — Tenant Configuration
 * File under test: functions/tenantConfig.js
 *
 * Mocks: Firestore (qbDb), SecretManagerServiceClient, storeSecret, getSecret, utcNowISO
 * Goal: 100% line, branch, function, and statement coverage for tenantConfig.js
 */

const { createTenantConfig } = require("../tenantConfig");

// ─────────────────────────────────────────────────────────────────────────────
// Mock Factories
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Build a fake Firestore doc snapshot.
 * @param {boolean} exists
 * @param {object}  data
 */
function makeDocSnap(exists, data = {}) {
    return { exists, data: () => data, id: "tenant1" };
}

/**
 * Build a fake Firestore qbDb that supports:
 *   qbDb.collection("qb_tenants").doc(id).get()
 *   qbDb.collection("qb_tenants").doc(id).update({})
 */
function makeQbDb({ snap, updateFn } = {}) {
    const docMock = {
        get: jest.fn().mockResolvedValue(snap ?? makeDocSnap(false)),
        update: updateFn ?? jest.fn().mockResolvedValue({}),
    };
    const collectionMock = { doc: jest.fn().mockReturnValue(docMock) };
    return {
        collection: jest.fn().mockReturnValue(collectionMock),
        _docMock: docMock,         // exposed for assertions
        _collectionMock: collectionMock,
    };
}

/** Fake secretClient */
function makeSecretClient() {
    return { accessSecretVersion: jest.fn() };
}

/** Fake logger */
function makeLogger() {
    return { info: jest.fn(), warn: jest.fn(), error: jest.fn() };
}

/** Fixed timestamp so assertions don't depend on real time */
const FIXED_NOW = "2026-02-26T10:00:00.000Z";
const utcNowISO = jest.fn().mockReturnValue(FIXED_NOW);

const PROJECT_ID = "test-project";

/** Helper to encode a Buffer payload as Secret Manager does */
function encodePayload(value) {
    return { payload: { data: Buffer.from(value, "utf8") } };
}

// ─────────────────────────────────────────────────────────────────────────────
// getTenantConfig
// ─────────────────────────────────────────────────────────────────────────────
describe("getTenantConfig", () => {
    let qbDb, secretClient, storeSecret, getSecret, logger, getTenantConfig;

    beforeEach(() => {
        secretClient = makeSecretClient();
        storeSecret = jest.fn().mockResolvedValue(undefined);
        getSecret = jest.fn();
        logger = makeLogger();
    });

    function build(snap) {
        qbDb = makeQbDb({ snap });
        ({ getTenantConfig } = createTenantConfig({
            qbDb, secretClient, storeSecret, getSecret, utcNowISO, PROJECT_ID, logger,
        }));
    }

    // ── Branch 1: tenantId is falsy ──────────────────────────────────────────
    it("throws 'tenantId is required' when tenantId is undefined", async () => {
        build(makeDocSnap(false));
        await expect(getTenantConfig(undefined)).rejects.toThrow("tenantId is required");
    });

    it("throws 'tenantId is required' when tenantId is empty string", async () => {
        build(makeDocSnap(false));
        await expect(getTenantConfig("")).rejects.toThrow("tenantId is required");
    });

    it("throws 'tenantId is required' when tenantId is null", async () => {
        build(makeDocSnap(false));
        await expect(getTenantConfig(null)).rejects.toThrow("tenantId is required");
    });

    // ── Branch 2: Tenant document does not exist ─────────────────────────────
    it("throws 'not found' when the tenant doc does not exist", async () => {
        build(makeDocSnap(false));
        await expect(getTenantConfig("tenant1")).rejects.toThrow(
            "Tenant tenant1 not found. Please register this QuickBooks company first."
        );
    });

    it("does not call getSecret when tenant doc does not exist", async () => {
        build(makeDocSnap(false));
        await getTenantConfig("tenant1").catch(() => {});
        expect(getSecret).not.toHaveBeenCalled();
    });

    // ── Branch 3: Tenant exists but isActive === false ───────────────────────
    it("throws 'is inactive' when isActive is explicitly false", async () => {
        build(makeDocSnap(true, { isActive: false, realmId: "123" }));
        await expect(getTenantConfig("tenant1")).rejects.toThrow("Tenant tenant1 is inactive");
    });

    it("does not call getSecret when tenant is inactive", async () => {
        build(makeDocSnap(true, { isActive: false }));
        await getTenantConfig("tenant1").catch(() => {});
        expect(getSecret).not.toHaveBeenCalled();
    });

    // ── Branch 4: Tenant active — uses DEFAULT optional fields ───────────────
    describe("when tenant exists and is active (defaults)", () => {
        const tenantData = {
            isActive: true,
            realmId: "realm-abc",
            // baseUrl, minorVersion, environment, metadata intentionally omitted
        };

        beforeEach(() => {
            build(makeDocSnap(true, tenantData));
            getSecret
                .mockResolvedValueOnce("clientId-val")      // clientId
                .mockResolvedValueOnce("clientSecret-val")  // clientSecret
                .mockResolvedValueOnce("refreshToken-val")  // refreshToken
                .mockResolvedValueOnce("webhookVerifier-val"); // webhookVerifier
        });

        it("returns the correct config shape with all secret values", async () => {
            const result = await getTenantConfig("tenant1");

            expect(result.clientId).toBe("clientId-val");
            expect(result.clientSecret).toBe("clientSecret-val");
            expect(result.refreshToken).toBe("refreshToken-val");
            expect(result.webhookVerifier).toBe("webhookVerifier-val");
            expect(result.realmId).toBe("realm-abc");
        });

        it("uses default baseUrl when not set in Firestore", async () => {
            const result = await getTenantConfig("tenant1");
            expect(result.baseUrl).toBe("https://quickbooks.api.intuit.com/v3/company");
        });

        it("uses default minorVersion '65' when not set in Firestore", async () => {
            const result = await getTenantConfig("tenant1");
            expect(result.minorVersion).toBe("65");
        });

        it("uses default environment 'production' when not set in Firestore", async () => {
            const result = await getTenantConfig("tenant1");
            expect(result.environment).toBe("production");
        });

        it("uses default empty object for metadata when not set in Firestore", async () => {
            const result = await getTenantConfig("tenant1");
            expect(result.metadata).toEqual({});
        });

        it("sets isActive to true when isActive is not false", async () => {
            const result = await getTenantConfig("tenant1");
            expect(result.isActive).toBe(true);
        });

        it("calls getSecret 4 times (clientId, clientSecret, refreshToken, webhookVerifier)", async () => {
            await getTenantConfig("tenant1");
            expect(getSecret).toHaveBeenCalledTimes(4);
            expect(getSecret).toHaveBeenCalledWith("tenant1", "clientId");
            expect(getSecret).toHaveBeenCalledWith("tenant1", "clientSecret");
            expect(getSecret).toHaveBeenCalledWith("tenant1", "refreshToken");
            expect(getSecret).toHaveBeenCalledWith("tenant1", "webhookVerifier");
        });
    });

    // ── Branch 5: Tenant active — CUSTOM optional fields provided ────────────
    describe("when tenant has custom optional fields", () => {
        const tenantData = {
            isActive: true,
            realmId: "realm-xyz",
            baseUrl: "https://sandbox.api.intuit.com/v3/company",
            minorVersion: "70",
            environment: "sandbox",
            metadata: { plan: "premium" },
        };

        beforeEach(() => {
            build(makeDocSnap(true, tenantData));
            getSecret.mockResolvedValue("some-val");
        });

        it("uses custom baseUrl from Firestore", async () => {
            const result = await getTenantConfig("tenant1");
            expect(result.baseUrl).toBe("https://sandbox.api.intuit.com/v3/company");
        });

        it("uses custom minorVersion from Firestore", async () => {
            const result = await getTenantConfig("tenant1");
            expect(result.minorVersion).toBe("70");
        });

        it("uses custom environment from Firestore", async () => {
            const result = await getTenantConfig("tenant1");
            expect(result.environment).toBe("sandbox");
        });

        it("uses custom metadata from Firestore", async () => {
            const result = await getTenantConfig("tenant1");
            expect(result.metadata).toEqual({ plan: "premium" });
        });
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// updateTenantRefreshToken
// ─────────────────────────────────────────────────────────────────────────────
describe("updateTenantRefreshToken", () => {
    let qbDb, secretClient, storeSecret, logger, updateTenantRefreshToken;

    function build() {
        qbDb = makeQbDb({ snap: makeDocSnap(true, {}) });
        secretClient = makeSecretClient();
        storeSecret = jest.fn().mockResolvedValue(undefined);
        logger = makeLogger();

        ({ updateTenantRefreshToken } = createTenantConfig({
            qbDb,
            secretClient,
            storeSecret,
            getSecret: jest.fn(),
            utcNowISO,
            PROJECT_ID,
            logger,
        }));
    }

    beforeEach(build);

    // ── Branch 1: Token is UNCHANGED ─────────────────────────────────────────
    describe("when the token is unchanged", () => {
        beforeEach(() => {
            secretClient.accessSecretVersion.mockResolvedValue([
                encodePayload("same-token"),
            ]);
        });

        it("does NOT call storeSecret", async () => {
            await updateTenantRefreshToken("tenant1", "same-token");
            expect(storeSecret).not.toHaveBeenCalled();
        });

        it("does NOT call qbDb.update", async () => {
            await updateTenantRefreshToken("tenant1", "same-token");
            expect(qbDb._docMock.update).not.toHaveBeenCalled();
        });

        it("logs an 'unchanged' message", async () => {
            await updateTenantRefreshToken("tenant1", "same-token");
            expect(logger.info).toHaveBeenCalledWith(
                expect.stringContaining("unchanged")
            );
        });
    });

    // ── Branch 2: Token has CHANGED ──────────────────────────────────────────
    describe("when the token has changed", () => {
        beforeEach(() => {
            secretClient.accessSecretVersion.mockResolvedValue([
                encodePayload("old-token"),
            ]);
        });

        it("calls storeSecret with the new token", async () => {
            await updateTenantRefreshToken("tenant1", "new-token");
            expect(storeSecret).toHaveBeenCalledWith("tenant1", "refreshToken", "new-token");
        });

        it("updates Firestore with lastTokenRefresh and updatedAt", async () => {
            await updateTenantRefreshToken("tenant1", "new-token");
            expect(qbDb._docMock.update).toHaveBeenCalledWith({
                lastTokenRefresh: FIXED_NOW,
                updatedAt: FIXED_NOW,
            });
        });

        it("logs a 'rotated successfully' message", async () => {
            await updateTenantRefreshToken("tenant1", "new-token");
            expect(logger.info).toHaveBeenCalledWith(
                expect.stringContaining("Refresh token rotated successfully")
            );
        });
    });

    // ── Branch 3: accessSecretVersion throws code 5 (token never stored) ────
    describe("when accessSecretVersion throws code 5 (secret not found)", () => {
        beforeEach(() => {
            const err = new Error("not found");
            err.code = 5;
            secretClient.accessSecretVersion.mockRejectedValue(err);
        });

        it("still calls storeSecret with the new token", async () => {
            await updateTenantRefreshToken("tenant1", "brand-new-token");
            expect(storeSecret).toHaveBeenCalledWith("tenant1", "refreshToken", "brand-new-token");
        });

        it("still updates Firestore", async () => {
            await updateTenantRefreshToken("tenant1", "brand-new-token");
            expect(qbDb._docMock.update).toHaveBeenCalledWith({
                lastTokenRefresh: FIXED_NOW,
                updatedAt: FIXED_NOW,
            });
        });

        it("does NOT throw", async () => {
            await expect(
                updateTenantRefreshToken("tenant1", "brand-new-token")
            ).resolves.toBeUndefined();
        });

        it("logs a 'rotated successfully' message", async () => {
            await updateTenantRefreshToken("tenant1", "brand-new-token");
            expect(logger.info).toHaveBeenCalledWith(
                expect.stringContaining("Refresh token rotated successfully")
            );
        });
    });

    // ── Branch 4: accessSecretVersion throws a non-5 error ──────────────────
    describe("when accessSecretVersion throws a non-5 error", () => {
        beforeEach(() => {
            const err = new Error("network failure");
            err.code = 14;
            secretClient.accessSecretVersion.mockRejectedValue(err);
        });

        it("re-throws the original error", async () => {
            await expect(
                updateTenantRefreshToken("tenant1", "any-token")
            ).rejects.toThrow("network failure");
        });

        it("does NOT call storeSecret", async () => {
            await updateTenantRefreshToken("tenant1", "any-token").catch(() => {});
            expect(storeSecret).not.toHaveBeenCalled();
        });

        it("does NOT update Firestore", async () => {
            await updateTenantRefreshToken("tenant1", "any-token").catch(() => {});
            expect(qbDb._docMock.update).not.toHaveBeenCalled();
        });
    });
});
