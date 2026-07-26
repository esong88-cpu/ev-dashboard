/**
 * Regression tests for Firebase RTDB array coercion in the dashboard.
 * Run: node --test test_dashboard_rtdb.mjs
 */

import test from "node:test";
import assert from "node:assert/strict";

/** Mirrors index.html recordMap. */
function recordMap(obj) {
    if (!obj || typeof obj !== "object") return {};
    if (Array.isArray(obj)) {
        return obj.reduce((acc, v, i) => {
            if (v !== null && v !== undefined) acc[String(i)] = v;
            return acc;
        }, {});
    }
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
        if (v !== null && v !== undefined) out[String(k)] = v;
    }
    return out;
}

const META_KEYS = new Set(["last_updated", "charging_limit_minutes"]);
const STALL_COUNT = 4;

/** Minimal mirror of index.html flattenPortsStable + stallSlotsFromRows. */
function stallKeysFromStations(stations) {
    const rows = [];
    for (const [id, entry] of Object.entries(stations || {})) {
        if (META_KEYS.has(id)) continue;
        if (!entry || typeof entry !== "object") continue;
        if (entry.error) continue;
        const ports = recordMap(entry.ports);
        const keys = Object.keys(ports).sort((a, b) => Number(a) - Number(b));
        for (const pk of keys) {
            rows.push({ key: `${id}-${pk}`, deviceId: id, status: ports[pk] });
        }
    }
    rows.sort((a, b) => {
        const da = parseInt(String(a.deviceId), 10) || 0;
        const db = parseInt(String(b.deviceId), 10) || 0;
        if (da !== db) return da - db;
        return (
            (parseInt(String(a.key).split("-").pop(), 10) || 0) -
            (parseInt(String(b.key).split("-").pop(), 10) || 0)
        );
    });
    return rows.slice(0, STALL_COUNT).map((r) => r.key);
}

test("recordMap drops null holes from RTDB arrays", () => {
    assert.deepEqual(recordMap([null, "Charging", "Available"]), {
        "1": "Charging",
        "2": "Available",
    });
});

test("recordMap preserves dict-shaped ports", () => {
    assert.deepEqual(recordMap({ "1": "Charging", "2": "Available" }), {
        "1": "Charging",
        "2": "Available",
    });
});

test("RTDB array-coerced ports do not hide real stalls behind ghost index 0", () => {
    // Production shape: two dual-port stations. RTDB returns ports as arrays
    // with a null at index 0 when keys are "1" and "2".
    const stations = {
        last_updated: "2026-07-17T08:24:26.789527+00:00",
        charging_limit_minutes: 120,
        99037: {
            device_id: 99037,
            ports: [null, "Available", "Available"],
            port_sessions: [null, { started_at: "2026-07-17T07:00:00+00:00" }],
        },
        1958701: {
            device_id: 1958701,
            ports: [null, "Charging", "Available"],
            port_sessions: [null, { started_at: "2026-07-17T06:00:00+00:00" }],
        },
    };

    const stallKeys = stallKeysFromStations(stations);

    assert.deepEqual(stallKeys, ["99037-1", "99037-2", "1958701-1", "1958701-2"]);
    assert.ok(!stallKeys.some((k) => k.endsWith("-0")), "ghost port 0 must not appear");
});

test("pre-fix typeof-object check would have surfaced ghost stalls", () => {
    // Documents the broken legacy behavior for confidence in the regression.
    const ports = [null, "Available", "Available"];
    const legacyKeys = Object.keys(ports).sort((a, b) => Number(a) - Number(b));
    assert.deepEqual(legacyKeys, ["0", "1", "2"]);

    const fixedKeys = Object.keys(recordMap(ports)).sort((a, b) => Number(a) - Number(b));
    assert.deepEqual(fixedKeys, ["1", "2"]);
});
