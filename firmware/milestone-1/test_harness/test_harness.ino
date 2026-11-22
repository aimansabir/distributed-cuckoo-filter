
#include <Arduino.h>

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 - Cuckoo Filter Implementation
// Copied from slave_node.ino so this sketch is fully self-contained.
// ─────────────────────────────────────────────────────────────────────────────

#define BUCKET_COUNT     256
#define SLOTS_PER_BUCKET 4
#define MAX_KICKS        500

uint16_t filter_buckets[BUCKET_COUNT][SLOTS_PER_BUCKET] = {0};

// Reset the filter back to empty
void resetFilter() {
    memset(filter_buckets, 0, sizeof(filter_buckets));
}

// FNV-1a fingerprint - same as master_node.ino computeFingerprint()
uint16_t computeFingerprint(const char *key) {
    uint32_t hash = 2166136261UL;
    while (*key) {
        hash ^= (uint8_t)(*key++);
        hash *= 16777619UL;
    }
    uint16_t fp = (uint16_t)((hash >> 16) ^ (hash & 0xFFFF));
    return fp == 0 ? 1 : fp;
}

// Primary bucket - same as master_node.ino computePrimaryBucket()
uint16_t computePrimaryBucket(const char *key) {
    uint32_t hash = 0;
    while (*key) {
        hash = hash * 31 + (uint8_t)(*key++);
    }
    return (uint16_t)(hash % BUCKET_COUNT);
}

// Alternate bucket - same as slave_node.ino getAltBucket()
uint16_t getAltBucket(uint16_t current_bucket, uint16_t fp) {
    uint32_t hash = fp * 0x5bd1e995;
    hash ^= hash >> 15;
    uint16_t offset = (uint16_t)(hash % BUCKET_COUNT);
    return (current_bucket ^ offset) % BUCKET_COUNT;
}

// Insert - same logic as slave_node.ino handleInsert()
bool filterInsert(uint16_t fingerprint, uint16_t bucket_index) {
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        if (filter_buckets[bucket_index][i] == 0) {
            filter_buckets[bucket_index][i] = fingerprint;
            return true;
        }
    }
    uint16_t alt_bucket = getAltBucket(bucket_index, fingerprint);
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        if (filter_buckets[alt_bucket][i] == 0) {
            filter_buckets[alt_bucket][i] = fingerprint;
            return true;
        }
    }
    uint16_t curr_bucket = (random(0, 2) == 0) ? bucket_index : alt_bucket;
    uint16_t curr_fp = fingerprint;
    for (int n = 0; n < MAX_KICKS; n++) {
        int slot = random(0, SLOTS_PER_BUCKET);
        uint16_t evicted_fp = filter_buckets[curr_bucket][slot];
        filter_buckets[curr_bucket][slot] = curr_fp;
        curr_fp = evicted_fp;
        curr_bucket = getAltBucket(curr_bucket, curr_fp);
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            if (filter_buckets[curr_bucket][i] == 0) {
                filter_buckets[curr_bucket][i] = curr_fp;
                return true;
            }
        }
    }
    return false;
}

// Delete - same logic as slave_node.ino handleDelete()
bool filterDelete(uint16_t fingerprint, uint16_t bucket_index) {
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        if (filter_buckets[bucket_index][i] == fingerprint) {
            filter_buckets[bucket_index][i] = 0;
            return true;
        }
    }
    uint16_t alt_bucket = getAltBucket(bucket_index, fingerprint);
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        if (filter_buckets[alt_bucket][i] == fingerprint) {
            filter_buckets[alt_bucket][i] = 0;
            return true;
        }
    }
    return false;
}

// Lookup - same logic as slave_node.ino handleLookup()
bool filterLookup(uint16_t fingerprint, uint16_t bucket_index) {
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        if (filter_buckets[bucket_index][i] == fingerprint)
            return true;
    }
    uint16_t alt_bucket = getAltBucket(bucket_index, fingerprint);
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        if (filter_buckets[alt_bucket][i] == fingerprint)
            return true;
    }
    return false;
}

// Convenience wrappers that take a string key directly
bool insertKey(const char *key) {
    return filterInsert(computeFingerprint(key), computePrimaryBucket(key));
}

bool deleteKey(const char *key) {
    return filterDelete(computeFingerprint(key), computePrimaryBucket(key));
}

bool lookupKey(const char *key) {
    return filterLookup(computeFingerprint(key), computePrimaryBucket(key));
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 - Test 1: Basic Correctness
// Insert 10 known keys, verify all are found.
// ─────────────────────────────────────────────────────────────────────────────

void testBasicCorrectness() {
    Serial.println("\n========================================");
    Serial.println("  TEST 1: Basic Correctness");
    Serial.println("========================================");
    resetFilter();

    const char *keys[] = {
        "sensor_temp_01", "sensor_temp_02", "sensor_temp_03",
        "device_alpha",   "device_beta",    "device_gamma",
        "user_001",       "user_002",       "user_003", "user_004"
    };
    int numKeys = 10;
    int insertPass = 0, lookupPass = 0, lookupFail = 0;

    // Insert all keys
    Serial.println("[TEST1] Inserting 10 keys...");
    for (int i = 0; i < numKeys; i++) {
        bool ok = insertKey(keys[i]);
        if (ok) insertPass++;
        Serial.printf("[TEST1]   INSERT \"%s\" -> %s\n", keys[i], ok ? "OK" : "FAILED");
    }

    // Look up all keys - all should be found
    Serial.println("[TEST1] Looking up all inserted keys...");
    for (int i = 0; i < numKeys; i++) {
        bool found = lookupKey(keys[i]);
        if (found) lookupPass++;
        else        lookupFail++;
        Serial.printf("[TEST1]   LOOKUP \"%s\" -> %s\n",
                      keys[i], found ? "FOUND (correct)" : "NOT FOUND (error!)");
    }

    Serial.println("[TEST1] --- RESULT ---");
    Serial.printf("[TEST1]   Inserts succeeded : %d / %d\n", insertPass, numKeys);
    Serial.printf("[TEST1]   Lookups correct   : %d / %d\n", lookupPass, numKeys);
    if (lookupFail == 0)
        Serial.println("[TEST1]   STATUS: PASSED");
    else
        Serial.printf("[TEST1]   STATUS: FAILED (%d keys not found after insert)\n",
                      lookupFail);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3 - Test 2: Delete Correctness
// Insert keys, delete them, verify they are gone.
// ─────────────────────────────────────────────────────────────────────────────

void testDeleteCorrectness() {
    Serial.println("\n========================================");
    Serial.println("  TEST 2: Delete Correctness");
    Serial.println("========================================");
    resetFilter();

    const char *keys[] = {"alpha", "beta", "gamma", "delta", "epsilon"};
    int numKeys = 5;
    int deletePass = 0, gonePass = 0, goneFail = 0;

    // Insert all
    for (int i = 0; i < numKeys; i++) insertKey(keys[i]);
    Serial.println("[TEST2] Inserted 5 keys. Now deleting...");

    // Delete all
    for (int i = 0; i < numKeys; i++) {
        bool ok = deleteKey(keys[i]);
        if (ok) deletePass++;
        Serial.printf("[TEST2]   DELETE \"%s\" -> %s\n", keys[i], ok ? "OK" : "NOT FOUND");
    }

    // Look up after delete - none should be found
    Serial.println("[TEST2] Looking up deleted keys (should all be NOT FOUND)...");
    for (int i = 0; i < numKeys; i++) {
        bool found = lookupKey(keys[i]);
        if (!found) gonePass++;
        else        goneFail++;
        Serial.printf("[TEST2]   LOOKUP \"%s\" -> %s\n",
                      keys[i], found ? "FOUND (error!)" : "NOT FOUND (correct)");
    }

    Serial.println("[TEST2] --- RESULT ---");
    Serial.printf("[TEST2]   Deletes succeeded    : %d / %d\n", deletePass, numKeys);
    Serial.printf("[TEST2]   Correctly gone after : %d / %d\n", gonePass, numKeys);
    if (goneFail == 0)
        Serial.println("[TEST2]   STATUS: PASSED");
    else
        Serial.printf("[TEST2]   STATUS: FAILED (%d keys still found after delete)\n",
                      goneFail);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 - Test 3: False Positive Rate
// Insert 200 keys, then query 1000 completely different keys.
// Any "found" result for a never-inserted key = false positive.
// ─────────────────────────────────────────────────────────────────────────────

void testFalsePositiveRate() {
    Serial.println("\n========================================");
    Serial.println("  TEST 3: False Positive Rate");
    Serial.println("========================================");
    resetFilter();

    // Insert 200 keys with prefix "inserted_key_"
    Serial.println("[TEST3] Inserting 200 keys...");
    char key[32];
    int inserted = 0;
    for (int i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "inserted_key_%d", i);
        if (insertKey(key)) inserted++;
    }
    Serial.printf("[TEST3] Successfully inserted: %d / 200\n", inserted);

    // Query 1000 keys with prefix "never_inserted_" - none were ever inserted
    Serial.println("[TEST3] Querying 1000 keys that were NEVER inserted...");
    int falsePositives = 0;
    int totalQueries   = 1000;
    for (int i = 0; i < totalQueries; i++) {
        snprintf(key, sizeof(key), "never_inserted_%d", i);
        if (lookupKey(key)) falsePositives++;
    }

    float fpRate = (float)falsePositives / (float)totalQueries * 100.0f;

    Serial.println("[TEST3] --- RESULT ---");
    Serial.printf("[TEST3]   Keys inserted       : %d\n",   inserted);
    Serial.printf("[TEST3]   Queries made        : %d\n",   totalQueries);
    Serial.printf("[TEST3]   False positives     : %d\n",   falsePositives);
    Serial.printf("[TEST3]   False positive rate : %.2f%%\n", fpRate);

    // Cuckoo filters typically achieve <3% FP rate
    if (fpRate < 3.0f)
        Serial.println("[TEST3]   STATUS: PASSED (FP rate within acceptable range < 3%)");
    else
        Serial.printf("[TEST3]   STATUS: WARNING (FP rate %.2f%% exceeds 3%% threshold)\n",
                      fpRate);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 - Test 4: Capacity Test
// Keep inserting until the filter is full. Report max capacity.
// ─────────────────────────────────────────────────────────────────────────────

void testCapacity() {
    Serial.println("\n========================================");
    Serial.println("  TEST 4: Capacity Test");
    Serial.println("========================================");
    resetFilter();

    char key[32];
    int count = 0;
    int maxPossible = BUCKET_COUNT * SLOTS_PER_BUCKET;  // 256 * 4 = 1024

    Serial.println("[TEST4] Inserting until filter is full...");
    for (int i = 0; i < 10000; i++) {
        snprintf(key, sizeof(key), "cap_test_key_%d", i);
        if (insertKey(key))
            count++;
        else
            break;
    }

    float loadFactor = (float)count / (float)maxPossible * 100.0f;

    Serial.println("[TEST4] --- RESULT ---");
    Serial.printf("[TEST4]   Max slots available : %d\n",    maxPossible);
    Serial.printf("[TEST4]   Keys inserted       : %d\n",    count);
    Serial.printf("[TEST4]   Load factor         : %.1f%%\n", loadFactor);

    if (loadFactor >= 90.0f)
        Serial.println("[TEST4]   STATUS: PASSED (load factor >= 90%)");
    else
        Serial.printf("[TEST4]   STATUS: NOTE (load factor %.1f%% - "
                      "may indicate kick-out inefficiency)\n", loadFactor);
}


void setup() {
    Serial.begin(115200);
    delay(1000);

    Serial.println("\n========================================");
    Serial.println("  Cuckoo Filter Test Harness ");
    Serial.println("  Distributed Cuckoo Filter M1       ");
    Serial.println("========================================");
    Serial.printf("  Filter size: %d buckets x %d slots = %d total slots\n",
                  BUCKET_COUNT, SLOTS_PER_BUCKET, BUCKET_COUNT * SLOTS_PER_BUCKET);

    testBasicCorrectness();
    testDeleteCorrectness();
    testFalsePositiveRate();
    testCapacity();

    Serial.println("\n========================================");
    Serial.println("  All tests complete.");
    Serial.println("========================================");
}

void loop() {
    delay(10000);
}
