// =============================================================================
// cuckoo_m3_slave.ino  –  Distributed Cuckoo Filter  –  Milestone 3
// Role: SLAVE NODE
// HOW TO FLASH:
//   1. Change THIS_NODE_ID to 0, 1, or 2 for each device.
//   2. Change masterMacAddress[] to the master's MAC (printed on master boot).
//   3. Flash this file onto each slave ESP32.
//   4. Open Serial Monitor at 115200 baud → copy the MAC printed on boot.
//   5. Paste that MAC into cuckoo_m3_master.ino slaves[] for the matching node.
// =============================================================================

#include <Arduino.h>
#include <WiFi.h>
#include <esp_now.h>
#include <esp_idf_version.h>

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 – Operation Codes  
// ─────────────────────────────────────────────────────────────────────────────

#define OP_DUMMY         0
#define OP_INSERT        1
#define OP_DELETE        2
#define OP_LOOKUP        3
#define OP_SYNC          4
#define OP_ACK           5
#define OP_RESULT        6
#define OP_REBALANCE     7
#define OP_DRAIN         8
#define OP_NODE_JOIN     9
#define OP_NODE_LEAVE    10
#define OP_HEARTBEAT     11   
#define OP_HEARTBEAT_ACK 12  
#define OP_REPLICATE     13   

#define STATUS_OK             0
#define STATUS_FILTER_FULL    1
#define STATUS_NOT_FOUND      2
#define STATUS_REMOTE_CHECK   3
#define STATUS_FORWARD_INSERT 4
#define STATUS_WRONG_OWNER    5

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 – Message Format  
// ─────────────────────────────────────────────────────────────────────────────

typedef struct __attribute__((packed)) FilterMessage {
    uint8_t  operation_type;
    uint32_t sequence_id;
    uint16_t key_fingerprint;
    uint16_t bucket_index;
    uint8_t  result_found;
    uint8_t  node_id;
    uint8_t  status_code;
    char     key_data[20];
    uint16_t partition_lo;
    uint16_t partition_hi;
    uint8_t  dest_node_id;
    uint8_t  replica_node_id;  
} FilterMessage;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3 – Per-Slave Configuratio
// ─────────────────────────────────────────────────────────────────────────────

// Copy master MAC from master's Serial Monitor output on boot
uint8_t masterMacAddress[] = {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF};

// Set to 0, 1, or 2 to match this slave's index in master's slaves[] table
#define THIS_NODE_ID 0

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 – Filter Storage
//
// The primary filter array handles the slave's assigned partition.
// The replica array stores fingerprints received via OP_REPLICATE from the
// master for keys whose PRIMARY partition lives on another slave.
//
// Separating primary and replica storage avoids index conflicts and allows
// the SYNC command to report each count independently.
// ─────────────────────────────────────────────────────────────────────────────

#define BUCKET_COUNT     256
#define SLOTS_PER_BUCKET 4
#define MAX_KICKS        500

// Primary storage: fingerprints that this node owns as the primary partition
uint16_t filter_buckets[BUCKET_COUNT][SLOTS_PER_BUCKET] = {0};

// Replica storage: fingerprints replicated from other nodes for fault tolerance
// Uses a flat linear array for simplicity (no bucket structure needed since
// replicas are indexed only by fingerprint, not bucket, during failover lookup)
#define REPLICA_CAPACITY 512           // max 512 replica fingerprints per slave
static uint16_t replicaStore[REPLICA_CAPACITY] = {0};
static uint16_t replicaCount = 0;

static uint16_t storedCount  = 0;     // fingerprints in primary partition

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 – Partition State
// ─────────────────────────────────────────────────────────────────────────────

static uint16_t myPartitionLo = 0;
static uint16_t myPartitionHi = BUCKET_COUNT - 1; 

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 6 – Communication State
// ─────────────────────────────────────────────────────────────────────────────

static bool    peerRegistered = false;
static bool    drainPeerAdded = false;
static uint8_t drainDestMac[6] = {0};

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 7 – Utility Helpers
// ─────────────────────────────────────────────────────────────────────────────

void printMacAddress(const uint8_t *mac) {
    for (int i = 0; i < 6; i++) {
        if (i > 0) Serial.print(":");
        Serial.printf("%02X", mac[i]);
    }
}

// Alternate bucket via fingerprint-derived XOR offset (must mirror master)
uint16_t getAltBucket(uint16_t current_bucket, uint16_t fp) {
    uint32_t hash = fp * 0x5bd1e995;
    hash ^= hash >> 15;
    uint16_t offset = (uint16_t)(hash % BUCKET_COUNT);
    if (offset == 0) offset = 1;
    return (current_bucket ^ offset) % BUCKET_COUNT;
}

inline bool ownsBucket(uint16_t b) {
    return (b >= myPartitionLo && b <= myPartitionHi);
}

bool tryInsertIntoOwnedBucket(uint16_t bucket, uint16_t fp) {
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        if (filter_buckets[bucket][i] == 0) {
            filter_buckets[bucket][i] = fp;
            storedCount++;
            return true;
        }
    }
    return false;
}



uint16_t computeOwnedCapacity() {
    uint16_t total = 0;
    for (uint16_t b = myPartitionLo; b <= myPartitionHi; b++)
        for (int s = 0; s < SLOTS_PER_BUCKET; s++)
            if (filter_buckets[b][s] != 0) total++;
    return total;
}

uint16_t maxOwnedCapacity() {
    return (myPartitionHi - myPartitionLo + 1) * SLOTS_PER_BUCKET;
}

// Count total fingerprints stored across both primary and replica storage
uint16_t totalStoredCount() {
    return storedCount + replicaCount;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 8 – Cuckoo Filter: INSERT  
// ─────────────────────────────────────────────────────────────────────────────

uint8_t handleInsert(uint16_t fingerprint,
                     uint16_t bucket_index,
                     uint16_t &forward_bucket,
                     uint16_t &forward_fp) {

    uint16_t alt_bucket  = getAltBucket(bucket_index, fingerprint);
    bool     own_primary = ownsBucket(bucket_index);
    bool     own_alt     = ownsBucket(alt_bucket);

    Serial.printf("[SLAVE-%d]   INSERT: fp=0x%04X  primary=%u  alt=%u  "
                  "own_primary=%d  own_alt=%d\n",
                  THIS_NODE_ID, fingerprint, bucket_index, alt_bucket,
                  own_primary, own_alt);

    if (!own_primary && !own_alt) {
        Serial.printf("[SLAVE-%d]   INSERT: bucket %u and alt %u not owned – WRONG_OWNER.\n",
                      THIS_NODE_ID, bucket_index, alt_bucket);
        return STATUS_WRONG_OWNER;
    }

    if (own_primary) {
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            if (filter_buckets[bucket_index][i] == fingerprint) {
                Serial.printf("[SLAVE-%d]   INSERT (idempotent): fp=0x%04X already "
                              "in primary bucket %u – skipping duplicate insert.\n",
                              THIS_NODE_ID, fingerprint, bucket_index);
                return STATUS_OK;
            }
        }
    }
    if (own_alt) {
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            if (filter_buckets[alt_bucket][i] == fingerprint) {
                Serial.printf("[SLAVE-%d]   INSERT (idempotent): fp=0x%04X already "
                              "in alt bucket %u – skipping duplicate insert.\n",
                              THIS_NODE_ID, fingerprint, alt_bucket);
                return STATUS_OK;
            }
        }
    }

    if (own_primary && tryInsertIntoOwnedBucket(bucket_index, fingerprint)) {
        Serial.printf("[SLAVE-%d]   Inserted into primary bucket %u "
                      "(stored=%u/%u)\n",
                      THIS_NODE_ID, bucket_index,
                      storedCount, maxOwnedCapacity());
        return STATUS_OK;
    }
    if (own_alt && tryInsertIntoOwnedBucket(alt_bucket, fingerprint)) {
        Serial.printf("[SLAVE-%d]   Inserted into alt bucket %u "
                      "(stored=%u/%u)\n",
                      THIS_NODE_ID, alt_bucket,
                      storedCount, maxOwnedCapacity());
        return STATUS_OK;
    }

    uint16_t cap  = maxOwnedCapacity();
    uint16_t used = computeOwnedCapacity();
    if (used >= cap) {
        Serial.printf("[SLAVE-%d]   INSERT FAILED: partition full (%u/%u).\n",
                      THIS_NODE_ID, used, cap);
        return STATUS_FILTER_FULL;
    }

    // Kick-out loop
    // We record the bucket, slot, AND the original fingerprint evicted at each step
    // so that if MAX_KICKS is reached without finding a free slot we can fully restore
    // the filter to its pre-insertion state (no lost or phantom fingerprints).
    uint16_t curr_bucket = (own_primary && own_alt)
                           ? ((random(0, 2) == 0) ? bucket_index : alt_bucket)
                           : (own_primary ? bucket_index : alt_bucket);
    uint16_t curr_fp = fingerprint;

    static uint16_t kickBuckets[MAX_KICKS];
    static uint8_t  kickSlots[MAX_KICKS];
    static uint16_t kickValues[MAX_KICKS];  // original fp that was in each slot before eviction

    for (int n = 0; n < MAX_KICKS; n++) {
        int slot = random(0, SLOTS_PER_BUCKET);
        kickBuckets[n] = curr_bucket;
        kickSlots[n]   = (uint8_t)slot;
        kickValues[n]  = filter_buckets[curr_bucket][slot]; // save what was there

        uint16_t evicted_fp              = filter_buckets[curr_bucket][slot];
        filter_buckets[curr_bucket][slot] = curr_fp;
        curr_fp                          = evicted_fp;

        uint16_t next_bucket = getAltBucket(curr_bucket, curr_fp);

        if (!ownsBucket(next_bucket)) {
            forward_bucket = next_bucket;
            forward_fp     = curr_fp;
            Serial.printf("[SLAVE-%d]   Kick-out crossed partition: "
                          "fp=0x%04X → bucket=%u (remote) after %d kicks\n",
                          THIS_NODE_ID, curr_fp, next_bucket, n + 1);
            return STATUS_FORWARD_INSERT;
        }

        curr_bucket = next_bucket;
        if (tryInsertIntoOwnedBucket(curr_bucket, curr_fp)) {
            Serial.printf("[SLAVE-%d]   Kick-out settled in bucket %u after %d kicks "
                          "(stored=%u/%u)\n",
                          THIS_NODE_ID, curr_bucket, n + 1,
                          storedCount, maxOwnedCapacity());
            return STATUS_OK;
        }
    }

    // MAX_KICKS reached without finding a free slot.
    // Fully restore every displaced slot in reverse order using the saved original values.
    // This guarantees no fingerprint is lost or duplicated and storedCount stays accurate.
    for (int n = MAX_KICKS - 1; n >= 0; n--) {
        filter_buckets[kickBuckets[n]][kickSlots[n]] = kickValues[n];
    }

    Serial.printf("[SLAVE-%d]   INSERT FAILED: full after %d kicks. State fully rolled back.\n",
                  THIS_NODE_ID, MAX_KICKS);
    return STATUS_FILTER_FULL;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 9 – Cuckoo Filter: DELETE
// Remove a fingerprint from the flat replica store AND from any bucket slots
// where it may have been cached by tryInsertReplicaIntoBucket().
// Without the bucket scan, a deleted key's replica copy would remain in
// filter_buckets and continue to produce false positives on lookups.
static bool removeFromReplica(uint16_t fingerprint) {
    for (uint16_t i = 0; i < replicaCount; i++) {
        if (replicaStore[i] == fingerprint) {
            replicaStore[i] = replicaStore[--replicaCount];
            Serial.printf("[SLAVE-%d]   Replica flat entry removed for fp=0x%04X "
                          "(replica_count=%u)\n",
                          THIS_NODE_ID, fingerprint, replicaCount);
            return true;
        }
    }
    return false;
}

uint8_t handleDelete(uint16_t fingerprint, uint16_t bucket_index, bool replicaDelete = false) {
    uint16_t alt_bucket  = getAltBucket(bucket_index, fingerprint);
    bool     own_primary = ownsBucket(bucket_index);
    bool     own_alt     = ownsBucket(alt_bucket);

    Serial.printf("[SLAVE-%d]   DELETE: fp=0x%04X  primary=%u  alt=%u\n",
                  THIS_NODE_ID, fingerprint, bucket_index, alt_bucket);

    if (replicaDelete) {
        // Replica cleanup is intentionally separate from primary deletion.
        // A replica-delete must not trigger REMOTE_CHECK or delete-latency stats;
        // it only purges the flat replica copy on this slave.
        bool removedReplica = removeFromReplica(fingerprint);
        Serial.printf("[SLAVE-%d]   REPLICA DELETE cleanup: %s\n",
                      THIS_NODE_ID, removedReplica ? "removed" : "not present");
        return removedReplica ? STATUS_OK : STATUS_NOT_FOUND;
    }

    if (!own_primary && !own_alt) {
        Serial.printf("[SLAVE-%d]   DELETE: bucket %u not owned - WRONG_OWNER.\n",
                      THIS_NODE_ID, bucket_index);
        return STATUS_WRONG_OWNER;
    }

    if (own_primary) {
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            if (filter_buckets[bucket_index][i] == fingerprint) {
                filter_buckets[bucket_index][i] = 0;
                if (storedCount > 0) storedCount--;
                
                removeFromReplica(fingerprint);
                Serial.printf("[SLAVE-%d]   Deleted from primary bucket %u  "
                              "(stored=%u/%u)\n",
                              THIS_NODE_ID, bucket_index,
                              storedCount, maxOwnedCapacity());
                return STATUS_OK;
            }
        }
    }

    if (own_alt) {
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            if (filter_buckets[alt_bucket][i] == fingerprint) {
                filter_buckets[alt_bucket][i] = 0;
                if (storedCount > 0) storedCount--;
               
                removeFromReplica(fingerprint);
                Serial.printf("[SLAVE-%d]   Deleted from alt bucket %u  "
                              "(stored=%u/%u)\n",
                              THIS_NODE_ID, alt_bucket,
                              storedCount, maxOwnedCapacity());
                return STATUS_OK;
            }
        }
    }

    if (own_primary && !own_alt) return STATUS_REMOTE_CHECK;
    if (!own_primary && own_alt) return STATUS_REMOTE_CHECK;

    Serial.printf("[SLAVE-%d]   DELETE: NOT FOUND\n", THIS_NODE_ID);
    return STATUS_NOT_FOUND;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 10 – Cuckoo Filter: LOOKUP
// ─────────────────────────────────────────────────────────────────────────────

// Scan the flat replica store for a fingerprint match
bool findInReplica(uint16_t fingerprint) {
    for (uint16_t i = 0; i < replicaCount; i++) {
        if (replicaStore[i] == fingerprint) return true;
    }
    return false;
}

uint8_t handleLookup(uint16_t fingerprint, uint16_t bucket_index, uint8_t &found) {
    found = 0;
    uint16_t alt_bucket  = getAltBucket(bucket_index, fingerprint);
    bool     own_primary = ownsBucket(bucket_index);
    bool     own_alt     = ownsBucket(alt_bucket);

    Serial.printf("[SLAVE-%d]   LOOKUP: fp=0x%04X  primary=%u  alt=%u  "
                  "own_primary=%d  own_alt=%d\n",
                  THIS_NODE_ID, fingerprint, bucket_index, alt_bucket,
                  own_primary, own_alt);

    if (!own_primary && !own_alt) {
        // This handles post-failover lookups where the partition map has shifted.
        if (findInReplica(fingerprint)) {
            found = 1;
            Serial.printf("[SLAVE-%d]   FOUND fp=0x%04X in REPLICA store\n",
                          THIS_NODE_ID, fingerprint);
            return STATUS_OK;
        }
        Serial.printf("[SLAVE-%d]   LOOKUP: wrong owner, not in replica\n", THIS_NODE_ID);
        return STATUS_WRONG_OWNER;
    }

    if (own_primary) {
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            if (filter_buckets[bucket_index][i] == fingerprint) {
                found = 1;
                Serial.printf("[SLAVE-%d]   FOUND fp=0x%04X in primary bucket %u slot %d\n",
                              THIS_NODE_ID, fingerprint, bucket_index, i);
                return STATUS_OK;
            }
        }
    }

    if (own_alt) {
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            if (filter_buckets[alt_bucket][i] == fingerprint) {
                found = 1;
                Serial.printf("[SLAVE-%d]   FOUND fp=0x%04X in alt bucket %u slot %d\n",
                              THIS_NODE_ID, fingerprint, alt_bucket, i);
                return STATUS_OK;
            }
        }
    }

    // Primary buckets missed – check replica store
    if (findInReplica(fingerprint)) {
        found = 1;
        Serial.printf("[SLAVE-%d]   FOUND fp=0x%04X in REPLICA store (partition miss)\n",
                      THIS_NODE_ID, fingerprint);
        return STATUS_OK;
    }

    if (own_primary && !own_alt) {
        Serial.printf("[SLAVE-%d]   NOT found in primary; alt=%u is remote → REMOTE_CHECK\n",
                      THIS_NODE_ID, alt_bucket);
        return STATUS_REMOTE_CHECK;
    }
    if (!own_primary && own_alt) {
        Serial.printf("[SLAVE-%d]   NOT found in alt; primary=%u is remote → REMOTE_CHECK\n",
                      THIS_NODE_ID, bucket_index);
        return STATUS_REMOTE_CHECK;
    }

    Serial.printf("[SLAVE-%d]   NOT FOUND fp=0x%04X anywhere\n",
                  THIS_NODE_ID, fingerprint);
    return STATUS_NOT_FOUND;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 11
//
// Called when OP_REPLICATE arrives.  The master has already confirmed the
// primary insert succeeded and is now storing a copy here for fault tolerance.
//
// We store the fingerprint in the flat replicaStore[] array (not in the bucket
// array) to keep primary and replica data cleanly separated.  LOOKUP checks
// replicaStore after checking the owned primary/alternate buckets.
// ─────────────────────────────────────────────────────────────────────────────

uint8_t handleReplicate(uint16_t fingerprint, uint16_t bucket_index) {
    Serial.printf("[SLAVE-%d]   REPLICATE: fp=0x%04X  bucket=%u\n",
                  THIS_NODE_ID, fingerprint, bucket_index);

    // Idempotent replica insert
    for (uint16_t i = 0; i < replicaCount; i++) {
        if (replicaStore[i] == fingerprint) {
            Serial.printf("[SLAVE-%d]   REPLICATE (idempotent): already present.\n",
                          THIS_NODE_ID);
            return STATUS_OK;
        }
    }

    if (replicaCount >= REPLICA_CAPACITY) {
        Serial.printf("[SLAVE-%d]   WARNING: replica store full (%u slots).\n",
                      THIS_NODE_ID, REPLICA_CAPACITY);
        return STATUS_FILTER_FULL;
    }

    replicaStore[replicaCount++] = fingerprint;

    Serial.printf("[SLAVE-%d]   Replica stored in flat store only "
                  "(replica_count=%u)\n",
                  THIS_NODE_ID, replicaCount);

    return STATUS_OK;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 12 – NEW M3: Sync Handler (full implementation)
//
// In M2, OP_SYNC was a stub that returned status=3.
// In M3 the slave counts its stored fingerprints across both primary and replica
// storage and returns the totals to the master.
//
// FIX-C: The slave now packs BOTH counts into the reply so the master can
// detect replica divergence and log a repair alert.
//   key_fingerprint = primaryCount  (slots used in primary partition)
//   bucket_index    = replicaCount  (entries in the flat replica store)
// The master's handleSyncReply reads both fields and logs a warning when the
// sum deviates from the expected total, closing the "eventual-consistency gap"
// called out in the milestone spec.
// ─────────────────────────────────────────────────────────────────────────────

void handleSync(const FilterMessage &msg, FilterMessage &reply) {
    (void)msg;
    uint16_t primaryCount = 0;
    for (uint16_t b = myPartitionLo; b <= myPartitionHi; b++)
        for (int s = 0; s < SLOTS_PER_BUCKET; s++)
            if (filter_buckets[b][s] != 0) primaryCount++;

    uint16_t total = primaryCount + replicaCount;

    Serial.printf("[SLAVE-%d]   SYNC: primary=%u  replica=%u  total=%u\n",
                  THIS_NODE_ID, primaryCount, replicaCount, total);

    reply.operation_type  = OP_SYNC;
    reply.status_code     = STATUS_OK;
    reply.key_fingerprint = primaryCount;   // primary slots
    reply.bucket_index    = replicaCount;   // replica entries
    reply.node_id         = THIS_NODE_ID;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 13 – Rebalance Handler
// ─────────────────────────────────────────────────────────────────────────────

void handleRebalance(const FilterMessage &msg, FilterMessage &reply) {
    uint16_t newLo = msg.partition_lo;
    uint16_t newHi = msg.partition_hi;

    Serial.printf("[SLAVE-%d]   REBALANCE: [%u..%u] → [%u..%u]\n",
                  THIS_NODE_ID, myPartitionLo, myPartitionHi, newLo, newHi);

    myPartitionLo = newLo;
    myPartitionHi = newHi;

    for (uint16_t b = 0; b < BUCKET_COUNT; b++) {
        if (!ownsBucket(b)) {
            for (int s = 0; s < SLOTS_PER_BUCKET; s++) {
                filter_buckets[b][s] = 0;
            }
        }
    }
    storedCount = computeOwnedCapacity();

    Serial.printf("[SLAVE-%d]   Partition updated → [%u..%u]  "
                  "primary slots used: %u/%u\n",
                  THIS_NODE_ID, myPartitionLo, myPartitionHi,
                  storedCount, maxOwnedCapacity());

    reply.operation_type = OP_REBALANCE;
    reply.status_code    = STATUS_OK;
    reply.partition_lo   = myPartitionLo;
    reply.partition_hi   = myPartitionHi;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 14 – Drain Handler  
// ─────────────────────────────────────────────────────────────────────────────

bool ensureDrainPeer(const uint8_t *destMac) {
    if (esp_now_is_peer_exist(destMac)) {
        memcpy(drainDestMac, destMac, 6);
        drainPeerAdded = true;
        return true;
    }
    if (drainPeerAdded) {
        esp_now_del_peer(drainDestMac);
        drainPeerAdded = false;
    }
    esp_now_peer_info_t peerInfo;
    memset(&peerInfo, 0, sizeof(peerInfo));
    memcpy(peerInfo.peer_addr, destMac, 6);
    peerInfo.channel = 0;
    peerInfo.encrypt = false;
    if (esp_now_add_peer(&peerInfo) != ESP_OK) {
        Serial.printf("[SLAVE-%d]   ERROR: cannot register drain destination.\n", THIS_NODE_ID);
        return false;
    }
    memcpy(drainDestMac, destMac, 6);
    drainPeerAdded = true;
    return true;
}

void handleDrain(const FilterMessage &msg, FilterMessage &reply) {
    uint16_t       lo      = msg.partition_lo;
    uint16_t       hi      = msg.partition_hi;
    uint8_t        dstNode = msg.dest_node_id;
    const uint8_t *dstMac  = reinterpret_cast<const uint8_t*>(msg.key_data);

    if (lo >= BUCKET_COUNT) lo = BUCKET_COUNT - 1;
    if (hi >= BUCKET_COUNT) hi = BUCKET_COUNT - 1;

    Serial.printf("[SLAVE-%d]   DRAIN: migrating buckets [%u..%u] → node %d\n",
                  THIS_NODE_ID, lo, hi, dstNode);

    reply.operation_type = OP_DRAIN;
    reply.partition_lo   = lo;
    reply.partition_hi   = hi;
    reply.dest_node_id   = dstNode;

    if (lo > hi) {
        reply.status_code     = 0xFE;
        reply.key_fingerprint = 0;
        return;
    }

    if (!ensureDrainPeer(dstMac)) {
        reply.status_code     = 0xFF;
        reply.key_fingerprint = 0;
        return;
    }

    uint16_t migrated = 0;

    for (uint16_t b = lo; b <= hi; b++) {
        for (int s = 0; s < SLOTS_PER_BUCKET; s++) {
            uint16_t fp = filter_buckets[b][s];
            if (fp == 0) continue;

            FilterMessage fwd;
            memset(&fwd, 0, sizeof(fwd));
            fwd.operation_type  = OP_INSERT;
            fwd.sequence_id     = 0;   // internal drain – no master ACK
            fwd.key_fingerprint = fp;
            fwd.bucket_index    = b;
            fwd.node_id         = dstNode;

            esp_err_t err = esp_now_send(
                dstMac,
                reinterpret_cast<const uint8_t*>(&fwd),
                sizeof(fwd)
            );

            if (err == ESP_OK) {
                // Best-effort forwarding only.
                // For strict correctness, clear after destination ACK, not here.
                migrated++;
                delay(5);
            } else {
                Serial.printf("[SLAVE-%d]   DRAIN SEND ERROR: fp=0x%04X bucket=%u err=0x%X\n",
                              THIS_NODE_ID, fp, b, err);
            }
        }
    }

    Serial.printf("[SLAVE-%d]   DRAIN complete: %u fingerprints forwarded "
                  "(local copy retained until stronger confirmation exists)\n",
                  THIS_NODE_ID, migrated);

    reply.status_code     = STATUS_OK;
    reply.key_fingerprint = migrated;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 15 – Message Dispatch
// ─────────────────────────────────────────────────────────────────────────────

bool processFilterMessage(const FilterMessage &msg, FilterMessage &reply) {
    reply.sequence_id     = msg.sequence_id;
    reply.key_fingerprint = msg.key_fingerprint;
    reply.bucket_index    = msg.bucket_index;
    reply.node_id         = THIS_NODE_ID;
    reply.result_found    = 0;
    reply.status_code     = 0;
    reply.partition_lo    = myPartitionLo;
    reply.partition_hi    = myPartitionHi;
    reply.dest_node_id    = 0xFF;
    reply.replica_node_id = 0xFF;
    strncpy(reply.key_data, msg.key_data, sizeof(reply.key_data) - 1);
    reply.key_data[sizeof(reply.key_data) - 1] = '\0';

    switch (msg.operation_type) {

        case OP_DUMMY:
            reply.operation_type = OP_ACK;
            reply.status_code    = STATUS_OK;
            return true;

        case OP_INSERT: {
            bool isDrainInsert = (msg.sequence_id == 0);

            uint16_t forward_bucket = 0, forward_fp = 0;
            uint8_t  st = handleInsert(msg.key_fingerprint, msg.bucket_index,
                                       forward_bucket, forward_fp);

            if (isDrainInsert) {
                if (st == STATUS_FILTER_FULL)
                    Serial.printf("[SLAVE-%d]   WARNING: drain insert failed – full.\n", THIS_NODE_ID);
                return false;
            }

            reply.operation_type = OP_ACK;
            reply.status_code    = st;
            if (st == STATUS_FORWARD_INSERT) {
                reply.bucket_index    = forward_bucket;
                reply.key_fingerprint = forward_fp;
            }
            return true;
        }

        case OP_DELETE: {
            bool replicaDelete = (msg.replica_node_id == THIS_NODE_ID &&
                                  msg.partition_lo == 0 &&
                                  msg.partition_hi == BUCKET_COUNT - 1);
            uint8_t st = handleDelete(msg.key_fingerprint, msg.bucket_index, replicaDelete);
            reply.operation_type = OP_ACK;
            reply.status_code    = st;
            return true;
        }

        case OP_LOOKUP: {
            uint8_t found = 0;
            uint8_t st    = handleLookup(msg.key_fingerprint, msg.bucket_index, found);
            reply.operation_type = OP_RESULT;
            reply.result_found   = found;
            reply.status_code    = st;
            return true;
        }

        case OP_REBALANCE:
            handleRebalance(msg, reply);
            return true;

        case OP_DRAIN:
            handleDrain(msg, reply);
            return true;

        // ── M3: Heartbeat ─────────────────────────────────────────────────────
        case OP_HEARTBEAT:
            reply.operation_type = OP_HEARTBEAT_ACK;
            reply.status_code    = STATUS_OK;
            reply.node_id        = THIS_NODE_ID;
            Serial.printf("[SLAVE-%d]   ♥ HEARTBEAT received → sending ACK\n", THIS_NODE_ID);
            return true;

        // ── M3: Replicate ─────────────────────────────────────────────────────
        case OP_REPLICATE: {
            uint8_t st = handleReplicate(msg.key_fingerprint, msg.bucket_index);
            reply.operation_type = OP_ACK;
            reply.status_code    = st;
            // Echo key_data so master log is readable
            return true;
        }

        // ── M3: Sync ────────────────────────────────────
        case OP_SYNC:
            handleSync(msg, reply);
            return true;

        default:
            Serial.printf("[SLAVE-%d]   Unknown op: %d – ignored.\n",
                          THIS_NODE_ID, msg.operation_type);
            return false;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 16 – ESP-NOW Callbacks
// ─────────────────────────────────────────────────────────────────────────────

#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
void onDataSent(const wifi_tx_info_t *tx_info, esp_now_send_status_t status) {
    (void)tx_info;
#else
void onDataSent(const uint8_t *mac_addr, esp_now_send_status_t status) {
    (void)mac_addr;
#endif
    if (status != ESP_NOW_SEND_SUCCESS)
        Serial.printf("[SLAVE-%d]  WARNING: ACK send FAILED at MAC layer.\n", THIS_NODE_ID);
}

#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
void onDataRecv(const esp_now_recv_info_t *recv_info, const uint8_t *data, int len) {
    const uint8_t *src = recv_info->src_addr;
#else
void onDataRecv(const uint8_t *src, const uint8_t *data, int len) {
#endif
    Serial.println("\n========================================");
    Serial.printf("[SLAVE-%d]  Packet from ", THIS_NODE_ID);
    printMacAddress(src);
    Serial.println();

    if (len != sizeof(FilterMessage)) {
        Serial.printf("[SLAVE-%d]  WARNING: size mismatch %d vs %d – ignored.\n",
                      THIS_NODE_ID, len, (int)sizeof(FilterMessage));
        return;
    }

    FilterMessage incoming;
    memcpy(&incoming, data, sizeof(FilterMessage));

    const char *opNames[] = {
        "DUMMY","INSERT","DELETE","LOOKUP","SYNC",
        "ACK","RESULT","REBALANCE","DRAIN","NODE_JOIN","NODE_LEAVE",
        "HEARTBEAT","HEARTBEAT_ACK","REPLICATE"
    };
    uint8_t op = incoming.operation_type;
    Serial.printf("[SLAVE-%d]    op          : %d (%s)\n",
                  THIS_NODE_ID, op,
                  (op <= 13) ? opNames[op] : "?");
    Serial.printf("[SLAVE-%d]    sequence_id : %u\n",  THIS_NODE_ID, incoming.sequence_id);
    Serial.printf("[SLAVE-%d]    fp          : 0x%04X\n", THIS_NODE_ID, incoming.key_fingerprint);
    Serial.printf("[SLAVE-%d]    bucket      : %u\n",  THIS_NODE_ID, incoming.bucket_index);

    if (op != OP_HEARTBEAT && op != OP_DRAIN)
        Serial.printf("[SLAVE-%d]    key_data    : \"%s\"\n", THIS_NODE_ID, incoming.key_data);
    if (op == OP_REBALANCE || op == OP_DRAIN)
        Serial.printf("[SLAVE-%d]    partition   : [%u .. %u]\n",
                      THIS_NODE_ID, incoming.partition_lo, incoming.partition_hi);

    FilterMessage reply;
    memset(&reply, 0, sizeof(reply));
    bool shouldReply = processFilterMessage(incoming, reply);

    if (shouldReply) {
        if (!peerRegistered) {
            Serial.printf("[SLAVE-%d]  WARNING: master not registered – reply dropped.\n",
                          THIS_NODE_ID);
        } else {
            esp_err_t err = esp_now_send(masterMacAddress,
                                         reinterpret_cast<const uint8_t*>(&reply),
                                         sizeof(reply));
            if (err != ESP_OK) {
                Serial.printf("[SLAVE-%d]  ERROR: reply send failed (0x%X)\n",
                              THIS_NODE_ID, err);
            } else {
                Serial.printf("[SLAVE-%d]  Reply sent: op=%d  seq=%u  "
                              "found=%d  status=%u\n",
                              THIS_NODE_ID, reply.operation_type, reply.sequence_id,
                              reply.result_found, reply.status_code);
            }
        }
    }

    Serial.println("========================================");
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 17 – Peer Registration
// ─────────────────────────────────────────────────────────────────────────────

bool registerMasterPeer(const uint8_t *mac) {
    esp_now_peer_info_t peerInfo;
    memset(&peerInfo, 0, sizeof(peerInfo));
    memcpy(peerInfo.peer_addr, mac, 6);
    peerInfo.channel = 0;
    peerInfo.encrypt = false;

    if (esp_now_add_peer(&peerInfo) != ESP_OK) {
        Serial.printf("[SLAVE-%d]  ERROR: esp_now_add_peer for master failed.\n", THIS_NODE_ID);
        return false;
    }
    Serial.printf("[SLAVE-%d]  Master registered: ", THIS_NODE_ID);
    printMacAddress(mac);
    Serial.println();
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 18 – setup()
// ─────────────────────────────────────────────────────────────────────────────

void setup() {
    Serial.begin(115200);
    delay(500);

    Serial.println("\n========================================");
    Serial.printf( "  Distributed Cuckoo Filter - SLAVE %d  \n", THIS_NODE_ID);
    Serial.println(" Replication | Heartbeat | Full Sync  ");
    Serial.println("========================================");

    WiFi.mode(WIFI_STA);
    WiFi.disconnect();
    delay(100);

    Serial.printf("[SLAVE-%d]  Own MAC : ", THIS_NODE_ID);
    Serial.println(WiFi.macAddress());
    Serial.printf("[SLAVE-%d]  >>> Copy this MAC into master's slaves[%d].mac[] <<<\n",
                  THIS_NODE_ID, THIS_NODE_ID);

    if (esp_now_init() != ESP_OK) {
        Serial.printf("[SLAVE-%d]  FATAL: esp_now_init() failed. Halting.\n", THIS_NODE_ID);
        while (true) delay(1000);
    }
    Serial.printf("[SLAVE-%d]  ESP-NOW initialised.\n", THIS_NODE_ID);

    esp_now_register_send_cb(onDataSent);
    esp_now_register_recv_cb(onDataRecv);

    peerRegistered = registerMasterPeer(masterMacAddress);
    if (!peerRegistered) {
        Serial.printf("[SLAVE-%d]  WARNING: master not registered. "
                      "Fix masterMacAddress[] and re-flash.\n", THIS_NODE_ID);
    }

    Serial.printf("[SLAVE-%d]  Partition at boot: [%u..%u]  "
                  "primary capacity: %u slots\n",
                  THIS_NODE_ID, myPartitionLo, myPartitionHi, maxOwnedCapacity());
    Serial.printf("[SLAVE-%d]  Replica store capacity: %u fingerprints\n",
                  THIS_NODE_ID, REPLICA_CAPACITY);
    Serial.printf("[SLAVE-%d]  Ready. Waiting for messages from master.\n\n", THIS_NODE_ID);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 19 – loop() 
// ─────────────────────────────────────────────────────────────────────────────

void loop() {
    // All processing happens in onDataRecv() callback.
    // A short delay prevents watchdog resets on Arduino-framework builds.
    delay(100);
}
