// =============================================================================
// slave_node_2.ino  –  Distributed Cuckoo Filter  –  Milestone 2
// Role: SLAVE NODE
//
// HOW TO USE:
//   1. Set THIS_NODE_ID to 0 for first slave, 1 for second, 2 for third.
//   2. Set masterMacAddress[] to the master ESP32's MAC (printed on master boot).
//   3. Flash this file onto each slave ESP32.
//   4. Open Serial Monitor at 115200 → copy the MAC address printed on boot.
//   5. Paste that MAC into master_node.ino slaves[] for the matching node.
// =============================================================================

#include <Arduino.h>
#include <WiFi.h>
#include <esp_now.h>

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 – Operation Codes  (keep in sync with master_node.ino)
// ─────────────────────────────────────────────────────────────────────────────

#define OP_DUMMY      0
#define OP_INSERT     1
#define OP_DELETE     2
#define OP_LOOKUP     3
#define OP_SYNC       4
#define OP_ACK        5
#define OP_RESULT     6
#define OP_REBALANCE  7
#define OP_DRAIN      8
#define OP_NODE_JOIN  9
#define OP_NODE_LEAVE 10

#define STATUS_OK             0
#define STATUS_FILTER_FULL    1
#define STATUS_NOT_FOUND      2
#define STATUS_REMOTE_CHECK   3
#define STATUS_FORWARD_INSERT 4
#define STATUS_WRONG_OWNER    5

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 – Message Format  (IDENTICAL to master_node)
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
} FilterMessage;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3 – Per-Slave Configuration
// !! Change THIS_NODE_ID and masterMacAddress before flashing each slave !!
// ─────────────────────────────────────────────────────────────────────────────

// Copy master MAC from master's Serial Monitor output at boot
uint8_t masterMacAddress[] = {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF};

// Set to 0, 1, or 2 to match this slave's index in master's slaves[] table
#define THIS_NODE_ID 0

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 – Filter Storage
// ─────────────────────────────────────────────────────────────────────────────

#define BUCKET_COUNT     256
#define SLOTS_PER_BUCKET 4
#define MAX_KICKS        500

// Total fingerprint capacity of this slave's filter = BUCKET_COUNT × SLOTS_PER_BUCKET
// With 3 nodes, each owns ~85 buckets → capacity ≈ 340 fingerprints per slave.
uint16_t filter_buckets[BUCKET_COUNT][SLOTS_PER_BUCKET] = {0};

// Track how many fingerprints are currently stored in owned partition
// so we can detect and warn about approaching capacity.
static uint16_t storedCount = 0;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 – Partition State
// ─────────────────────────────────────────────────────────────────────────────

static uint16_t myPartitionLo = 0;
static uint16_t myPartitionHi = BUCKET_COUNT - 1;  // owns all until master narrows it

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

// Alternate bucket via fingerprint-derived XOR offset (must mirror master helper)
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

// Try to insert fp into a slot we own; returns true on success
bool tryInsertIntoOwnedBucket(uint16_t bucket, uint16_t fp) {
    if (!ownsBucket(bucket)) return false;
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        if (filter_buckets[bucket][i] == 0) {
            filter_buckets[bucket][i] = fp;
            storedCount++;
            return true;
        }
    }
    return false;
}

// Compute capacity used in owned partition for overflow warnings
uint16_t computeOwnedCapacity() {
    uint16_t total = 0;
    for (uint16_t b = myPartitionLo; b <= myPartitionHi; b++) {
        for (int s = 0; s < SLOTS_PER_BUCKET; s++) {
            if (filter_buckets[b][s] != 0) total++;
        }
    }
    return total;
}

uint16_t maxOwnedCapacity() {
    return (myPartitionHi - myPartitionLo + 1) * SLOTS_PER_BUCKET;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 8 – Cuckoo Filter: INSERT
//
// [FIX F] Returns STATUS_FILTER_FULL with a clear error log when genuinely full.
//         storedCount is maintained to allow capacity monitoring.
// ─────────────────────────────────────────────────────────────────────────────

uint8_t handleInsert(uint16_t fingerprint,
                     uint16_t bucket_index,
                     uint16_t &forward_bucket,
                     uint16_t &forward_fp) {

    uint16_t alt_bucket  = getAltBucket(bucket_index, fingerprint);
    bool     own_primary = ownsBucket(bucket_index);
    bool     own_alt     = ownsBucket(alt_bucket);

    //  Log key details for every insert attempt
    Serial.printf("[SLAVE-%d]   INSERT: fp=0x%04X  primary=%u  alt=%u  "
                  "own_primary=%d  own_alt=%d\n",
                  THIS_NODE_ID, fingerprint, bucket_index, alt_bucket,
                  own_primary, own_alt);

    if (!own_primary && !own_alt) {
        Serial.printf("[SLAVE-%d]   INSERT FAILED: wrong owner for bucket %u "
                      "(partition=[%u..%u])\n",
                      THIS_NODE_ID, bucket_index, myPartitionLo, myPartitionHi);
        return STATUS_WRONG_OWNER;
    }

    // Try direct insert into owned bucket – no kick-out required
    if (own_primary && tryInsertIntoOwnedBucket(bucket_index, fingerprint)) {
        Serial.printf("[SLAVE-%d]   Inserted directly into primary bucket %u "
                      "(stored=%u/%u)\n",
                      THIS_NODE_ID, bucket_index,
                      storedCount, maxOwnedCapacity());
        return STATUS_OK;
    }
    if (own_alt && tryInsertIntoOwnedBucket(alt_bucket, fingerprint)) {
        Serial.printf("[SLAVE-%d]   Inserted directly into alt bucket %u "
                      "(stored=%u/%u)\n",
                      THIS_NODE_ID, alt_bucket,
                      storedCount, maxOwnedCapacity());
        return STATUS_OK;
    }

    // Check capacity before attempting kick-out loop
    uint16_t cap  = maxOwnedCapacity();
    uint16_t used = computeOwnedCapacity();
    if (used >= cap) {
        Serial.printf("[SLAVE-%d]   INSERT FAILED: partition full (%u/%u slots used). "
                      "No kick-out possible.\n",
                      THIS_NODE_ID, used, cap);
        return STATUS_FILTER_FULL;
    }

    // Both candidate buckets are full – start kick-out from an owned bucket
    uint16_t curr_bucket = (own_primary && own_alt)
                           ? ((random(0, 2) == 0) ? bucket_index : alt_bucket)
                           : (own_primary ? bucket_index : alt_bucket);
    uint16_t curr_fp = fingerprint;

    Serial.printf("[SLAVE-%d]   Starting kick-out from bucket %u\n",
                  THIS_NODE_ID, curr_bucket);

    for (int n = 0; n < MAX_KICKS; n++) {
        int slot = random(0, SLOTS_PER_BUCKET);

        uint16_t evicted_fp              = filter_buckets[curr_bucket][slot];
        filter_buckets[curr_bucket][slot] = curr_fp;
        curr_fp                          = evicted_fp;

        uint16_t next_bucket = getAltBucket(curr_bucket, curr_fp);

        if (!ownsBucket(next_bucket)) {
            // Evicted fp's home is on another node → master must re-route it
            forward_bucket = next_bucket;
            forward_fp     = curr_fp;
            Serial.printf("[SLAVE-%d]   Kick-out crossed partition boundary: "
                          "fp=0x%04X → bucket=%u (remote) after %d kicks\n",
                          THIS_NODE_ID, curr_fp, next_bucket, n + 1);
            return STATUS_FORWARD_INSERT;
        }

        curr_bucket = next_bucket;

        if (tryInsertIntoOwnedBucket(curr_bucket, curr_fp)) {
            Serial.printf("[SLAVE-%d]   Kick-out settled: fp=0x%04X in bucket %u "
                          "after %d kicks (stored=%u/%u)\n",
                          THIS_NODE_ID, curr_fp, curr_bucket, n + 1,
                          storedCount, maxOwnedCapacity());
            return STATUS_OK;
        }
    }

    // Exhausted MAX_KICKS without finding a free slot – genuinely full
    Serial.printf("[SLAVE-%d]   INSERT FAILED: partition full after %d kick-out "
                  "attempts (stored=%u/%u). Returning STATUS_FILTER_FULL.\n",
                  THIS_NODE_ID, MAX_KICKS, storedCount, maxOwnedCapacity());
    return STATUS_FILTER_FULL;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 9 – Cuckoo Filter: DELETE
//
// STATUS_REMOTE_CHECK only when unscanned alt-bucket is genuinely remote.
// Full logging for every path.
// ─────────────────────────────────────────────────────────────────────────────

uint8_t handleDelete(uint16_t fingerprint, uint16_t bucket_index) {
    uint16_t alt_bucket  = getAltBucket(bucket_index, fingerprint);
    bool     own_primary = ownsBucket(bucket_index);
    bool     own_alt     = ownsBucket(alt_bucket);

    Serial.printf("[SLAVE-%d]   DELETE: fp=0x%04X  primary=%u  alt=%u  "
                  "own_primary=%d  own_alt=%d\n",
                  THIS_NODE_ID, fingerprint, bucket_index, alt_bucket,
                  own_primary, own_alt);

    if (!own_primary && !own_alt) {
        Serial.printf("[SLAVE-%d]   DELETE FAILED: wrong owner for bucket %u\n",
                      THIS_NODE_ID, bucket_index);
        return STATUS_WRONG_OWNER;
    }

    if (own_primary) {
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            if (filter_buckets[bucket_index][i] == fingerprint) {
                filter_buckets[bucket_index][i] = 0;
                if (storedCount > 0) storedCount--;
                Serial.printf("[SLAVE-%d]   Deleted from primary bucket %u slot %d "
                              "(stored=%u/%u)\n",
                              THIS_NODE_ID, bucket_index, i,
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
                Serial.printf("[SLAVE-%d]   Deleted from alt bucket %u slot %d "
                              "(stored=%u/%u)\n",
                              THIS_NODE_ID, alt_bucket, i,
                              storedCount, maxOwnedCapacity());
                return STATUS_OK;
            }
        }
    }

    //  fp not found in local bucket(s). Is the other candidate remote?
    if (own_primary && !own_alt) {
        Serial.printf("[SLAVE-%d]   Not in primary bucket %u; "
                      "alt bucket %u is remote → REMOTE_CHECK\n",
                      THIS_NODE_ID, bucket_index, alt_bucket);
        return STATUS_REMOTE_CHECK;
    }
    if (!own_primary && own_alt) {
        Serial.printf("[SLAVE-%d]   Not in alt bucket %u; "
                      "primary bucket %u is remote → REMOTE_CHECK\n",
                      THIS_NODE_ID, alt_bucket, bucket_index);
        return STATUS_REMOTE_CHECK;
    }

    // Both buckets are local and fp is not in either → definitively not here
    Serial.printf("[SLAVE-%d]   DELETE: NOT FOUND in any local bucket\n", THIS_NODE_ID);
    return STATUS_NOT_FOUND;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 10 – Cuckoo Filter: LOOKUP
//
// Same REMOTE_CHECK correction as DELETE.
//  Full logging for every path.
// ─────────────────────────────────────────────────────────────────────────────

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
        Serial.printf("[SLAVE-%d]   LOOKUP FAILED: wrong owner for bucket %u\n",
                      THIS_NODE_ID, bucket_index);
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

    // Signal REMOTE_CHECK only if the unsearched bucket is on another node
    if (own_primary && !own_alt) {
        Serial.printf("[SLAVE-%d]   NOT found in primary; "
                      "alt bucket %u is remote → REMOTE_CHECK\n",
                      THIS_NODE_ID, alt_bucket);
        return STATUS_REMOTE_CHECK;
    }
    if (!own_primary && own_alt) {
        Serial.printf("[SLAVE-%d]   NOT found in alt; "
                      "primary bucket %u is remote → REMOTE_CHECK\n",
                      THIS_NODE_ID, bucket_index);
        return STATUS_REMOTE_CHECK;
    }

    // Both buckets local and fp absent → definitively not stored here
    Serial.printf("[SLAVE-%d]   NOT FOUND fp=0x%04X in any local bucket\n",
                  THIS_NODE_ID, fingerprint);
    return STATUS_NOT_FOUND;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 11 – Rebalance Handler
// ─────────────────────────────────────────────────────────────────────────────

void handleRebalance(const FilterMessage &msg, FilterMessage &reply) {
    uint16_t newLo = msg.partition_lo;
    uint16_t newHi = msg.partition_hi;

    Serial.printf("[SLAVE-%d]   REBALANCE: [%u..%u] → [%u..%u]\n",
                  THIS_NODE_ID, myPartitionLo, myPartitionHi, newLo, newHi);

    myPartitionLo = newLo;
    myPartitionHi = newHi;

    // Recount storedCount against new partition boundaries
    storedCount = computeOwnedCapacity();

    Serial.printf("[SLAVE-%d]   Partition updated → [%u..%u]  "
                  "owned slots used: %u/%u\n",
                  THIS_NODE_ID, myPartitionLo, myPartitionHi,
                  storedCount, maxOwnedCapacity());

    reply.operation_type = OP_REBALANCE;
    reply.status_code    = STATUS_OK;
    reply.partition_lo   = myPartitionLo;
    reply.partition_hi   = myPartitionHi;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 12 – Drain Handler
//
// Drain-INSERT messages sent to this slave (seq_id == 0) are handled
//         by processFilterMessage without sending an ACK.
// ensureDrainPeer() checks esp_now_is_peer_exist() before add_peer.
// ─────────────────────────────────────────────────────────────────────────────

bool ensureDrainPeer(const uint8_t *destMac) {
    // Avoid "peer already exists" error on repeated drains
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
        Serial.printf("[SLAVE-%d]   ERROR: cannot register drain destination peer.\n",
                      THIS_NODE_ID);
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

    Serial.printf("[SLAVE-%d]   DRAIN: migrating buckets [%u..%u] → node %d\n",
                  THIS_NODE_ID, lo, hi, dstNode);

    reply.operation_type = OP_DRAIN;
    reply.partition_lo   = lo;
    reply.partition_hi   = hi;
    reply.dest_node_id   = dstNode;

    if (!ensureDrainPeer(dstMac)) {
        Serial.printf("[SLAVE-%d]   DRAIN ERROR: could not register dest peer.\n",
                      THIS_NODE_ID);
        reply.status_code     = 0xFF;
        reply.key_fingerprint = 0;
        return;
    }

    uint16_t migrated = 0;

    for (uint16_t b = lo; b <= hi; b++) {
        for (int s = 0; s < SLOTS_PER_BUCKET; s++) {
            uint16_t fp = filter_buckets[b][s];
            if (fp == 0) continue;

            // Build a drain-INSERT for the destination slave.
            // seq_id = 0 marks it as internal migration (no ACK sent back to master).
            FilterMessage fwd;
            memset(&fwd, 0, sizeof(fwd));
            fwd.operation_type  = OP_INSERT;
            fwd.sequence_id     = 0;      //  internal drain, no master ACK
            fwd.key_fingerprint = fp;
            fwd.bucket_index    = b;
            fwd.node_id         = dstNode;

            esp_err_t err = esp_now_send(dstMac,
                                         reinterpret_cast<const uint8_t*>(&fwd),
                                         sizeof(fwd));
            if (err == ESP_OK) {
                filter_buckets[b][s] = 0;
                if (storedCount > 0) storedCount--;
                migrated++;
                delay(5);  // throttle to avoid ESP-NOW TX queue overflow
            } else {
                Serial.printf("[SLAVE-%d]   DRAIN SEND ERROR: "
                              "fp=0x%04X bucket=%u err=0x%X\n",
                              THIS_NODE_ID, fp, b, err);
            }
        }
    }

    Serial.printf("[SLAVE-%d]   DRAIN complete: %u fingerprints migrated to node %d "
                  "(remaining stored=%u/%u)\n",
                  THIS_NODE_ID, migrated, dstNode,
                  storedCount, maxOwnedCapacity());

    reply.status_code     = STATUS_OK;
    reply.key_fingerprint = migrated;  // pass count back to master for its DRAIN ACK log
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 13 – Message Dispatch
// ─────────────────────────────────────────────────────────────────────────────

// Returns true if a reply should be sent to master
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
    strncpy(reply.key_data, msg.key_data, sizeof(reply.key_data) - 1);
    reply.key_data[sizeof(reply.key_data) - 1] = '\0';

    switch (msg.operation_type) {

        case OP_DUMMY:
            reply.operation_type = OP_ACK;
            reply.status_code    = STATUS_OK;
            return true;

        case OP_INSERT: {
            // seq_id == 0 → internal drain migration, process silently, no reply
            bool isDrainInsert = (msg.sequence_id == 0);

            uint16_t forward_bucket = 0, forward_fp = 0;
            uint8_t  st = handleInsert(msg.key_fingerprint, msg.bucket_index,
                                       forward_bucket, forward_fp);

            if (isDrainInsert) {
                // Drain inserts: just insert, no ACK to avoid flooding master
                if (st == STATUS_FILTER_FULL) {
                    Serial.printf("[SLAVE-%d]   WARNING: drain insert failed – "
                                  "partition full.\n", THIS_NODE_ID);
                }
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
            uint8_t st = handleDelete(msg.key_fingerprint, msg.bucket_index);
            reply.operation_type = OP_ACK;
            reply.status_code    = st;
            return true;
        }

        case OP_LOOKUP: {
            uint8_t found = 0;
            uint8_t st    = handleLookup(msg.key_fingerprint, msg.bucket_index, found);
            // [FIX B] OP_RESULT distinguishes lookup replies from insert/delete acks
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

        case OP_SYNC:
            // Full state sync deferred to Milestone 3
            reply.operation_type = OP_SYNC;
            reply.status_code    = 3;  // 3 = partial sync only
            return true;

        default:
            Serial.printf("[SLAVE-%d]   Unknown op: %d – ignored.\n",
                          THIS_NODE_ID, msg.operation_type);
            return false;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 14 – ESP-NOW Callbacks
// ─────────────────────────────────────────────────────────────────────────────

#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
void onDataSent(const wifi_tx_info_t *tx_info, esp_now_send_status_t status) {
#else
void onDataSent(const uint8_t *mac_addr, esp_now_send_status_t status) {
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

    // Log all incoming fields
    const char *opNames[] = {
        "DUMMY","INSERT","DELETE","LOOKUP","SYNC",
        "ACK","RESULT","REBALANCE","DRAIN","NODE_JOIN","NODE_LEAVE"
    };
    Serial.printf("[SLAVE-%d]    op          : %d (%s)\n",
                  THIS_NODE_ID, incoming.operation_type,
                  (incoming.operation_type <= 10) ? opNames[incoming.operation_type] : "?");
    Serial.printf("[SLAVE-%d]    sequence_id : %u\n",  THIS_NODE_ID, incoming.sequence_id);
    Serial.printf("[SLAVE-%d]    fp          : 0x%04X\n", THIS_NODE_ID, incoming.key_fingerprint);
    Serial.printf("[SLAVE-%d]    bucket      : %u\n",  THIS_NODE_ID, incoming.bucket_index);
    if (incoming.operation_type != OP_DRAIN)
        Serial.printf("[SLAVE-%d]    key_data    : \"%s\"\n", THIS_NODE_ID, incoming.key_data);
    if (incoming.operation_type == OP_REBALANCE || incoming.operation_type == OP_DRAIN)
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
// SECTION 15 – Peer Registration
// ─────────────────────────────────────────────────────────────────────────────

bool registerMasterPeer(const uint8_t *mac) {
    esp_now_peer_info_t peerInfo;
    memset(&peerInfo, 0, sizeof(peerInfo));
    memcpy(peerInfo.peer_addr, mac, 6);
    peerInfo.channel = 0;
    peerInfo.encrypt = false;

    if (esp_now_add_peer(&peerInfo) != ESP_OK) {
        Serial.printf("[SLAVE-%d]  ERROR: esp_now_add_peer for master failed.\n",
                      THIS_NODE_ID);
        return false;
    }
    Serial.printf("[SLAVE-%d]  Master registered: ", THIS_NODE_ID);
    printMacAddress(mac);
    Serial.println();
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 16 – setup()
// ─────────────────────────────────────────────────────────────────────────────

void setup() {
    Serial.begin(115200);
    delay(500);

    Serial.println("\n========================================");
    Serial.printf( "  Distributed Cuckoo Filter - SLAVE %d  \n", THIS_NODE_ID);
    Serial.println("         Milestone 2 (M2)             ");
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
                  "max capacity: %u slots\n",
                  THIS_NODE_ID, myPartitionLo, myPartitionHi, maxOwnedCapacity());
    Serial.printf("[SLAVE-%d]  Ready. Waiting for messages.\n\n", THIS_NODE_ID);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 17 – loop()  (slave is fully event-driven)
// ─────────────────────────────────────────────────────────────────────────────

void loop() {
    delay(100);
}
