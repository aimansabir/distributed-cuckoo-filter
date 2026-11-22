// =============================================================================
// master_node_2.ino  –  Distributed Cuckoo Filter  –  Milestone 2
// Role: MASTER NODE
//
// HOW TO USE:
//   1. Flash slave_node_2.ino on each slave ESP32.
//      Set THIS_NODE_ID = 0 on slave 0, THIS_NODE_ID = 1 on slave 1,
//      THIS_NODE_ID = 2 on slave 2.
//   2. Open each slave's Serial Monitor → copy the MAC address printed on boot.
//   3. Paste both MACs into the slaves[] table in SECTION 3 below.
//   4. Flash this file onto the master ESP32.
//   5. Open master Serial Monitor at 115200 baud.
//
#include <Arduino.h>
#include <WiFi.h>
#include <esp_now.h>

// Forward declarations
bool registerSlavePeer(uint8_t nodeIndex);
void captureTestLatency(const struct FilterMessage &reply);

// ─────────────────────────────────────────────────────────────────────────────
// TEST HARNESS – Key dataset for Milestone 2 tests
// ─────────────────────────────────────────────────────────────────────────────

const char* testKeys[] = {
    "apple","banana","cherry","date","grape",
    "kiwi","mango","orange","pear","plum",
    "melon","berry","lemon","lime","guava",
    "papaya","fig","apricot","peach","coconut"
};
const int totalTestKeys = 20;

// Per-key latency tracking (INSERT + LOOKUP)
static unsigned long insertSentAt[20]  = {0};
static unsigned long lookupSentAt[20]  = {0};
static unsigned long insertLatency[20] = {0};
static unsigned long lookupLatency[20] = {0};
static bool          insertDone[20]    = {false};
static bool          lookupDone[20]    = {false};
static bool          deleteDone[20]    = {false};
static bool          verifyDone[20]    = {false};

// Sequence-id → test-key index map (for latency capture in callbacks)
// We store seq_id % 256 → testKey index for each pending test op
static int8_t seqToTestIdx[256];  // -1 = not a test op

// Test phase state machine
static int  testPhase  = 0;   // 0=idle,1=insert,2=lookup,3=delete,4=verify,5=latency,6=done
static int  testKeyIdx = 0;
static bool testHarnessActive = false;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 – Operation Codes  (keep in sync with slave_node)
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
#define STATUS_REMOTE_CHECK   3   // slave owns only one of the two candidate buckets
#define STATUS_FORWARD_INSERT 4   // kick-out crossed a partition boundary
#define STATUS_WRONG_OWNER    5

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 – Message Format  (IDENTICAL in master and slave)
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
// SECTION 3 – Slave Peer Table
// !! REPLACE PLACEHOLDER MACs WITH REAL MACs BEFORE FLASHING !!
// Flash each slave first → read its MAC from Serial Monitor → paste here.
// ─────────────────────────────────────────────────────────────────────────────

#define MAX_SLAVES   3
#define BUCKET_COUNT 256  // must match slave_node_2.ino

typedef struct {
    uint8_t  mac[6];
    bool     registered;
    bool     online;
    uint8_t  node_id;
    uint16_t partition_lo;
    uint16_t partition_hi;
} SlaveInfo;

SlaveInfo slaves[MAX_SLAVES] = {
    // node 0 – paste MAC from slave 0 Serial Monitor here
    { {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0x00}, false, false, 0, 0, 0 },
    // node 1 – paste MAC from slave 1 Serial Monitor here
    { {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0x01}, false, false, 1, 0, 0 },
    // node 2 – paste MAC from slave 2 Serial Monitor here (used after addNode(2))
    { {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0x02}, false, false, 2, 0, 0 },
};

static uint8_t activeSlaveCount = 2;  // start with 2 slaves online

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 – Internal State
// ─────────────────────────────────────────────────────────────────────────────

static uint32_t messageCounter = 0;

// Ring buffer indexed by (sequence_id % PENDING_SLOTS).
// Each slot stores everything needed to detect timeout and retry.

#define PENDING_SLOTS       256
#define REQUEST_TIMEOUT_MS  3000  
#define MAX_RETRIES         3      

typedef struct {
    uint32_t      sentAt;          // millis() when last sent
    uint8_t       op;              // original OP_ code (INSERT/DELETE/LOOKUP)
    uint8_t       targetNode;      // which slave it was sent to
    uint8_t       retryCount;      // how many retries so far
    bool          active;          // slot is waiting for a reply
    FilterMessage msg;             // full message copy for retry
} PendingRequest;

static PendingRequest pending[PENDING_SLOTS];

// ── Known-set for correctness tracking ───────────────────────────────────────
#define KNOWN_SET_SIZE 128
static char    knownKeys[KNOWN_SET_SIZE][20] = {{}};
static bool    keyPresent[KNOWN_SET_SIZE]    = {false};
static uint8_t knownKeyCount                = 0;

// ── Statistics counters ───────────────────────────────────────────────────────
static uint32_t totalInserts      = 0;
static uint32_t successfulInserts = 0;
static uint32_t failedInserts     = 0;
static uint32_t totalDeletes      = 0;
static uint32_t successfulDeletes = 0;
static uint32_t totalLookups      = 0;
static uint32_t truePositives     = 0;
static uint32_t trueNegatives     = 0;
static uint32_t falsePositives    = 0;
static uint32_t falseNegatives    = 0;
static uint32_t totalTimeouts     = 0;   
static uint32_t totalRetries      = 0;   

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 – Known-Set Helpers
// ─────────────────────────────────────────────────────────────────────────────

void markKeyInserted(const char *key) {
    for (int i = 0; i < knownKeyCount; i++) {
        if (strcmp(knownKeys[i], key) == 0) { keyPresent[i] = true; return; }
    }
    if (knownKeyCount < KNOWN_SET_SIZE) {
        strncpy(knownKeys[knownKeyCount], key, 19);
        knownKeys[knownKeyCount][19] = '\0';
        keyPresent[knownKeyCount++] = true;
    }
}

void markKeyDeleted(const char *key) {
    for (int i = 0; i < knownKeyCount; i++) {
        if (strcmp(knownKeys[i], key) == 0) { keyPresent[i] = false; return; }
    }
}

bool wasKeyInserted(const char *key) {
    for (int i = 0; i < knownKeyCount; i++) {
        if (strcmp(knownKeys[i], key) == 0) return keyPresent[i];
    }
    return false;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 6 – Hash / Fingerprint Helpers
// ─────────────────────────────────────────────────────────────────────────────

uint16_t computeFingerprint(const char *key) {
    uint32_t hash = 2166136261UL;
    while (*key) { hash ^= (uint8_t)(*key++); hash *= 16777619UL; }
    uint16_t fp = (uint16_t)((hash >> 16) ^ (hash & 0xFFFF));
    return fp == 0 ? 1 : fp;
}

uint16_t computePrimaryBucket(const char *key) {
    uint32_t hash = 0;
    while (*key) hash = hash * 31 + (uint8_t)(*key++);
    return (uint16_t)(hash % BUCKET_COUNT);
}

// Mirrors slave's getAltBucket() – needed by master to resolve REMOTE_CHECK
uint16_t computeAltBucket(uint16_t primary, uint16_t fp) {
    uint32_t hash = fp * 0x5bd1e995;
    hash ^= hash >> 15;
    uint16_t offset = (uint16_t)(hash % BUCKET_COUNT);
    if (offset == 0) offset = 1;
    return (primary ^ offset) % BUCKET_COUNT;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 7 – Partitioning Strategy
//   N=2: node0 → [0..127],  node1 → [128..255]
//   N=3: node0 → [0..84],   node1 → [85..169],  node2 → [170..255]
// ─────────────────────────────────────────────────────────────────────────────

void recomputePartitions() {
    if (activeSlaveCount == 0) return;

    uint16_t base  = BUCKET_COUNT / activeSlaveCount;
    uint16_t extra = BUCKET_COUNT % activeSlaveCount;
    uint16_t lo    = 0;
    uint8_t  slot  = 0;

    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (!slaves[i].online) {
            slaves[i].partition_lo = 0;
            slaves[i].partition_hi = 0;
            continue;
        }
        uint16_t span      = base + (slot < extra ? 1 : 0);
        slaves[i].partition_lo = lo;
        slaves[i].partition_hi = lo + span - 1;
        lo += span;
        slot++;
    }

    Serial.println("[MASTER] Partition map recalculated:");
    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (slaves[i].online)
            Serial.printf("[MASTER]   node %d  buckets [%3u .. %3u]\n",
                          i, slaves[i].partition_lo, slaves[i].partition_hi);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 8 – Routing Helpers
// ─────────────────────────────────────────────────────────────────────────────

uint8_t selectTargetNode(uint16_t bucket_index) {
    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (slaves[i].online &&
            bucket_index >= slaves[i].partition_lo &&
            bucket_index <= slaves[i].partition_hi)
            return i;
    }
    Serial.printf("[MASTER] WARNING: no owner for bucket %u – defaulting to node 0\n",
                  bucket_index);
    return 0;
}

void printMacAddress(const uint8_t *mac) {
    for (int i = 0; i < 6; i++) {
        if (i > 0) Serial.print(":");
        Serial.printf("%02X", mac[i]);
    }
}


void sendToNode(uint8_t nodeIdx, FilterMessage &msg) {
    if (nodeIdx >= MAX_SLAVES || !slaves[nodeIdx].registered) {
        Serial.printf("[MASTER] ERROR: cannot send – node %d not registered.\n", nodeIdx);
        return;
    }

    // Record in pending table for timeout / retry tracking
    uint8_t slot = msg.sequence_id % PENDING_SLOTS;
    pending[slot].sentAt      = millis();
    pending[slot].op          = msg.operation_type;
    pending[slot].targetNode  = nodeIdx;
    pending[slot].retryCount  = pending[slot].active ? pending[slot].retryCount : 0;
    pending[slot].active      = true;
    memcpy(&pending[slot].msg, &msg, sizeof(FilterMessage));

    esp_err_t err = esp_now_send(slaves[nodeIdx].mac,
                                 reinterpret_cast<const uint8_t*>(&msg),
                                 sizeof(msg));
    if (err != ESP_OK)
        Serial.printf("[MASTER] ERROR: esp_now_send failed (0x%X)\n", err);
}

// Build and dispatch an operation for a given string key
uint8_t routeRequest(uint8_t op, const char *key) {
    FilterMessage msg;
    memset(&msg, 0, sizeof(msg));

    msg.operation_type  = op;
    msg.sequence_id     = ++messageCounter;
    msg.key_fingerprint = computeFingerprint(key);
    msg.bucket_index    = computePrimaryBucket(key);
    strncpy(msg.key_data, key, sizeof(msg.key_data) - 1);
    msg.key_data[sizeof(msg.key_data) - 1] = '\0';

    uint8_t tgt      = selectTargetNode(msg.bucket_index);
    msg.node_id      = tgt;
    msg.partition_lo = slaves[tgt].partition_lo;
    msg.partition_hi = slaves[tgt].partition_hi;

    const char *opNames[] = {
        "DUMMY","INSERT","DELETE","LOOKUP","SYNC",
        "ACK","RESULT","REBALANCE","DRAIN","NODE_JOIN","NODE_LEAVE"
    };
    Serial.println("----------------------------------------");
    Serial.printf("[MASTER] >> %s seq=%u  key=\"%s\"\n",
                  opNames[op], msg.sequence_id, key);
    Serial.printf("[MASTER]    fp=0x%04X  bucket=%u  node=%d  [%u..%u]\n",
                  msg.key_fingerprint, msg.bucket_index,
                  tgt, msg.partition_lo, msg.partition_hi);

    if (op == OP_INSERT) totalInserts++;
    if (op == OP_DELETE) totalDeletes++;
    if (op == OP_LOOKUP) totalLookups++;

    sendToNode(tgt, msg);
    return tgt;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 9 
// Called every loop() iteration. Walks all active pending slots and:
//   - If elapsed > REQUEST_TIMEOUT_MS and retries remain → re-send the message.
//   - If retries exhausted → log failure, clear slot, update stats.
// ─────────────────────────────────────────────────────────────────────────────

void checkTimeouts() {
    uint32_t now = millis();

    for (int i = 0; i < PENDING_SLOTS; i++) {
        if (!pending[i].active) continue;
        if (now - pending[i].sentAt < REQUEST_TIMEOUT_MS) continue;

        // This slot has timed out
        uint8_t  node = pending[i].targetNode;
        uint32_t seq  = pending[i].msg.sequence_id;

        if (pending[i].retryCount < MAX_RETRIES) {
            //Retry: re-send the exact same message
            pending[i].retryCount++;
            pending[i].sentAt = now;
            totalRetries++;

            Serial.printf("[MASTER] TIMEOUT: seq=%u op=%d node=%d – "
                          "retry %d/%d\n",
                          seq, pending[i].op, node,
                          pending[i].retryCount, MAX_RETRIES);

            esp_now_send(slaves[node].mac,
                         reinterpret_cast<const uint8_t*>(&pending[i].msg),
                         sizeof(FilterMessage));
        } else {
            totalTimeouts++;
            Serial.printf("[MASTER] GAVE UP: seq=%u op=%d node=%d – "
                          "no response after %d retries. "
                          "Node may be offline.\n",
                          seq, pending[i].op, node, MAX_RETRIES);

            // Count as a failure in stats so reports are accurate
            if (pending[i].op == OP_INSERT) failedInserts++;

            pending[i].active = false;
        }
    }
}

// Mark a pending slot as completed (called when a reply arrives)
void clearPending(uint32_t seq_id) {
    pending[seq_id % PENDING_SLOTS].active = false;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 10 – Remote-Check Resolution  
//
// When a slave returns STATUS_REMOTE_CHECK:
//   - It searched its owned bucket, did not find the fingerprint.
//   - The alternate bucket belongs to a different node.
// The master computes the alt-bucket, selects the owning node, and forwards.
// ─────────────────────────────────────────────────────────────────────────────

void issueRemoteCheck(uint8_t op, const FilterMessage &originalReply) {
    uint16_t fp      = originalReply.key_fingerprint;
    uint16_t primBkt = originalReply.bucket_index;
    uint16_t altBkt  = computeAltBucket(primBkt, fp);
    uint8_t  altNode = selectTargetNode(altBkt);

    Serial.printf("[MASTER] REMOTE_CHECK: forwarding %s for fp=0x%04X "
                  "to alt-bucket %u on node %d\n",
                  op == OP_LOOKUP ? "LOOKUP" : "DELETE", fp, altBkt, altNode);

    if (!slaves[altNode].registered) {
        Serial.printf("[MASTER] ERROR: alt node %d not registered – "
                      "REMOTE_CHECK dropped.\n", altNode);
        return;
    }

    FilterMessage fwd;
    memset(&fwd, 0, sizeof(fwd));
    fwd.operation_type  = op;
    fwd.sequence_id     = ++messageCounter;
    fwd.key_fingerprint = fp;
    fwd.bucket_index    = altBkt;
    fwd.node_id         = altNode;
    fwd.partition_lo    = slaves[altNode].partition_lo;
    fwd.partition_hi    = slaves[altNode].partition_hi;
    strncpy(fwd.key_data, originalReply.key_data, sizeof(fwd.key_data) - 1);
    fwd.key_data[sizeof(fwd.key_data) - 1] = '\0';

    sendToNode(altNode, fwd);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 11 – Reply Handlers
// ─────────────────────────────────────────────────────────────────────────────

void handleInsertAck(const FilterMessage &reply) {
    uint32_t latency = millis() - pending[reply.sequence_id % PENDING_SLOTS].sentAt;
    captureTestLatency(reply);   // test harness latency capture
    clearPending(reply.sequence_id);

    Serial.printf("[MASTER] INSERT ACK: seq=%u  key=\"%s\"  status=%u  latency=%u ms\n",
                  reply.sequence_id, reply.key_data, reply.status_code, latency);

    if (reply.status_code == STATUS_OK) {
        successfulInserts++;
        markKeyInserted(reply.key_data);  // [FIX 3] only on confirmed OK
        Serial.printf("[MASTER]   OK – key confirmed stored on node %d.\n", reply.node_id);
    } else if (reply.status_code == STATUS_FILTER_FULL) {
        failedInserts++;
        Serial.printf("[MASTER]   FILTER FULL on node %d – insert failed.\n", reply.node_id);
    } else {
        failedInserts++;
        Serial.printf("[MASTER]   Insert not confirmed (status=%u).\n", reply.status_code);
    }
}

void aggregateLookupResult(const FilterMessage &reply) {
    uint32_t latency = millis() - pending[reply.sequence_id % PENDING_SLOTS].sentAt;
    clearPending(reply.sequence_id);
    captureTestLatency(reply);   // test harness latency capture

    bool expected = wasKeyInserted(reply.key_data);
    bool actual   = (reply.result_found == 1);

    Serial.printf("[MASTER] LOOKUP RESULT: seq=%u  key=\"%s\"  found=%s  latency=%u ms\n",
                  reply.sequence_id, reply.key_data,
                  actual ? "YES" : "NO", latency);

    if ( expected &&  actual) { truePositives++;  Serial.println("[MASTER]   → TRUE  POSITIVE"); }
    if (!expected && !actual) { trueNegatives++;  Serial.println("[MASTER]   → TRUE  NEGATIVE"); }
    if (!expected &&  actual) { falsePositives++; Serial.println("[MASTER]   → FALSE POSITIVE (fp collision)"); }
    if ( expected && !actual) { falseNegatives++; Serial.println("[MASTER]   → FALSE NEGATIVE (unexpected!)"); }
}

void handleDeleteAck(const FilterMessage &reply) {
    uint32_t latency = millis() - pending[reply.sequence_id % PENDING_SLOTS].sentAt;
    captureTestLatency(reply);   // test harness latency capture
    clearPending(reply.sequence_id);

    Serial.printf("[MASTER] DELETE ACK: seq=%u  key=\"%s\"  status=%u  latency=%u ms\n",
                  reply.sequence_id, reply.key_data, reply.status_code, latency);

    if (reply.status_code == STATUS_OK) {
        successfulDeletes++;
        markKeyDeleted(reply.key_data);
        Serial.printf("[MASTER]   OK – key removed from node %d.\n", reply.node_id);
    } else if (reply.status_code == STATUS_NOT_FOUND) {
        Serial.printf("[MASTER]   NOT FOUND on node %d.\n", reply.node_id);
    }
}

void handleRebalanceAck(const FilterMessage &reply) {
    clearPending(reply.sequence_id);
    Serial.printf("[MASTER] REBALANCE ACK: seq=%u  node=%d  range=[%u..%u]  status=%u\n",
                  reply.sequence_id, reply.node_id,
                  reply.partition_lo, reply.partition_hi, reply.status_code);
}

void handleDrainAck(const FilterMessage &reply) {
    clearPending(reply.sequence_id);
    Serial.printf("[MASTER] DRAIN ACK: seq=%u  node=%d → dest=%d  "
                  "range=[%u..%u]  migrated=%u  status=%u\n",
                  reply.sequence_id, reply.node_id, reply.dest_node_id,
                  reply.partition_lo, reply.partition_hi,
                  reply.key_fingerprint,
                  reply.status_code);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 12 – Partition Rebalancing
// ─────────────────────────────────────────────────────────────────────────────

void sendRebalanceCommand(uint8_t nodeIdx) {
    if (!slaves[nodeIdx].registered || !slaves[nodeIdx].online) return;

    FilterMessage msg;
    memset(&msg, 0, sizeof(msg));
    msg.operation_type = OP_REBALANCE;
    msg.sequence_id    = ++messageCounter;
    msg.node_id        = nodeIdx;
    msg.partition_lo   = slaves[nodeIdx].partition_lo;
    msg.partition_hi   = slaves[nodeIdx].partition_hi;

    Serial.printf("[MASTER] >> REBALANCE → node %d: [%u..%u]  seq=%u\n",
                  nodeIdx, msg.partition_lo, msg.partition_hi, msg.sequence_id);
    sendToNode(nodeIdx, msg);
}

void sendDrainCommand(uint8_t srcNode, uint8_t dstNode, uint16_t lo, uint16_t hi) {
    if (!slaves[srcNode].registered || !slaves[srcNode].online) return;

    FilterMessage msg;
    memset(&msg, 0, sizeof(msg));
    msg.operation_type = OP_DRAIN;
    msg.sequence_id    = ++messageCounter;
    msg.node_id        = srcNode;
    msg.dest_node_id   = dstNode;
    msg.partition_lo   = lo;
    msg.partition_hi   = hi;
    memcpy(msg.key_data, slaves[dstNode].mac, 6);  // pack dest MAC for slave→slave send

    Serial.printf("[MASTER] >> DRAIN: node %d → node %d  [%u..%u]  seq=%u\n",
                  srcNode, dstNode, lo, hi, msg.sequence_id);
    sendToNode(srcNode, msg);
}

// [FIX 4] addNode aborts cleanly if MAC is wrong / peer registration fails
void addNode(uint8_t nodeIdx) {
    if (nodeIdx >= MAX_SLAVES) return;
    if (slaves[nodeIdx].online) {
        Serial.printf("[MASTER] addNode(%d): already online.\n", nodeIdx);
        return;
    }

    Serial.printf("[MASTER] ===== NODE JOIN: node %d =====\n", nodeIdx);

    uint16_t old_lo[MAX_SLAVES], old_hi[MAX_SLAVES];
    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        old_lo[i] = slaves[i].partition_lo;
        old_hi[i] = slaves[i].partition_hi;
    }

    slaves[nodeIdx].online = true;

    if (!slaves[nodeIdx].registered) {
        if (!registerSlavePeer(nodeIdx)) {
            Serial.printf("[MASTER] ERROR: node %d registration failed – "
                          "addNode aborted. Fix MAC in slaves[%d].\n",
                          nodeIdx, nodeIdx);
            slaves[nodeIdx].online = false;  // roll back
            return;
        }
    }

    activeSlaveCount++;
    recomputePartitions();

    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (i == nodeIdx || !slaves[i].online) continue;
        int olo = max((int)old_lo[i], (int)slaves[nodeIdx].partition_lo);
        int ohi = min((int)old_hi[i], (int)slaves[nodeIdx].partition_hi);
        if (olo <= ohi) {
            sendDrainCommand(i, nodeIdx, (uint16_t)olo, (uint16_t)ohi);
            delay(60);
        }
    }

    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (slaves[i].online) { sendRebalanceCommand(i); delay(60); }
    }

    Serial.printf("[MASTER] NODE JOIN complete. Active slaves: %d\n", activeSlaveCount);
}

void removeNode(uint8_t nodeIdx, bool graceful = true) {
    if (nodeIdx >= MAX_SLAVES || !slaves[nodeIdx].online) return;

    Serial.printf("[MASTER] ===== NODE LEAVE: node %d (graceful=%s) =====\n",
                  nodeIdx, graceful ? "yes" : "no");

    uint16_t leaving_lo = slaves[nodeIdx].partition_lo;
    uint16_t leaving_hi = slaves[nodeIdx].partition_hi;

    slaves[nodeIdx].online = false;
    activeSlaveCount--;
    recomputePartitions();

    if (graceful) {
        for (uint8_t i = 0; i < MAX_SLAVES; i++) {
            if (i == nodeIdx || !slaves[i].online) continue;
            int olo = max((int)leaving_lo, (int)slaves[i].partition_lo);
            int ohi = min((int)leaving_hi, (int)slaves[i].partition_hi);
            if (olo <= ohi) {
                sendDrainCommand(nodeIdx, i, (uint16_t)olo, (uint16_t)ohi);
                delay(60);
            }
        }
    } else {
        Serial.println("[MASTER] WARNING: ungraceful leave – "
                       "data in lost partition recovered via replication in M3.");
    }

    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (slaves[i].online) { sendRebalanceCommand(i); delay(60); }
    }

    Serial.printf("[MASTER] NODE LEAVE complete. Active slaves: %d\n", activeSlaveCount);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 13 – Peer Registration
// ─────────────────────────────────────────────────────────────────────────────

bool registerSlavePeer(uint8_t nodeIndex) {
    if (nodeIndex >= MAX_SLAVES) return false;
    esp_now_peer_info_t peerInfo;
    memset(&peerInfo, 0, sizeof(peerInfo));
    memcpy(peerInfo.peer_addr, slaves[nodeIndex].mac, 6);
    peerInfo.channel = 0;
    peerInfo.encrypt = false;

    esp_err_t err = esp_now_add_peer(&peerInfo);
    if (err != ESP_OK) {
        Serial.printf("[MASTER] ERROR: esp_now_add_peer node %d failed (0x%X)\n",
                      nodeIndex, err);
        return false;
    }
    slaves[nodeIndex].registered = true;
    Serial.printf("[MASTER] Node %d registered: ", nodeIndex);
    printMacAddress(slaves[nodeIndex].mac);
    Serial.println();
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 14 – Stats Report
// ─────────────────────────────────────────────────────────────────────────────

void printStats() {
    Serial.println("\n╔═══════════════════════════════════════════╗");
    Serial.println("║      MILESTONE 2 – STATS REPORT           ║");
    Serial.println("╠═══════════════════════════════════════════╣");
    Serial.printf( "║  Total Inserts    : %6u                 ║\n", totalInserts);
    Serial.printf( "║  Successful       : %6u                 ║\n", successfulInserts);
    Serial.printf( "║  Failed (full)    : %6u                 ║\n", failedInserts);
    Serial.printf( "║  Total Deletes    : %6u                 ║\n", totalDeletes);
    Serial.printf( "║  Successful Del.  : %6u                 ║\n", successfulDeletes);
    Serial.printf( "║  Total Lookups    : %6u                 ║\n", totalLookups);
    Serial.printf( "║  True  Positives  : %6u                 ║\n", truePositives);
    Serial.printf( "║  True  Negatives  : %6u                 ║\n", trueNegatives);
    Serial.printf( "║  False Positives  : %6u                 ║\n", falsePositives);
    Serial.printf( "║  False Negatives  : %6u  (unexpected)   ║\n", falseNegatives);
    if (totalLookups > 0) {
        float fpr = (100.0f * falsePositives) / totalLookups;
        Serial.printf("║  FP Rate          : %5.2f%%                  ║\n", fpr);
    }
    Serial.println("╠═══════════════════════════════════════════╣");
    Serial.printf( "║  Timeouts         : %6u                 ║\n", totalTimeouts);
    Serial.printf( "║  Retries          : %6u                 ║\n", totalRetries);
    Serial.println("╚═══════════════════════════════════════════╝\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 15 – ESP-NOW Callbacks
// ─────────────────────────────────────────────────────────────────────────────

#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
void onDataSent(const wifi_tx_info_t *tx_info, esp_now_send_status_t status) {
#else
void onDataSent(const uint8_t *mac_addr, esp_now_send_status_t status) {
#endif
    if (status != ESP_NOW_SEND_SUCCESS)
        Serial.println("[MASTER] SEND FAILED at MAC layer.");
}

#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
void onDataRecv(const esp_now_recv_info_t *recv_info, const uint8_t *data, int len) {
    const uint8_t *mac_addr = recv_info->src_addr;
#else
void onDataRecv(const uint8_t *mac_addr, const uint8_t *data, int len) {
#endif
    if (len != sizeof(FilterMessage)) {
        Serial.printf("[MASTER] WARNING: unexpected packet size %d (expected %d)\n",
                      len, (int)sizeof(FilterMessage));
        return;
    }

    FilterMessage incoming;
    memcpy(&incoming, data, sizeof(FilterMessage));

    Serial.print("[MASTER] << Reply from ");
    printMacAddress(mac_addr);
    Serial.printf("  op=%d  seq=%u  status=%u\n",
                  incoming.operation_type, incoming.sequence_id, incoming.status_code);

    switch (incoming.operation_type) {

        // ── OP_ACK: INSERT ack or DELETE ack ───────────────────────────────
        case OP_ACK: {
            uint8_t origOp = pending[incoming.sequence_id % PENDING_SLOTS].op;

            if (incoming.status_code == STATUS_FORWARD_INSERT) {
                // Cross-partition kick-out: re-route the evicted fingerprint
                Serial.printf("[MASTER] Cross-partition kick-out: "
                              "fp=0x%04X  bucket=%u – re-routing.\n",
                              incoming.key_fingerprint, incoming.bucket_index);

                clearPending(incoming.sequence_id);  // original done; new seq will track fwd

                uint8_t newTarget = selectTargetNode(incoming.bucket_index);
                if (newTarget >= MAX_SLAVES || !slaves[newTarget].registered) {
                    Serial.println("[MASTER] ERROR: no registered node for kick-out target.");
                    failedInserts++;
                    break;
                }
                FilterMessage fwd;
                memset(&fwd, 0, sizeof(fwd));
                fwd.operation_type  = OP_INSERT;
                fwd.sequence_id     = ++messageCounter;
                fwd.key_fingerprint = incoming.key_fingerprint;
                fwd.bucket_index    = incoming.bucket_index;
                fwd.node_id         = newTarget;
                fwd.partition_lo    = slaves[newTarget].partition_lo;
                fwd.partition_hi    = slaves[newTarget].partition_hi;
                strncpy(fwd.key_data, incoming.key_data, sizeof(fwd.key_data) - 1);
                sendToNode(newTarget, fwd);

            } else if (incoming.status_code == STATUS_REMOTE_CHECK) {
                // [FIX 2] DELETE hit one local bucket but alt bucket is on another node
                if (origOp == OP_DELETE) {
                    clearPending(incoming.sequence_id);
                    issueRemoteCheck(OP_DELETE, incoming);
                } else {
                    Serial.printf("[MASTER] REMOTE_CHECK on INSERT (unexpected) seq=%u\n",
                                  incoming.sequence_id);
                    clearPending(incoming.sequence_id);
                }
            } else {
                // Normal INSERT or DELETE completion
                if (origOp == OP_DELETE) {
                    handleDeleteAck(incoming);
                } else {
                    handleInsertAck(incoming);
                }
            }
            break;
        }

        // ── OP_RESULT: LOOKUP result ────────────────────────────────────────
        case OP_RESULT: {
            if (incoming.status_code == STATUS_REMOTE_CHECK && !incoming.result_found) {
                // [FIX 1] Key not in local bucket; alt bucket is remote → check other node
                clearPending(incoming.sequence_id);
                issueRemoteCheck(OP_LOOKUP, incoming);
            } else {
                // Definitive answer (found, or both buckets exhausted)
                aggregateLookupResult(incoming);
            }
            break;
        }

        case OP_REBALANCE:
            handleRebalanceAck(incoming);
            break;

        case OP_DRAIN:
            handleDrainAck(incoming);
            break;

        case OP_SYNC:
            clearPending(incoming.sequence_id);
            Serial.printf("[MASTER] SYNC CONFIRM: seq=%u  node=%d  status=%u\n",
                          incoming.sequence_id, incoming.node_id, incoming.status_code);
            break;

        default:
            Serial.printf("[MASTER] Unknown incoming op: %d\n", incoming.operation_type);
            break;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 16 – Test Harness (Milestone 2)
//
// The harness runs AFTER the demo phases complete (demoPhase == 6).
// It uses the existing routeRequest() machinery — no separate response flag
// needed because the master already tracks pending slots asynchronously.
//
// Phases:
//   1 – INSERT all 20 keys, one per loop tick
//   2 – LOOKUP all 20 keys (should all be FOUND)
//   3 – DELETE all 20 keys
//   4 – VERIFY (LOOKUP after delete, should all be NOT FOUND)
//   5 – LATENCY report (uses timestamps captured during INSERT + LOOKUP phases)
//   6 – Done / idle
// ─────────────────────────────────────────────────────────────────────────────

// Called from onDataRecv to capture latency for test-harness keys
void captureTestLatency(const FilterMessage &reply) {
    int8_t idx = seqToTestIdx[reply.sequence_id % 256];
    if (idx < 0 || idx >= totalTestKeys) return;

    uint8_t origOp = pending[reply.sequence_id % PENDING_SLOTS].op;

    if (origOp == OP_INSERT && !insertDone[idx]) {
        insertLatency[idx] = millis() - insertSentAt[idx];
        insertDone[idx] = true;
    } else if (origOp == OP_LOOKUP && testPhase == 2 && !lookupDone[idx]) {
        lookupLatency[idx] = millis() - lookupSentAt[idx];
        lookupDone[idx] = true;
    } else if (origOp == OP_DELETE && !deleteDone[idx]) {
        deleteDone[idx] = true;
    } else if (origOp == OP_LOOKUP && testPhase == 4 && !verifyDone[idx]) {
        verifyDone[idx] = true;
    }
}

void runTestHarnessStep() {
    if (!testHarnessActive) return;

    switch (testPhase) {

        // ── Phase 1: INSERT all keys ──────────────────────────────────────────
        case 1: {
            if (testKeyIdx == 0)
                Serial.println("\n===== MILESTONE 2 DISTRIBUTED TESTS =====\n"
                               "--- INSERT TEST ---");

            if (testKeyIdx < totalTestKeys) {
                Serial.printf("[TEST] INSERT [%d/%d]: %s\n",
                              testKeyIdx + 1, totalTestKeys, testKeys[testKeyIdx]);
                uint32_t seq = messageCounter + 1;  // next seq_id
                insertSentAt[testKeyIdx] = millis();
                seqToTestIdx[seq % 256] = (int8_t)testKeyIdx;
                routeRequest(OP_INSERT, testKeys[testKeyIdx]);
                testKeyIdx++;
            } else {
                Serial.println("[TEST] INSERT phase complete. Waiting 2s...");
                delay(2000);
                testPhase = 2;
                testKeyIdx = 0;
            }
            break;
        }

        // ── Phase 2: LOOKUP all keys (should all be FOUND) ───────────────────
        case 2: {
            if (testKeyIdx == 0)
                Serial.println("\n--- LOOKUP TEST (expect all FOUND) ---");

            if (testKeyIdx < totalTestKeys) {
                Serial.printf("[TEST] LOOKUP [%d/%d]: %s\n",
                              testKeyIdx + 1, totalTestKeys, testKeys[testKeyIdx]);
                uint32_t seq = messageCounter + 1;
                lookupSentAt[testKeyIdx] = millis();
                seqToTestIdx[seq % 256] = (int8_t)testKeyIdx;
                routeRequest(OP_LOOKUP, testKeys[testKeyIdx]);
                testKeyIdx++;
            } else {
                Serial.println("[TEST] LOOKUP phase complete. Waiting 2s...");
                delay(2000);
                testPhase = 3;
                testKeyIdx = 0;
            }
            break;
        }

        // ── Phase 3: DELETE all keys ──────────────────────────────────────────
        case 3: {
            if (testKeyIdx == 0)
                Serial.println("\n--- DELETE TEST ---");

            if (testKeyIdx < totalTestKeys) {
                Serial.printf("[TEST] DELETE [%d/%d]: %s\n",
                              testKeyIdx + 1, totalTestKeys, testKeys[testKeyIdx]);
                uint32_t seq = messageCounter + 1;
                seqToTestIdx[seq % 256] = (int8_t)testKeyIdx;
                routeRequest(OP_DELETE, testKeys[testKeyIdx]);
                testKeyIdx++;
            } else {
                Serial.println("[TEST] DELETE phase complete. Waiting 2s...");
                delay(2000);
                testPhase = 4;
                testKeyIdx = 0;
            }
            break;
        }

        // ── Phase 4: VERIFY DELETE (LOOKUP, expect NOT FOUND) ────────────────
        case 4: {
            if (testKeyIdx == 0)
                Serial.println("\n--- VERIFY DELETE (expect all NOT FOUND) ---");

            if (testKeyIdx < totalTestKeys) {
                Serial.printf("[TEST] VERIFY [%d/%d]: %s\n",
                              testKeyIdx + 1, totalTestKeys, testKeys[testKeyIdx]);
                uint32_t seq = messageCounter + 1;
                seqToTestIdx[seq % 256] = (int8_t)testKeyIdx;
                routeRequest(OP_LOOKUP, testKeys[testKeyIdx]);
                testKeyIdx++;
            } else {
                delay(2000);
                testPhase = 5;
                testKeyIdx = 0;
            }
            break;
        }

        // ── Phase 5: Print latency report ────────────────────────────────────
        case 5: {
            Serial.println("\n--- LATENCY REPORT ---");
            Serial.println("  Key              | INSERT ms | LOOKUP ms");
            Serial.println("  -----------------|-----------|----------");
            unsigned long totalIns = 0, totalLkp = 0;
            for (int i = 0; i < totalTestKeys; i++) {
                Serial.printf("  %-16s |   %5lu   |   %5lu\n",
                              testKeys[i], insertLatency[i], lookupLatency[i]);
                totalIns += insertLatency[i];
                totalLkp += lookupLatency[i];
            }
            Serial.println("  -----------------|-----------|----------");
            Serial.printf("  AVG              |   %5lu   |   %5lu\n",
                          totalIns / totalTestKeys, totalLkp / totalTestKeys);

            Serial.println("\n===== ALL TESTS COMPLETE =====");
            testPhase = 6;
            testHarnessActive = false;
            break;
        }

        default:
            break;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 17 – setup()
// ─────────────────────────────────────────────────────────────────────────────

void setup() {
    Serial.begin(115200);
    delay(500);

    // Initialise pending table
    memset(pending, 0, sizeof(pending));
    memset(seqToTestIdx, -1, sizeof(seqToTestIdx));

    Serial.println("\n========================================");
    Serial.println("  Distributed Cuckoo Filter - MASTER  ");
    Serial.println("         Milestone 2 (M2)             ");
    Serial.println("========================================");

    WiFi.mode(WIFI_STA);
    WiFi.disconnect();
    delay(100);

    Serial.print("[MASTER] Own MAC: ");
    Serial.println(WiFi.macAddress());

    if (esp_now_init() != ESP_OK) {
        Serial.println("[MASTER] FATAL: esp_now_init() failed. Halting.");
        while (true) delay(1000);
    }
    Serial.println("[MASTER] ESP-NOW initialised.");

    esp_now_register_send_cb(onDataSent);
    esp_now_register_recv_cb(onDataRecv);

    // Register and bring online the initial 2 slaves
    for (uint8_t i = 0; i < activeSlaveCount; i++) {
        slaves[i].online = true;
        if (!registerSlavePeer(i)) {
            Serial.printf("[MASTER] WARNING: node %d registration failed – "
                          "fix MAC in slaves[%d] and re-flash.\n", i, i);
            slaves[i].online = false;
            activeSlaveCount--;
        }
    }

    recomputePartitions();

    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (slaves[i].online) { sendRebalanceCommand(i); delay(80); }
    }

    Serial.println("[MASTER] Ready.\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 17 – loop()
//
// Demo phases:
//   A – Basic ops on 2 nodes
//   B – Add node 2 → 3-way rebalance
//   C – Ops across all 3 nodes
//   D – Graceful remove node 1 → 2-way rebalance
//   E – Verify surviving keys + new ops on 2 remaining nodes
//   F – Print stats, then repeat
//
// checkTimeouts() is called every iteration to detect and retry
// any requests that have not received a reply within REQUEST_TIMEOUT_MS.
// ─────────────────────────────────────────────────────────────────────────────

static unsigned long lastOpTime  = 0;
static int           opStep      = 0;
static int           demoPhase   = 0;
static bool          phasePrinted = false;

void loop() {
    // [FIX 6 & 7] Always check for timed-out requests every loop tick
    checkTimeouts();

    // Run test harness step-by-step (uses its own 500 ms pacing)
    if (testHarnessActive) {
        static unsigned long lastTestStep = 0;
        if (millis() - lastTestStep >= 500) {
            lastTestStep = millis();
            runTestHarnessStep();
        }
        return;  // don't run demo phases while test harness is active
    }

    if (millis() - lastOpTime < 2000) return;
    lastOpTime = millis();

    switch (demoPhase) {

        // ── Phase A: 2-node baseline ──────────────────────────────────────────
        case 0: {
            if (!phasePrinted) {
                Serial.println("\n=== PHASE A: 2-node distributed operations ===");
                phasePrinted = true;
            }
            const char* keys[] = {
                "sensor_temp_01", "sensor_hum_02", "sensor_pres_03",
                "sensor_temp_01", "sensor_hum_02",
                "sensor_hum_02",  "sensor_hum_02"
            };
            uint8_t ops[] = {
                OP_INSERT, OP_INSERT, OP_INSERT,
                OP_LOOKUP, OP_LOOKUP,
                OP_DELETE, OP_LOOKUP
            };
            if (opStep < 7) {
                routeRequest(ops[opStep], keys[opStep]);
                opStep++;
            } else {
                opStep = 0; demoPhase = 1; phasePrinted = false;
            }
            break;
        }

        // ── Phase B: add node 2 ───────────────────────────────────────────────
        case 1: {
            if (!phasePrinted) {
                Serial.println("\n=== PHASE B: adding node 2 (rebalancing 2 → 3) ===");
                addNode(2);
                phasePrinted = true;
            }
            if (opStep++ >= 4) {
                opStep = 0; demoPhase = 2; phasePrinted = false;
            }
            break;
        }

        // ── Phase C: 3-node ops ───────────────────────────────────────────────
        case 2: {
            if (!phasePrinted) {
                Serial.println("\n=== PHASE C: 3-node distributed operations ===");
                phasePrinted = true;
            }
            const char* keys[] = {
                "actuator_fan_01", "actuator_led_02",
                "sensor_temp_01",  "actuator_fan_01",
                "nonexistent_key"
            };
            uint8_t ops[] = {
                OP_INSERT, OP_INSERT,
                OP_LOOKUP, OP_LOOKUP,
                OP_LOOKUP   // never inserted → expect NOT FOUND
            };
            if (opStep < 5) {
                routeRequest(ops[opStep], keys[opStep]);
                opStep++;
            } else {
                opStep = 0; demoPhase = 3; phasePrinted = false;
            }
            break;
        }

        // ── Phase D: graceful remove node 1 ──────────────────────────────────
        case 3: {
            if (!phasePrinted) {
                Serial.println("\n=== PHASE D: removing node 1 gracefully ===");
                removeNode(1, true);
                phasePrinted = true;
            }
            if (opStep++ >= 4) {
                opStep = 0; demoPhase = 4; phasePrinted = false;
            }
            break;
        }

        // ── Phase E: verify surviving keys + new ops on nodes 0 & 2 ──────────
        case 4: {
            if (!phasePrinted) {
                Serial.println("\n=== PHASE E: 2-node ops after rebalance (nodes 0 & 2) ===");
                phasePrinted = true;
            }
            const char* keys[] = {
                "sensor_temp_01",  "actuator_fan_01",
                "brand_new_key_1", "brand_new_key_1",
                "brand_new_key_2", "brand_new_key_2"
            };
            uint8_t ops[] = {
                OP_LOOKUP, OP_LOOKUP,
                OP_INSERT, OP_LOOKUP,
                OP_INSERT, OP_LOOKUP
            };
            if (opStep < 6) {
                routeRequest(ops[opStep], keys[opStep]);
                opStep++;
            } else {
                opStep = 0; demoPhase = 5; phasePrinted = false;
            }
            break;
        }

        // ── Phase F: print stats, then start test harness ────────────────────
        case 5: {
            printStats();
            opStep = 0; demoPhase = 6; phasePrinted = false;
            // ── Launch test harness once demo completes ──
            Serial.println("\n[MASTER] Demo phases done. Starting Milestone 2 test harness...");
            delay(3000);
            memset(seqToTestIdx, -1, sizeof(seqToTestIdx));
            memset(insertDone,   0,  sizeof(insertDone));
            memset(lookupDone,   0,  sizeof(lookupDone));
            memset(deleteDone,   0,  sizeof(deleteDone));
            memset(verifyDone,   0,  sizeof(verifyDone));
            testPhase  = 1;
            testKeyIdx = 0;
            testHarnessActive = true;
            break;
        }

        // ── Phase G: idle after test harness ─────────────────────────────────
        case 6: {
            // Test harness runs via runTestHarnessStep(); nothing to do here
            break;
        }
    }
}
