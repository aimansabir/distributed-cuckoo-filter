// =============================================================================
// cuckoo_m3_master.ino  –  Distributed Cuckoo Filter  –  Milestone 3
// Role: MASTER NODE
//
// HOW TO FLASH:
//   1. Flash cuckoo_m3_slave.ino on each of the 3 slave ESP32s.
//      Set THIS_NODE_ID = 0 / 1 / 2 on each slave respectively.
//   2. Open each slave's Serial Monitor → copy the MAC printed on boot.
//   3. Paste all three MACs into the slaves[] table in SECTION 3 below.
//   4. Flash THIS file on the master ESP32.
//   5. Open master Serial Monitor at 115200 baud.
//
// For 3-node testing set activeSlaveCount = 3 before flashing (default below).
// =============================================================================

#include <Arduino.h>
#include <WiFi.h>
#include <esp_now.h>
#include <esp_idf_version.h>

// Forward declarations
bool registerSlavePeer(uint8_t nodeIndex);
void captureTestLatency(const struct FilterMessage &reply);
void handleNodeFailure(uint8_t failedNode);
void sendRebalanceCommand(uint8_t nodeIdx);
void sendSyncRequest(uint8_t nodeIdx);
void triggerRepairSweep();

uint32_t failoverCount = 0;

#define SEQ_RING 1024
static int16_t seqToBenchIdx[SEQ_RING];
// marks which sequence IDs belong to Phase 6 FP-test queries (FIX-3)
static bool    seqIsFpTest[SEQ_RING];
// marks which sequence IDs belong to Phase 3a DELETE-test queries
static bool    seqIsDeleteTest[SEQ_RING];

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 – Operation Codes 
// ─────────────────────────────────────────────────────────────────────────────

#define OP_DUMMY        0
#define OP_INSERT       1
#define OP_DELETE       2
#define OP_LOOKUP       3
#define OP_SYNC         4   // request slave to report fingerprint count
#define OP_ACK          5
#define OP_RESULT       6
#define OP_REBALANCE    7
#define OP_DRAIN        8
#define OP_NODE_JOIN    9
#define OP_NODE_LEAVE   10
#define OP_HEARTBEAT    11  
#define OP_HEARTBEAT_ACK 12 
#define OP_REPLICATE    13  

#define STATUS_OK             0
#define STATUS_FILTER_FULL    1
#define STATUS_NOT_FOUND      2
#define STATUS_REMOTE_CHECK   3
#define STATUS_FORWARD_INSERT 4
#define STATUS_WRONG_OWNER    5

// Master-only marker: tells the master this request is already the second
// bucket check. Prevents LOOKUP/DELETE from bouncing forever between buckets.
#define REMOTE_PROBE_MARKER 0xFE

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
// SECTION 3 – Slave Peer Table
// ─────────────────────────────────────────────────────────────────────────────

#define MAX_SLAVES   3
#define BUCKET_COUNT 256  // must match slave

typedef struct {
    uint8_t  mac[6];
    bool     registered;
    bool     online;
    uint8_t  node_id;
    uint16_t partition_lo;
    uint16_t partition_hi;
    // M3 heartbeat fields
    uint32_t lastHeartbeatSentAt;    // millis() of last HEARTBEAT sent to this node
    uint32_t lastHeartbeatAckAt;     // millis() of last HEARTBEAT_ACK received
    uint8_t  missedHeartbeats;       // consecutive missed ACKs
    bool     failDetected;           // true once we've declared this node failed
    // M3 sync field
    uint16_t reportedCount;          // fingerprint count from last SYNC reply
} SlaveInfo;

SlaveInfo slaves[MAX_SLAVES] = {
    // node 0 – paste MAC from slave 0 Serial Monitor here
    { {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0x00}, false, false, 0, 0, 0, 0, 0, 0, false, 0 },
    // node 1 – paste MAC from slave 1 Serial Monitor here
    { {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0x01}, false, false, 1, 0, 0, 0, 0, 0, false, 0 },
    // node 2 – paste MAC from slave 2 Serial Monitor here
    { {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0x02}, false, false, 2, 0, 0, 0, 0, 0, false, 0 },
};

static uint8_t activeSlaveCount = 3;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 – M3 Heartbeat & Replication Config
// ─────────────────────────────────────────────────────────────────────────────

// How often the master sends a ping to each slave (milliseconds)
#define HEARTBEAT_INTERVAL_MS   2000

// After this many consecutive missed pongs, the slave is declared FAILED
#define HEARTBEAT_MISS_LIMIT    3

// Replication factor: each primary write is mirrored to this many extra nodes.
// 1 = one replica (two total copies).  Set to 0 to disable replication.
#define REPLICATION_FACTOR      1

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 – Internal Pending-Request Tracker
// ─────────────────────────────────────────────────────────────────────────────

#define PENDING_SLOTS      1024
#define REQUEST_TIMEOUT_MS 3000
#define MAX_RETRIES        3

typedef struct {
    uint32_t      sentAt;
    uint8_t       op;
    uint8_t       targetNode;
    uint8_t       retryCount;
    bool          active;
    FilterMessage msg;
} PendingRequest;

static PendingRequest pending[PENDING_SLOTS];
static uint32_t       messageCounter = 0;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 6 – Known-Set  (ground truth for FP/FN tracking)
// ─────────────────────────────────────────────────────────────────────────────

#define KNOWN_SET_SIZE 256
static char    knownKeys[KNOWN_SET_SIZE][20] = {{}};
static bool    keyPresent[KNOWN_SET_SIZE]    = {false};
static uint8_t knownKeyCount                = 0;

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
// SECTION 7 – Hash / Fingerprint Helpers  
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

uint16_t computeAltBucket(uint16_t primary, uint16_t fp) {
    uint32_t hash = fp * 0x5bd1e995;
    hash ^= hash >> 15;
    uint16_t offset = (uint16_t)(hash % BUCKET_COUNT);
    if (offset == 0) offset = 1;
    return (primary ^ offset) % BUCKET_COUNT;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 8 – Partitioning
// Divides BUCKET_COUNT evenly across activeSlaveCount online nodes.
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

    Serial.println("[MASTER] Partition map:");
    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (slaves[i].online)
            Serial.printf("[MASTER]   node %d  buckets [%3u .. %3u]\n",
                          i, slaves[i].partition_lo, slaves[i].partition_hi);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 9 – Routing Helpers
// ─────────────────────────────────────────────────────────────────────────────

uint8_t selectTargetNode(uint16_t bucket_index) {
    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (slaves[i].online &&
            bucket_index >= slaves[i].partition_lo &&
            bucket_index <= slaves[i].partition_hi)
            return i;
    }
    Serial.printf("[MASTER] ROUTING ERROR: no owner for bucket %u – check partition map!\n",
                  bucket_index);
    for (uint8_t i = 0; i < MAX_SLAVES; i++)
        if (slaves[i].online) return i;
    return 0;
}

// For replication: pick the next online node that is NOT the primary
uint8_t selectReplicaNode(uint8_t primaryNode) {
    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (i != primaryNode && slaves[i].online) return i;
    }
    return 0xFF;  // no replica available (single-node mode)
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

    uint16_t slot = msg.sequence_id % PENDING_SLOTS;
    pending[slot].sentAt     = millis();
    pending[slot].op         = msg.operation_type;
    pending[slot].targetNode = nodeIdx;
    pending[slot].retryCount = 0;   // always reset – prevents stale count on slot reuse
    pending[slot].active     = true;
    memcpy(&pending[slot].msg, &msg, sizeof(FilterMessage));

    esp_err_t err = esp_now_send(slaves[nodeIdx].mac,
                                 reinterpret_cast<const uint8_t*>(&msg),
                                 sizeof(msg));
    if (err != ESP_OK)
        Serial.printf("[MASTER] ERROR: esp_now_send to node %d failed (0x%X)\n", nodeIdx, err);
}

// Preserve benchmark/test metadata when a logical request is re-issued under
// a new sequence ID, such as REMOTE_CHECK forwarding or forwarded INSERT.
void copySequenceTracking(uint32_t oldSeq, uint32_t newSeq) {
    uint16_t oldSlot = oldSeq % SEQ_RING;
    uint16_t newSlot = newSeq % SEQ_RING;

    seqToBenchIdx[newSlot]   = seqToBenchIdx[oldSlot];
    seqIsFpTest[newSlot]     = seqIsFpTest[oldSlot];
    seqIsDeleteTest[newSlot] = seqIsDeleteTest[oldSlot];
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 10 – Main Route + Replicate
//
// routeRequest() sends a primary operation to the appropriate slave and, for
// INSERT, issues a REPLICATE message to one additional slave so that every
// fingerprint is stored on two nodes.  This means a single node failure does
// not lose any data.
// ─────────────────────────────────────────────────────────────────────────────

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
    msg.replica_node_id = 0xFF;

    const char *opNames[] = {
        "DUMMY","INSERT","DELETE","LOOKUP","SYNC",
        "ACK","RESULT","REBALANCE","DRAIN","NODE_JOIN","NODE_LEAVE",
        "HEARTBEAT","HEARTBEAT_ACK","REPLICATE"
    };
    Serial.println("----------------------------------------");
    Serial.printf("[MASTER] >> %s seq=%u  key=\"%s\"\n",
                  opNames[op], msg.sequence_id, key);
    Serial.printf("[MASTER]    fp=0x%04X  bucket=%u  node=%d  [%u..%u]\n",
                  msg.key_fingerprint, msg.bucket_index,
                  tgt, msg.partition_lo, msg.partition_hi);

    sendToNode(tgt, msg);

    // NOTE: Replication for OP_INSERT is NOT sent here.
    // It is sent only after the primary INSERT ACK is confirmed STATUS_OK
    // inside handleInsertAck().  This guarantees we never replicate a key
    // that was never durably stored on the primary node.

    return tgt;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 11 – Timeout / Retry
// ─────────────────────────────────────────────────────────────────────────────

static uint32_t totalTimeouts = 0;
static uint32_t totalRetries  = 0;

void checkTimeouts() {
    uint32_t now = millis();
    for (int i = 0; i < PENDING_SLOTS; i++) {
        if (!pending[i].active) continue;
        if (now - pending[i].sentAt < REQUEST_TIMEOUT_MS) continue;

        uint8_t  node = pending[i].targetNode;
        uint32_t seq  = pending[i].msg.sequence_id;

        // Skip heartbeats – they have their own miss counter
        if (pending[i].op == OP_HEARTBEAT) {
            pending[i].active = false;
            continue;
        }

        if (pending[i].retryCount < MAX_RETRIES) {
            pending[i].retryCount++;
            pending[i].sentAt = now;
            totalRetries++;
            Serial.printf("[MASTER] TIMEOUT: seq=%u op=%d node=%d – retry %d/%d\n",
                          seq, pending[i].op, node,
                          pending[i].retryCount, MAX_RETRIES);
            esp_now_send(slaves[node].mac,
                         reinterpret_cast<const uint8_t*>(&pending[i].msg),
                         sizeof(FilterMessage));
        } else {
            totalTimeouts++;
            Serial.printf("[MASTER] GAVE UP: seq=%u op=%d node=%d – "
                          "no response after %d retries.\n",
                          seq, pending[i].op, node, MAX_RETRIES);
            pending[i].active = false;
        }
    }
}

void clearPending(uint32_t seq_id) {
    pending[seq_id % PENDING_SLOTS].active = false;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 12 – Heartbeat System
//
// Every HEARTBEAT_INTERVAL_MS the master sends OP_HEARTBEAT to each online
// slave.  If a slave misses HEARTBEAT_MISS_LIMIT consecutive pings its
// missedHeartbeats counter reaches the limit and handleNodeFailure() is called.
//
// On OP_HEARTBEAT_ACK the slave's missedHeartbeats counter is reset to zero.
// ─────────────────────────────────────────────────────────────────────────────

static uint32_t lastHeartbeatTime      = 0;
static uint32_t lastHeartbeatCheckTime = 0; 

void sendHeartbeats() {
    bool sentAny = false;

    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (!slaves[i].online || !slaves[i].registered) continue;

        FilterMessage hb;
        memset(&hb, 0, sizeof(hb));
        hb.operation_type = OP_HEARTBEAT;
        hb.sequence_id    = ++messageCounter;
        hb.node_id        = i;

        slaves[i].lastHeartbeatSentAt = millis();

        sendToNode(i, hb);
        sentAny = true;

        Serial.printf("[MASTER] HEARTBEAT -> node %d  seq=%u\n", i, hb.sequence_id);
    }

    (void)sentAny;
}

void checkHeartbeatMisses() {
    uint32_t now = millis();
    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (!slaves[i].online || slaves[i].failDetected) continue;
        if (slaves[i].lastHeartbeatSentAt == 0) continue;

        if (now - slaves[i].lastHeartbeatSentAt < HEARTBEAT_INTERVAL_MS) continue;

        // If no ACK arrived since the last ping was sent...
        if (slaves[i].lastHeartbeatAckAt < slaves[i].lastHeartbeatSentAt) {
            slaves[i].missedHeartbeats++;
            Serial.printf("[MASTER] ⚠ Node %d missed heartbeat (%d/%d)\n",
                          i, slaves[i].missedHeartbeats, HEARTBEAT_MISS_LIMIT);
        } else {
            slaves[i].missedHeartbeats = 0;  // pong received in time
        }

        if (slaves[i].missedHeartbeats >= HEARTBEAT_MISS_LIMIT) {
            Serial.printf("[MASTER] ✗ Node %d declared FAILED after %d missed heartbeats!\n",
                          i, slaves[i].missedHeartbeats);
            slaves[i].failDetected = true;
            handleNodeFailure(i);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 13 – Failover
//
// When a node is declared failed:
//   1. Mark it offline.
//   2. Recompute partition map across surviving nodes.
//   3. Inform surviving slaves of their new partition ranges via REBALANCE.
//   4. Because data was replicated (REPLICATION_FACTOR >= 1), the fingerprints
//      of the lost partition are already present on at least one survivor –
//      no re-insert phase is required.
//   5. Issue a SYNC sweep to verify replica counts match expectations.
// ─────────────────────────────────────────────────────────────────────────────

void handleNodeFailure(uint8_t failedNode) {
    failoverCount++;

    Serial.printf("\n[MASTER] ========== FAILOVER: node %d ==========\n", failedNode);
    Serial.println("[MASTER] Replication ensures no data loss.");

    slaves[failedNode].online = false;
    if (activeSlaveCount > 0) activeSlaveCount--;

    recomputePartitions();

    // Tell survivors about their new partition ranges
    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (slaves[i].online && slaves[i].registered) {
            sendRebalanceCommand(i);
            delay(80);
        }
    }

    // Repair sweep: after an ungraceful heartbeat-detected failure, the new
    // partition owner may not be the same node that held the original replica.
    // Re-replicating known live keys to all survivors prevents post-failover
    // false negatives under the new partition map.
    delay(500);
    triggerRepairSweep();

    // Sync sweep: verify that replicas are consistent after repair.
    delay(500);
    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (slaves[i].online && slaves[i].registered) {
            sendSyncRequest(i);
            delay(80);
        }
    }

    Serial.printf("[MASTER] Failover complete. Active slaves: %d\n", activeSlaveCount);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 14 – Remote-Check Resolution 
// ─────────────────────────────────────────────────────────────────────────────

void issueRemoteCheck(uint8_t op, const FilterMessage &originalReply) {
    uint16_t fp      = originalReply.key_fingerprint;
    uint16_t primBkt = originalReply.bucket_index;
    uint16_t altBkt  = computeAltBucket(primBkt, fp);
    uint8_t  altNode = selectTargetNode(altBkt);

    Serial.printf("[MASTER] REMOTE_CHECK: forwarding %s fp=0x%04X -> alt-bucket %u node %d\n",
                  op == OP_LOOKUP ? "LOOKUP" : "DELETE", fp, altBkt, altNode);

    if (!slaves[altNode].registered) {
        Serial.printf("[MASTER] ERROR: alt node %d not registered - REMOTE_CHECK dropped.\n",
                      altNode);
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

    // second bucket check.
    // If this also returns STATUS_REMOTE_CHECK, the key is NOT_FOUND.
    fwd.dest_node_id    = REMOTE_PROBE_MARKER;
    fwd.replica_node_id = 0xFF;

    strncpy(fwd.key_data, originalReply.key_data, sizeof(fwd.key_data) - 1);
    fwd.key_data[sizeof(fwd.key_data) - 1] = '\0';

    copySequenceTracking(originalReply.sequence_id, fwd.sequence_id);

    sendToNode(altNode, fwd);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 15 – Partition Rebalancing & Draining  
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
    // Graceful leave marks the source offline before repartitioning, but the
    // old source is still physically reachable and must be allowed to drain.
    if (!slaves[srcNode].registered) return;

    FilterMessage msg;
    memset(&msg, 0, sizeof(msg));
    msg.operation_type = OP_DRAIN;
    msg.sequence_id    = ++messageCounter;
    msg.node_id        = srcNode;
    msg.dest_node_id   = dstNode;
    msg.partition_lo   = lo;
    msg.partition_hi   = hi;
    memcpy(msg.key_data, slaves[dstNode].mac, 6);

    Serial.printf("[MASTER] >> DRAIN: node %d → node %d  [%u..%u]  seq=%u\n",
                  srcNode, dstNode, lo, hi, msg.sequence_id);
    sendToNode(srcNode, msg);
}

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
            slaves[nodeIdx].online = false;
            Serial.printf("[MASTER] ERROR: node %d registration failed – addNode aborted.\n", nodeIdx);
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

            sendRebalanceCommand(i);
            delay(60);
        }
    }
    // Send initial rebalance to the newly joined node
    sendRebalanceCommand(nodeIdx);

    Serial.printf("[MASTER] NODE JOIN complete. Active slaves: %d\n", activeSlaveCount);
}

void removeNode(uint8_t nodeIdx, bool graceful = true) {
    if (nodeIdx >= MAX_SLAVES || !slaves[nodeIdx].online) return;

    Serial.printf("[MASTER] ===== NODE LEAVE: node %d (graceful=%s) =====\n",
                  nodeIdx, graceful ? "yes" : "no");

    uint16_t leaving_lo = slaves[nodeIdx].partition_lo;
    uint16_t leaving_hi = slaves[nodeIdx].partition_hi;

    slaves[nodeIdx].online = false;
    // Prevent the heartbeat watchdog from calling handleNodeFailure() again
    // for this node after removeNode() has already handled the failover.
    slaves[nodeIdx].failDetected = true;
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
                
                sendRebalanceCommand(nodeIdx);
                delay(60);
            }
        }
    } else {
        // Ungraceful: replication already covers data loss
        Serial.println("[MASTER] Ungraceful leave – replicas cover lost partition.");
        // Still rebalance survivors so they know their new expanded ranges.
        for (uint8_t i = 0; i < MAX_SLAVES; i++) {
            if (slaves[i].online && slaves[i].registered) {
                sendRebalanceCommand(i);
                delay(60);
            }
        }
        delay(500);
        triggerRepairSweep();
    }

    Serial.printf("[MASTER] NODE LEAVE complete. Active slaves: %d\n", activeSlaveCount);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 16 – Sync Request
//
// OP_SYNC asks a slave to report its current fingerprint count.
// ─────────────────────────────────────────────────────────────────────────────

// How many successful primary inserts should each online node hold (approx).
// After a 3→2 failover: each survivor absorbs half of node-1's ~20 keys, so
// the expected primary count grows.  We track the actual confirmed count via
// successfulInserts and estimate per-node share.

void triggerRepairSweep() {
    Serial.println("[MASTER] *** REPAIR SWEEP: re-replicating all known keys to all online nodes ***");
    uint32_t sent = 0;
    for (int i = 0; i < knownKeyCount; i++) {
        if (!keyPresent[i]) continue;
        uint16_t fp  = computeFingerprint(knownKeys[i]);
        uint16_t bkt = computePrimaryBucket(knownKeys[i]);
        for (uint8_t rep = 0; rep < MAX_SLAVES; rep++) {
            if (!slaves[rep].online || !slaves[rep].registered) continue;
            FilterMessage rm;
            memset(&rm, 0, sizeof(rm));
            rm.operation_type  = OP_REPLICATE;
            rm.sequence_id     = ++messageCounter;
            rm.key_fingerprint = fp;
            rm.bucket_index    = bkt;
            rm.node_id         = rep;
            rm.partition_lo    = 0;
            rm.partition_hi    = BUCKET_COUNT - 1;
            rm.replica_node_id = rep;
            strncpy(rm.key_data, knownKeys[i], sizeof(rm.key_data) - 1);
            rm.key_data[sizeof(rm.key_data) - 1] = 0;
            sendToNode(rep, rm);
            sent++;
            delay(20);
        }
    }
    Serial.printf("[MASTER] Repair sweep queued %u replica message(s).\n", sent);
}
void sendSyncRequest(uint8_t nodeIdx) {
    if (!slaves[nodeIdx].registered || !slaves[nodeIdx].online) return;

    FilterMessage msg;
    memset(&msg, 0, sizeof(msg));
    msg.operation_type = OP_SYNC;
    msg.sequence_id    = ++messageCounter;
    msg.node_id        = nodeIdx;

    Serial.printf("[MASTER] >> SYNC → node %d  seq=%u\n", nodeIdx, msg.sequence_id);
    sendToNode(nodeIdx, msg);
}

void handleSyncReply(const FilterMessage &reply) {
    clearPending(reply.sequence_id);
    uint8_t n = reply.node_id;
    if (n >= MAX_SLAVES) return;

    // FIX-C: read both counts packed by the slave
    uint16_t primaryCount  = reply.key_fingerprint;
    uint16_t replicaCount  = reply.bucket_index;
    uint16_t totalReported = primaryCount + replicaCount;

    slaves[n].reportedCount = totalReported;

    Serial.printf("[MASTER] SYNC REPLY: node=%d  primary=%u  replica=%u  "
                  "total=%u  seq=%u\n",
                  n, primaryCount, replicaCount, totalReported,
                  reply.sequence_id);

    // Divergence check: after a failover each survivor should hold its own
    // primary keys PLUS at least one set of replicated keys.  We flag a
    // mismatch when replica count is 0 and we know replication was enabled.
    if (REPLICATION_FACTOR >= 1 && activeSlaveCount >= 2 && replicaCount == 0) {
        Serial.printf("[MASTER] ⚠ CONSISTENCY WARNING: node %d reports 0 "
                      "replicas — possible replication gap. Triggering repair.\n", n);
        triggerRepairSweep();
    } else {
        Serial.printf("[MASTER]   Consistency OK: node %d holds %u primary + "
                      "%u replica fingerprints.\n", n, primaryCount, replicaCount);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 17 – Peer Registration
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
// SECTION 18 – Statistics Counters
// ─────────────────────────────────────────────────────────────────────────────

static uint32_t totalInserts      = 0;
static uint32_t successfulInserts = 0;
static uint32_t failedInserts     = 0;
static uint32_t totalDeletes      = 0;
static uint32_t successfulDeletes = 0;
static uint32_t totalLookups      = 0;
static uint32_t fpLookups         = 0;   
static uint32_t truePositives     = 0;
static uint32_t trueNegatives     = 0;
static uint32_t falsePositives    = 0;
static uint32_t falseNegatives    = 0;
static uint32_t replicaInserts    = 0; 
// failoverCount is declared at file top 

void printStats() {
    
    float fpr = (fpLookups > 0) ? (100.0f * falsePositives / fpLookups) : 0.0f;

    Serial.println("\n╔══════════════════════════════════════════════╗");
    Serial.println("║    MILESTONE 3 – PERFORMANCE REPORT          ║");
    Serial.println("╠══════════════════════════════════════════════╣");
    Serial.printf( "║  Topology      : %d active node(s)             ║\n", activeSlaveCount);
    Serial.printf( "║  Replication   : %d replica(s) per key         ║\n", REPLICATION_FACTOR);
    Serial.println("╠══════════════════════════════════════════════╣");
    Serial.printf( "║  Total Inserts    : %6u                      ║\n", totalInserts);
    Serial.printf( "║  Successful       : %6u                      ║\n", successfulInserts);
    Serial.printf( "║  Failed (full)    : %6u                      ║\n", failedInserts);
    Serial.printf( "║  Replica Inserts  : %6u                      ║\n", replicaInserts);
    Serial.printf( "║  Total Deletes    : %6u  (Phase 3a)          ║\n", totalDeletes);
    Serial.printf( "║  Successful Del.  : %6u                      ║\n", successfulDeletes);
    Serial.printf( "║  Total Lookups    : %6u                      ║\n", totalLookups);
    Serial.printf( "║  FP-Test Lookups  : %6u  (Phase 7 only)     ║\n", fpLookups);
    Serial.printf( "║  True  Positives  : %6u  (Ph2+Ph3b+Ph5)     ║\n", truePositives);
    Serial.printf( "║  True  Negatives  : %6u  (Ph3b+Ph7)         ║\n", trueNegatives);
    Serial.printf( "║  False Positives  : %6u                      ║\n", falsePositives);
    Serial.printf( "║  False Negatives  : %6u  (unexpected!)       ║\n", falseNegatives);
    Serial.printf( "║  FP Rate          : %5.2f%%  (Ph7 unseen keys) ║\n", fpr);
    Serial.println("╠══════════════════════════════════════════════╣");
    Serial.printf( "║  Timeouts         : %6u                      ║\n", totalTimeouts);
    Serial.printf( "║  Retries          : %6u                      ║\n", totalRetries);
    Serial.printf( "║  Failovers        : %6u                      ║\n", failoverCount);
    Serial.println("╚══════════════════════════════════════════════╝\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 19 – Reply Handlers
// ─────────────────────────────────────────────────────────────────────────────

static void sendReplicateAfterAck(const char *key, uint16_t fp,
                                   uint16_t bkt, uint8_t primaryNode) {
    if (REPLICATION_FACTOR < 1) return;
    uint8_t repNode = selectReplicaNode(primaryNode);
    if (repNode == 0xFF || repNode >= MAX_SLAVES || !slaves[repNode].registered) return;

    FilterMessage rep;
    memset(&rep, 0, sizeof(rep));
    rep.operation_type  = OP_REPLICATE;
    rep.sequence_id     = ++messageCounter;
    rep.key_fingerprint = fp;
    rep.bucket_index    = bkt;
    rep.node_id         = repNode;
    rep.partition_lo    = 0;
    rep.partition_hi    = BUCKET_COUNT - 1;
    rep.replica_node_id = repNode;
    strncpy(rep.key_data, key, sizeof(rep.key_data) - 1);
    rep.key_data[sizeof(rep.key_data) - 1] = '\0';

    Serial.printf("[MASTER]    → REPLICATE seq=%u  key=\"%s\"  to node %d\n",
                  rep.sequence_id, key, repNode);
    sendToNode(repNode, rep);
}

// Helper: issue a replica DELETE to the replica node that holds a copy of this key.
// Called only after the primary DELETE ACK is confirmed STATUS_OK.
static void sendReplicaDeleteAfterAck(const char *key, uint16_t fp,
                                       uint16_t bkt, uint8_t primaryNode) {
    if (REPLICATION_FACTOR < 1) return;

    // Broadcast replica cleanup to every online non-primary node.  A repair
    // sweep may have placed this replica on more than one survivor, so deleting
    // only selectReplicaNode(primaryNode) can leave a stale ghost copy behind.
    for (uint8_t repNode = 0; repNode < MAX_SLAVES; repNode++) {
        if (repNode == primaryNode) continue;
        if (!slaves[repNode].online || !slaves[repNode].registered) continue;

        FilterMessage del;
        memset(&del, 0, sizeof(del));
        del.operation_type  = OP_DELETE;
        del.sequence_id     = ++messageCounter;
        del.key_fingerprint = fp;
        del.bucket_index    = bkt;
        del.node_id         = repNode;
        del.partition_lo    = 0;
        del.partition_hi    = BUCKET_COUNT - 1;
        del.replica_node_id = repNode;  // marks this as replica-cleanup, not primary delete
        strncpy(del.key_data, key, sizeof(del.key_data) - 1);
        del.key_data[sizeof(del.key_data) - 1] = '\0';

        Serial.printf("[MASTER]    -> REPLICA DELETE seq=%u  key=\"%s\"  to node %d\n",
                      del.sequence_id, key, repNode);
        sendToNode(repNode, del);
        delay(20);
    }
}
void handleInsertAck(const FilterMessage &reply) {
    uint32_t latency = millis() - pending[reply.sequence_id % PENDING_SLOTS].sentAt;
    captureTestLatency(reply);  
    clearPending(reply.sequence_id);

    Serial.printf("[MASTER] INSERT ACK: seq=%u  key=\"%s\"  status=%u  latency=%u ms\n",
                  reply.sequence_id, reply.key_data, reply.status_code, latency);

    if (reply.status_code == STATUS_OK) {
        successfulInserts++;
        markKeyInserted(reply.key_data);
        Serial.printf("[MASTER]   OK – confirmed on node %d.\n", reply.node_id);

        uint16_t fp  = computeFingerprint(reply.key_data);
        uint16_t bkt = computePrimaryBucket(reply.key_data);
        sendReplicateAfterAck(reply.key_data, fp, bkt, reply.node_id);
    } else if (reply.status_code == STATUS_FILTER_FULL) {
        failedInserts++;
        Serial.printf("[MASTER]   FILTER FULL on node %d.\n", reply.node_id);
    } else {
        failedInserts++;
    }
}

void handleReplicateAck(const FilterMessage &reply) {
    uint32_t latency = millis() - pending[reply.sequence_id % PENDING_SLOTS].sentAt;
    clearPending(reply.sequence_id);

    if (reply.status_code == STATUS_OK) {
        replicaInserts++;
        Serial.printf("[MASTER] REPLICATE ACK: seq=%u  node=%d  status=OK  latency=%u ms\n",
                      reply.sequence_id, reply.node_id, latency);
    } else {
        Serial.printf("[MASTER] REPLICATE FAILED: seq=%u  node=%d  status=%u  latency=%u ms\n",
                      reply.sequence_id, reply.node_id, reply.status_code, latency);
    }
}

void aggregateLookupResult(const FilterMessage &reply) {
    uint32_t latency = millis() - pending[reply.sequence_id % PENDING_SLOTS].sentAt;
    captureTestLatency(reply);  
    clearPending(reply.sequence_id);

    totalLookups++;   
    if (seqIsFpTest[reply.sequence_id % SEQ_RING]) {
        fpLookups++;
        seqIsFpTest[reply.sequence_id % SEQ_RING] = false;
    }

    bool expected = wasKeyInserted(reply.key_data);
    bool actual   = (reply.result_found == 1);

    Serial.printf("[MASTER] LOOKUP RESULT: seq=%u  key=\"%s\"  found=%s  latency=%u ms\n",
                  reply.sequence_id, reply.key_data,
                  actual ? "YES" : "NO", latency);

    if ( expected &&  actual) { truePositives++;  Serial.println("[MASTER]   → TRUE  POSITIVE"); }
    if (!expected && !actual) { trueNegatives++;  Serial.println("[MASTER]   → TRUE  NEGATIVE"); }
    if (!expected &&  actual) { falsePositives++; Serial.println("[MASTER]   → FALSE POSITIVE"); }
    if ( expected && !actual) { falseNegatives++; Serial.println("[MASTER]   → FALSE NEGATIVE (unexpected!)"); }
}

void handleDeleteAck(const FilterMessage &reply) {
    PendingRequest &slot = pending[reply.sequence_id % PENDING_SLOTS];
    bool isReplicaDelete = (slot.msg.operation_type == OP_DELETE &&
                            slot.msg.replica_node_id == slot.targetNode &&
                            slot.msg.partition_lo == 0 &&
                            slot.msg.partition_hi == BUCKET_COUNT - 1);

    uint32_t latency = millis() - slot.sentAt;
    if (!isReplicaDelete) captureTestLatency(reply);   // primary delete latency only
    clearPending(reply.sequence_id);

    Serial.printf("[MASTER] DELETE ACK: seq=%u  key=\"%s\"  status=%u  latency=%u ms\n",
                  reply.sequence_id, reply.key_data, reply.status_code, latency);

    if (isReplicaDelete) {
        Serial.printf("[MASTER]   Replica cleanup ACK from node %d (status=%u).\n",
                      reply.node_id, reply.status_code);
        return;
    }

    if (reply.status_code == STATUS_OK) {
        successfulDeletes++;
        markKeyDeleted(reply.key_data);
        Serial.printf("[MASTER]   OK - removed from node %d.\n", reply.node_id);

        // Mirror the delete to all replica holders so a deleted key cannot
        // reappear after failover or after a repair sweep.
        uint16_t fp  = computeFingerprint(reply.key_data);
        uint16_t bkt = computePrimaryBucket(reply.key_data);
        sendReplicaDeleteAfterAck(reply.key_data, fp, bkt, reply.node_id);
    } else if (reply.status_code == STATUS_NOT_FOUND) {
        Serial.printf("[MASTER]   NOT FOUND on node %d.\n", reply.node_id);
    } else if (reply.status_code == STATUS_WRONG_OWNER) {
        Serial.printf("[MASTER]   WRONG_OWNER on node %d - no primary delete performed.\n",
                      reply.node_id);
    } else {
        Serial.printf("[MASTER]   DELETE ACK: unexpected status %u on node %d.\n",
                      reply.status_code, reply.node_id);
    }
}

void handleRebalanceAck(const FilterMessage &reply) {
    clearPending(reply.sequence_id);
    Serial.printf("[MASTER] REBALANCE ACK: seq=%u  node=%d  [%u..%u]  status=%u\n",
                  reply.sequence_id, reply.node_id,
                  reply.partition_lo, reply.partition_hi, reply.status_code);
}

void handleDrainAck(const FilterMessage &reply) {
    clearPending(reply.sequence_id);
    Serial.printf("[MASTER] DRAIN ACK: seq=%u  node=%d → dest=%d  "
                  "[%u..%u]  migrated=%u  status=%u\n",
                  reply.sequence_id, reply.node_id, reply.dest_node_id,
                  reply.partition_lo, reply.partition_hi,
                  reply.key_fingerprint, reply.status_code);

 
    uint8_t dstNode = reply.dest_node_id;
    if (dstNode < MAX_SLAVES && slaves[dstNode].online && slaves[dstNode].registered) {
        Serial.printf("[MASTER] DRAIN complete – sending REBALANCE to dest node %d\n",
                      dstNode);
        sendRebalanceCommand(dstNode);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 20 – ESP-NOW Callbacks
// ─────────────────────────────────────────────────────────────────────────────

#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
void onDataSent(const wifi_tx_info_t *tx_info, esp_now_send_status_t status) {
    (void)tx_info;
#else
void onDataSent(const uint8_t *mac_addr, esp_now_send_status_t status) {
    (void)mac_addr;
#endif
    if (status != ESP_NOW_SEND_SUCCESS)
        Serial.println("[MASTER] MAC-layer SEND FAILED.");
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

        case OP_HEARTBEAT_ACK: {
            // Reset missed-heartbeat counter for this node
            uint8_t n = incoming.node_id;
            if (n < MAX_SLAVES) {
                slaves[n].lastHeartbeatAckAt = millis();
                slaves[n].missedHeartbeats   = 0;
            }
            clearPending(incoming.sequence_id);
            Serial.printf("[MASTER] ♥ HEARTBEAT_ACK from node %d\n", n);
            break;
        }

        case OP_ACK: {
            uint8_t origOp = pending[incoming.sequence_id % PENDING_SLOTS].op;

            if (incoming.status_code == STATUS_FORWARD_INSERT) {
                // Cross-partition kick-out
                clearPending(incoming.sequence_id);

                uint8_t newTarget = selectTargetNode(incoming.bucket_index);
                if (newTarget >= MAX_SLAVES || !slaves[newTarget].registered) {
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
                fwd.replica_node_id = 0xFF;

                strncpy(fwd.key_data, incoming.key_data, sizeof(fwd.key_data) - 1);
                fwd.key_data[sizeof(fwd.key_data) - 1] = '\0';

                copySequenceTracking(incoming.sequence_id, fwd.sequence_id);

                sendToNode(newTarget, fwd);

            } else if (incoming.status_code == STATUS_REMOTE_CHECK) {
                bool alreadyRemoteProbe =
                    (pending[incoming.sequence_id % PENDING_SLOTS].msg.dest_node_id == REMOTE_PROBE_MARKER);

                if (origOp == OP_DELETE) {
                    if (alreadyRemoteProbe) {
                        // Both candidate buckets have now been checked.
                        incoming.status_code = STATUS_NOT_FOUND;
                        handleDeleteAck(incoming);
                    } else {
                        clearPending(incoming.sequence_id);
                        issueRemoteCheck(OP_DELETE, incoming);
                    }
                } else {
                    clearPending(incoming.sequence_id);
                }
            } else {
                // Normal INSERT or DELETE ACK
                if (origOp == OP_DELETE) {
                    handleDeleteAck(incoming);
                } else if (origOp == OP_REPLICATE) {
                    handleReplicateAck(incoming);
                } else {
                    handleInsertAck(incoming);
                }
            }
            break;
        }

        case OP_RESULT: {
            if (incoming.status_code == STATUS_REMOTE_CHECK && !incoming.result_found) {
                bool alreadyRemoteProbe =
                    (pending[incoming.sequence_id % PENDING_SLOTS].msg.dest_node_id == REMOTE_PROBE_MARKER);

                if (alreadyRemoteProbe) {
                    // Both candidate buckets have now been checked.
                    incoming.status_code = STATUS_NOT_FOUND;
                    incoming.result_found = 0;
                    aggregateLookupResult(incoming);
                } else {
                    clearPending(incoming.sequence_id);
                    issueRemoteCheck(OP_LOOKUP, incoming);
                }
            } else {
                
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
            handleSyncReply(incoming);
            break;

        default:
            Serial.printf("[MASTER] Unknown incoming op: %d\n", incoming.operation_type);
            break;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 21 – Milestone 3 Benchmark Test Harness
//
// Test sequence (runs automatically after the demo phases):
//
//  Phase 1  – 3-node INSERT       : Insert all 80 keys. Measure per-op latency.
//  Phase 2  – 3-node LOOKUP       : Lookup all 80. Expect all FOUND → TP.
//  Phase 3a – DELETE test         : Delete 10 known keys. Measure delete latency.
//  Phase 3b – Lookup after DELETE : Lookup those 10 keys → expect NOT FOUND (TN).
//  Phase 4  – Simulate crash      : removeNode(1, false) – ungraceful (no drain).
//                                   Because replicas exist, data is not lost.
//  Phase 5  – Post-crash LOOKUP   : Lookup all 80 keys on 2-node topology.
//                                   Expect all still-present keys FOUND.
//  Phase 6  – 2-node INSERT extra : Insert 20 new keys on surviving nodes.
//  Phase 7  – FP test             : Lookup 40 keys that were NEVER inserted.
//                                   Any "found" response is a false positive.
//  Phase 8  – SYNC sweep          : Ask each survivor its fingerprint count.
//  Phase 9  – Stats report        : Print all counters and latency table.
// ─────────────────────────────────────────────────────────────────────────────

// 80-key dataset (IoT sensor/actuator namespace) 
const char* benchKeys[] = {
    // Temperature sensors
    "temp_sensor_01","temp_sensor_02","temp_sensor_03","temp_sensor_04","temp_sensor_05",
    "temp_sensor_06","temp_sensor_07","temp_sensor_08","temp_sensor_09","temp_sensor_10",
    // Humidity sensors
    "hum_sensor_01","hum_sensor_02","hum_sensor_03","hum_sensor_04","hum_sensor_05",
    "hum_sensor_06","hum_sensor_07","hum_sensor_08","hum_sensor_09","hum_sensor_10",
    // Pressure sensors
    "pres_sensor_01","pres_sensor_02","pres_sensor_03","pres_sensor_04","pres_sensor_05",
    "pres_sensor_06","pres_sensor_07","pres_sensor_08","pres_sensor_09","pres_sensor_10",
    // Actuators: fans, LEDs, valves
    "fan_actuator_01","fan_actuator_02","fan_actuator_03","fan_actuator_04","fan_actuator_05",
    "led_actuator_01","led_actuator_02","led_actuator_03","led_actuator_04","led_actuator_05",
    "valve_ctrl_01","valve_ctrl_02","valve_ctrl_03","valve_ctrl_04","valve_ctrl_05",
    // Device registry
    "device_esp32_a","device_esp32_b","device_esp32_c","device_esp32_d","device_esp32_e",
    // Event log keys
    "evt_boot_01","evt_boot_02","evt_alert_03","evt_alert_04","evt_reset_05",
    // Config keys
    "cfg_ssid_main","cfg_ssid_bkup","cfg_interval","cfg_threshold","cfg_version",
    // Additional IoT keys for broader coverage
    "co2_sensor_01","co2_sensor_02","co2_sensor_03","co2_sensor_04","co2_sensor_05",
    "light_sensor_01","light_sensor_02","light_sensor_03","light_sensor_04","light_sensor_05",
    "motor_ctrl_01","motor_ctrl_02","motor_ctrl_03","motor_ctrl_04","motor_ctrl_05",
    "gps_node_01","gps_node_02","gps_node_03","gps_node_04","gps_node_05",
    "ota_job_01","ota_job_02","ota_job_03","ota_job_04","ota_job_05",
    "auth_token_01","auth_token_02","auth_token_03","auth_token_04","auth_token_05"
};
const int benchKeyCount = 80;

// 10 keys that WILL be deleted in Phase 3b (DELETE test).
// These are a subset of benchKeys so we can verify delete-then-lookup returns NOT FOUND.
const char* deleteTestKeys[] = {
    "temp_sensor_01","hum_sensor_01","pres_sensor_01",
    "fan_actuator_01","led_actuator_01",
    "device_esp32_a","evt_boot_01",
    "cfg_ssid_main","co2_sensor_01","motor_ctrl_01"
};
const int deleteTestKeyCount = 10;

// 40 keys that are NEVER inserted – used for FP rate measurement
const char* fpTestKeys[] = {
    "ghost_key_001","ghost_key_002","ghost_key_003","ghost_key_004","ghost_key_005",
    "ghost_key_006","ghost_key_007","ghost_key_008","ghost_key_009","ghost_key_010",
    "phantom_01","phantom_02","phantom_03","phantom_04","phantom_05",
    "phantom_06","phantom_07","phantom_08","phantom_09","phantom_10",
    "never_inserted_a","never_inserted_b","never_inserted_c","never_inserted_d","never_inserted_e",
    "never_inserted_f","never_inserted_g","never_inserted_h","never_inserted_i","never_inserted_j",
    "zz_fake_1","zz_fake_2","zz_fake_3","zz_fake_4","zz_fake_5",
    "zz_fake_6","zz_fake_7","zz_fake_8","zz_fake_9","zz_fake_10"
};
const int fpTestKeyCount = 40;

// 20 extra keys for post-crash insert phase
const char* extraKeys[] = {
    "new_device_01","new_device_02","new_device_03","new_device_04","new_device_05",
    "new_sensor_06","new_sensor_07","new_sensor_08","new_sensor_09","new_sensor_10",
    "extra_cfg_01","extra_cfg_02","extra_cfg_03","extra_cfg_04","extra_cfg_05",
    "extra_act_06","extra_act_07","extra_act_08","extra_act_09","extra_act_10"
};
const int extraKeyCount = 20;

// Latency tracking arrays (one entry per benchKey)
static unsigned long benchInsertSentAt[80]  = {0};
static unsigned long benchLookupSentAt[80]  = {0};
static unsigned long benchInsertLatency[80] = {0};
static unsigned long benchLookupLatency[80] = {0};
static bool          benchInsertDone[80]    = {false};
static bool          benchLookupDone[80]    = {false};

// Phase-4 (post-crash) arrays kept separate so Phase-2 done-flags don't block them.
static unsigned long bench4LookupSentAt[80]  = {0};
static unsigned long bench4LookupLatency[80] = {0};
static bool          bench4LookupDone[80]    = {false};
static bool          benchPhase4Active       = false;

// Delete-phase latency tracking (one entry per deleteTestKey)
static unsigned long benchDeleteSentAt[10]   = {0};
static unsigned long benchDeleteLatency[10]  = {0};
static bool          benchDeleteDone[10]     = {false};

// Sequence-id → bench key index (for latency capture)
// SEQ_RING, seqToBenchIdx[], seqIsFpTest[] are declared at the top of the file
// (before the reply handlers that reference them).  Only the state machine vars
// remain here.

// Test state machine
static int  benchPhase       = 0;
static int  benchKeyIdx      = 0;
static bool benchActive      = false;
static bool crashSimulated   = false;
// FIX-7: non-blocking inter-phase wait
static uint32_t benchWaitUntil  = 0;
static int      benchNextPhase  = 0;
static bool     benchWaiting    = false;

void captureTestLatency(const FilterMessage &reply) {
    int16_t idx = seqToBenchIdx[reply.sequence_id % SEQ_RING];   // FIX-4: int16_t
    if (idx < 0) return;

    uint8_t origOp = pending[reply.sequence_id % PENDING_SLOTS].op;

    if (origOp == OP_INSERT && idx < benchKeyCount && !benchInsertDone[idx]) {
        benchInsertLatency[idx] = millis() - benchInsertSentAt[idx];
        benchInsertDone[idx]    = true;
    } else if (origOp == OP_DELETE &&
               seqIsDeleteTest[reply.sequence_id % SEQ_RING] &&
               idx < deleteTestKeyCount && !benchDeleteDone[idx]) {
        benchDeleteLatency[idx] = millis() - benchDeleteSentAt[idx];
        benchDeleteDone[idx]    = true;
        seqIsDeleteTest[reply.sequence_id % SEQ_RING] = false;
    } else if (origOp == OP_LOOKUP && idx < benchKeyCount) {
        if (benchPhase4Active && !bench4LookupDone[idx]) {
            bench4LookupLatency[idx] = millis() - bench4LookupSentAt[idx];
            bench4LookupDone[idx]    = true;
        } else if (!benchPhase4Active && !benchLookupDone[idx]) {
            benchLookupLatency[idx] = millis() - benchLookupSentAt[idx];
            benchLookupDone[idx]    = true;
        }
    }
}

// Non-blocking inter-phase wait helper (file-scope so it is defined exactly once).
// Usage inside runBenchmarkStep: BENCH_WAIT(ms, nextPhase); return;
#define BENCH_WAIT(ms, nxt) do { \
    benchWaitUntil = millis() + (ms); \
    benchNextPhase = (nxt); \
    benchWaiting   = true; \
} while(0)

void runBenchmarkStep() {
    if (!benchActive) return;

    if (benchWaiting) {
        if (millis() < benchWaitUntil) return;   // still waiting
        benchWaiting = false;
        benchPhase   = benchNextPhase;
        benchKeyIdx  = 0;
        return;
    }

    // BENCH_WAIT is defined at file scope (above runBenchmarkStep).

    switch (benchPhase) {

        // ── Phase 1: INSERT 80 keys across 3 nodes ────────────────────────────
        case 1: {
            if (benchKeyIdx == 0) {
                Serial.println("\n╔══════════════════════════════════════════════╗");
                Serial.println("║    MILESTONE 3 BENCHMARK HARNESS             ║");
                Serial.println("╠══════════════════════════════════════════════╣");
                Serial.println("║  Phase 1: INSERT 80 keys (3-node topology)   ║");
                Serial.println("╚══════════════════════════════════════════════╝");
            }
            if (benchKeyIdx < benchKeyCount) {
                const char *k = benchKeys[benchKeyIdx];
                uint32_t seq  = messageCounter + 1;
                benchInsertSentAt[benchKeyIdx] = millis();
                seqToBenchIdx[seq % SEQ_RING] = (int16_t)benchKeyIdx;
                totalInserts++;
                routeRequest(OP_INSERT, k);
                Serial.printf("[BENCH] INSERT [%d/%d] \"%s\"\n",
                              benchKeyIdx + 1, benchKeyCount, k);
                benchKeyIdx++;
            } else {
                Serial.println("[BENCH] Phase 1 complete – waiting 4s for ACKs...");
                BENCH_WAIT(4000, 2);
            }
            break;
        }

        // ── Phase 2: LOOKUP 80 keys – expect all FOUND (True Positives) ──────
        case 2: {
            if (benchKeyIdx == 0)
                Serial.println("\n[BENCH] Phase 2: LOOKUP all 80 keys (3-node, expect all FOUND)");

            if (benchKeyIdx < benchKeyCount) {
                uint32_t seq  = messageCounter + 1;
                benchLookupSentAt[benchKeyIdx] = millis();
                seqToBenchIdx[seq % SEQ_RING] = (int16_t)benchKeyIdx;
                routeRequest(OP_LOOKUP, benchKeys[benchKeyIdx]);
                benchKeyIdx++;
            } else {
                Serial.println("[BENCH] Phase 2 complete – waiting 3s...");
                BENCH_WAIT(3000, 30);
            }
            break;
        }

        // ── Phase 3a: DELETE 10 known keys ────────────────────────────────────
        // Verifies that DELETE removes the fingerprint so subsequent lookups
        // correctly return NOT FOUND (no deterministic false positive).
        case 30: {
            if (benchKeyIdx == 0) {
                Serial.println("\n╔══════════════════════════════════════════════╗");
                Serial.println("║  Phase 3a: DELETE 10 keys (correctness test)  ║");
                Serial.println("╚══════════════════════════════════════════════╝");
            }
            if (benchKeyIdx < deleteTestKeyCount) {
                const char *k = deleteTestKeys[benchKeyIdx];
                uint32_t seq  = messageCounter + 1;
                benchDeleteSentAt[benchKeyIdx] = millis();
                seqToBenchIdx[seq % SEQ_RING] = (int16_t)benchKeyIdx;
                seqIsDeleteTest[seq % SEQ_RING] = true;
                totalDeletes++;
                routeRequest(OP_DELETE, k);
                Serial.printf("[BENCH] DELETE [%d/%d] \"%s\"\n",
                              benchKeyIdx + 1, deleteTestKeyCount, k);
                benchKeyIdx++;
            } else {
                Serial.println("[BENCH] Phase 3a complete – waiting 2s for ACKs...");
                BENCH_WAIT(2000, 31);
            }
            break;
        }

        // ── Phase 3b: Lookup deleted keys – expect NOT FOUND (True Negatives) ─
        case 31: {
            if (benchKeyIdx == 0)
                Serial.println("\n[BENCH] Phase 3b: LOOKUP 10 deleted keys (expect NOT FOUND → TN)");

            if (benchKeyIdx < deleteTestKeyCount) {
                routeRequest(OP_LOOKUP, deleteTestKeys[benchKeyIdx]);
                benchKeyIdx++;
            } else {
                Serial.println("[BENCH] Phase 3b complete – waiting 2s...");
                BENCH_WAIT(2000, 4);
            }
            break;
        }

        // ── Phase 4: Simulate node 1 crash (ungraceful) ───────────────────────
        case 4: {
            if (!crashSimulated) {
                Serial.println("\n╔══════════════════════════════════════════════╗");
                Serial.println("║  Phase 4: SIMULATING NODE 1 CRASH            ║");
                Serial.println("║  (ungraceful removal – no drain)             ║");
                Serial.println("║  Replication covers lost partition.          ║");
                Serial.println("╚══════════════════════════════════════════════╝");
                // Mark fail-detected BEFORE removeNode() so the heartbeat watchdog
                // cannot race and call handleNodeFailure() concurrently, which would
                // double-count failoverCount and attempt a second failover.
                slaves[1].failDetected = true;
                removeNode(1, false);
                failoverCount++;   // count this benchmark-simulated crash
                crashSimulated = true;
                BENCH_WAIT(2000, 5);
            }
            break;
        }

        // ── Phase 5: LOOKUP all 80 keys post-crash (2-node topology) ─────────
        // If replication worked, still-present keys (non-deleted) should be FOUND.
        // The 10 deleted keys should remain NOT FOUND (not ghost-resurrected).
        case 5: {
            if (benchKeyIdx == 0) {
                Serial.println("\n[BENCH] Phase 5: LOOKUP all 80 keys (2-node after crash)");
                benchPhase4Active = true;
            }

            if (benchKeyIdx < benchKeyCount) {
                uint32_t seq = messageCounter + 1;
                bench4LookupSentAt[benchKeyIdx] = millis();
                seqToBenchIdx[seq % SEQ_RING] = (int16_t)benchKeyIdx;
                routeRequest(OP_LOOKUP, benchKeys[benchKeyIdx]);
                benchKeyIdx++;
            } else {
                benchPhase4Active = false;
                Serial.println("[BENCH] Phase 5 complete – waiting 3s...");
                BENCH_WAIT(3000, 6);
            }
            break;
        }

        // ── Phase 6: INSERT 20 extra keys on surviving 2-node cluster ─────────
        case 6: {
            if (benchKeyIdx == 0)
                Serial.println("\n[BENCH] Phase 6: INSERT 20 new keys (2-node topology)");

            if (benchKeyIdx < extraKeyCount) {
                totalInserts++;
                routeRequest(OP_INSERT, extraKeys[benchKeyIdx]);
                Serial.printf("[BENCH] INSERT EXTRA [%d/%d] \"%s\"\n",
                              benchKeyIdx + 1, extraKeyCount, extraKeys[benchKeyIdx]);
                benchKeyIdx++;
            } else {
                Serial.println("[BENCH] Phase 6 complete – waiting 3s...");
                BENCH_WAIT(3000, 7);
            }
            break;
        }

        // ── Phase 7: FP Test – lookup 40 keys that were NEVER inserted ────────
        case 7: {
            if (benchKeyIdx == 0)
                Serial.println("\n[BENCH] Phase 7: FALSE POSITIVE TEST (40 unseen keys)");

            if (benchKeyIdx < fpTestKeyCount) {
                uint32_t seq = messageCounter + 1;
                seqIsFpTest[seq % SEQ_RING] = true;
                routeRequest(OP_LOOKUP, fpTestKeys[benchKeyIdx]);
                benchKeyIdx++;
            } else {
                Serial.println("[BENCH] Phase 7 complete – waiting 3s...");
                BENCH_WAIT(3000, 8);
            }
            break;
        }

        // ── Phase 8: SYNC sweep – verify replica fingerprint counts ───────────
        case 8: {
            Serial.println("\n[BENCH] Phase 8: SYNC sweep across all online nodes");
            for (uint8_t i = 0; i < MAX_SLAVES; i++) {
                if (slaves[i].online && slaves[i].registered) {
                    sendSyncRequest(i);
                    delay(200);
                }
            }
            Serial.println("[BENCH] SYNC sweep sent. Waiting for replies...");
            BENCH_WAIT(2000, 9);
            break;
        }

        // ── Phase 9: Print full report ─────────────────────────────────────────
        case 9: {
            // ── INSERT + LOOKUP + POST-CRASH latency table ──
            Serial.println("\n╔══════════════════════════════════════════════════════════════╗");
            Serial.println("║   LATENCY REPORT  (INSERT | LOOKUP | POST-CRASH LOOKUP)      ║");
            Serial.println("╠══════════════════════════════════════════════════════════════╣");
            Serial.println("║  Key                 | INSERT ms | LOOKUP ms | POST-CRASH ms ║");
            Serial.println("║  --------------------|-----------|-----------|-------------- ║");

            unsigned long sumIns = 0, sumLkp = 0, sumLkp4 = 0;
            int countIns = 0, countLkp = 0, countLkp4 = 0;
            for (int i = 0; i < benchKeyCount; i++) {
                Serial.printf("║  %-20s |   %5lu   |   %5lu   |      %5lu    ║\n",
                              benchKeys[i],
                              benchInsertLatency[i],
                              benchLookupLatency[i],
                              bench4LookupLatency[i]);
                if (benchInsertDone[i])  { sumIns  += benchInsertLatency[i];  countIns++;  }
                if (benchLookupDone[i])  { sumLkp  += benchLookupLatency[i];  countLkp++;  }
                if (bench4LookupDone[i]) { sumLkp4 += bench4LookupLatency[i]; countLkp4++; }
            }
            Serial.println("║  --------------------|-----------|-----------|-------------- ║");
            Serial.printf( "║  AVG                 |   %5lu   |   %5lu   |      %5lu    ║\n",
                           countIns  ? sumIns  / countIns  : 0,
                           countLkp  ? sumLkp  / countLkp  : 0,
                           countLkp4 ? sumLkp4 / countLkp4 : 0);
            Serial.println("╚══════════════════════════════════════════════════════════════╝\n");

            // ── DELETE latency table ──
            Serial.println("╔══════════════════════════════════════╗");
            Serial.println("║   DELETE LATENCY REPORT              ║");
            Serial.println("╠══════════════════════════════════════╣");
            Serial.println("║  Key                 | DELETE ms    ║");
            Serial.println("║  --------------------|------------- ║");
            unsigned long sumDel = 0; int countDel = 0;
            for (int i = 0; i < deleteTestKeyCount; i++) {
                Serial.printf("║  %-20s |   %5lu       ║\n",
                              deleteTestKeys[i], benchDeleteLatency[i]);
                if (benchDeleteDone[i]) { sumDel += benchDeleteLatency[i]; countDel++; }
            }
            Serial.println("║  --------------------|------------- ║");
            Serial.printf( "║  AVG                 |   %5lu       ║\n",
                           countDel ? sumDel / countDel : 0);
            Serial.println("╚══════════════════════════════════════╝\n");

            // ── SYNC results ──
            Serial.println("[BENCH] SYNC results:");
            for (uint8_t i = 0; i < MAX_SLAVES; i++) {
                if (slaves[i].online)
                    Serial.printf("[BENCH]   node %d: %u fingerprints stored\n",
                                  i, slaves[i].reportedCount);
            }

            printStats();

            Serial.println("\n[BENCH] ===== ALL MILESTONE 3 TESTS COMPLETE =====");
            benchActive = false;
            benchPhase  = 10;
            break;
        }

        case 10:
        default:
            break;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 22 – setup()
// ─────────────────────────────────────────────────────────────────────────────

void setup() {
    Serial.begin(115200);
    delay(500);

    memset(pending,          0, sizeof(pending));
    memset(seqToBenchIdx,   -1, sizeof(seqToBenchIdx));   // FIX-D: SEQ_RING=1024
    memset(seqIsFpTest,      0, sizeof(seqIsFpTest));      // FIX-3 + FIX-D
    memset(seqIsDeleteTest,  0, sizeof(seqIsDeleteTest));

    Serial.println("\n========================================");
    Serial.println("  Distributed Cuckoo Filter - MASTER  ");
    Serial.println("         Milestone 3 (M3)             ");
    Serial.println(" Replication | Heartbeat | Benchmarks ");
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

    // Register all configured slaves and bring online.  Count successes after
    // the loop so one failed registration cannot make the for-loop skip a later node.
    uint8_t configuredSlaves = activeSlaveCount;
    activeSlaveCount = 0;
    for (uint8_t i = 0; i < configuredSlaves && i < MAX_SLAVES; i++) {
        slaves[i].online = true;
        slaves[i].lastHeartbeatAckAt = millis();  // assume alive at boot
        if (!registerSlavePeer(i)) {
            Serial.printf("[MASTER] WARNING: node %d registration failed.\n", i);
            slaves[i].online = false;
        } else {
            activeSlaveCount++;
        }
    }

    // Both heartbeat timers start at millis().  The per-node guard inside
    // checkHeartbeatMisses() (now - lastHeartbeatSentAt < HEARTBEAT_INTERVAL_MS)
    // prevents spurious misses when send and check fire in the same loop() tick.
    lastHeartbeatTime      = millis();
    lastHeartbeatCheckTime = millis();

    recomputePartitions();

    // Send initial REBALANCE to each slave
    for (uint8_t i = 0; i < MAX_SLAVES; i++) {
        if (slaves[i].online) { sendRebalanceCommand(i); delay(80); }
    }

    Serial.println("[MASTER] Ready. Starting benchmark in 5s...\n");
    delay(5000);

    // Launch benchmark harness directly
    benchPhase  = 1;
    benchKeyIdx = 0;
    benchActive = true;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 23 – loop()
//
// Three concurrent concerns run every tick:
//   1. checkTimeouts()    – retry any pending request that has not been ACKed.
//   2. Heartbeat sending  – every HEARTBEAT_INTERVAL_MS ping all live slaves.
//   3. Benchmark harness  – advance the test state machine every 600 ms.
// ─────────────────────────────────────────────────────────────────────────────

static unsigned long lastBenchStep = 0;

void loop() {
    // 1. Timeout / retry check – runs every loop tick
    checkTimeouts();

    // 2. Heartbeat: SEND fires first, CHECK fires one interval later (staggered at boot)
    if (millis() - lastHeartbeatTime >= HEARTBEAT_INTERVAL_MS) {
        lastHeartbeatTime = millis();
        sendHeartbeats();
    }
    if (millis() - lastHeartbeatCheckTime >= HEARTBEAT_INTERVAL_MS) {
        lastHeartbeatCheckTime = millis();
        checkHeartbeatMisses();
    }

    // 3. Benchmark harness – one step every 600 ms
    if (benchActive) {
        if (millis() - lastBenchStep >= 600) {
            lastBenchStep = millis();
            runBenchmarkStep();
        }
    }
}
