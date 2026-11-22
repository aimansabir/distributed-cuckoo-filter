// =============================================================================
// master_node.ino
// Distributed Cuckoo Filter – Milestone 1
// Role: MASTER NODE
//
// Responsibilities:
//   - Initializes ESP-NOW
//   - Registers one or more slave peers by MAC address
//   - Periodically sends a dummy FilterMessage to the slave every 3 seconds
//   - Receives acknowledgment messages from the slave
// =============================================================================

#include <Arduino.h>
#include <WiFi.h>
#include <esp_now.h>

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 - Operation Codes (keep in sync with slave_node.ino)
// ─────────────────────────────────────────────────────────────────────────────

#define OP_DUMMY   0
#define OP_INSERT  1
#define OP_DELETE  2
#define OP_LOOKUP  3
#define OP_SYNC    4
#define OP_ACK     5
#define OP_RESULT  6

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 - Message Format
// Keep IDENTICAL in both master_node.ino and slave_node.ino.
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
} FilterMessage;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3 - Slave Peer Table
// ─────────────────────────────────────────────────────────────────────────────

#define MAX_SLAVES 4

typedef struct {
    uint8_t mac[6];
    bool    registered;
    uint8_t node_id;
} SlaveInfo;

SlaveInfo slaves[MAX_SLAVES] = {
    { {0xF0, 0x08, 0xD1, 0x62, 0xDA, 0x30}, false, 0 },
    { {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, false, 1 },
    { {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, false, 2 },
    { {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, false, 3 },
};

static uint8_t activeSlaveCount = 1;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 - Internal State
// ─────────────────────────────────────────────────────────────────────────────

static uint32_t messageCounter   = 0;
static uint32_t pendingLookupSeq = 0;
static bool     awaitingLookup   = false;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 - Hash / Partitioning Helpers
// ─────────────────────────────────────────────────────────────────────────────

uint16_t computeFingerprint(const char *key) {
    uint32_t hash = 2166136261UL;
    while (*key) {
        hash ^= (uint8_t)(*key++);
        hash *= 16777619UL;
    }
    uint16_t fp = (uint16_t)((hash >> 16) ^ (hash & 0xFFFF));
    return fp == 0 ? 1 : fp;
}

#define BUCKET_COUNT 256

uint16_t computePrimaryBucket(const char *key) {
    uint32_t hash = 0;
    while (*key) {
        hash = hash * 31 + (uint8_t)(*key++);
    }
    return (uint16_t)(hash % BUCKET_COUNT);
}

uint8_t selectTargetNode(uint16_t bucket_index) {
    if (activeSlaveCount == 0) return 0;
    return (uint8_t)(bucket_index % activeSlaveCount);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 6 - Helper: Print MAC Address
// ─────────────────────────────────────────────────────────────────────────────

void printMacAddress(const uint8_t *mac) {
    for (int i = 0; i < 6; i++) {
        if (i > 0) Serial.print(":");
        Serial.printf("%02X", mac[i]);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 7 - Master Routing Logic 
// ─────────────────────────────────────────────────────────────────────────────

uint8_t buildMessage(FilterMessage &msg, uint8_t op, const char *key) {
    msg.operation_type  = op;
    msg.sequence_id     = ++messageCounter;
    msg.key_fingerprint = computeFingerprint(key);
    msg.bucket_index    = computePrimaryBucket(key);
    msg.result_found    = 0;
    msg.status_code     = 0;
    msg.node_id         = selectTargetNode(msg.bucket_index);
    strncpy(msg.key_data, key, sizeof(msg.key_data) - 1);
    msg.key_data[sizeof(msg.key_data) - 1] = '\0';
    return msg.node_id;
}

void routeRequest(uint8_t op, const char *key) {
    FilterMessage msg;
    uint8_t targetNode = buildMessage(msg, op, key);

    if (targetNode >= MAX_SLAVES || !slaves[targetNode].registered) {
        Serial.printf("[MASTER] ROUTE ERROR: node %d not registered. seq=%u skipped.\n",
                      targetNode, msg.sequence_id);
        return;
    }

    const char *opNames[] = {"DUMMY","INSERT","DELETE","LOOKUP","SYNC","ACK","RESULT"};
    Serial.println("----------------------------------------");
    Serial.printf("[MASTER] >> Routing %s (seq=%u) to node %d\n",
                  opNames[op], msg.sequence_id, targetNode);
    Serial.printf("[MASTER]    key             : \"%s\"\n",  msg.key_data);
    Serial.printf("[MASTER]    key_fingerprint : 0x%04X\n",  msg.key_fingerprint);
    Serial.printf("[MASTER]    bucket_index    : %u\n",      msg.bucket_index);

    if (op == OP_LOOKUP) {
        pendingLookupSeq = msg.sequence_id;
        awaitingLookup   = true;
    }

    esp_err_t err = esp_now_send(
        slaves[targetNode].mac,
        reinterpret_cast<const uint8_t *>(&msg),
        sizeof(msg)
    );
    if (err != ESP_OK) {
        Serial.printf("[MASTER] ERROR: esp_now_send failed (0x%X)\n", err);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 8 - Result Aggregation Handlers 
// ─────────────────────────────────────────────────────────────────────────────

void aggregateLookupResult(const uint8_t *fromMac, const FilterMessage &reply) {
    Serial.printf("[MASTER] LOOKUP RESULT: seq=%u  key=\"%s\"  found=%s  status=%u\n",
                  reply.sequence_id, reply.key_data,
                  reply.result_found ? "YES" : "NO", reply.status_code);
    if (reply.sequence_id == pendingLookupSeq) awaitingLookup = false;
}

void handleInsertAck(const FilterMessage &reply) {
    if (reply.status_code == 0)
        Serial.printf("[MASTER] INSERT ACK: seq=%u  key=\"%s\"  OK\n",
                      reply.sequence_id, reply.key_data);
    else if (reply.status_code == 1)
        Serial.printf("[MASTER] INSERT ACK: seq=%u  FILTER FULL on node %d\n",
                      reply.sequence_id, reply.node_id);
    else
        Serial.printf("[MASTER] INSERT ACK: seq=%u  status=%u\n",
                      reply.sequence_id, reply.status_code);
}

void handleDeleteAck(const FilterMessage &reply) {
    if (reply.status_code == 0)
        Serial.printf("[MASTER] DELETE ACK: seq=%u  key=\"%s\"  OK\n",
                      reply.sequence_id, reply.key_data);
    else if (reply.status_code == 2)
        Serial.printf("[MASTER] DELETE ACK: seq=%u  key NOT FOUND on node %d\n",
                      reply.sequence_id, reply.node_id);
    else
        Serial.printf("[MASTER] DELETE ACK: seq=%u  status=%u\n",
                      reply.sequence_id, reply.status_code);
}

void handleSyncConfirmation(const uint8_t *fromMac, const FilterMessage &reply) {
    Serial.printf("[MASTER] SYNC CONFIRM: seq=%u  node=%d  status=%u\n",
                  reply.sequence_id, reply.node_id, reply.status_code);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 9 - ESP-NOW Callbacks (SDK 3.x signatures)
// ─────────────────────────────────────────────────────────────────────────────

void onDataSent(const wifi_tx_info_t *tx_info, esp_now_send_status_t status) {
    Serial.print("[MASTER] Send status -> ");
    printMacAddress(tx_info->des_addr);
    Serial.println(status == ESP_NOW_SEND_SUCCESS ? " : DELIVERED" : " : FAILED");
}

void onDataRecv(const esp_now_recv_info *recv_info, const uint8_t *data, int len) {
    if (len != sizeof(FilterMessage)) {
        Serial.printf("[MASTER] WARNING: unexpected size %d (expected %d)\n",
                      len, (int)sizeof(FilterMessage));
        return;
    }

    FilterMessage incoming;
    memcpy(&incoming, data, sizeof(FilterMessage));

    Serial.print("[MASTER] Incoming from ");
    printMacAddress(recv_info->src_addr);
    Serial.println();

    switch (incoming.operation_type) {
        case OP_ACK:
            if      (incoming.status_code == 1) handleInsertAck(incoming);
            else if (incoming.status_code == 2) handleDeleteAck(incoming);
            else Serial.printf("[MASTER]   ACK OK  seq=%u  node=%d  key=\"%s\"\n",
                               incoming.sequence_id, incoming.node_id, incoming.key_data);
            break;
        case OP_RESULT:
            aggregateLookupResult(recv_info->src_addr, incoming);
            break;
        case OP_SYNC:
            handleSyncConfirmation(recv_info->src_addr, incoming);
            break;
        default:
            Serial.printf("[MASTER]   Unknown op: %d\n", incoming.operation_type);
            break;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 10 - Peer Registration
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
// SECTION 11 - setup()
// ─────────────────────────────────────────────────────────────────────────────

void setup() {
    Serial.begin(115200);
    delay(500);

    Serial.println("\n========================================");
    Serial.println("  Distributed Cuckoo Filter - MASTER  ");
    Serial.println("========================================");

    WiFi.mode(WIFI_STA);
    WiFi.disconnect();
    delay(100);

    Serial.print("[MASTER] Own MAC : ");
    Serial.println(WiFi.macAddress());
    Serial.println("[MASTER] (Share above MAC with slave for ACK replies)");

    if (esp_now_init() != ESP_OK) {
        Serial.println("[MASTER] FATAL: esp_now_init() failed. Halting.");
        while (true) { delay(1000); }
    }
    Serial.println("[MASTER] ESP-NOW initialised.");

    esp_now_register_send_cb(onDataSent);
    esp_now_register_recv_cb(onDataRecv);

    for (uint8_t i = 0; i < activeSlaveCount; i++) {
        if (!registerSlavePeer(i))
            Serial.printf("[MASTER] WARNING: Node %d not registered. "
                          "Fix slaves[%d].mac[] and re-flash.\n", i, i);
    }

    Serial.println("[MASTER] Ready. Routing test operations every 3 s.\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 12 - loop()
// Cycles INSERT, LOOKUP, DELETE, LOOKUP, SYNC for M1 testing.
// Unregistered nodes are silently skipped so placeholder MACs are safe.
// ─────────────────────────────────────────────────────────────────────────────

void loop() {
    routeRequest(OP_INSERT, "sensor_temp_01");
    delay(3000);
    routeRequest(OP_LOOKUP, "sensor_temp_01");
    delay(3000);
    routeRequest(OP_DELETE, "sensor_temp_01");
    delay(3000);
    routeRequest(OP_LOOKUP, "sensor_temp_01");
    delay(3000);
    routeRequest(OP_SYNC, "");
    delay(3000);
}
