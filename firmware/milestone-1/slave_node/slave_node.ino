// =============================================================================
// slave_node.ino
// Distributed Cuckoo Filter - Milestone 1
// Role: SLAVE NODE
//
// Responsibilities:
//   - Initializes ESP-NOW
//   - Registers the master as a peer (needed to send ACKs back)
//   - Receives FilterMessage packets from the master
//   - Prints received contents for debug
//   - Sends an OP_ACK message back to the master
// =============================================================================

#include <Arduino.h>
#include <WiFi.h>
#include <esp_now.h>

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 - Operation Codes (keep in sync with master_node.ino)
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
// MUST be IDENTICAL to master_node.ino.
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
// SECTION 3 - MAC Address Configuration
// ─────────────────────────────────────────────────────────────────────────────

uint8_t masterMacAddress[] = {0xF0, 0x08, 0xD1, 0x62, 0xDA, 0x30};
#define THIS_NODE_ID 0

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 - Internal State
// ─────────────────────────────────────────────────────────────────────────────

static bool peerRegistered = false;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 - Helper: Print MAC Address
// ─────────────────────────────────────────────────────────────────────────────

void printMacAddress(const uint8_t *mac) {
    for (int i = 0; i < 6; i++) {
        if (i > 0) Serial.print(":");
        Serial.printf("%02X", mac[i]);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 6 - Cuckoo Filter Stubs
// Replace return statements with real filter calls.
// ─────────────────────────────────────────────────────────────────────────────

#define BUCKET_COUNT 256
#define SLOTS_PER_BUCKET 4
#define MAX_KICKS 500

// The memory structure for the ESP32
uint16_t filter_buckets[BUCKET_COUNT][SLOTS_PER_BUCKET] = {0};

// Helper function to calculate the alternate bucket
uint16_t getAltBucket(uint16_t current_bucket, uint16_t fp) {
    uint32_t hash = fp * 0x5bd1e995;
    hash ^= hash >> 15;
    uint16_t offset = (uint16_t)(hash % BUCKET_COUNT);
    return (current_bucket ^ offset) % BUCKET_COUNT;
}

bool handleInsert(uint16_t fingerprint, uint16_t bucket_index) {
    Serial.printf("[SLAVE]   INSERT: fp=0x%04X  bucket=%u\n", fingerprint, bucket_index);

    // 1. Try primary bucket
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        if (filter_buckets[bucket_index][i] == 0) {
            filter_buckets[bucket_index][i] = fingerprint;
            return true;
        }
    }

    // 2. Try alternate bucket
    uint16_t alt_bucket = getAltBucket(bucket_index, fingerprint);
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        if (filter_buckets[alt_bucket][i] == 0) {
            filter_buckets[alt_bucket][i] = fingerprint;
            return true;
        }
    }

    // 3. Kick-out loop
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

    Serial.println("[SLAVE]   INSERT FAILED: Filter is FULL.");
    return false;
}

bool handleDelete(uint16_t fingerprint, uint16_t bucket_index) {
    Serial.printf("[SLAVE]   DELETE: fp=0x%04X  bucket=%u\n", fingerprint, bucket_index);

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

bool handleLookup(uint16_t fingerprint, uint16_t bucket_index) {
    Serial.printf("[SLAVE]   LOOKUP: fp=0x%04X  bucket=%u\n", fingerprint, bucket_index);

    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        if (filter_buckets[bucket_index][i] == fingerprint) {
            return true;
        }
    }

    uint16_t alt_bucket = getAltBucket(bucket_index, fingerprint);
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        if (filter_buckets[alt_bucket][i] == fingerprint) {
            return true;
        }
    }

    return false;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 7 - Application Dispatch 
// ─────────────────────────────────────────────────────────────────────────────

bool processFilterMessage(const FilterMessage &msg, FilterMessage &reply) {
    reply.sequence_id     = msg.sequence_id;
    reply.key_fingerprint = msg.key_fingerprint;
    reply.bucket_index    = msg.bucket_index;
    reply.node_id         = THIS_NODE_ID;
    reply.result_found    = 0;
    reply.status_code     = 0;
    strncpy(reply.key_data, msg.key_data, sizeof(reply.key_data) - 1);
    reply.key_data[sizeof(reply.key_data) - 1] = '\0';

    switch (msg.operation_type) {

        case OP_DUMMY:
            Serial.println("[SLAVE]   Processing: DUMMY");
            reply.operation_type = OP_ACK;
            return true;

        case OP_INSERT: {
            Serial.println("[SLAVE]   Processing: INSERT");
            bool ok = handleInsert(msg.key_fingerprint, msg.bucket_index);
            reply.operation_type = OP_ACK;
            reply.status_code    = ok ? 0 : 1;
            return true;
        }

        case OP_DELETE: {
            Serial.println("[SLAVE]   Processing: DELETE");
            bool ok = handleDelete(msg.key_fingerprint, msg.bucket_index);
            reply.operation_type = OP_ACK;
            reply.status_code    = ok ? 0 : 2;
            return true;
        }

        case OP_LOOKUP: {
            Serial.println("[SLAVE]   Processing: LOOKUP");
            bool found = handleLookup(msg.key_fingerprint, msg.bucket_index);
            reply.operation_type = OP_RESULT;
            reply.result_found   = found ? 1 : 0;
            return true;
        }

        case OP_SYNC:
            Serial.println("[SLAVE]   Processing: SYNC");
            reply.operation_type = OP_SYNC;
            reply.status_code    = 3;
            // TODO (m3): serialise filter state into reply
            return true;

        default:
            Serial.printf("[SLAVE]   Unknown op: %d\n", msg.operation_type);
            return false;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 8 - ESP-NOW Callbacks (SDK 3.x signatures)
// ─────────────────────────────────────────────────────────────────────────────

void onDataSent(const wifi_tx_info_t *tx_info, esp_now_send_status_t status) {
    Serial.print("[SLAVE]  ACK send status -> ");
    printMacAddress(tx_info->des_addr);
    Serial.println(status == ESP_NOW_SEND_SUCCESS ? " : DELIVERED" : " : FAILED");
}

void onDataRecv(const esp_now_recv_info *recv_info, const uint8_t *data, int len) {
    Serial.println("\n========================================");
    Serial.print("[SLAVE]  Packet from ");
    printMacAddress(recv_info->src_addr);
    Serial.println();

    if (len != sizeof(FilterMessage)) {
        Serial.printf("[SLAVE]  WARNING: unexpected size %d (expected %d). Ignoring.\n",
                      len, (int)sizeof(FilterMessage));
        return;
    }

    FilterMessage incoming;
    memcpy(&incoming, data, sizeof(FilterMessage));

    Serial.printf("[SLAVE]    op              : %d\n",     incoming.operation_type);
    Serial.printf("[SLAVE]    sequence_id     : %u\n",     incoming.sequence_id);
    Serial.printf("[SLAVE]    key_fingerprint : 0x%04X\n", incoming.key_fingerprint);
    Serial.printf("[SLAVE]    bucket_index    : %u\n",     incoming.bucket_index);
    Serial.printf("[SLAVE]    key_data        : \"%s\"\n", incoming.key_data);

    FilterMessage reply;
    bool shouldReply = processFilterMessage(incoming, reply);

    if (shouldReply) {
        if (peerRegistered) {
            esp_err_t err = esp_now_send(
                masterMacAddress,
                reinterpret_cast<const uint8_t *>(&reply),
                sizeof(reply)
            );
            if (err != ESP_OK)
                Serial.printf("[SLAVE]  ERROR: esp_now_send failed (0x%X)\n", err);
            else
                Serial.printf("[SLAVE]  Reply queued: op=%d  seq=%u  found=%d  status=%u\n",
                              reply.operation_type, reply.sequence_id,
                              reply.result_found, reply.status_code);
        } else {
            Serial.println("[SLAVE]  WARNING: master not registered; cannot reply.");
        }
    }
    Serial.println("========================================");
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 9 - Peer Registration
// ─────────────────────────────────────────────────────────────────────────────

bool registerMasterPeer(const uint8_t *mac) {
    esp_now_peer_info_t peerInfo;
    memset(&peerInfo, 0, sizeof(peerInfo));
    memcpy(peerInfo.peer_addr, mac, 6);
    peerInfo.channel = 0;
    peerInfo.encrypt = false;
    esp_err_t err = esp_now_add_peer(&peerInfo);
    if (err != ESP_OK) {
        Serial.printf("[SLAVE]  ERROR: esp_now_add_peer failed (0x%X)\n", err);
        return false;
    }
    Serial.print("[SLAVE]  Master registered: ");
    printMacAddress(mac);
    Serial.println();
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 10 - setup()
// ─────────────────────────────────────────────────────────────────────────────

void setup() {
    Serial.begin(115200);
    delay(500);

    Serial.println("\n========================================");
    Serial.println("  Distributed Cuckoo Filter - SLAVE   ");
    Serial.println("========================================");

    WiFi.mode(WIFI_STA);
    WiFi.disconnect();
    delay(100);

    Serial.print("[SLAVE]  Own MAC : ");
    Serial.println(WiFi.macAddress());
    Serial.println("[SLAVE]  (Copy above into master's slaves[0].mac[])");

    if (esp_now_init() != ESP_OK) {
        Serial.println("[SLAVE]  FATAL: esp_now_init() failed. Halting.");
        while (true) { delay(1000); }
    }
    Serial.println("[SLAVE]  ESP-NOW initialised.");

    esp_now_register_send_cb(onDataSent);
    esp_now_register_recv_cb(onDataRecv);

    peerRegistered = registerMasterPeer(masterMacAddress);
    if (!peerRegistered) {
        Serial.println("[SLAVE]  WARNING: Master not registered.");
        Serial.println("[SLAVE]           Can RECEIVE but cannot send replies.");
        Serial.println("[SLAVE]           Fix masterMacAddress[] and re-flash.");
    }

    Serial.println("[SLAVE]  Ready. Waiting for messages.\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 11 - loop()
// Slave is event-driven. No polling needed.
// ─────────────────────────────────────────────────────────────────────────────

void loop() {
    delay(100);
}
