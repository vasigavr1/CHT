//
// Created by vasilis on 14/09/20.
//

#ifndef ODYSSEY_CHT_MESSAGES_H
#define ODYSSEY_CHT_MESSAGES_H

#include "top.h"

/// PREP_QP_ID
#define PREP_CREDITS 5
#define PREP_COALESCE 16
#define COM_CREDITS 80
//#define MAX_PREP_SIZE 500

#define MAX_PREP_WRS (MESSAGES_IN_BCAST_BATCH)

#define MAX_PREP_BUF_SLOTS_TO_BE_POLLED ( PREP_CREDITS * REM_MACH_NUM)
#define MAX_RECV_PREP_WRS (PREP_CREDITS * REM_MACH_NUM)
#define PREP_BUF_SLOTS (MAX_RECV_PREP_WRS)

#define PREP_MES_HEADER 12 // opcode(1), coalesce_num(1) l_id (8)
//#define EFFECTIVE_MAX_PREP_SIZE (MAX_PREP_SIZE - PREP_MES_HEADER)
#define PREP_SIZE (16 + VALUE_SIZE)
//#define PREP_COALESCE (EFFECTIVE_MAX_PREP_SIZE / PREP_SIZE)
#define PREP_SEND_SIZE (PREP_MES_HEADER + (PREP_COALESCE * PREP_SIZE))
#define PREP_RECV_SIZE (GRH_SIZE + PREP_SEND_SIZE)

#define PREP_FIFO_SIZE (SESSIONS_PER_THREAD + 1)


typedef struct cht_prepare {
  uint64_t version;
  mica_key_t key;
  uint8_t value[VALUE_SIZE];
} __attribute__((__packed__)) cht_prep_t;

// prepare message
typedef struct cht_prep_message {
  uint64_t l_id;
  uint8_t opcode;
  uint8_t coalesce_num;
  uint8_t m_id;
  uint8_t unused;
  cht_prep_t prepare[PREP_COALESCE];
} __attribute__((__packed__)) cht_prep_mes_t;

typedef struct cht_prep_message_ud_req {
  uint8_t grh[GRH_SIZE];
  cht_prep_mes_t prepare;
} cht_prep_mes_ud_t;



#define COM_WRS MESSAGES_IN_BCAST_BATCH
#define RECV_COM_WRS (REM_MACH_NUM * COM_CREDITS)
#define COM_BUF_SLOTS RECV_COM_WRS
#define MAX_LIDS_IN_A_COMMIT SESSIONS_PER_THREAD


#define MAX_W_COALESCE 16
#define CHT_MAX_W_WRS (MACHINE_NUM + (SESSIONS_PER_THREAD / MAX_W_COALESCE))
#define CHT_MAX_RECV_W_WRS (REM_MACH_NUM * SESSIONS_PER_THREAD)
#define CHT_BUF_SLOTS CHT_MAX_RECV_W_WRS
#define CHT_W_HEADER 4

typedef struct cht_write {
  mica_key_t key;	/* 8B */
  uint32_t sess_id;
  uint8_t value[VALUE_SIZE];
} __attribute__((__packed__)) cht_write_t;

typedef struct cht_w_message {
  uint8_t coalesce_num;
  uint8_t opcode;
  uint8_t m_id;
  uint8_t unused;
  cht_write_t write[MAX_W_COALESCE];
} __attribute__((__packed__)) cht_w_mes_t;


typedef struct cht_w_message_ud_req {
  uint8_t unused[GRH_SIZE];
  cht_w_mes_t w_mes;
} cht_w_mes_ud_t;


#define CHT_W_SIZE sizeof(cht_write_t)

#endif //ODYSSEY_CHT_MESSAGES_H
