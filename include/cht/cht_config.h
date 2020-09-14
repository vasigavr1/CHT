//
// Created by vasilis on 14/09/20.
//

#ifndef ODYSSEY_CHT_CONFIG_H
#define ODYSSEY_CHT_CONFIG_H



#include "fifo.h"
#include <cht_messages.h>
#include <network_context.h>



#define QP_NUM 3
#define PREP_QP_ID 0
#define ACK_QP_ID 1
#define COM_QP_ID 2

#define CHT_TRACE_BATCH SESSIONS_PER_THREAD
#define CHT_PENDING_WRITES (SESSIONS_PER_THREAD + 1)
#define CHT_W_ROB_SIZE (CHT_PENDING_WRITES)

#define CHT_PREP_MCAST_QP 0
#define CHT_COM_MCAST_QP 1
#define COMMIT_FIFO_SIZE 1

/*------------------------------------------------
 * ----------------KVS----------------------------
 * ----------------------------------------------*/
#define MICA_VALUE_SIZE (VALUE_SIZE + (FIND_PADDING_CUST_ALIGN(VALUE_SIZE, 32)))
#define MICA_OP_SIZE_  (20 + ((MICA_VALUE_SIZE)))
#define MICA_OP_PADDING_SIZE  (FIND_PADDING(MICA_OP_SIZE_))
#define MICA_OP_SIZE  (MICA_OP_SIZE_ + MICA_OP_PADDING_SIZE)


struct mica_op {
  uint8_t value[MICA_VALUE_SIZE];
  struct key key;
  seqlock_t seqlock;
  uint32_t key_id; // strictly for debug
  uint8_t padding[MICA_OP_PADDING_SIZE];
};


/*------------------------------------------------
 * -----------------------------------------------
 * ----------------------------------------------*/

typedef enum op_state {INVALID, SEMIVALID,
  VALID, SENT,
  READY, SEND_COMMITTS} w_state_t;

typedef enum {NOT_USED, LOCAL_PREP, REMOTE_WRITE} source_t;

typedef struct w_rob {
  uint64_t version;
  uint64_t l_id; // TODO not needed
  mica_op_t *kv_ptr;

  uint16_t sess_id;
  uint16_t id;

  w_state_t w_state;
  uint8_t m_id;
  uint8_t acks_seen;
  uint8_t val_len;
  bool inv_applied;

} cht_w_rob_t;

// A data structute that keeps track of the outstanding writes
typedef struct cht_ctx {
  // reorder buffers
  fifo_t *w_rob;
  fifo_t *loc_w_rob; //points in the w_rob

  //ptrs_to_inv_t *ptrs_to_inv;

  trace_t *trace;
  uint32_t trace_iter;
  uint16_t last_session;

  ctx_trace_op_t *ops;
  //cht_resp_t *resp;

  fifo_t *buf_ops;

  uint64_t *inserted_w_id;
  uint64_t *committed_w_id;

  uint32_t *index_to_req_array; // [SESSIONS_PER_THREAD]
  bool *stalled;

  bool all_sessions_stalled;

  uint32_t stalled_sessions_dbg_counter;
} cht_ctx_t;


typedef struct thread_stats { // 2 cache lines
  long long cache_hits_per_thread;
  long long remotes_per_client;
  long long locals_per_client;

  long long preps_sent;
  long long acks_sent;
  long long coms_sent;
  long long writes_sent;
  uint64_t reads_sent;

  long long prep_sent_mes_num;
  long long acks_sent_mes_num;
  long long coms_sent_mes_num;
  long long writes_sent_mes_num;
  uint64_t reads_sent_mes_num;


  long long received_coms;
  long long received_acks;
  long long received_preps;
  long long received_writes;

  long long received_coms_mes_num;
  long long received_acks_mes_num;
  long long received_preps_mes_num;
  long long received_writes_mes_num;


  uint64_t batches_per_thread; // Leader only
  uint64_t total_writes; // Leader only

  uint64_t stalled_gid;
  uint64_t stalled_ack_prep;
  uint64_t stalled_com_credit;
  //long long unused[3]; // padding to avoid false sharing
} thread_stats_t;

extern t_stats_t t_stats[WORKERS_PER_MACHINE];

#endif //ODYSSEY_CHT_CONFIG_H
