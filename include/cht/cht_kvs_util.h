//
// Created by vasilis on 14/09/20.
//

#ifndef ODYSSEY_CHT_KVS_UTIL_H
#define ODYSSEY_CHT_KVS_UTIL_H


#include <network_context.h>
#include <netw_func.h>
#include "kvs.h"
#include "cht_config.h"


///* ---------------------------------------------------------------------------
////------------------------------ KVS_API -----------------------------
////---------------------------------------------------------------------------*/

static inline void cht_KVS_batch_op_trace(context_t *ctx, uint16_t op_num)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *op = cht_ctx->ops;
  uint16_t op_i;
  if (ENABLE_ASSERTIONS) {
    assert(op != NULL);
    assert(op_num > 0 && op_num <= CHT_TRACE_BATCH);
  }

  unsigned int bkt[CHT_TRACE_BATCH];
  struct mica_bkt *bkt_ptr[CHT_TRACE_BATCH];
  unsigned int tag[CHT_TRACE_BATCH];
  mica_op_t *kv_ptr[CHT_TRACE_BATCH];	/* Ptr to KV item in log */


  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &op[op_i].key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  uint32_t write_i = 0;

  uint8_t temp_resp;
  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_check_key(kv_ptr[op_i], op[op_i].key, op_i);
    if (op[op_i].opcode == KVS_OP_GET) {
      KVS_local_read(kv_ptr[op_i], op[op_i].value_to_read,
                     &temp_resp, ctx->t_id);
      signal_completion_to_client(op[op_i].session_id,
                                  op[op_i].index_to_req_array,
                                  ctx->t_id);
    }
    else if (op[op_i].opcode == KVS_OP_PUT) {
      cht_w_rob_t *w_rob = (cht_w_rob_t *)
        get_fifo_push_slot(cht_ctx->loc_w_rob);

      w_rob->kv_ptr = kv_ptr[op_i];
      w_rob->w_state = SEMIVALID;
      ctx_insert_mes(ctx, PREP_QP_ID, (uint32_t) PREP_SIZE, 1, false, &op[op_i], LOCAL_PREP);
    }
    else if (ENABLE_ASSERTIONS) {
      my_printf(red, "wrong Opcode in cache: %d, req %d \n", op[op_i].opcode, op_i);
      assert(0);
    }


  }
}

#endif //ODYSSEY_CHT_KVS_UTIL_H
