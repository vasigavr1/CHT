//
// Created by vasilis on 15/09/20.
//

#ifndef ODYSSEY_CHT_RESERVE_STATIONS_H
#define ODYSSEY_CHT_RESERVE_STATIONS_H

#include <od_netw_func.h>
#include "cht_debug_util.h"
#include "od_network_context.h"

static inline void see_steering_percentages(context_t *ctx,
                                            mica_key_t key)
{
  if (ENABLE_ASSERTIONS && SEE_STEERING_PERCENTAGES) {
    uint8_t rm_id = (uint8_t) (key.bkt % MACHINE_NUM);
    if (ctx->t_id == 0 && ctx->m_id != rm_id) {
      t_stats[ctx->t_id].steered_writes[rm_id]++;
      if (t_stats[ctx->t_id].steered_writes[rm_id] % K_128 == 0) {
        uint64_t total = 0;

        for (int i = 0; i < MACHINE_NUM; ++i) {
          total += t_stats[ctx->t_id].steered_writes[i];
        }
        my_printf(green, "---------------\n");
        for (int i = 0; i < MACHINE_NUM; ++i) {
          if (i == ctx->m_id) continue;

          my_printf(cyan, "Machine %d: %.2f  \n", i,
                    100 * (double) t_stats[ctx->t_id].steered_writes[i] / (double) total);
        }
        my_printf(green, "---------------\n");
      }
    }
  }
}



static inline uint8_t get_key_owner(context_t *ctx,
                                    mica_key_t key)
{
  see_steering_percentages(ctx, key);
  return ENABLE_MULTIPLE_LEADERS ?
         (uint8_t) (key.bkt % MACHINE_NUM) : (uint8_t) CHT_LDR_MACHINE;
}

static inline uint8_t get_fifo_i(context_t *ctx,
                                 uint8_t rm_id)
{
  return rm_id > ctx->m_id ? (uint8_t) (rm_id - 1) : rm_id;
}

static inline bool filter_remote_writes(context_t *ctx,
                                        ctx_trace_op_t *op)
{
  if (DISABLE_WRITE_STEERING) return false;
  if (op->opcode != KVS_OP_PUT) return false;

  uint8_t rm_id = get_key_owner(ctx, op->key);
  if (rm_id == ctx->m_id) return false;
  else {
    od_insert_mes(ctx, W_QP_ID, (uint32_t) CHT_W_SIZE, 1, false,
                  op, NOT_USED, get_fifo_i(ctx, rm_id));
  }
  return true;
}




static inline void cht_apply_writes(context_t *ctx)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  uint16_t op_num = cht_ctx->ptrs_to_ops->op_num;

  for (int w_i = 0; w_i < op_num; ++w_i) {
    cht_w_rob_t *w_rob = (cht_w_rob_t *) cht_ctx->ptrs_to_ops->ops[w_i];
    assert(w_rob->w_state == INVALID);
    if (ENABLE_ASSERTIONS) {
      assert(w_rob->version > 0);
      assert(w_rob != NULL);
      assert(w_rob->kv_ptr != NULL);
    }
    w_rob->w_state = INVALID;
    mica_op_t *kv_ptr = w_rob->kv_ptr;
    lock_seqlock(&kv_ptr->seqlock);
    {
      if (ENABLE_ASSERTIONS) assert(kv_ptr->version > 0);
      if (kv_ptr->version == w_rob->version) {
        if (ENABLE_ASSERTIONS)
          assert(kv_ptr->state == CHT_INV);
        kv_ptr->state = CHT_V;
      }
    }
    unlock_seqlock(&kv_ptr->seqlock);
  }
}


static inline void cht_complete_local_write(context_t * ctx,
                                            cht_w_rob_t *w_rob)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  uint16_t sess_id = w_rob->sess_id;
  if (ENABLE_ASSERTIONS) {
    assert(sess_id < SESSIONS_PER_THREAD);
    if (!cht_ctx->stalled[sess_id]) {
      if (DEBUG_WRITES)
        my_printf(red, "Session not stalled: owner %u/%u \n",
                  w_rob->owner_m_id, w_rob->sess_id);
      assert(false);
    }
    else {
      if (DEBUG_WRITES)
        my_printf(green, "Session is stalled: owner %u/%u \n",
                  w_rob->owner_m_id, w_rob->sess_id);
    }
  }
  cht_ctx->all_sessions_stalled = false;
  signal_completion_to_client(sess_id,
                              cht_ctx->index_to_req_array[sess_id],
                              ctx->t_id);
  cht_ctx->stalled[sess_id] = false;
}



static inline void cht_fill_w_rob(context_t *ctx,
                                  cht_prep_t *prep,
                                  cht_w_rob_t *w_rob)
{
  if (ENABLE_ASSERTIONS) {
    assert(w_rob->w_state == SEMIVALID);
    assert(w_rob->acks_seen == 0);
  }
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  w_rob->w_state = VALID;
  w_rob->sess_id = prep->sess_id;
  w_rob->l_id = cht_ctx->inserted_w_id[w_rob->coordin_m_id];

  if (DEBUG_WRITES)
    my_printf(cyan, "W_rob insert sess %u write %lu, w_rob_i %u\n",
              w_rob->sess_id, w_rob->l_id,
              cht_ctx->loc_w_rob->push_ptr);

}

static inline void cht_fill_prep_and_w_rob(context_t *ctx,
                                           cht_prep_t *prep, void *source,
                                           cht_w_rob_t *w_rob,
                                           source_t source_flag)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  prep->version = w_rob->version;
  prep->m_id = w_rob->owner_m_id;
  ctx_trace_op_t *op = (ctx_trace_op_t *) source;
  cht_write_t *write = (cht_write_t *) source;
  switch (source_flag) {
    case NOT_USED:break;
    case LOCAL_PREP:
      if (ENABLE_ASSERTIONS) {
        assert (prep->m_id == ctx->m_id);
        assert(cht_ctx->stalled[op->session_id]);
      }
      prep->key = op->key;
      memcpy(prep->value, op->value_to_write, (size_t) VALUE_SIZE);
      prep->sess_id = op->session_id;

      cht_ctx->index_to_req_array[op->session_id] = op->index_to_req_array;
      break;
    case REMOTE_WRITE:
      if (ENABLE_ASSERTIONS) assert (prep->m_id != ctx->m_id);
      prep->key = write->key;
      memcpy(prep->value, write->value, (size_t) VALUE_SIZE);
      prep->sess_id = write->sess_id;
      break;
  }
  cht_fill_w_rob(ctx, prep, w_rob);
}

#endif //ODYSSEY_CHT_RESERVE_STATIONS_H
