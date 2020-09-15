//
// Created by vasilis on 14/09/20.
//

#ifndef ODYSSEY_CHT_KVS_UTIL_H
#define ODYSSEY_CHT_KVS_UTIL_H


#include <network_context.h>
#include <netw_func.h>
#include "kvs.h"
#include "cht_config.h"

static inline void check_opcode_is_read(ctx_trace_op_t *op, 
                                        uint16_t op_i)
{
  if (ENABLE_ASSERTIONS && op[op_i].opcode != KVS_OP_GET) {
    my_printf(red, "wrong Opcode in cache: %d, req %d \n", op[op_i].opcode, op_i);
    assert(0);
  }
}

static inline void cht_insert_buffered_op(context_t *ctx,
                                      mica_op_t *kv_ptr,
                                      ctx_trace_op_t *op)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  cht_buf_op_t *buf_op = (cht_buf_op_t *) get_fifo_push_slot(cht_ctx->buf_reads);
  buf_op->op.opcode = op->opcode;
  buf_op->op.key = op->key;
  buf_op->op.session_id = op->session_id;
  buf_op->op.index_to_req_array = op->index_to_req_array;
  buf_op->kv_ptr = kv_ptr;

  buf_op->op.value_to_read = op->value_to_read;


  fifo_incr_push_ptr(cht_ctx->buf_reads);
  fifo_increm_capacity(cht_ctx->buf_reads);
}


static inline void init_w_rob_on_rem_prep(context_t *ctx,
                                          mica_op_t *kv_ptr,
                                          cht_prep_mes_t *prep_mes,
                                          cht_prep_t *prep)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  cht_w_rob_t *w_rob = (cht_w_rob_t *)
    get_fifo_push_slot(&cht_ctx->w_rob[prep_mes->m_id]);
  if (DEBUG_WRITES)
    my_printf(cyan, "Remote W_rob %u for prep from %u with l_id %lu -->%lu, inserted w_id = %u,"
                "owner %u/%u\n",
              w_rob->id,  prep_mes->m_id, prep_mes->l_id,
              prep_mes->l_id + prep_mes->coalesce_num,
              cht_ctx->inserted_w_id[prep_mes->m_id], prep->m_id, prep->sess_id);
  if (ENABLE_ASSERTIONS) {
    assert(w_rob->w_state == INVALID);
    w_rob->l_id = cht_ctx->inserted_w_id[prep_mes->m_id];
    assert(w_rob->coordin_m_id == prep_mes->m_id);
    if (prep->m_id == ctx->m_id) assert(cht_ctx->stalled[prep->sess_id]);
  }
  w_rob->w_state = VALID;
  cht_ctx->inserted_w_id[prep_mes->m_id]++;
  // w_rob capacity is already incremented when polling
  // to achieve back pressure at polling
  w_rob->version = prep->version;

  w_rob->owner_m_id = prep->m_id;
  w_rob->sess_id = prep->sess_id;
  w_rob->kv_ptr = kv_ptr;
  fifo_incr_push_ptr(&cht_ctx->w_rob[prep_mes->m_id]);
}

///* ---------------------------------------------------------------------------
////------------------------------ REQ PROCESSING -----------------------------
////---------------------------------------------------------------------------*/

static inline void cht_rem_prep(context_t *ctx,
                              mica_op_t *kv_ptr,
                              cht_prep_mes_t *prep_mes,
                              cht_prep_t *prep)
{

  
  lock_seqlock(&kv_ptr->seqlock);
  if (prep->version > kv_ptr->version) {
    kv_ptr->state = CHT_INV;
    kv_ptr->version = prep->version;
    memcpy(kv_ptr->value, prep->value, VALUE_SIZE);
  }
  unlock_seqlock(&kv_ptr->seqlock);

  init_w_rob_on_rem_prep(ctx, kv_ptr, prep_mes, prep);
 
}

static inline void cht_loc_read(context_t *ctx,
                               mica_op_t *kv_ptr,
                               ctx_trace_op_t *op)
{
  if (ENABLE_ASSERTIONS) {
    assert(op->value_to_read != NULL);
    assert(kv_ptr != NULL);
  }
  bool success = false;
  uint32_t debug_cntr = 0;
  uint64_t tmp_lock = read_seqlock_lock_free(&kv_ptr->seqlock);
  do {
    debug_stalling_on_lock(&debug_cntr, "local read", ctx->t_id);
    if (kv_ptr->state == CHT_V) {
      memcpy(op->value_to_read, kv_ptr->value, (size_t) VALUE_SIZE);
      success = true;
    }
  } while (!(check_seqlock_lock_free(&kv_ptr->seqlock, &tmp_lock)));

  if (success) {
    cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
    signal_completion_to_client(op->session_id, op->index_to_req_array, ctx->t_id);
    cht_ctx->all_sessions_stalled = false;
    cht_ctx->stalled[op->session_id] = false;
  }
  else cht_insert_buffered_op(ctx, kv_ptr, op);
}


static inline void cht_loc_or_rem_write(context_t *ctx,
                                        mica_op_t *kv_ptr,
                                        void *source,
                                        source_t source_flag,
                                        uint8_t m_id)
{
  uint8_t *value_ptr = source_flag == LOCAL_PREP ?
                       ((ctx_trace_op_t *) source)->value_to_write :
                       ((cht_write_t *) source)->value;

  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  cht_w_rob_t *w_rob = (cht_w_rob_t *)
    get_fifo_push_slot(cht_ctx->loc_w_rob);
  if (ENABLE_ASSERTIONS) assert(w_rob->w_state == INVALID);

  lock_seqlock(&kv_ptr->seqlock);
  {
    kv_ptr->state = CHT_INV;
    kv_ptr->version++;
    w_rob->version = kv_ptr->version;
    memcpy(kv_ptr->value, value_ptr, VALUE_SIZE);
  }
  unlock_seqlock(&kv_ptr->seqlock);

  w_rob->owner_m_id = m_id;
  w_rob->kv_ptr = kv_ptr;
  w_rob->w_state = SEMIVALID;

  //if (DEBUG_PREPARES)


  ctx_insert_mes(ctx, PREP_QP_ID, (uint32_t) PREP_SIZE, 1, false, source, source_flag, 0);
}


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


  uint32_t buf_ops_num = cht_ctx->buf_reads->capacity;
  for (op_i = 0; op_i < buf_ops_num; ++op_i) {
    cht_buf_op_t *buf_read = (cht_buf_op_t *) get_fifo_pull_slot(cht_ctx->buf_reads);
    check_state_with_allowed_flags(2, buf_read->op.opcode,  KVS_OP_GET);
    cht_loc_read(ctx, buf_read->kv_ptr, &buf_read->op);
    fifo_incr_pull_ptr(cht_ctx->buf_reads);
    fifo_decrem_capacity(cht_ctx->buf_reads);
  }


  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_check_key(kv_ptr[op_i], op[op_i].key, op_i);

    if (op[op_i].opcode == KVS_OP_PUT) {
      cht_loc_or_rem_write(ctx, kv_ptr[op_i], &op[op_i], LOCAL_PREP, ctx->m_id);
    }
    else {
      check_opcode_is_read(op, op_i);
      cht_loc_read(ctx, kv_ptr[op_i], &op[op_i]);
    }
    
    
  }
}


static inline void cht_KVS_batch_op_preps(context_t *ctx)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  cht_ptrs_to_op_t *ptrs_to_prep = cht_ctx->ptrs_to_ops;
  cht_prep_mes_t **prep_mes = (cht_prep_mes_t **) cht_ctx->ptrs_to_ops->ptr_to_mes;
  cht_prep_t **preps = (cht_prep_t **) ptrs_to_prep->ops;
  uint16_t op_num = ptrs_to_prep->op_num;

  uint16_t op_i;
  if (ENABLE_ASSERTIONS) {
    assert(preps != NULL);
    assert(op_num > 0 && op_num <= MAX_INCOMING_PREP);
  }

  unsigned int bkt[MAX_INCOMING_PREP];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_PREP];
  unsigned int tag[MAX_INCOMING_PREP];
  mica_op_t *kv_ptr[MAX_INCOMING_PREP];	/* Ptr to KV item in log */

  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &preps[op_i]->key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_check_key(kv_ptr[op_i], preps[op_i]->key, op_i);
    cht_rem_prep(ctx, kv_ptr[op_i], prep_mes[op_i], preps[op_i]);
  }
}


static inline void cht_KVS_batch_op_writes(context_t *ctx)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  cht_ptrs_to_op_t *ptrs_to_writes = cht_ctx->ptrs_to_ops;
  cht_w_mes_t **w_mes = (cht_w_mes_t **) cht_ctx->ptrs_to_ops->ptr_to_mes;
  cht_write_t **writes = (cht_write_t **) ptrs_to_writes->ops;
  uint16_t op_num = ptrs_to_writes->op_num;

  uint16_t op_i;
  if (ENABLE_ASSERTIONS) {
    assert(writes != NULL);
    assert(op_num > 0 && op_num <= MAX_INCOMING_PREP);
  }

  unsigned int bkt[MAX_INCOMING_PREP];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_PREP];
  unsigned int tag[MAX_INCOMING_PREP];
  mica_op_t *kv_ptr[MAX_INCOMING_PREP];	/* Ptr to KV item in log */

  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &writes[op_i]->key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_check_key(kv_ptr[op_i], writes[op_i]->key, op_i);
    cht_loc_or_rem_write(ctx, kv_ptr[op_i], (void *) writes[op_i],
                         REMOTE_WRITE, w_mes[op_i]->m_id);
  }

}


#endif //ODYSSEY_CHT_KVS_UTIL_H
