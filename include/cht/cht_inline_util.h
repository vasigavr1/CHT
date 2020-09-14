//
// Created by vasilis on 14/09/20.
//

#ifndef ODYSSEY_CHT_INLINE_UTIL_H
#define ODYSSEY_CHT_INLINE_UTIL_H


#include <netw_func.h>
#include "network_context.h"
#include "cht_kvs_util.h"
#include "cht_debug_util.h"


static inline void cht_batch_from_trace_to_KVS(context_t *ctx)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *ops = cht_ctx->ops;
  trace_t *trace = cht_ctx->trace;

  uint16_t op_i = 0;
  int working_session = -1;

  if (all_sessions_are_stalled(ctx, cht_ctx->all_sessions_stalled,
                               &cht_ctx->stalled_sessions_dbg_counter))
    return;
  if (!find_starting_session(ctx, cht_ctx->last_session,
                             cht_ctx->stalled, &working_session)) return;

  bool passed_over_all_sessions = false;

  /// main loop
  while (op_i < CHT_TRACE_BATCH && !passed_over_all_sessions) {

    ctx_fill_trace_op(ctx, &trace[cht_ctx->trace_iter], &ops[op_i], working_session);
    cht_ctx->stalled[working_session] = ops[op_i].opcode == KVS_OP_PUT;
    while (!pull_request_from_this_session(cht_ctx->stalled[working_session],
                                           (uint16_t) working_session, ctx->t_id)) {

      MOD_INCR(working_session, SESSIONS_PER_THREAD);
      if (working_session == cht_ctx->last_session) {
        passed_over_all_sessions = true;
        // If clients are used the condition does not guarantee that sessions are stalled
        if (!ENABLE_CLIENTS) cht_ctx->all_sessions_stalled = true;
        break;
      }
    }
    if (!ENABLE_CLIENTS) {
      cht_ctx->trace_iter++;
      if (trace[cht_ctx->trace_iter].opcode == NOP) cht_ctx->trace_iter = 0;
    }
    op_i++;
  }
  cht_ctx->last_session = (uint16_t) working_session;
  t_stats[ctx->t_id].cache_hits_per_thread += op_i;
  cht_KVS_batch_op_trace(ctx, op_i);

}



///* ---------------------------------------------------------------------------
////------------------------------INSERT HELPERS -----------------------------
////---------------------------------------------------------------------------*/

static inline void cht_insert_prep_help(context_t *ctx, void* inv_ptr,
                                        void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;

  uint16_t sess_id; uint8_t source_m_id;
  cht_prep_t *prep = (cht_prep_t *) inv_ptr;


  if (source_flag == LOCAL_PREP) {
    source_m_id = ctx->m_id; //TODO
    ctx_trace_op_t *op = (ctx_trace_op_t *) source;
    memcpy(prep->value, op->value_to_write, VALUE_SIZE);
    prep->key = op->key;
    cht_ctx->index_to_req_array[op->session_id] = op->index_to_req_array;
    sess_id = op->session_id;
  }
  else {
    assert(source_flag == REMOTE_WRITE);
    source_m_id = 0;
    sess_id = 0;
  }


  fifo_t *working_fifo = cht_ctx->loc_w_rob;
  cht_w_rob_t *w_rob = (cht_w_rob_t *) get_fifo_push_slot(working_fifo);
  if (ENABLE_ASSERTIONS) {
    if (source_flag == LOCAL_PREP) assert(w_rob->w_state == SEMIVALID);
    else assert(w_rob->w_state == INVALID);
  }
  w_rob->w_state = VALID;
  w_rob->sess_id = sess_id;
  w_rob->l_id = cht_ctx->inserted_w_id[source_m_id];
  w_rob->acks_seen = 0;

  //check_w_rob_in_insert_help(ctx, op, w_rob);
  //fill_prep(prep, op, w_rob);
  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  cht_prep_mes_t *prep_mes = (cht_prep_mes_t *) get_fifo_push_slot(send_fifo);
  prep_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  // If it's the first message give it an lid
  if (slot_meta->coalesce_num == 1) {
    prep_mes->l_id = cht_ctx->inserted_w_id[source_m_id];
    fifo_set_push_backward_ptr(send_fifo, working_fifo->push_ptr);
  }
  // Bookkeeping
  fifo_incr_push_ptr(working_fifo);
  cht_ctx->inserted_w_id[source_m_id]++;
}


///* ---------------------------------------------------------------------------
////------------------------------SEND HELPERS -----------------------------
////---------------------------------------------------------------------------*/

static inline void cht_send_preps_helper(context_t *ctx)
{
  cht_checks_and_stats_on_bcasting_preps(ctx);
}

///* ---------------------------------------------------------------------------
////------------------------------POLL HANDLERS -----------------------------
////---------------------------------------------------------------------------*/

static inline void insert_in_w_rob()
{
  
}


static inline bool cht_prepare_handler(context_t *ctx)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cht_prep_mes_ud_t *incoming_preps = (volatile cht_prep_mes_ud_t *) recv_fifo->fifo;
  cht_prep_mes_t *prep_mes = (cht_prep_mes_t *) &incoming_preps[recv_fifo->pull_ptr].prepare;

  uint8_t coalesce_num = prep_mes->coalesce_num;

  fifo_t *w_rob_fifo = &cht_ctx->w_rob[prep_mes->m_id];
  bool preps_fit_in_w_rob =
    w_rob_fifo->capacity + coalesce_num <= w_rob_fifo->max_size;

  if (!preps_fit_in_w_rob) return false;
  fifo_increase_capacity(w_rob_fifo, coalesce_num);

  cht_check_polled_prep_and_print(ctx, prep_mes);

  ctx_ack_insert(ctx, ACK_QP_ID, coalesce_num,  prep_mes->l_id, prep_mes->m_id);


  for (uint8_t prep_i = 0; prep_i < coalesce_num; prep_i++) {
    cht_check_prepare_and_print(ctx, prep_mes, prep_i);
    fill_cht_ctx_entry(ctx, prep_mes, prep_i);
  }

  if (ENABLE_ASSERTIONS) prep_mes->opcode = 0;

  return true;
}



static inline void cht_main_loop(context_t *ctx)
{
  if (ctx->t_id == 0) my_printf(yellow, "CHT main loop \n");


  while(true) {

    cht_batch_from_trace_to_KVS(ctx);

    ctx_send_broadcasts(ctx, PREP_QP_ID);
    //
    ctx_poll_incoming_messages(ctx, PREP_QP_ID);

    //ctx_send_acks(ctx, ACK_QP_ID);
    //
    //ctx_poll_incoming_messages(ctx, ACK_QP_ID);

    //cht_propagate_updates(ctx);

    //ctx_send_broadcasts(ctx, COM_QP_ID);
    //
    //ctx_poll_incoming_messages(ctx, COM_QP_ID);

  }
}

#endif //ODYSSEY_CHT_INLINE_UTIL_H
