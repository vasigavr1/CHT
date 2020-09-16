//
// Created by vasilis on 14/09/20.
//

#ifndef ODYSSEY_CHT_INLINE_UTIL_H
#define ODYSSEY_CHT_INLINE_UTIL_H



#include "cht_kvs_util.h"
#include "cht_reserve_stations.h"




static inline void cht_batch_from_trace_to_KVS(context_t *ctx)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *ops = cht_ctx->ops;
  trace_t *trace = cht_ctx->trace;

  uint16_t kvs_op_i = 0, op_num = 0;
  int working_session = -1;

  if (all_sessions_are_stalled(ctx, cht_ctx->all_sessions_stalled,
                               &cht_ctx->stalled_sessions_dbg_counter))
    return;
  if (!find_starting_session(ctx, cht_ctx->last_session,
                             cht_ctx->stalled, &working_session)) return;

  bool passed_over_all_sessions = false;

  /// main loop
  while (kvs_op_i < CHT_TRACE_BATCH && !passed_over_all_sessions) {

    ctx_fill_trace_op(ctx, &trace[cht_ctx->trace_iter], &ops[kvs_op_i], working_session);
    cht_ctx->stalled[working_session] = true;
    passed_over_all_sessions =
      ctx_find_next_working_session(ctx, &working_session,
                                    cht_ctx->stalled,
                                    cht_ctx->last_session,
                                    &cht_ctx->all_sessions_stalled);
    if (!ENABLE_CLIENTS) {
      cht_ctx->trace_iter++;
      if (trace[cht_ctx->trace_iter].opcode == NOP) cht_ctx->trace_iter = 0;
    }

    if (!filter_remote_writes(ctx, &ops[kvs_op_i]))
      kvs_op_i++;
    op_num++;
  }
  cht_ctx->last_session = (uint16_t) working_session;
  t_stats[ctx->t_id].cache_hits_per_thread += op_num;
  if (kvs_op_i > 0) cht_KVS_batch_op_trace(ctx, kvs_op_i);
}

///* ---------------------------------------------------------------------------
////------------------------------ COMMIT WRITES -----------------------------
////---------------------------------------------------------------------------*/



static inline void cht_commit_writes(context_t *ctx)
{
  uint16_t write_num = 0, local_op_i = 0;
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  cht_w_rob_t **ptrs_to_w_rob = (cht_w_rob_t **) cht_ctx->ptrs_to_ops->ops;
  for (int m_i = 0; m_i < MACHINE_NUM; ++m_i) {
    cht_w_rob_t *w_rob = (cht_w_rob_t *) get_fifo_pull_slot(&cht_ctx->w_rob[m_i]);
    while (w_rob->w_state == READY) {

      w_rob->w_state = INVALID;
      if (ENABLE_ASSERTIONS) {
        assert(write_num < CHT_UPDATE_BATCH);
        assert(w_rob->coordin_m_id == m_i);
      }
      ptrs_to_w_rob[write_num] = w_rob;
      __builtin_prefetch(&w_rob->kv_ptr->seqlock, 0, 0);
      //my_printf(green, "Commit sess %u write %lu, version: %lu \n",
      //          w_rob->sess_id, cht_ctx->committed_w_id[m_i] + write_num, w_rob->version);

      if (m_i == ctx->m_id) {
        assert(w_rob->acks_seen == REM_MACH_NUM);
        w_rob->acks_seen = 0;
        local_op_i++;
      }

      if (w_rob->owner_m_id == ctx->m_id) {
        cht_complete_local_write(ctx, w_rob);
      }


      fifo_incr_pull_ptr(&cht_ctx->w_rob[m_i]);
      fifo_decrem_capacity(&cht_ctx->w_rob[m_i]);
      w_rob = (cht_w_rob_t *) get_fifo_pull_slot(&cht_ctx->w_rob[m_i]);
      write_num++;
    }
  }

  if (write_num > 0) {
    cht_ctx->ptrs_to_ops->op_num = write_num;
    cht_apply_writes(ctx);
    if (local_op_i > 0) {
      ctx_insert_commit(ctx, COM_QP_ID, local_op_i, cht_ctx->committed_w_id[ctx->m_id]);
      cht_ctx->committed_w_id[ctx->m_id] += local_op_i;
    }
  }
}

///* ---------------------------------------------------------------------------
////------------------------------INSERT HELPERS -----------------------------
////---------------------------------------------------------------------------*/


static inline void cht_insert_prep_help(context_t *ctx, void* prep_ptr,
                                        void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  cht_prep_t *prep = (cht_prep_t *) prep_ptr;

  fifo_t *working_fifo = cht_ctx->loc_w_rob;
  cht_w_rob_t *w_rob = (cht_w_rob_t *) get_fifo_push_slot(working_fifo);

  cht_fill_prep_and_w_rob(ctx, prep, source, w_rob, (source_t) source_flag);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  cht_prep_mes_t *prep_mes = (cht_prep_mes_t *) get_fifo_push_slot(send_fifo);
  prep_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  // If it's the first message give it an lid
  if (slot_meta->coalesce_num == 1) {
    prep_mes->l_id = cht_ctx->inserted_w_id[w_rob->coordin_m_id];
    fifo_set_push_backward_ptr(send_fifo, working_fifo->push_ptr);
  }
  // Bookkeeping
  fifo_increm_capacity(working_fifo);
  fifo_incr_push_ptr(working_fifo);
  cht_ctx->inserted_w_id[w_rob->coordin_m_id]++;
}


static inline void cht_insert_write_help(context_t *ctx, void *w_ptr,
                                         void *source, uint32_t source_flag)
{
  cht_write_t *write = (cht_write_t *) w_ptr;
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[W_QP_ID];
  ctx_trace_op_t *op = (ctx_trace_op_t *) source;
  uint8_t rm_id = get_key_owner(ctx, op->key);
  fifo_t *send_fifo = &qp_meta->send_fifo[get_fifo_i(ctx, rm_id)];


  if (DEBUG_WRITES)
    my_printf(yellow, "Inserting write: owner %u/%u, to rm: %u in fifo %u \n",
              ctx->m_id, op->session_id, rm_id, get_fifo_i(ctx, rm_id));

 
  write->key = op->key;
  write->sess_id = op->session_id;
  memcpy(write->value, op->value_to_write, VALUE_SIZE);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  if (slot_meta->coalesce_num == 1) {
    slot_meta->rm_id = rm_id;
  }

  cht_w_mes_t *w_mes = (cht_w_mes_t *) get_fifo_pull_slot(send_fifo);
  w_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  
  cht_ctx->index_to_req_array[op->session_id] = op->index_to_req_array;
}

///* ---------------------------------------------------------------------------
////------------------------------SEND HELPERS -----------------------------
////---------------------------------------------------------------------------*/

static inline void cht_send_preps_helper(context_t *ctx)
{
  cht_checks_and_stats_on_bcasting_preps(ctx);
  ctx_refill_recvs(ctx, W_QP_ID);
}

static inline void cht_send_acks_helper(context_t *ctx)
{
  ctx_refill_recvs(ctx, COM_QP_ID);
}

static inline void cht_send_commits_helper(context_t *ctx)
{
  cht_checks_and_stats_on_bcasting_commits(ctx);
}

static inline void cht_checks_and_stats_on_sending_writes(context_t * ctx)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[W_QP_ID];
  uint64_t fifo_i = ctx->ctx_tmp->counter;
  fifo_t *send_fifo = &qp_meta->send_fifo[fifo_i];
  cht_w_mes_t *w_mes = (cht_w_mes_t *) get_fifo_pull_slot(send_fifo);
  uint16_t coalesce_num = get_fifo_slot_meta_pull(send_fifo)->coalesce_num;
  w_mes->coalesce_num = (uint8_t) coalesce_num;

  if (ENABLE_ASSERTIONS) {
    assert(send_fifo->net_capacity >= coalesce_num);
    qp_meta->outstanding_messages += coalesce_num;
  }
  if (DEBUG_WRITES) {
    printf("Wrkr %d sends out a w_mes with %u writes to machine %u \n", ctx->t_id,
           w_mes->coalesce_num, get_fifo_slot_meta_pull(send_fifo)->rm_id);
    for (int w_i = 0; w_i < w_mes->coalesce_num; ++w_i) {
      cht_write_t *write = &w_mes->write[w_i];
      my_printf(yellow, "Write %u/%u owner %u/%u\n",
                w_i, w_mes->coalesce_num, w_mes->m_id, write->sess_id);
    }

  }

  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].writes_sent += coalesce_num;
    t_stats[ctx->t_id].writes_sent_mes_num++;
  }
}

static inline void cht_send_writes_helper(context_t *ctx)
{
  cht_checks_and_stats_on_sending_writes(ctx);
}

///* ---------------------------------------------------------------------------
////------------------------------POLL HANDLERS -----------------------------
////---------------------------------------------------------------------------*/


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
    w_rob_fifo->capacity + coalesce_num <= SESSIONS_PER_THREAD; //w_rob_fifo->max_size;

  if (!preps_fit_in_w_rob) return false;
  fifo_increase_capacity(w_rob_fifo, coalesce_num);

  cht_check_polled_prep_and_print(ctx, prep_mes);

  ctx_ack_insert(ctx, ACK_QP_ID, coalesce_num,  prep_mes->l_id, prep_mes->m_id);

  cht_ptrs_to_op_t *ptrs_to_prep = cht_ctx->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_prep->op_num = 0;
  
  for (uint8_t prep_i = 0; prep_i < coalesce_num; prep_i++) {
    check_w_rob_when_handling_a_prep(ctx, ptrs_to_prep,
                                     w_rob_fifo,
                                     prep_mes, prep_i);

    cht_check_prepare_and_print(ctx, prep_mes, prep_i);
    ptrs_to_prep->ops[ptrs_to_prep->op_num] = &prep_mes->prepare[prep_i];
    ptrs_to_prep->ptr_to_mes[ptrs_to_prep->op_num] = prep_mes;
    ptrs_to_prep->op_num++;
  }

  if (ENABLE_ASSERTIONS) prep_mes->opcode = 0;

  return true;
}


static inline void cht_apply_acks(context_t *ctx,
                                 ctx_ack_mes_t *ack,
                                 uint32_t ack_num, uint32_t ack_ptr)
{

  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {
    cht_w_rob_t *w_rob = (cht_w_rob_t *) get_fifo_slot(cht_ctx->loc_w_rob, ack_ptr);
    w_rob->acks_seen++;
    cht_check_when_applying_acks(ctx, w_rob, ack, ack_num, ack_ptr, ack_i);
    if (w_rob->acks_seen == REM_MACH_NUM)
      w_rob->w_state = READY;
    MOD_INCR(ack_ptr, CHT_PENDING_WRITES);
  }
}

static inline bool cht_ack_handler(context_t *ctx)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ctx_ack_mes_ud_t *incoming_acks = (volatile ctx_ack_mes_ud_t *) recv_fifo->fifo;
  ctx_ack_mes_t *ack = (ctx_ack_mes_t *) &incoming_acks[recv_fifo->pull_ptr].ack;
  uint32_t ack_num = ack->ack_num;
  uint64_t l_id = ack->l_id;
  uint64_t pull_lid = cht_ctx->committed_w_id[ctx->m_id]; // l_id at the pull pointer
  uint32_t ack_ptr; // a pointer in the FIFO, from where ack should be added

  ctx_increase_credits_on_polling_ack(ctx, ACK_QP_ID, ack);

  per_qp_meta_t *com_qp_meta = &ctx->qp_meta[COM_QP_ID];
  com_qp_meta->credits[ack->m_id] = com_qp_meta->max_credits;


  if ((cht_ctx->loc_w_rob->capacity == 0 ) ||
      (pull_lid >= l_id && (pull_lid - l_id) >= ack_num))
    return true;

  ack_ptr = ctx_find_when_the_ack_points_acked(ack, cht_ctx->loc_w_rob,
                                               pull_lid, &ack_num);

  // Apply the acks that refer to stored writes
  cht_apply_acks(ctx, ack, ack_num, ack_ptr);

  return true;
}

static inline bool cht_commit_handler(context_t *ctx)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ctx_com_mes_ud_t *incoming_coms = (volatile ctx_com_mes_ud_t *) recv_fifo->fifo;

  ctx_com_mes_t *com = (ctx_com_mes_t *) &incoming_coms[recv_fifo->pull_ptr].com;
  uint32_t com_num = com->com_num;
  uint64_t l_id = com->l_id;
  cht_check_polled_commit_and_print(ctx, com, recv_fifo->pull_ptr);

  fifo_t *w_rob_fifo = &cht_ctx->w_rob[com->m_id];
  /// loop through each commit
  for (uint16_t com_i = 0; com_i < com_num; com_i++) {
    cht_w_rob_t * w_rob = get_fifo_slot_mod(w_rob_fifo, (uint32_t) (l_id + com_i));
    cht_check_each_commit(ctx, com, w_rob, com_i);
    cht_ctx->committed_w_id[com->m_id]++;
    w_rob->w_state = READY;
  } ///

  if (ENABLE_ASSERTIONS) com->opcode = 0;
  return true;
}


static inline bool cht_write_handler(context_t *ctx)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  if (ENABLE_ASSERTIONS) assert(cht_ctx->w_rob->capacity < CHT_PENDING_WRITES);
  per_qp_meta_t *qp_meta = &ctx->qp_meta[W_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cht_w_mes_ud_t *incoming_ws = (volatile cht_w_mes_ud_t *) qp_meta->recv_fifo->fifo;
  cht_w_mes_t *w_mes = (cht_w_mes_t *) &incoming_ws[recv_fifo->pull_ptr].w_mes;
  if (DEBUG_WRITES) printf("Wrkr %u sees a write Opcode %d at offset %d  \n",
                           ctx->t_id, w_mes->opcode, recv_fifo->pull_ptr);

  uint8_t w_num = w_mes->coalesce_num;
  if (cht_ctx->w_rob->capacity + w_num > CHT_PENDING_WRITES) {
    return false;
  }

  cht_ptrs_to_op_t *ptrs_to_prep = cht_ctx->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_prep->op_num = 0;

  for (uint16_t w_i = 0; w_i < w_num; w_i++) {

    if (DEBUG_WRITES)
      my_printf(green, "Write %u/%u owner %u/%u\n",
      w_i, w_num, w_mes->m_id, w_mes->write[w_i].sess_id);
    ptrs_to_prep->ops[ptrs_to_prep->op_num] = &w_mes->write[w_i];
    ptrs_to_prep->ptr_to_mes[ptrs_to_prep->op_num] = w_mes;
    ptrs_to_prep->op_num++;

    //ctx_insert_mes(ctx, PREP_QP_ID, (uint32_t) PREP_SIZE, 1,
    //               false, (void *) write, REMOTE_WRITE, 0);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].received_writes += w_num;
    t_stats[ctx->t_id].received_writes_mes_num++;
  }
  return true;
}

///* ---------------------------------------------------------------------------
////------------------------------ MAIN LOOP -----------------------------
////---------------------------------------------------------------------------*/


static inline void cht_main_loop(context_t *ctx)
{
  if (ctx->t_id == 0) my_printf(yellow, "CHT main loop \n");


  while(true) {

    cht_batch_from_trace_to_KVS(ctx);

    ctx_send_unicasts(ctx, W_QP_ID);

    ctx_poll_incoming_messages(ctx, W_QP_ID);

    ctx_send_broadcasts(ctx, PREP_QP_ID);


    ctx_poll_incoming_messages(ctx, PREP_QP_ID);

    ctx_send_acks(ctx, ACK_QP_ID);

    ctx_poll_incoming_messages(ctx, ACK_QP_ID);

    ctx_poll_incoming_messages(ctx, COM_QP_ID);

    cht_commit_writes(ctx);

    ctx_send_broadcasts(ctx, COM_QP_ID);
  }
}

#endif //ODYSSEY_CHT_INLINE_UTIL_H
