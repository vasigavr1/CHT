//
// Created by vasilis on 14/09/20.
//

#ifndef ODYSSEY_CHT_DEBUG_UTIL_H
#define ODYSSEY_CHT_DEBUG_UTIL_H

#include "cht_config.h"


static inline void cht_checks_and_stats_on_bcasting_preps(context_t *ctx)
{
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;

  // Create the broadcast messages
  cht_prep_mes_t *prep_buf = (cht_prep_mes_t *) qp_meta->send_fifo->fifo;
  cht_prep_mes_t *prep_mes = &prep_buf[send_fifo->pull_ptr];

  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  uint8_t coalesce_num = (uint8_t) slot_meta->coalesce_num;
  if (ENABLE_ASSERTIONS) {
    assert(send_fifo->net_capacity >= coalesce_num);
    qp_meta->outstanding_messages += coalesce_num;
    assert(prep_mes->coalesce_num == (uint8_t) slot_meta->coalesce_num);
    uint32_t backward_ptr = fifo_get_pull_backward_ptr(send_fifo);
    if (DEBUG_PREPARES)
      printf("Wrkr %d has %u prep_mes bcasts to send credits %d\n", ctx->t_id,
             send_fifo->net_capacity, qp_meta->credits[1]);
    for (uint16_t i = 0; i < coalesce_num; i++) {
      cht_w_rob_t *w_rob = (cht_w_rob_t *) get_fifo_slot_mod(cht_ctx->loc_w_rob, backward_ptr + i);
      if (ENABLE_ASSERTIONS) assert(w_rob->w_state == VALID);
      w_rob->w_state = SENT;
      if (DEBUG_PREPARES)
        printf("prep_mes %d, total message capacity %d\n",
               i,  slot_meta->byte_size);
    }

    if (DEBUG_PREPARES)
      my_printf(green, "Wrkr %d : I BROADCAST a prep_mes message %d of "
                  "%u preps with total w_size %u,  with  credits: %d, lid: %lu  \n",
                ctx->t_id, prep_mes->opcode, coalesce_num, slot_meta->byte_size,
                qp_meta->credits[0], prep_mes->l_id);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].preps_sent +=
      coalesce_num;
    t_stats[ctx->t_id].prep_sent_mes_num++;
  }
}



static inline void cht_check_polled_prep_and_print(context_t *ctx,
                                                  cht_prep_mes_t* prep_mes)
{

  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  if (DEBUG_PREPARES)
    my_printf(green, "Wrkr %d sees a prep_mes message "
                "with %d prepares at index %u l_id %u \n",
              ctx->t_id, prep_mes->coalesce_num, recv_fifo->pull_ptr,
              prep_mes->l_id);
  if (ENABLE_ASSERTIONS) {
    assert(prep_mes->opcode == KVS_OP_PUT);
    assert(prep_mes->coalesce_num > 0 && prep_mes->coalesce_num <= PREP_COALESCE);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].received_preps += prep_mes->coalesce_num;
    t_stats[ctx->t_id].received_preps_mes_num++;
  }
}


static inline void cht_check_prepare_and_print(context_t *ctx,
                                              cht_prep_mes_t *prep_mes,
                                              uint8_t prep_i)
{
  if (ENABLE_ASSERTIONS) {
    cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
    cht_prep_t *prepare = &prep_mes->prepare[prep_i];

    if (DEBUG_PREPARES)
      my_printf(green, "Wrkr %u, prep_i %u new write from m_id %u \n",
                ctx->t_id, prep_i, prep_mes->m_id);
  }
}

static inline void check_w_rob_when_handling_a_prep(context_t *ctx,
                                                    cht_ptrs_to_op_t *ptrs_to_prep,
                                                    fifo_t *w_rob_fifo,
                                                    cht_prep_mes_t *prep_mes,
                                                    uint8_t prep_i)
{
  if (ENABLE_ASSERTIONS) {
    cht_prep_t *prep= &prep_mes->prepare[prep_i];
    cht_w_rob_t *w_rob = (cht_w_rob_t *)
      get_fifo_push_relative_slot(w_rob_fifo, prep_i);
    assert(w_rob->w_state == INVALID);
    w_rob->l_id = prep_mes->l_id + prep_i;
    assert(ptrs_to_prep->op_num < MAX_INCOMING_PREP);

    cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;

    if (prep->m_id == ctx->m_id)
      assert(cht_ctx->stalled[prep->sess_id]);
  }
}



static inline void cht_check_when_applying_acks(context_t *ctx,
                                            cht_w_rob_t *w_rob,
                                            ctx_ack_mes_t *ack,
                                            uint32_t ack_num,
                                            uint32_t ack_ptr,
                                            uint16_t ack_i)
{
  if (ENABLE_ASSERTIONS) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
    cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
    uint64_t pull_lid = cht_ctx->committed_w_id[ctx->m_id];
    if (ENABLE_ASSERTIONS && (ack_ptr == cht_ctx->loc_w_rob->push_ptr)) {
      uint32_t origin_ack_ptr = (uint32_t) (ack_ptr - ack_i + CHT_PENDING_WRITES) % CHT_PENDING_WRITES;
      //my_printf(red, "Origin ack_ptr %u/%u, acks %u/%u, w_pull_ptr %u, w_push_ptr % u, capacity %u \n",
      //          origin_ack_ptr, (cht_ctx->loc_w_rob->pull_ptr + (ack->l_id - pull_lid)) % CHT_PENDING_WRITES,
      //          ack_i, ack_num, cht_ctx->loc_w_rob->pull_ptr, cht_ctx->loc_w_rob->push_ptr, cht_ctx->loc_w_rob->capacity);
    }

    assert((w_rob->l_id % cht_ctx->w_rob->max_size) == ack_ptr);
    if (w_rob->acks_seen == REM_MACH_NUM) {
      qp_meta->outstanding_messages--;
      assert(w_rob->w_state == SENT);
      if (DEBUG_ACKS)
        printf("Worker %d, sess %u: valid ack %u/%u write at ptr %d is ready \n",
               ctx->t_id, w_rob->sess_id, ack_i, ack_num,  ack_ptr);
    }
  }
}



static inline void cht_checks_and_stats_on_bcasting_commits(context_t *ctx)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  ctx_com_mes_t *com_mes = (ctx_com_mes_t *) get_fifo_pull_slot(send_fifo);
  if (DEBUG_COMMITS)
    my_printf(green, "Wrkr %u, Broadcasting commit %u, lid %lu, com_num %u \n",
              ctx->t_id, com_mes->opcode, com_mes->l_id, com_mes->com_num);
  if (ENABLE_ASSERTIONS) {
    assert(com_mes->com_num == get_fifo_slot_meta_pull(send_fifo)->coalesce_num);
    assert(send_fifo->net_capacity >= com_mes->com_num);

    assert(send_fifo != NULL);
    if (send_fifo->capacity > COMMIT_FIFO_SIZE)
      printf("com fifo capacity %u/%d \n", send_fifo->capacity, COMMIT_FIFO_SIZE);
    assert(send_fifo->capacity <= COMMIT_FIFO_SIZE);
    assert(com_mes->com_num > 0 && com_mes->com_num <= MAX_LIDS_IN_A_COMMIT);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].coms_sent += com_mes->com_num;
    t_stats[ctx->t_id].coms_sent_mes_num++;
  }
}


static inline void cht_check_polled_commit_and_print(context_t *ctx,
                                                     ctx_com_mes_t *com,
                                                     uint32_t buf_ptr)
{
  if (ENABLE_ASSERTIONS) {
    cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
    assert(com->m_id < MACHINE_NUM && com->m_id != ctx->m_id);
    if (DEBUG_COMMITS)
      my_printf(yellow, "Wrkr %d com opcode %d from machine %u, with %d coms for l_id %lu, waiting %u"
                  " at offset %d at address %p \n",
                ctx->t_id, com->opcode, com->m_id, com->com_num, com->l_id,
                cht_ctx->committed_w_id[com->m_id],
                buf_ptr, (void *) com);
    if (ENABLE_ASSERTIONS) {
      assert(com->opcode == COMMIT_OP);
      assert(com->com_num > 0 && com->com_num <= MAX_LIDS_IN_A_COMMIT);
    }
  }

  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].received_coms += com->com_num;
    t_stats[ctx->t_id].received_coms_mes_num++;
  }
}


static inline void cht_check_each_commit(context_t *ctx,
                                        ctx_com_mes_t *com,
                                        cht_w_rob_t *w_rob,
                                        uint16_t com_i)
{
  uint32_t com_num = com->com_num;
  cht_ctx_t *cht_ctx = (cht_ctx_t *) ctx->appl_ctx;
  uint64_t l_id = com->l_id;

  if (DEBUG_COMMITS)
    my_printf(yellow, "Wrkr %u, Com %u/%u, l_id %lu \n",
              ctx->t_id, com_i, com_num, l_id + com_i);

  if (ENABLE_ASSERTIONS) {
    assert(w_rob->w_state == VALID);
    assert(w_rob->l_id == l_id + com_i);
    assert(l_id + com_i == cht_ctx->committed_w_id[com->m_id]);
    assert(w_rob->coordin_m_id == com->m_id);
  }
}




#endif //ODYSSEY_CHT_DEBUG_UTIL_H
