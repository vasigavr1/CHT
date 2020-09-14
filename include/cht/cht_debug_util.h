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
      my_printf(green, "Wrkr %u, prep_i %u new write at "
                  "ptr %u with g_id %lu and m_id %u \n",
                ctx->t_id, prep_i, prep_mes->m_id);
  }
}


#endif //ODYSSEY_CHT_DEBUG_UTIL_H
