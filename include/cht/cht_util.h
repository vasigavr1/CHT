//
// Created by vasilis on 14/09/20.
//

#ifndef ODYSSEY_CHT_UTIL_H
#define ODYSSEY_CHT_UTIL_H

#include "cht_config.h"
#include "network_context.h"
#include "init_func.h"
#include <cht_inline_util.h>
#include "../../../odlib/include/trace/trace_util.h"


void cht_stats(stats_ctx_t *ctx);

static void cht_static_assert_compile_parameters()
{

  emphatic_print(green, "CHT");
  //static_assert(PREP_SIZE == sizeof(cht_prep_t));
  //static_assert(PREP_SEND_SIZE == sizeof(cht_prep_mes_t));
}

static void cht_init_globals()
{

}


static void cht_init_functionality(int argc, char *argv[])
{
  generic_static_assert_compile_parameters();
  cht_static_assert_compile_parameters();
  generic_init_globals(QP_NUM);
  cht_init_globals();
  handle_program_inputs(argc, argv);
}


static void cht_qp_meta_mfs(context_t *ctx)
{
  mf_t *mfs = calloc(QP_NUM, sizeof(mf_t));

  mfs[PREP_QP_ID].recv_handler = cht_prepare_handler;
  mfs[PREP_QP_ID].send_helper = cht_send_preps_helper;
  mfs[PREP_QP_ID].insert_helper = cht_insert_prep_help;
  mfs[PREP_QP_ID].recv_kvs = cht_KVS_batch_op_preps;
  //mfs[PREP_QP_ID].polling_debug = cht_debug_info_bookkeep;


  mfs[ACK_QP_ID].recv_handler = cht_ack_handler;
  mfs[ACK_QP_ID].send_helper = cht_send_acks_helper;

  mfs[COM_QP_ID].recv_handler = cht_commit_handler;
  mfs[COM_QP_ID].send_helper = cht_send_commits_helper;
  //mfs[COMMIT_W_QP_ID].polling_debug = cht_debug_info_bookkeep;
  //
  //mfs[R_QP_ID].recv_handler = r_handler;
  //
  //mfs[R_QP_ID].recv_kvs = cht_KVS_batch_op_reads;
  //mfs[R_QP_ID].insert_helper = insert_r_rep_help;
  //mfs[R_QP_ID].polling_debug = cht_debug_info_bookkeep;



  ctx_set_qp_meta_mfs(ctx, mfs);
  free(mfs);
}


static void cht_init_send_fifos(context_t *ctx)
{
  fifo_t *send_fifo = ctx->qp_meta[COM_QP_ID].send_fifo;
  ctx_com_mes_t *commits = (ctx_com_mes_t *) send_fifo->fifo;

  for (uint32_t i = 0; i < COMMIT_FIFO_SIZE; i++) {
    commits[i].opcode = COMMIT_OP;
    commits[i].m_id = ctx->m_id;
  }
  //
  ctx_ack_mes_t *ack_send_buf = (ctx_ack_mes_t *) ctx->qp_meta[ACK_QP_ID].send_fifo->fifo; //calloc(MACHINE_NUM, sizeof(ctx_ack_mes_t));
  assert(ctx->qp_meta[ACK_QP_ID].send_fifo->max_byte_size == CTX_ACK_SIZE * MACHINE_NUM);
  memset(ack_send_buf, 0, ctx->qp_meta[ACK_QP_ID].send_fifo->max_byte_size);
  for (int i = 0; i < MACHINE_NUM; i++) {
    ack_send_buf[i].m_id = (uint8_t) machine_id;
    ack_send_buf[i].opcode = OP_ACK;
  }

  cht_prep_mes_t *preps = (cht_prep_mes_t *) ctx->qp_meta[PREP_QP_ID].send_fifo->fifo;
  for (int i = 0; i < PREP_FIFO_SIZE; i++) {
    preps[i].opcode = KVS_OP_PUT;
    preps[i].m_id = ctx->m_id;
    for (uint16_t j = 0; j < PREP_COALESCE; j++) {
      //preps[i].prepare[j].opcode = KVS_OP_PUT;
      // preps[i].prepare[j].val_len = VALUE_SIZE >> SHIFT_BITS;
    }
  }
}


static void cht_init_qp_meta(context_t *ctx)
{
  per_qp_meta_t *qp_meta = ctx->qp_meta;
///
  create_per_qp_meta(&qp_meta[PREP_QP_ID], MAX_PREP_WRS,
                     MAX_RECV_PREP_WRS, SEND_BCAST_RECV_BCAST, RECV_REQ,
                     ACK_QP_ID,
                     REM_MACH_NUM, REM_MACH_NUM, PREP_BUF_SLOTS,
                     PREP_RECV_SIZE, PREP_SEND_SIZE, ENABLE_MULTICAST, ENABLE_MULTICAST,
                     CHT_PREP_MCAST_QP, 0, PREP_FIFO_SIZE,
                     PREP_CREDITS, PREP_MES_HEADER,
                     "send preps", "recv preps");
//
//
  crate_ack_qp_meta(&qp_meta[ACK_QP_ID],
                    PREP_QP_ID, REM_MACH_NUM,
                    REM_MACH_NUM, PREP_CREDITS);
//
  create_per_qp_meta(&qp_meta[COM_QP_ID], COM_WRS,
                     RECV_COM_WRS, SEND_BCAST_RECV_BCAST, RECV_SEC_ROUND,
                     COM_QP_ID,
                     REM_MACH_NUM, REM_MACH_NUM, COM_BUF_SLOTS,
                     CTX_COM_RECV_SIZE, CTX_COM_SEND_SIZE, ENABLE_MULTICAST, ENABLE_MULTICAST,
                     CHT_COM_MCAST_QP, 0, COMMIT_FIFO_SIZE,
                     COM_CREDITS, CTX_COM_SEND_SIZE,
                     "send commits", "recv commits");


  cht_qp_meta_mfs(ctx);
  cht_init_send_fifos(ctx);

}

static void* set_up_cht_ctx(context_t *ctx)
{
  cht_ctx_t* cht_ctx = (cht_ctx_t*) calloc(1,sizeof(cht_ctx_t));

  cht_ctx->w_rob = fifo_constructor(CHT_W_ROB_SIZE, sizeof(cht_w_rob_t), false, 0, MACHINE_NUM);
  cht_ctx->loc_w_rob = &cht_ctx->w_rob[ctx->m_id];
  //cht_ctx->loc_w_rob_ptr = fifo_constructor(cht_PENDING_WRITES, sizeof(w_rob_t*), false, 0, 1);

  cht_ctx->index_to_req_array = (uint32_t *) calloc(SESSIONS_PER_THREAD, sizeof(uint32_t));
  cht_ctx->stalled = (bool *) malloc(SESSIONS_PER_THREAD * sizeof(bool));
  cht_ctx->committed_w_id = calloc(MACHINE_NUM, sizeof(uint64_t));
  cht_ctx->inserted_w_id = calloc(MACHINE_NUM, sizeof(uint64_t));

  cht_ctx->ops = (ctx_trace_op_t *) calloc((size_t) CHT_TRACE_BATCH, sizeof(ctx_trace_op_t));

  cht_ctx->buf_reads = fifo_constructor(2 * SESSIONS_PER_THREAD, sizeof(cht_buf_op_t), false, 0, 1);

  for (int i = 0; i < SESSIONS_PER_THREAD; i++) cht_ctx->stalled[i] = false;
  for (uint8_t m_id = 0; m_id < MACHINE_NUM; ++m_id) {
    for (uint32_t i = 0; i < CHT_W_ROB_SIZE; i++) {
      cht_w_rob_t *w_rob = get_fifo_slot(&cht_ctx->w_rob[m_id], i);
      w_rob->w_state = INVALID;
      w_rob->m_id = m_id;
      w_rob->id = (uint16_t) i;
      if (m_id == ctx->m_id) {
        w_rob->inv_applied = true;
      }
    }
  }

  cht_ctx->ptrs_to_w = calloc(1, sizeof(cht_ptrs_to_w_t));
  cht_ctx->ptrs_to_w->w_rob = calloc(CHT_UPDATE_BATCH, sizeof(cht_w_rob_t*));

  cht_ctx->ptrs_to_prep = calloc(1, sizeof(ptrs_to_prep_t));
  cht_ctx->ptrs_to_prep->ptr_to_ops = calloc(MAX_INCOMING_PREP, sizeof(cht_prep_t*));
  cht_ctx->ptrs_to_prep->ptr_to_mes = calloc(MAX_INCOMING_PREP, sizeof(cht_prep_mes_t*));

  if (!ENABLE_CLIENTS)
    cht_ctx->trace = trace_init(ctx->t_id);

  return (void *) cht_ctx;
}

typedef struct stats {
  double batch_size_per_thread[WORKERS_PER_MACHINE];
  double com_batch_size[WORKERS_PER_MACHINE];
  double prep_batch_size[WORKERS_PER_MACHINE];
  double ack_batch_size[WORKERS_PER_MACHINE];
  double write_batch_size[WORKERS_PER_MACHINE];
  double stalled_gid[WORKERS_PER_MACHINE];
  double stalled_ack_prep[WORKERS_PER_MACHINE];
  double stalled_com_credit[WORKERS_PER_MACHINE];


  double cache_hits_per_thread[WORKERS_PER_MACHINE];


  double preps_sent[WORKERS_PER_MACHINE];
  double acks_sent[WORKERS_PER_MACHINE];
  double coms_sent[WORKERS_PER_MACHINE];

  double received_coms[WORKERS_PER_MACHINE];
  double received_acks[WORKERS_PER_MACHINE];
  double received_preps[WORKERS_PER_MACHINE];

  double write_ratio_per_client[WORKERS_PER_MACHINE];
} all_stats_t;


#endif //ODYSSEY_CHT_UTIL_H