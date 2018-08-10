#define TKVDB_MEMNODE_TYPE tkvdb_memnode_alignval
#define TKVDB_MEMNODE_TYPE_COMMON tkvdb_memnode_alignval_common
#define TKVDB_MEMNODE_TYPE_LEAF tkvdb_memnode_alignval_leaf
#define TKVDB_IMPL_PUT tkvdb_put_alignval
#define TKVDB_IMPL_GET tkvdb_get_alignval
#define TKVDB_IMPL_CURSOR_PUSH tkvdb_cursor_push_alignval
#define TKVDB_IMPL_CURSOR_POP tkvdb_cursor_pop_alignval
#define TKVDB_IMPL_CURSOR_APPEND tkvdb_cursor_append_alignval
#define TKVDB_IMPL_NODE_ALLOC tkvdb_node_alloc_alignval
#define TKVDB_IMPL_NODE_NEW tkvdb_node_new_alignval
#define TKVDB_IMPL_CLONE_SUBNODES tkvdb_clone_subnodes_alignval
#define TKVDB_IMPL_SEEK tkvdb_seek_alignval
#define TKVDB_IMPL_FIRST tkvdb_first_alignval
#define TKVDB_IMPL_LAST tkvdb_last_alignval
#define TKVDB_IMPL_NEXT tkvdb_next_alignval
#define TKVDB_IMPL_PREV tkvdb_prev_alignval
#define TKVDB_IMPL_SMALLEST tkvdb_smallest_alignval
#define TKVDB_IMPL_BIGGEST tkvdb_biggest_alignval
#define TKVDB_IMPL_CURSOR_APPEND_SYM tkvdb_cursor_append_sym_alignval
#define TKVDB_IMPL_CURSOR_LOAD_ROOT tkvdb_cursor_load_root_alignval
#define TKVDB_IMPL_NODE_READ tkvdb_node_read_alignval
#define TKVDB_IMPL_NODE_FREE tkvdb_node_free_alignval
#define TKVDB_IMPL_MEMNODE tkvdb_memnode_alignval
#define TKVDB_IMPL_TR_RESET tkvdb_tr_reset_alignval
#define TKVDB_IMPL_TR_FREE tkvdb_tr_free_alignval
#define TKVDB_IMPL_ROLLBACK tkvdb_rollback_alignval
#define TKVDB_IMPL_NODE_TO_BUF tkvdb_node_to_buf_alignval
#define TKVDB_IMPL_NODE_CALC_DISKSIZE tkvdb_node_calc_disksize_alignval
#define TKVDB_IMPL_DO_COMMIT tkvdb_do_commit_alignval
#define TKVDB_IMPL_COMMIT tkvdb_commit_alignval
#define TKVDB_IMPL_DO_DEL tkvdb_do_del_alignval
#define TKVDB_IMPL_DEL tkvdb_del_alignval
#include "impl/memnode.h"
#include "impl/node.c"
#include "impl/put.c"
#include "impl/get.c"
#include "impl/cursor.c"
#include "impl/tr.c"
#include "impl/del.c"
#undef TKVDB_IMPL_PUT
#undef TKVDB_IMPL_GET
#undef TKVDB_IMPL_CURSOR_PUSH
#undef TKVDB_IMPL_CURSOR_POP
#undef TKVDB_IMPL_CURSOR_APPEND
#undef TKVDB_IMPL_NODE_ALLOC
#undef TKVDB_IMPL_NODE_NEW
#undef TKVDB_IMPL_CLONE_SUBNODES
#undef TKVDB_IMPL_SEEK
#undef TKVDB_IMPL_FIRST
#undef TKVDB_IMPL_LAST
#undef TKVDB_IMPL_NEXT
#undef TKVDB_IMPL_PREV
#undef TKVDB_IMPL_SMALLEST
#undef TKVDB_IMPL_BIGGEST
#undef TKVDB_IMPL_CURSOR_APPEND_SYM
#undef TKVDB_IMPL_CURSOR_LOAD_ROOT
#undef TKVDB_IMPL_NODE_READ
#undef TKVDB_IMPL_NODE_FREE
#undef TKVDB_IMPL_MEMNODE
#undef TKVDB_IMPL_TR_RESET
#undef TKVDB_IMPL_TR_FREE
#undef TKVDB_IMPL_ROLLBACK
#undef TKVDB_IMPL_NODE_TO_BUF
#undef TKVDB_IMPL_NODE_CALC_DISKSIZE
#undef TKVDB_IMPL_DO_COMMIT
#undef TKVDB_IMPL_COMMIT
#undef TKVDB_IMPL_DO_DEL
#undef TKVDB_IMPL_DEL
#undef TKVDB_MEMNODE_TYPE
#undef TKVDB_MEMNODE_TYPE_COMMON
#undef TKVDB_MEMNODE_TYPE_LEAF


#define TKVDB_MEMNODE_TYPE tkvdb_memnode_generic
#define TKVDB_MEMNODE_TYPE_COMMON tkvdb_memnode_generic_common
#define TKVDB_MEMNODE_TYPE_LEAF tkvdb_memnode_generic_leaf
#define TKVDB_IMPL_PUT tkvdb_put_generic
#define TKVDB_IMPL_GET tkvdb_get_generic
#define TKVDB_IMPL_CURSOR_PUSH tkvdb_cursor_push_generic
#define TKVDB_IMPL_CURSOR_POP tkvdb_cursor_pop_generic
#define TKVDB_IMPL_CURSOR_APPEND tkvdb_cursor_append_generic
#define TKVDB_IMPL_NODE_ALLOC tkvdb_node_alloc_generic
#define TKVDB_IMPL_NODE_NEW tkvdb_node_new_generic
#define TKVDB_IMPL_CLONE_SUBNODES tkvdb_clone_subnodes_generic
#define TKVDB_IMPL_SEEK tkvdb_seek_generic
#define TKVDB_IMPL_FIRST tkvdb_first_generic
#define TKVDB_IMPL_LAST tkvdb_last_generic
#define TKVDB_IMPL_NEXT tkvdb_next_generic
#define TKVDB_IMPL_PREV tkvdb_prev_generic
#define TKVDB_IMPL_SMALLEST tkvdb_smallest_generic
#define TKVDB_IMPL_BIGGEST tkvdb_biggest_generic
#define TKVDB_IMPL_CURSOR_APPEND_SYM tkvdb_cursor_append_sym_generic
#define TKVDB_IMPL_CURSOR_LOAD_ROOT tkvdb_cursor_load_root_generic
#define TKVDB_IMPL_NODE_READ tkvdb_node_read_generic
#define TKVDB_IMPL_NODE_FREE tkvdb_node_free_generic
#define TKVDB_IMPL_MEMNODE tkvdb_memnode_generic
#define TKVDB_IMPL_TR_RESET tkvdb_tr_reset_generic
#define TKVDB_IMPL_TR_FREE tkvdb_tr_free_generic
#define TKVDB_IMPL_ROLLBACK tkvdb_rollback_generic
#define TKVDB_IMPL_NODE_TO_BUF tkvdb_node_to_buf_generic
#define TKVDB_IMPL_NODE_CALC_DISKSIZE tkvdb_node_calc_disksize_generic
#define TKVDB_IMPL_DO_COMMIT tkvdb_do_commit_generic
#define TKVDB_IMPL_COMMIT tkvdb_commit_generic
#define TKVDB_IMPL_DO_DEL tkvdb_do_del_generic
#define TKVDB_IMPL_DEL tkvdb_del_generic
#include "impl/memnode.h"
#include "impl/node.c"
#include "impl/put.c"
#include "impl/get.c"
#include "impl/cursor.c"
#include "impl/tr.c"
#include "impl/del.c"
#undef TKVDB_IMPL_PUT
#undef TKVDB_IMPL_GET
#undef TKVDB_IMPL_CURSOR_PUSH
#undef TKVDB_IMPL_CURSOR_POP
#undef TKVDB_IMPL_CURSOR_APPEND
#undef TKVDB_IMPL_NODE_ALLOC
#undef TKVDB_IMPL_NODE_NEW
#undef TKVDB_IMPL_CLONE_SUBNODES
#undef TKVDB_IMPL_SEEK
#undef TKVDB_IMPL_FIRST
#undef TKVDB_IMPL_LAST
#undef TKVDB_IMPL_NEXT
#undef TKVDB_IMPL_PREV
#undef TKVDB_IMPL_SMALLEST
#undef TKVDB_IMPL_BIGGEST
#undef TKVDB_IMPL_CURSOR_APPEND_SYM
#undef TKVDB_IMPL_CURSOR_LOAD_ROOT
#undef TKVDB_IMPL_NODE_READ
#undef TKVDB_IMPL_NODE_FREE
#undef TKVDB_IMPL_MEMNODE
#undef TKVDB_IMPL_TR_RESET
#undef TKVDB_IMPL_TR_FREE
#undef TKVDB_IMPL_ROLLBACK
#undef TKVDB_IMPL_NODE_TO_BUF
#undef TKVDB_IMPL_NODE_CALC_DISKSIZE
#undef TKVDB_IMPL_DO_COMMIT
#undef TKVDB_IMPL_COMMIT
#undef TKVDB_IMPL_DO_DEL
#undef TKVDB_IMPL_DEL
#undef TKVDB_MEMNODE_TYPE
#undef TKVDB_MEMNODE_TYPE_COMMON
#undef TKVDB_MEMNODE_TYPE_LEAF


