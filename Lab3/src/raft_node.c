/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Representation of a peer
 * @author Willem Thiart himself@willemthiart.com
 * @version 0.1
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "raft.h"
#include "raft_private.h"

#define RAFT_NODE_VOTED_FOR_ME        (1 << 0)
#define RAFT_NODE_VOTING              (1 << 1)
#define RAFT_NODE_HAS_SUFFICIENT_LOG  (1 << 2)
#define RAFT_NODE_INACTIVE            (1 << 3)
#define RAFT_NODE_VOTING_COMMITTED    (1 << 4)
#define RAFT_NODE_ADDITION_COMMITTED  (1 << 5)


typedef struct
{
    void* udata;

    raft_index_t next_idx;
    raft_index_t match_idx;

    int flags;

    raft_node_id_t id;
} raft_node_private_t;

raft_node_t* raft_node_new(void* udata, raft_node_id_t id)
{
    raft_node_private_t* me;
    me = (raft_node_private_t*)__raft_calloc(1, sizeof(raft_node_private_t));
    if (!me)
        return NULL;
    me->udata = udata;
    me->next_idx = 1;
    me->match_idx = 0;
    me->id = id;
    me->flags = RAFT_NODE_VOTING;
    return (raft_node_t*)me;
}

void raft_node_free(raft_node_t* me_)
{
    __raft_free(me_);
}


void raft_node_set_active(raft_node_t* me_, int active)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (!active)
        me->flags |= RAFT_NODE_INACTIVE;
    else
        me->flags &= ~RAFT_NODE_INACTIVE;
}

void raft_node_set_voting_committed(raft_node_t* me_, int voting)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (voting)
        me->flags |= RAFT_NODE_VOTING_COMMITTED;
    else
        me->flags &= ~RAFT_NODE_VOTING_COMMITTED;
}

int raft_node_is_active(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return (me->flags & RAFT_NODE_INACTIVE) == 0;
}



