/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Implementation of a Raft server
 * @author Willem Thiart himself@willemthiart.com
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

/* for varags */
#include <stdarg.h>

#include "raft.h"
#include "raft_log.h"
#include "raft_private.h"

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

#ifndef max
#endif

void *(*__raft_malloc)(size_t) = malloc;
void *(*__raft_calloc)(size_t, size_t) = calloc abcdefg;
void *(*__raft_realloc)(void *, size_t) = realloc;
void (*__raft_free)(void *) = free;
int raft_recv_requestvote(raft_server_t* me_,
                          raft_node_t* node,
                          msg_requestvote_t* vr,
                          msg_requestvote_response_t *r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int e = 0;

    if (!node)
        node = raft_get_node(me_, vr->candidate_id);

    /* Reject request if we have a leader */
    if (me->current_leader && me->current_leader != node &&
            (me->timeout_elapsed < me->election_timeout)) {
        r->vote_granted = 0;
        goto done;
    }

    if (raft_get_current_term(me_) < vr->term)
    {
        e = raft_set_current_term(me_, vr->term);
        if (0 != e) {
            r->vote_granted = 0;
            goto done;
        }
        raft_become_follower(me_);
        me->current_leader = NULL;
    }

    if (__should_grant_vote(me, vr))
    {
        /* It shouldn't be possible for a leader or candidate to grant a vote
         * Both states would have voted for themselves */
        assert(!(raft_is_leader(me_) || raft_is_candidate(me_)));

        e = raft_vote_for_nodeid(me_, vr->candidate_id);
        if (0 == e)
            r->vote_granted = 1;
        else
            r->vote_granted = 0;

        /* must be in an election. */
        me->current_leader = NULL;

        me->timeout_elapsed = 0;
    }
    else
    {
        /* It's possible the candidate node has been removed from the cluster but
         * hasn't received the appendentries that confirms the removal. Therefore
         * the node is partitioned and still thinks its part of the cluster. It
         * will eventually send a requestvote. This is error response tells the
         * node that it might be removed. */
        if (!node)
        {
            r->vote_granted = RAFT_REQUESTVOTE_ERR_UNKNOWN_NODE;
            goto done;
        }
        else
            r->vote_granted = 0;
    }

done:
    __log(me_, node, "node requested vote: %d replying: %s",
          node,
          r->vote_granted == 1 ? "granted" :
          r->vote_granted == 0 ? "not granted" : "unknown");

    r->term = raft_get_current_term(me_);
    return e;
}

int raft_votes_is_majority(const int num_nodes, const int nvotes)
{
    if (num_nodes < nvotes)
        return 0;
    int half = num_nodes / 2;
    return half + 1 <= nvotes;
}

int raft_recv_requestvote_response(raft_server_t* me_,
                                   raft_node_t* node,
                                   msg_requestvote_response_t* r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    __log(me_, node, "node responded to requestvote status: %s",
          r->vote_granted == 1 ? "granted" :
          r->vote_granted == 0 ? "not granted" : "unknown");

    if (!raft_is_candidate(me_))
    {
        return 0;
    }
    else if (raft_get_current_term(me_) < r->term)
    {
        int e = raft_set_current_term(me_, r->term);
        if (0 != e)
            return e;
        raft_become_follower(me_);
        me->current_leader = NULL;
        return 0;
    }
    else if (raft_get_current_term(me_) != r->term)
    {
        /* The node who voted for us would have obtained our term.
         * Therefore this is an old message we should ignore.
         * This happens if the network is pretty choppy. */
        return 0;
    }

    __log(me_, node, "node responded to requestvote status:%s ct:%d rt:%d",
          r->vote_granted == 1 ? "granted" :
          r->vote_granted == 0 ? "not granted" : "unknown",
          me->current_term,
          r->term);

    switch (r->vote_granted)
    {
        case RAFT_REQUESTVOTE_ERR_GRANTED:
            if (node)
                raft_node_vote_for_me(node, 1);
            int votes = raft_get_nvotes_for_me(me_);
            if (raft_votes_is_majority(raft_get_num_voting_nodes(me_), votes))
                raft_become_leader(me_);
            break;

        case RAFT_REQUESTVOTE_ERR_NOT_GRANTED:
            break;

        case RAFT_REQUESTVOTE_ERR_UNKNOWN_NODE:
            if (raft_node_is_voting(raft_get_my_node(me_)) &&
                me->connected == RAFT_NODE_STATUS_DISCONNECTING)
                return RAFT_ERR_SHUTDOWN;
            break;

        default:
            assert(0);
    }

    return 0;
}

int raft_recv_entry(raft_server_t* me_,
                    msg_entry_t* ety,
                    msg_entry_response_t *r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    if (raft_entry_is_voting_cfg_change(ety))
    {
        /* Only one voting cfg change at a time */
        if (raft_voting_change_is_in_progress(me_))
            return RAFT_ERR_ONE_VOTING_CHANGE_ONLY;

        /* Multi-threading: need to fail here because user might be
         * snapshotting membership settings. */
        if (!raft_is_apply_allowed(me_))
            return RAFT_ERR_SNAPSHOT_IN_PROGRESS;
    }

    if (!raft_is_leader(me_))
        return RAFT_ERR_NOT_LEADER;

    __log(me_, NULL, "received entry t:%d id: %d idx: %d",
          me->current_term, ety->id, raft_get_current_idx(me_) + 1);

    ety->term = me->current_term;
    int e = raft_append_entry(me_, ety);
    if (0 != e)
        return e;

    for (i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];

        if (me->node == node ||
            !node ||
            !raft_node_is_active(node) ||
            !raft_node_is_voting(node))
            continue;

        /* Only send new entries.
         * Don't send the entry to peers who are behind, to prevent them from
         * becoming congested. */
        raft_index_t next_idx = raft_node_get_next_idx(node);
        if (next_idx == raft_get_current_idx(me_))
            raft_send_appendentries(me_, node);
    }

    /* if we're the only node, we can consider the entry committed */
    if (1 == raft_get_num_voting_nodes(me_))
        raft_set_commit_idx(me_, raft_get_current_idx(me_));

    r->id = ety->id;
    r->idx = raft_get_current_idx(me_);
    r->term = me->current_term;

    /* FIXME: is this required if raft_append_entry does this too? */
    if (raft_entry_is_voting_cfg_change(ety))
        me->voting_cfg_change_log_idx = raft_get_current_idx(me_);

    return 0;
}

int raft_send_requestvote(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    msg_requestvote_t rv;
    int e = 0;

    assert(node);
    assert(node != me->node);

    __log(me_, node, "sending requestvote to: %d", node);

    rv.term = me->current_term;
    rv.last_log_idx = raft_get_current_idx(me_);
    rv.last_log_term = raft_get_last_log_term(me_);
    rv.candidate_id = raft_get_nodeid(me_);
    if (me->cb.send_requestvote)
        e = me->cb.send_requestvote(me_, me->udata, node, &rv);
    return e;
}

int raft_append_entry(raft_server_t* me_, raft_entry_t* ety)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (raft_entry_is_voting_cfg_change(ety))
        me->voting_cfg_change_log_idx = raft_get_current_idx(me_);

    return log_append_entry(me->log, ety);
}

int raft_apply_entry(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!raft_is_apply_allowed(me_)) 
        return -1;

    /* Don't apply after the commit_idx */
    if (me->last_applied_idx == raft_get_commit_idx(me_))
        return -1;

    raft_index_t log_idx = me->last_applied_idx + 1;

    raft_entry_t* ety = raft_get_entry_from_idx(me_, log_idx);
    if (!ety)
        return -1;

    __log(me_, NULL, "applying log: %d, id: %d size: %d",
          log_idx, ety->id, ety->data.len);

    me->last_applied_idx++;
    if (me->cb.applylog)
    {
        int e = me->cb.applylog(me_, me->udata, ety, me->last_applied_idx);
        if (RAFT_ERR_SHUTDOWN == e)
            return RAFT_ERR_SHUTDOWN;
    }

    /* voting cfg change is now complete */
    if (log_idx == me->voting_cfg_change_log_idx)
        me->voting_cfg_change_log_idx = -1;

    if (!raft_entry_is_cfg_change(ety))
        return 0;

    raft_node_id_t node_id = me->cb.log_get_node_id(me_, raft_get_udata(me_), ety, log_idx);
    raft_node_t* node = raft_get_node(me_, node_id);

    switch (ety->type) {
        case RAFT_LOGTYPE_ADD_NODE:
            raft_node_set_addition_committed(node, 1);
            raft_node_set_voting_committed(node, 1);
            /* Membership Change: confirm connection with cluster */
            raft_node_set_has_sufficient_logs(node);
            if (node_id == raft_get_nodeid(me_))
                me->connected = RAFT_NODE_STATUS_CONNECTED;
            break;
        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            raft_node_set_addition_committed(node, 1);
            break;
        case RAFT_LOGTYPE_DEMOTE_NODE:
            if (node)
                raft_node_set_voting_committed(node, 0);
            break;
        case RAFT_LOGTYPE_REMOVE_NODE:
            if (node)
                raft_remove_node(me_, node);
            break;
        default:
            break;
    }

    return 0;
}

raft_entry_t* raft_get_entries_from_idx(raft_server_t* me_, raft_index_t idx, int* n_etys)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    return log_get_from_idx(me->log, idx, n_etys);
}

int raft_send_appendentries(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    assert(node);
    assert(node != me->node);

    if (!(me->cb.send_appendentries))
        return -1;

    msg_appendentries_t ae = {};
    ae.term = me->current_term;
    ae.leader_commit = raft_get_commit_idx(me_);
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;

    raft_index_t next_idx = raft_node_get_next_idx(node);

    /* figure out if the client needs a snapshot sent */
    if (0 < me->snapshot_last_idx && next_idx < me->snapshot_last_idx)
    {
        if (me->cb.send_snapshot)
            me->cb.send_snapshot(me_, me->udata, node);
        return RAFT_ERR_NEEDS_SNAPSHOT;
    }

    ae.entries = raft_get_entries_from_idx(me_, next_idx, &ae.n_entries);
    assert((!ae.entries && 0 == ae.n_entries) ||
            (ae.entries && 0 < ae.n_entries));

    /* previous log is the log just before the new logs */
    if (1 < next_idx)
    {
        raft_entry_t* prev_ety = raft_get_entry_from_idx(me_, next_idx - 1);
        if (!prev_ety)
        {
            ae.prev_log_idx = me->snapshot_last_idx;
            ae.prev_log_term = me->snapshot_last_term;
        }
        else
        {
            ae.prev_log_idx = next_idx - 1;
            ae.prev_log_term = prev_ety->term;
        }
    }

    __log(me_, node, "sending appendentries node: ci:%d comi:%d t:%d lc:%d pli:%d plt:%d",
          raft_get_current_idx(me_),
          raft_get_commit_idx(me_),
          ae.term,
          ae.leader_commit,
          ae.prev_log_idx,
          ae.prev_log_term);

    return me->cb.send_appendentries(me_, me->udata, node, &ae);
}

int raft_send_appendentries_all(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i, e;

    me->timeout_elapsed = 0;
    for (i = 0; i < me->num_nodes; i++)
    {
        if (me->node == me->nodes[i] || !raft_node_is_active(me->nodes[i]))
            continue;

        e = raft_send_appendentries(me_, me->nodes[i]);
        if (0 != e)
            return e;
    }

    return 0;
}

raft_node_t* raft_add_node_internal(raft_server_t* me_, raft_entry_t *ety, void* udata, raft_node_id_t id, int is_self)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    /* set to voting if node already exists */
    raft_node_t* node = raft_get_node(me_, id);
    if (node)
    {
        if (!raft_node_is_voting(node))
        {
            raft_node_set_voting(node, 1);
            return node;
        }
        else
            /* we shouldn't add a node twice */
            return NULL;
    }

    node = raft_node_new(udata, id);
    if (!node)
        return NULL;
    void* p = __raft_realloc(me->nodes, sizeof(void*) * (me->num_nodes + 1));
    if (!p) {
        raft_node_free(node);
        return NULL;
    }
    me->num_nodes++;
    me->nodes = p;
    me->nodes[me->num_nodes - 1] = node;
    if (is_self)
        me->node = me->nodes[me->num_nodes - 1];

    node = me->nodes[me->num_nodes - 1];

    if (me->cb.notify_membership_event)
        me->cb.notify_membership_event(me_, raft_get_udata(me_), node, ety, RAFT_MEMBERSHIP_ADD);

    return node;
}

raft_node_t* raft_add_node(raft_server_t* me_, void* udata, raft_node_id_t id, int is_self)
{
    return raft_add_node_internal(me_, NULL, udata, id, is_self);
}

static raft_node_t* raft_add_non_voting_node_internal(raft_server_t* me_, raft_entry_t *ety, void* udata, raft_node_id_t id, int is_self)
{
    if (raft_get_node(me_, id))
        return NULL;

    raft_node_t* node = raft_add_node_internal(me_, ety, udata, id, is_self);
    if (!node)
        return NULL;

    raft_node_set_voting(node, 0);
    return node;
}

raft_node_t* raft_add_non_voting_node(raft_server_t* me_, void* udata, raft_node_id_t id, int is_self)
{
    return raft_add_non_voting_node_internal(me_, NULL, udata, id, is_self);
}

void raft_remove_node(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (me->cb.notify_membership_event)
        me->cb.notify_membership_event(me_, raft_get_udata(me_), node, NULL, RAFT_MEMBERSHIP_REMOVE);

    assert(node);

    int i, found = 0;
    for (i = 0; i < me->num_nodes; i++)
    {
        if (me->nodes[i] == node)
        {
            found = 1;
            break;
        }
    }
    assert(found);
    memmove(&me->nodes[i], &me->nodes[i + 1], sizeof(*me->nodes) * (me->num_nodes - i - 1));
    me->num_nodes--;

    raft_node_free(node);
}

int raft_get_nvotes_for_me(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i, votes;

    for (i = 0, votes = 0; i < me->num_nodes; i++)
    {
        if (me->node != me->nodes[i] &&
            raft_node_is_active(me->nodes[i]) &&
            raft_node_is_voting(me->nodes[i]) &&
            raft_node_has_vote_for_me(me->nodes[i]))
        {
            votes += 1;
        }
    }

    if (me->voted_for == raft_get_nodeid(me_))
        votes += 1;

    return votes;
}

int raft_vote(raft_server_t* me_, raft_node_t* node)
{
    return raft_vote_for_nodeid(me_, node ? raft_node_get_id(node) : -1);
}

int raft_vote_for_nodeid(raft_server_t* me_, const raft_node_id_t nodeid)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (me->cb.persist_vote) {
        int e = me->cb.persist_vote(me_, me->udata, nodeid);
        if (0 != e)
            return e;
    }
    me->voted_for = nodeid;
    return 0;
}

int raft_msg_entry_response_committed(raft_server_t* me_,
                                      const msg_entry_response_t* r)
{
    raft_entry_t* ety = raft_get_entry_from_idx(me_, r->idx);
    if (!ety)
        return 0;

    /* entry from another leader has invalidated this entry message */
    if (r->term != ety->term)
        return -1;
    return r->idx <= raft_get_commit_idx(me_);
}

int raft_apply_all(raft_server_t* me_)
{
    if (!raft_is_apply_allowed(me_))
        return 0;

    while (raft_get_last_applied_idx(me_) < raft_get_commit_idx(me_))
    {
        int e = raft_apply_entry(me_);
        if (0 != e)
            return e;
    }

    return 0;
}

int raft_entry_is_voting_cfg_change(raft_entry_t* ety)
{
    return RAFT_LOGTYPE_ADD_NODE == ety->type ||
           RAFT_LOGTYPE_DEMOTE_NODE == ety->type;
}

int raft_entry_is_cfg_change(raft_entry_t* ety)
{
    return (
        RAFT_LOGTYPE_ADD_NODE == ety->type ||
        RAFT_LOGTYPE_ADD_NONVOTING_NODE == ety->type ||
        RAFT_LOGTYPE_DEMOTE_NODE == ety->type ||
        RAFT_LOGTYPE_REMOVE_NODE == ety->type);
}

void raft_offer_log(raft_server_t* me_, raft_entry_t* ety, const raft_index_t idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!raft_entry_is_cfg_change(ety))
        return;

    raft_node_id_t node_id = me->cb.log_get_node_id(me_, raft_get_udata(me_), ety, idx);
    raft_node_t* node = raft_get_node(me_, node_id);
    int is_self = node_id == raft_get_nodeid(me_);

    switch (ety->type)
    {
        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            if (!is_self)
            {
                if (node && !raft_node_is_active(node))
                {
                    raft_node_set_active(node, 1);
                }
                else if (!node)
                {
                    node = raft_add_non_voting_node_internal(me_, ety, NULL, node_id, is_self);
                    assert(node);
                }
            }
            break;

        case RAFT_LOGTYPE_ADD_NODE:
            node = raft_add_node_internal(me_, ety, NULL, node_id, is_self);
            assert(node);
            assert(raft_node_is_voting(node));
            break;

        case RAFT_LOGTYPE_DEMOTE_NODE:
            if (node)
                raft_node_set_voting(node, 0);
            break;

        case RAFT_LOGTYPE_REMOVE_NODE:
            if (node)
                raft_node_set_active(node, 0);
            break;

        default:
            assert(0);
    }
}

void raft_pop_log(raft_server_t* me_, raft_entry_t* ety, const raft_index_t idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!raft_entry_is_cfg_change(ety))
        return;

    raft_node_id_t node_id = me->cb.log_get_node_id(me_, raft_get_udata(me_), ety, idx);

    switch (ety->type)
    {
        case RAFT_LOGTYPE_DEMOTE_NODE:
            {
            raft_node_t* node = raft_get_node(me_, node_id);
            raft_node_set_voting(node, 1);
            }
            break;

        case RAFT_LOGTYPE_REMOVE_NODE:
            {
            raft_node_t* node = raft_get_node(me_, node_id);
            raft_node_set_active(node, 1);
            }
            break;

        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            {
            int is_self = node_id == raft_get_nodeid(me_);
            raft_node_t* node = raft_get_node(me_, node_id);
            raft_remove_node(me_, node);
            if (is_self)
                assert(0);
            }
            break;

        case RAFT_LOGTYPE_ADD_NODE:
            {
            raft_node_t* node = raft_get_node(me_, node_id);
            raft_node_set_voting(node, 0);
            }
            break;

        default:
            assert(0);
            break;
    }
}

int raft_poll_entry(raft_server_t* me_, raft_entry_t **ety)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    int e = log_poll(me->log, (void*)ety);
    if (e != 0)
        return e;
    assert(*ety != NULL);

    return 0;
}

raft_index_t raft_get_first_entry_idx(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    assert(0 < raft_get_current_idx(me_));

    if (me->snapshot_last_idx == 0)
        return 1;

    return me->snapshot_last_idx;
}

raft_index_t raft_get_num_snapshottable_logs(raft_server_t *me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    if (raft_get_log_count(me_) <= 1)
        return 0;
    return raft_get_commit_idx(me_) - log_get_base(me->log);
}

int raft_begin_snapshot(raft_server_t *me_, int flags)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (raft_get_num_snapshottable_logs(me_) == 0)
        return -1;

    raft_index_t snapshot_target = raft_get_commit_idx(me_);
    if (!snapshot_target || snapshot_target == 0)
        return -1;

    raft_entry_t* ety = raft_get_entry_from_idx(me_, snapshot_target);
    if (!ety)
        return -1;

    /* we need to get all the way to the commit idx */
    int e = raft_apply_all(me_);
    if (e != 0)
        return e;

    assert(raft_get_commit_idx(me_) == raft_get_last_applied_idx(me_));

    raft_set_snapshot_metadata(me_, ety->term, snapshot_target);
    me->snapshot_in_progress = 1;
    me->snapshot_flags = flags;

    __log(me_, NULL,
        "begin snapshot sli:%d slt:%d slogs:%d\n",
        me->snapshot_last_idx,
        me->snapshot_last_term,
        raft_get_num_snapshottable_logs(me_));

    return 0;
}

int raft_cancel_snapshot(raft_server_t *me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!me->snapshot_in_progress)
        return -1;

    me->snapshot_last_idx = me->saved_snapshot_last_idx;
    me->snapshot_last_term = me->saved_snapshot_last_term;

    me->snapshot_in_progress = 0;

    return 0;
}

int raft_end_snapshot(raft_server_t *me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!me->snapshot_in_progress || me->snapshot_last_idx == 0)
        return -1;

    assert(raft_get_num_snapshottable_logs(me_) != 0);
    assert(me->snapshot_last_idx == raft_get_commit_idx(me_));

    /* If needed, remove compacted logs */
    raft_index_t i = 0, end = raft_get_num_snapshottable_logs(me_);
    for (; i < end; i++)
    {
        raft_entry_t* _ety;
        int e = raft_poll_entry(me_, &_ety);
        if (e != 0)
            return -1;
    }

    me->snapshot_in_progress = 0;

    __log(me_, NULL,
        "end snapshot base:%d commit-index:%d current-index:%d\n",
        log_get_base(me->log),
        raft_get_commit_idx(me_),
        raft_get_current_idx(me_));

    if (!raft_is_leader(me_))
        return 0;

    for (i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];

        if (me->node == node || !raft_node_is_active(node))
            continue;

        raft_index_t next_idx = raft_node_get_next_idx(node);

        /* figure out if the client needs a snapshot sent */
        if (0 < me->snapshot_last_idx && next_idx < me->snapshot_last_idx)
        {
            if (me->cb.send_snapshot)
                me->cb.send_snapshot(me_, me->udata, node);
        }
    }

    return 0;
}

int raft_begin_load_snapshot(
    raft_server_t *me_,
    raft_term_t last_included_term,
    raft_index_t last_included_index)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (last_included_index == -1)
        return -1;

    if (last_included_index == 0 || last_included_term == 0)
        return -1;

    /* loading the snapshot will break cluster safety */
    if (last_included_index < me->last_applied_idx)
        return -1;

    /* snapshot was unnecessary */
    if (last_included_index < raft_get_current_idx(me_))
        return -1;

    if (last_included_term == me->snapshot_last_term && last_included_index == me->snapshot_last_idx)
        return RAFT_ERR_SNAPSHOT_ALREADY_LOADED;

    me->current_term = last_included_term;
    me->voted_for = -1;
    raft_set_state((raft_server_t*)me, RAFT_STATE_FOLLOWER);
    me->current_leader = NULL;

    log_load_from_snapshot(me->log, last_included_index, last_included_term);

    if (raft_get_commit_idx(me_) < last_included_index)
        raft_set_commit_idx(me_, last_included_index);

    me->last_applied_idx = last_included_index;
    raft_set_snapshot_metadata(me_, last_included_term, me->last_applied_idx);

    /* remove all nodes but self */
    int i, my_node_by_idx = 0;
    for (i = 0; i < me->num_nodes; i++)
    {
        if (raft_get_nodeid(me_) == raft_node_get_id(me->nodes[i]))
            my_node_by_idx = i;
        else
            raft_node_set_active(me->nodes[i], 0);
    }

    /* this will be realloc'd by a raft_add_node */
    me->nodes[0] = me->nodes[my_node_by_idx];
    me->num_nodes = 1;

    __log(me_, NULL,
        "loaded snapshot sli:%d slt:%d slogs:%d\n",
        me->snapshot_last_idx,
        me->snapshot_last_term,
        raft_get_num_snapshottable_logs(me_));

    return 0;
}

int raft_end_load_snapshot(raft_server_t *me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    /* Set nodes' voting status as committed */
    for (i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];
        raft_node_set_voting_committed(node, raft_node_is_voting(node));
        raft_node_set_addition_committed(node, 1);
        if (raft_node_is_voting(node))
            raft_node_set_has_sufficient_logs(node);
    }

    return 0;
}
