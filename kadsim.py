# -*- coding: utf-8 -*-

import random
import math

'''
TODO:
divide k-bucket===
k-bucket===
route table===
find_node iteration===
touch route table===
cmd_find_node===
store
find_key
key spread
test
'''

# params

# M bits id space
M = 8
# capacity of single k-bucket
# also a key will have K replications
K = 3
# send query to ALPHA nodes
ALPHA = 3

# constants

# inidicates a non-exists node
ID_NONE = -1

# auto

MAX_ID = (2 ** M) - 1

# network data

# node table: node_id => node
# only for tracking purpose. nodes can't communicate by id
node_id_map = {}

# address table: address => node
# to communicate with a node, you must get the address of it
#node_addr_map = {}

# query rounds should not exeed log(n, 2)
MAX_ROUNDS = 1

# util funcs

# XOR
def xor(a, b):
    return a ^ b

# node with distance d should be put in k_number-th k-bucket
def k_number(d):
    k = int(math.log(d, 2))
    if k >= M:
        print 'distance too big', d
        return None
    else:
        return k

# select nearest n nodes from seq
def nearest_n(seq, n, origin):
    s = sorted(seq, key=lambda i:xor(i, origin))
    return s[:n] if len(s) > n else s

def get_global_nearest(n, target):
    if target in node_id_map:
        return [target,]
    else:
        return nearest_n(node_id_map.keys(), n, target)

# readable form of a node_id
def readable_id(node_id):
    return '%s(%d)' % (bin(node_id), node_id)

def get_addr_by_id(node_id):
    return 'addr_%d' % node_id

def gen_node_id():
    if len(node_id_map) > MAX_ID:
        print 'all id used!!!'
        return None

    while(True):
        nid = random.randint(0, MAX_ID)
        if nid not in node_id_map:
            return nid

def update_max_rounds():
    global MAX_ROUNDS
    n = len(node_id_map)
    if n == 0:
        MAX_ROUNDS = 1
    else:
        MAX_ROUNDS = int(math.log(len(node_id_map), 2)) + 1

def add_by_id(node):
    node_id_map[node.node_id] = node
    update_max_rounds()

def del_by_id(node):
    if node in node_id_map:
        del node_id_map[node]
        update_max_rounds()

# return True if target online
def ping(target_id):
    return target_id in node_id_map

#def add_by_addr(node):
#    node_addr_map[node.addr()] = node

# classes

class KBucket(object):
    '''
    a single k-bucket
    each record has a timestamp
    records are sorted by timestamp
    '''
    def __init__(self, bucket_id):
        '''
        bucket_id shoud be in [0,M)
        '''
        self.bucket_id = bucket_id
        self.addrs = []
    
    def info(self):
        return 'k-bucket %d' % self.bucket_id

    def len(self):
        return len(self.addrs)

    def touch(self, nid):
        #print 'touch', nid, self.addrs
        if nid in self.addrs:
            # old node. move to rear
            self.addrs = [i for i in self.addrs if i != nid]
            self.addrs.append(nid)
        else:
            # new node
            if self.len() < K:
                # k-bucket not full, save new node
                self.addrs.append(nid)
            else:
                # k-bucket full, check oldest node
                oldest = self.addrs[0]
                if ping(oldest):
                    # oldest node online. move to rear
                    self.addrs = [i for i in self.addrs if i != oldest]
                    self.addrs.append(oldest)
                else:
                    # oldest node offline. replace it with new node
                    self.addrs = [i for i in self.addrs if i != oldest]
                    self.addrs.append(nid)

        #print 'after', self.addrs

    def remove(self, nid):
        if nid in self.addrs:
            self.addrs = [i for i in self.addrs if i != nid]

    # find node in this bucket by id
    # if node not found, return all known nodes
    def find_node(self, target_id):
        return [target_id,] if target_id in self.addrs else self.addrs

class RouteTable(object):
    '''
    route table consists of multiple k-buckets
    '''
    def __init__(self, node_id):
        # k-buckets
        self.kbs = {}
        self.node_id = node_id

    def touch(self, nid):
        if nid == self.node_id:
            print 'why touch self?', readable_id(nid)
            return

        # find proper k-bucket or create one
        d = xor(self.node_id, nid)
        bucket_id = k_number(d)
        if bucket_id not in self.kbs:
            self.kbs[bucket_id] = KBucket(bucket_id)
        
        # touch
        self.kbs[bucket_id].touch(nid)
        print 'node %d touch %d' % (self.node_id, nid)

    def remove(self, nid):
        # find proper k-bucket or create one
        d = xor(self.node_id, nid)
        bucket_id = k_number(d)
        if bucket_id in self.kbs:
            self.kbs[bucket_id].remove(nid)

    def print_table(self):
        print 'route table of node %s' % readable_id(self.node_id)
        for v in self.kbs.values():
            print 'bucket %d:' % v.bucket_id, v.addrs

    # find node in this route table by id
    # return k nearest known nodes
    # also touch caller
    def find_node(self, target_id, caller_id):
        # find node in all k-buckets
        result = set()
        for bucket in self.kbs.values():
            for n in bucket.find_node(target_id):
                result.add(n)

        # touch caller after query! otherwise query will end too soon in bootstrap
        if caller_id != ID_NONE:
            self.touch(caller_id)

        # take nearest k nodes
        #print result
        result = list(result)
        return nearest_n(result, K, target_id)

class Node(object):
    '''
    a node in kad network
    '''
    def __init__(self, node_id, bootstrap_id):
        self.node_id = node_id
        self.route = RouteTable(node_id)

        # bootstrap
        if bootstrap_id != ID_NONE:
            self.route.touch(bootstrap_id)
            self.find_node_iter(self.node_id, ID_NONE)

    def info(self):
        return readable_id(self.node_id)

    def addr(self):
        return get_addr_by_id(self.node_id)

    # check if now is better(nearer) than prev
    # better means total distance is smaller, or length differ
    def better(self, prev, now, target_id):
        if len(prev) != len(now):
            return True
        else:
            return sum([xor(x, target_id) for x in now]) < sum([xor(x, target_id) for x in prev])

    # find node in local route table
    def find_node_local(self, target_id, caller_id):
        # serch in local route table
        results = self.route.find_node(target_id, caller_id)
        return [target_id,] if target_id in results else results

    # find node iteratively
    def find_node_iter(self, target_id, caller_id):
        # serch in local route table
        results = self.route.find_node(target_id, caller_id)
        if target_id in results:
            print 'found target %d in local route table' % target_id
            return [target_id,]

        # target not found locally, send query
        queried = set()
        # should not query self
        queried.add(self.node_id)
        rounds = 0
        while(True):
            rounds += 1
            if rounds > MAX_ROUNDS:
                print '''##########################
WARNING: rounds %d exceeds theoretical upper bound %d
##########################''' % (rounds, MAX_ROUNDS)

            # find node on all nodes in results
            # FIXME: only find one node in each iteration?
            new_results = set(results)
            print 'before query:', results
            for node in results:
                # skip queried nodes
                if node in queried:
                    continue

                if node in node_id_map:
                    queried.add(node)
                    # remember to touch quried node
                    self.route.touch(node)
                    print 'query node %d for %d' % (node, target_id)
                    tmp_res = node_id_map[node].find_node_local(target_id, self.node_id)
                    print 'ndoe %d return' % node, tmp_res
                    for i in tmp_res:
                        new_results.add(i)
                else:
                    new_results.remove(node)
                    # FIXME: need to take out failed node from route table?
                    self.route.remove(node)

            # stop iteration when target is found
            # FIXME: what if target is invalid? caller need to retry?
            # FIXME: maybe we should return k nearest nodes including target?
            if target_id in new_results:
                print 'found target %d with query, iteration ends at round %d' % (target_id, rounds)
                return [target_id,]

            # only take k nearest nodes
            new_results = list(new_results)
            new_results = nearest_n(new_results, K, target_id)

            # stop iteration when no nearer node could be found
            if not self.better(results, new_results, target_id):
                print new_results, 'not better than', results, 'iteration ends at round %d' % rounds
                return new_results

            # prepare for next round of iteration
            results = new_results
            print 'after query:', results

# cmd funcs

# help
def cmd_help(args):
    #print '''sorry can't help'''
    for k in CMD_MAP:
        print k

def cmd_distance(args):
    if len(args) != 2:
        print 'params should be: id1 id2'
    
    id1, id2 = [int(i) for i in args]
    print '%s <-> %s = %s' % (readable_id(id1), readable_id(id2), readable_id(xor(id1,id2)))

# add node to network
# args: node_id, bootstrap_id
def cmd_new_node(args):
    if len(args) != 2:
        print 'params should be: node_id bootstrap_id'
        return

    nid, bid = [int(i) for i in args]
    if nid in node_id_map:
        print 'node_id %s duplicated' % readable_id(nid)
        return

    node = Node(nid, bid)
    add_by_id(node)
    #add_by_addr(node)

# add node with a random id and random bootstrap
# args: none
def cmd_new_node_random(args):
    # generate random id
    nid = gen_node_id()
    # pick random bootstrap
    bootstrap = ID_NONE if len(node_id_map) == 0 else random.choice(node_id_map.keys())
    print 'new random node %d, bootstrap %d' % (nid, bootstrap)
    cmd_new_node((nid, bootstrap))

# del a node to simulate node failure
# args: node_id
def cmd_del(args):
    if len(args) != 1:
        print 'params should be: node_id'
        return

    node_id = int(args[0])
    del_by_id(node_id)

# list node_id_map
# args: none
def cmd_list_id(args):
    for k in node_id_map:
        print readable_id(k)
    print 'total %d nodes' % len(node_id_map)

# list node_addr_map
# args: none
def cmd_list_addr(args):
    pass
    #for k in node_addr_map:
    #    print k

def cmd_node(args):
    if len(args) != 1:
        print 'params should be: node_id'
        return

    nid = int(args[0])
    if nid not in node_id_map:
        print 'node %s does not exist' % readable_id(nid)
    else:
        print node_id_map[nid].info()
        node_id_map[nid].route.print_table()

# find target node from some node
# args: source_node_id target_id
def cmd_find_node(args):
    if len(args) != 2:
        print 'params should be: source_node_id target_id'
        return
    
    sid, tid = [int(i) for i in args]
    if sid not in node_id_map:
        print 'error: source node %s does not exists' % readable_id(sid)
    else:
        result = node_id_map[sid].find_node_iter(tid, ID_NONE)
        
        # result is local optimal. check if it's global optimal
        go = get_global_nearest(K, tid)
        if cmp(result, go) == 0:
            print 'result', result, 'is global optimal'
        else:
            print 'result', result, 'is NOT global optimal', go

# cmd registry
CMD_MAP = {
    'help': cmd_help,
    'dist': cmd_distance,
    'new_rand': cmd_new_node_random,
    'new': cmd_new_node,
    'del': cmd_del,
    'list': cmd_list_id,
    'list_addr': cmd_list_addr,
    'node': cmd_node,
    'findn': cmd_find_node, 
}

def process_cmd(cmd_str):
    # parse cmd
    tokens = cmd_str.split()
    #print tokens

    if len(tokens) == 0:
        return

    cmd = tokens[0]
    args = tokens[1:] if len(tokens) > 1 else []

    # process each kind of cmd
    if cmd in CMD_MAP:
        CMD_MAP[cmd](args)
    else:
        print 'unknown cmd'

def cmd_loop():
    while(True):
        cmd_str = raw_input('> ')
        process_cmd(cmd_str)

def utest():
    b = KBucket(0)
    print b.info()

    global node_id_map 

    # k-bucket
    node_id_map = [1,2,3,4]
    b.touch(1)
    assert cmp(b.addrs,[1]) == 0 
    b.touch(2)
    assert cmp(b.addrs,[1,2]) == 0 
    b.touch(3)
    assert cmp(b.addrs,[1,2,3]) == 0 
    b.touch(2)
    assert cmp(b.addrs,[1,3,2]) == 0 
    b.touch(1)
    assert cmp(b.addrs,[3,2,1]) == 0 
    b.touch(4)
    assert cmp(b.addrs,[2,1,3]) == 0 
    node_id_map = [2,3,4]
    b.touch(4)
    assert cmp(b.addrs,[1,3,2]) == 0 
    b.touch(4)
    assert cmp(b.addrs,[3,2,4]) == 0 

    # route table
    node_id_map = [1,2,3,4]
    r = RouteTable(0)
    r.touch(1)
    r.touch(2)
    r.touch(3)
    r.touch(2)
    r.touch(1)
    r.touch(4)
    node_id_map = [2,3,4]
    r.touch(4)
    r.touch(4)
    r.touch(5)
    r.touch(6)
    r.touch(7)
    r.touch(8)
    r.touch(2**8 - 1)
    r.print_table()

    print r.find_node(255, ID_NONE)
    print r.find_node(254, ID_NONE)
    print r.find_node(7, ID_NONE)
    print r.find_node(100, ID_NONE)

    node_id_map = {}

# init network by insert some nodes
def init():
    for i in range(10):
        cmd_new_node_random([]), i
    cmd_list_id([])

if __name__ == '__main__':
    print '''########################################
 kadsim - a kademlia simulator by stanz
########################################'''
    print 'Constants'
    print 'M:', M
    print 'K,', K
    print 'ALPHA', ALPHA

    utest()

    init()

    cmd_loop()