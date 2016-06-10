from bigmmap import BigMMap
from db import NodeRef, WayRelRef, Members
from os.path import join

CACHE = True
COORD_MULTIPLIER = 1e7
node_mmap = None
bbox_mmap = None

NODE_CACHE_MAX_SIZE = 100000
node_cache = {}
last_nodes = []

BBOX_CACHE_MAX_SIZE = 10000
bbox_cache = {}
last_bboxes = []

def open(path):
    global node_mmap, bbox_mmap
    node_mmap = BigMMap(join(path, 'nodes.bin'))
    bbox_mmap = BigMMap(join(path, 'ways.bin'))

def flush():
    node_mmap.flush()
    bbox_mmap.flush()

def close():
    node_mmap.close()
    bbox_mmap.close()

def coord_to_int32(coord):
    return None if coord is None else int(round(coord * COORD_MULTIPLIER))

def int32_to_coord(value):
    return None if value is None else float(value) / COORD_MULTIPLIER

def split_comma(s):
    if len(s) == 0:
        return []
    return s.split(',')

def split_comma_i(s):
    return [int(x) for x in split_comma(s)]

def fetch_node_tuple(node_id):
    if CACHE and node_id in node_cache:
        return node_cache[node_id]
    base = node_id * 3
    lat = int32_to_coord(node_mmap[base])
    lon = int32_to_coord(node_mmap[base + 1])
    ref = node_mmap[base + 2]
    t = (lat, lon, ref)
    if CACHE:
        node_cache[node_id] = t
    last_nodes.append(node_id)
    return t

def store_node_coords(node_id, lat, lon):
    t = fetch_node_tuple(node_id)
    if lat == t[0] and lon == t[1]:
        return
    if CACHE:
        n = (lat, lon, t[2])
        node_cache[node_id] = n
    base = node_id * 3
    node_mmap[base] = coord_to_int32(lat)
    node_mmap[base + 1] = coord_to_int32(lon)
    for ref in fetch_node_refs(node_id):
        if ref > 0:
            update_way_bbox(ref)

def fetch_way_bbox(way_id):
    if CACHE and way_id in bbox_cache:
        return bbox_cache[way_id]
    base = way_id * 4
    bbox = [int32_to_coord(bbox_mmap[base + x]) for x in range(4)]
    if bbox[0] is None or bbox[1] is None or bbox[2] is None:
        return None
    if CACHE:
        bbox_cache[way_id] = bbox
    last_bboxes.append(way_id)
    return bbox

def store_way_bbox(way_id, bbox):
    if bbox is None:
        return
    if CACHE:
        if way_id not in bbox_cache:
            last_bboxes.append(way_id)
        bbox_cache[way_id] = bbox
    base = way_id * 4
    for n in range(4):
        bbox_mmap[base + n] = coord_to_int32(bbox[n])

def add_node_ref(node_id, wr_id):
    t = fetch_node_tuple(node_id)
    if t[2] is None:
        t = (t[0], t[1], wr_id)
        if CACHE:
            node_cache[node_id] = t
        node_mmap[node_id * 3 + 2] = wr_id
    elif t[2] != wr_id:
        try:
            nr = NodeRef.get(NodeRef.node_id == node_id)
            refs = set(split_comma_i(nr.refs))
            if wr_id not in refs:
                refs.add(wr_id)
                nr.refs = ','.join([str(x) for x in refs])
                nr.save()
        except NodeRef.DoesNotExist:
            nr = NodeRef()
            nr.node_id = node_id
            nr.refs = str(wr_id)
            nr.save()

def remove_node_ref(node_id, wr_id):
    t = fetch_node_tuple(node_id)
    refs = set(fetch_node_refs(node_id))
    if wr_id in refs:
        refs.remove(wr_id)
    were_refs = len(refs) > 0
    if wr_id == t[2]:
        newval = None if len(refs) == 0 else refs.pop()
        t = (t[0], t[1], newval)
        if CACHE:
            node_cache[node_id] = t
        node_mmap[node_id * 3 + 2] = newval
        if were_refs:
            nr = NodeRef.get(NodeRef.node_id == node_id)
            if len(refs) > 0:
                nr.refs = ','.join([str(x) for x in refs])
                nr.save()
            else:
                nr.delete_instance()

def fetch_node_refs(node_id):
    refs = []
    t = fetch_node_tuple(node_id)
    if t[2] is not None:
        refs.append(t[2])
        try:
            nr = NodeRef.get(NodeRef.node_id == node_id)
            refs.extend(split_comma_i(nr.refs))
        except NodeRef.DoesNotExist:
            pass
    return refs

def add_wr_ref(wr_id, ref_id):
    try:
        wr = WayRelRef.get(WayRelRef.wr_id == wr_id)
    except WayRelRef.DoesNotExist:
        wr = WayRelRef()
        wr.wr_id = wr_id
        wr.refs = ''
    refs = split_comma(wr.refs)
    try:
        refs.index(str(-ref_id))
    except ValueError:
        wr.refs = ','.join(refs + [str(-ref_id)])
        wr.save()

def remove_wr_ref(wr_id, ref_id):
    try:
        wr = WayRelRef.get(WayRelRef.wr_id == wr_id)
        refs = split_comma(wr.refs)
        try:
            refs.remove(str(-ref_id))
            wr.refs = ','.join(refs)
            wr.save()
        except ValueError:
            pass
    except WayRelRef.DoesNotExist:
        pass

def fetch_wr_refs(wr_id):
    try:
        wr = WayRelRef.get(WayRelRef.wr_id == wr_id)
        return [-int(x) for x in split_comma(wr.refs)]
    except WayRelRef.DoesNotExist:
        return []

def fetch_way_nodes(way_id):
    try:
        member = Members.get(Members.wr_id == way_id)
        return split_comma_i(member.members)
    except Members.DoesNotExist:
        return []

def calc_bbox(nodes):
    bbox = None
    for n in nodes:
        t = fetch_node_tuple(n)
        if t[0] is not None and t[1] is not None:
            if bbox is None:
                bbox = [t[0], t[1], t[0], t[1]]
            else:
                bbox[0] = min(bbox[0], t[0])
                bbox[1] = min(bbox[1], t[1])
                bbox[2] = max(bbox[0], t[0])
                bbox[3] = max(bbox[1], t[1])
    return bbox

def update_way_nodes(way_id, nodes):
    try:
        way = Members.get(Members.wr_id == way_id)
    except Members.DoesNotExist:
        way = Members()
        way.wr_id = way_id
        way.members = ''
    new_members = ','.join([str(x) for x in nodes])
    if new_members == way.members:
        return
    old_nodes = set(split_comma_i(way.members))
    way.members = new_members
    way.save()
    bbox = calc_bbox(nodes)
    store_way_bbox(way_id, bbox)
    # Update references for nodes
    for n in nodes:
        add_node_ref(n, way_id)
        try:
            old_nodes.remove(n)
        except KeyError:
            pass
    for n in old_nodes:
        remove_node_ref(n, way_id)

def update_way_bbox(way_id):
    nodes = fetch_way_nodes(way_id)
    bbox = calc_bbox(nodes)
    store_way_bbox(way_id, bbox)

def update_relation_members(rel_id, members):
    try:
        rel = Members.get(Members.wr_id == rel_id)
    except Members.DoesNotExist:
        rel = Members()
        rel.wr_id = rel_id
        rel.members = ''
    new_members = ','.join(members)
    if new_members == rel.members:
        return
    old_members = set(split_comma(rel.members))
    rel.members = new_members
    rel.save()
    # Update references for individual objects
    for m in members:
        typ = m[0]
        ref = int(m[1:])
        if typ == 'n':
            add_node_ref(ref, rel_id)
        else:
            add_wr_ref(ref, rel_id)
        try:
            old_members.remove(m)
        except KeyError:
            pass
    for m in old_members:
        typ = m[0]
        ref = int(m[1:])
        if typ == 'n':
            remove_node_ref(ref, rel_id)
        else:
            remove_wr_ref(ref, rel_id)

def delete_wr(wr_id):
    if wr_id > 0:
        update_way_nodes(wr_id, [])
    else:
        update_relation_members(wr_id, [])

def purge_node_cache():
    global last_nodes, last_bboxes
    if len(last_nodes) > NODE_CACHE_MAX_SIZE:
        for i in range(0, len(last_nodes) - NODE_CACHE_MAX_SIZE):
            del node_cache[last_nodes[i]]
        last_nodes = last_nodes[len(last_nodes) - NODE_CACHE_MAX_SIZE:]
    if len(last_bboxes) > BBOX_CACHE_MAX_SIZE:
        for i in range(0, len(last_bboxes) - BBOX_CACHE_MAX_SIZE):
            del bbox_cache[last_bboxes[i]]
        last_bboxes = last_bboxes[len(last_bboxes) - BBOX_CACHE_MAX_SIZE:]
