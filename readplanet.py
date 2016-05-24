#!/usr/bin/env python
import changelib, sys, os, subprocess, re
import db

NODE_COUNT = 4400*1024*1024
WAY_COUNT = NODE_COUNT / 10
REPLICATION_BASE_URL = 'http://planet.openstreetmap.org/replication'

def extract_attr(line, attr):
    s = ' {0}='.format(attr)
    l = len(s)
    try:
        p = line.index(s)
        return line[p+l+1:line.index(line[p+l], p+l+1)]
    except ValueError:
        return None

if not os.path.exists(os.path.join(db.path, 'nodes.bin')):
    print 'Creating files'
    BLOCK_SIZE = 16*1024*1024
    for name in (('nodes.bin', NODE_COUNT*12), ('ways.bin', WAY_COUNT*16)):
      res = subprocess.call(['dd', 'if=/dev/zero', 'of='+os.path.join(db.path, name[0]), 'bs={0}'.format(BLOCK_SIZE), 'count={0}'.format(name[1]/BLOCK_SIZE)])
      if res != 0:
          print 'DD returned code', res
          sys.exit(1)

print 'Parsing the planet'
db.database.connect()
db.database.create_tables([db.NodeRef, db.WayRelRef, db.Members], safe=True)
changelib.CACHE = False
changelib.open(db.path)
cnt = 0
members = []
last_date = ''
wr_id = None
for line in sys.stdin:
    cnt += 1
    if cnt >= 1000000:
        changelib.flush()
        cnt = 0
    timestamp = extract_attr(line, 'timestamp')
    if timestamp is not None and timestamp > last_date:
        last_date = timestamp
    if '<node' in line:
        node_id = int(extract_attr(line, 'id'))
        lat = float(extract_attr(line, 'lat'))
        lon = float(extract_attr(line, 'lon'))
        changelib.store_node_coords(node_id, lat, lon)
    elif '<way' in line:
        wr_id = int(extract_attr(line, 'id'))
    elif '<relation' in line:
        wr_id = -int(extract_attr(line, 'id'))
    elif wr_id and '<nd ' in line:
        members.append(int(extract_attr(line, 'ref')))
    elif wr_id and '<member ' in line:
        typ = extract_attr(line, 'type')
        ref = extract_attr(line, 'ref')
        members.append(typ[0] + ref)
    elif '</way>' in line:
        changelib.update_way_nodes(wr_id, members)
        members = []
        wr_id = None
    elif '</relation>' in line:
        changelib.update_relation_members(wr_id, members)
        members = []
        wr_id = None
changelib.close()

print 'Last date:', last_date
