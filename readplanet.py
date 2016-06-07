#!/usr/bin/env python
import changelib, sys, os, subprocess
from db import database, NodeRef, WayRelRef, Members
from imposm.parser import OSMParser

NODE_COUNT = 4400*1024*1024
WAY_COUNT = NODE_COUNT / 10

if len(sys.argv) < 2:
    print 'Imports planet data into binary files for changechange.'
    print 'Usage: {0} <planet_file.osm.pbf> [<path_to_db>]'.format(sys.argv[0])
    sys.exit(1)

path = os.path.dirname(sys.argv[0]) if len(sys.argv) < 3 or not os.path.exists(sys.argv[2]) else sys.argv[2]
database.init(os.path.join(path, 'changechange.db'))

if not os.path.exists(os.path.join(path, 'nodes.bin')):
    print 'Creating files'
    BLOCK_SIZE = 16*1024*1024
    for name in (('nodes.bin', NODE_COUNT*12), ('ways.bin', WAY_COUNT*16)):
      res = subprocess.call(['dd', 'if=/dev/zero', 'of='+os.path.join(path, name[0]), 'bs={0}'.format(BLOCK_SIZE), 'count={0}'.format(name[1]/BLOCK_SIZE)])
      if res != 0:
          print 'DD returned code', res
          sys.exit(1)

class ParserForChange():
    def __init__(self):
        self.cnt = 0

    def flush(self):
        self.cnt += 1
        if self.cnt > 1000000:
            changelib.flush()
            self.cnt = 0

    def print_state(self, typ, ident):
        sys.stdout.write('\rProcessing {0} {1}{2}'.format(typ, ident, ' ' *
                                                          10))
        sys.stdout.flush()
        self.flush()

    def got_coords(self, coords_list):
        for coords in coords_list:
            self.print_state('node', coords[0])
            changelib.store_node_coords(coords[0], coords[2], coords[1])

    def got_way(self, ways):
        for way in ways:
            self.print_state('way', way[0])
            changelib.update_way_nodes(way[0], way[2])

    def got_relation(self, relations):
        for rel in relations:
            self.print_state('relation', rel[0])
            members = [x[1][0] + str(x[0]) for x in rel[2]]
            changelib.update_relation_members(rel[0], members)

database.connect()
database.create_tables([NodeRef, WayRelRef, Members], safe=True)
changelib.CACHE = False
changelib.open(path)
p = ParserForChange()
op = OSMParser(concurrency=1, coords_callback=p.got_coords,
               ways_callback=p.got_way, relations_callback=p.got_relation)
op.parse(sys.argv[1])
print
changelib.close()
