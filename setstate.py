#!/usr/bin/env python
import sys, re
from db import database, State

def parse_state(s):
    if s.isdigit():
        return int(s)
    m = re.search(r'\.org/replication.*/(\d{3})/(\d{3})/(\d{3})\.', s)
    if m:
        return int(m.group(1) + m.group(2) + m.group(3))
    return 0

if len(sys.argv) < 3:
    print 'Set state values for changechange.'
    print 'Usage: {0} [path_to_db] <changeset_state> <replication_state>'.format(sys.argv[0])
    sys.exit(1)

changeset = parse_state(sys.argv[1])
if changeset < 1800000 and len(sys.argv) > 3:
    changeset = parse_state(sys.argv[2])
    i = 3
else:
    i = 2
replication = parse_state(sys.argv[i])
if changeset < 1800000 or replication < 1800000:
    print 'Too old replication values:', changeset, replication
    sys.exit(1)

database.connect()
database.create_tables([State], safe=True)
try:
    st = State.get(State.id == 1)
except State.DoesNotExist:
    st = State()
st.changeset = changeset
st.replication = replication
st.save()
