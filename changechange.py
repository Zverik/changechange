#!/usr/bin/env python
import os, sys, urllib2, gzip, re, datetime
from StringIO import StringIO
from lxml import etree
import db, changelib

REPLICATION_BASE_URL = 'http://planet.openstreetmap.org/replication'
API_BASE_URL = 'http://api.openstreetmap.org/api/0.6'
TARGET_OSC_PATH = os.path.dirname(sys.argv[0])

def download_last_state():
    """Downloads last data and changeset replication seq number."""
    state = urllib2.urlopen(REPLICATION_BASE_URL + '/minute/state.txt').read()
    m = re.search(r'sequenceNumber=(\d+)', state)
    seq1 = int(m.group(1))

    state = urllib2.urlopen(REPLICATION_BASE_URL + '/changesets/state.yaml').read()
    m = re.search(r'sequence:\s+(\d+)', state)
    seq2 = int(m.group(1))
    # Not checking to throw exception in case of an error
    return [seq1, seq2]

def read_last_state():
    try:
        st = db.State.get(db.State.id == 1)
        return [st.replication, st.changeset]
    except:
        return None

def write_last_state(state):
    try:
        st = db.State.get(db.State.id == 1)
    except:
        st = db.State()
    st.changeset = state[1]
    st.replication = state[0]
    st.save()

def get_replication_url(state, subdir):
    return '{0}/{1}/{2:03}/{3:03}/{4:03}.{5}.gz'.format(
            REPLICATION_BASE_URL,
            subdir,
            int(state / 1000000),
            int(state / 1000) % 1000,
            state % 1000,
            'osm' if subdir == 'changesets' else 'osc')

def get_replication_target_path(state):
    return os.path.join(TARGET_OSC_PATH, '{0:03}'.format(int(state / 1000000)), '{0:03}'.format(int(state / 1000) % 1000), '{0:03}.osc.gz'.format(state % 1000))

def process_replication_changesets(state):
    """Downloads replication archive for a given state, and returns a dict of changeset xml strings."""
    response = urllib2.urlopen(get_replication_url(state, 'changesets'))
    data = response.read()
    gz = gzip.GzipFile(fileobj=StringIO(data))
    for event, element in etree.iterparse(gz):
        if element.tag == 'changeset':
            try:
                ch = db.Changeset.get(db.Changeset.changeset == int(element.get('id')))
            except db.Changeset.DoesNotExist:
                ch = db.Changeset()
                ch.changeset = int(element.get('id'))
            ch.timestamp = datetime.datetime.now()
            ch.xml = etree.tostring(element)
            ch.save()
            element.clear()

def fetch_changeset_from_api(changeset):
    response = urllib2.urlopen('{0}/changeset/{1}'.format(API_BASE_URL, changeset))
    return response.read()

def enrich_replication(state):
    """Downloads replication archive for a given state, and creates an enriched osc.gz."""
    response = urllib2.urlopen(get_replication_url(state, 'minute'))
    data = response.read()
    gz = gzip.GzipFile(fileobj=StringIO(data))
    filename = get_replication_target_path(state)
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
    gzout = gzip.GzipFile(filename, 'wb')
    gzout.write("""<?xml version="1.0" encoding="utf-8"?>\n<osmChange version="0.6" generator="Changechange">\n""")
    action = None
    printed_changesets = set()
    for event, element in etree.iterparse(gz, events=('start', 'end')):
        if element.tag in ('create', 'modify', 'delete') and event == 'start':
            action = element.tag
        elif element.tag in ('node', 'way', 'relation') and event == 'end':
            el_id = int(element.get('id'))
            if element.tag == 'relation':
                el_id = -el_id
            # Print changeset if needed
            changeset = int(element.get('changeset'))
            if changeset not in printed_changesets:
                try:
                    chdata = db.Changeset.get(db.Changeset.changeset == changeset).xml
                except db.Changeset.DoesNotExist:
                    chdata = fetch_changeset_from_api(changeset)
                gzout.write(chdata)
                printed_changesets.add(changeset)

            # Add and/or record geometry
            if element.tag == 'node':
                if element.get('lat'):
                    changelib.store_node_coords(el_id, float(element.get('lat')), float(element.get('lon')))
            elif element.tag == 'way':
                if action == 'delete' and not element.find('nd'):
                    # Add nodes to deleted ways, so their geometry is not empty
                    for n in changelib.fetch_way_nodes(el_id):
                        ndel = etree.Element('nd')
                        ndel.set('ref', str(n))
                        element.append(ndel)
                nodes = []
                for nd in element.findall('nd'):
                    ref = int(nd.get('ref'))
                    nodes.append(ref)
                    node_data = changelib.fetch_node_tuple(ref)
                    if node_data is not None:
                        # We leave the possibility of an absent node
                        nd.set('lat', str(node_data[0]))
                        nd.set('lon', str(node_data[1]))
                if action != 'delete':
                    changelib.update_way_nodes(el_id, nodes)
                else:
                    changelib.delete_wr(el_id)
            elif element.tag == 'relation':
                # We are not adding members to deleted relations, since we don't know their roles
                members = []
                for member in element.findall('member'):
                    members.append(member.get('type')[0] + member.get('ref'))
                    ref = int(member.get('ref'))
                    if member.get('type') == 'node':
                        node_data = changelib.fetch_node_tuple(ref)
                        if node_data is not None:
                            member.set('lat', str(node_data[0]))
                            member.set('lon', str(node_data[1]))
                    elif member.get('type') == 'way':
                        bbox = changelib.fetch_way_bbox(ref)
                        if bbox is not None:
                            member.set('minlat', str(bbox[0]))
                            member.set('minlon', str(bbox[1]))
                            member.set('maxlat', str(bbox[2]))
                            member.set('maxlon', str(bbox[3]))
                if action != 'delete':
                    changelib.update_relation_members(el_id, members)
                else:
                    changelib.delete_wr(el_id)

            # Add referencing objects
            if element.tag == 'node':
                refs = changelib.fetch_node_refs(el_id)
            else:
                refs = changelib.fetch_wr_refs(el_id)
            for ref in refs:
                refel = etree.Element('ref')
                refel.set('type', 'way' if ref > 0 else 'relation')
                refel.set('ref', str(ref))
                element.append(refel)
            # Print and forget
            p = etree.Element(action)
            p.append(element)
            gzout.write(etree.tostring(p))
            element.clear()
            p.clear()
    changelib.purge_node_cache()
    changelib.flush()

if __name__ == '__main__':
    try:
        cur_state = download_last_state()
    except Exception as e:
        print 'Failed to download last state:', e
        sys.exit(1)

    db.database.connect()
    db.database.create_tables([db.Changeset, db.NodeRef, db.WayRelRef, db.Members, db.State], safe=True)

    state = read_last_state()
    if state is None:
        state = [x-1 for x in cur_state]

    # Delete old changesets
    tooold = datetime.datetime.now() - datetime.timedelta(days=2)
    query = db.Changeset.delete().where(db.Changeset.timestamp < tooold)
    query.execute()

    # Process changeset replication
    sys.stdout.write('Downloading changesets')
    with db.database.atomic():
        while state[1] < cur_state[1]:
            sys.stdout.write('.')
            sys.stdout.flush()
            state[1] += 1
            process_replication_changesets(state[1])
            write_last_state(state)
    print

    # Process data replication
    changelib.open(db.path)
    while state[0] < cur_state[0] - 1:
        with db.database.atomic():
            state[0] += 1
            print state[0]
            enrich_replication(state[0])
            write_last_state(state)
    changelib.close()
