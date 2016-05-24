import os, sys
from peewee import *

path = os.path.dirname(sys.argv[0]) if len(sys.argv) < 2 or not os.path.exists(sys.argv[1]) else sys.argv[1]
database = SqliteDatabase(os.path.join(path, 'changechange.db'))

class BaseModel(Model):
    class Meta:
        database = database

class State(BaseModel):
    changeset = IntegerField()
    replication = IntegerField()

class Changeset(BaseModel):
    changeset = IntegerField(unique=True)
    timestamp = DateTimeField(index=True)
    xml = TextField()

class NodeRef(BaseModel):
    node_id = IntegerField(unique=True)
    refs = CharField(max_length=250)

class WayRelRef(BaseModel):
    wr_id = IntegerField(unique=True)
    refs = CharField(max_length=250)

class Members(BaseModel):
    wr_id = IntegerField(unique=True)
    members = TextField()
