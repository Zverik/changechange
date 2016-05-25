from peewee import *

database = SqliteDatabase(None)

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
