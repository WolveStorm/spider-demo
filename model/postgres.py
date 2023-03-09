from peewee import *
host='192.168.0.204'
# host='192.168.0.104'

# Connect to a Postgres database.
pg_db = PostgresqlDatabase(database='game_info', user='postgres', password='123456', host=host, port=5432)


class BaseModel(Model):
    class Meta:
        database = pg_db


class GameInfo(BaseModel):
    name = CharField(index=True)
    avatar_url = CharField()
    company = CharField()
    score = CharField()
    download_times = CharField()
    apk_url = CharField()
    description = TextField()

if __name__ == '__main__':
    pg_db.connect()
    pg_db.create_tables([GameInfo])