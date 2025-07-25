import databases
import sqlalchemy
from geoalchemy2 import Geometry
from app.core.config import config

metadata = sqlalchemy.MetaData()

# Link table definition
link_table = sqlalchemy.Table(
    "link",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("link_id", sqlalchemy.Integer, unique=True, nullable=False),
    sqlalchemy.Column("road_name", sqlalchemy.String(255)),
    sqlalchemy.Column("geometry", Geometry('GEOMETRY', srid=4326)),
    sqlalchemy.Column("created_at", sqlalchemy.DateTime, server_default=sqlalchemy.func.current_timestamp()),
)

# Speed record table definition
speed_record_table = sqlalchemy.Table(
    "speed_record",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("link_id", sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column("date_time", sqlalchemy.DateTime, nullable=False),
    sqlalchemy.Column("average_speed", sqlalchemy.Numeric(10, 2)),
    sqlalchemy.Column("day_of_week", sqlalchemy.Integer),
    sqlalchemy.Column("period", sqlalchemy.Integer),
    sqlalchemy.Column("created_at", sqlalchemy.DateTime, server_default=sqlalchemy.func.current_timestamp()),
)


engine = sqlalchemy.create_engine(config.DATABASE_URL)
#engine = sqlalchemy.create_async_engine(config.DATABASE_URL)


metadata.create_all(engine)

#database = databases.Database(config.DATABASE_URL, force_rollback=config.DB_FORCE_ROLLBACK)
database = databases.Database(config.DATABASE_URL, force_rollback=False)

