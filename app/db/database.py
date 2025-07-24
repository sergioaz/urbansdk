import databases
import sqlalchemy
from app.core.config import config
#from routers.post import comment_table

metadata = sqlalchemy.MetaData()

# Link info table definition
link_info_table = sqlalchemy.Table(
    "link_info",
    metadata,
    sqlalchemy.Column("link_id", sqlalchemy.BigInteger, primary_key=True),
    sqlalchemy.Column("length", sqlalchemy.Numeric),
    sqlalchemy.Column("road_name", sqlalchemy.Text),
    sqlalchemy.Column("usdk_speed_category", sqlalchemy.Integer),
    sqlalchemy.Column("funclass_id", sqlalchemy.Integer),
    sqlalchemy.Column("speedcat", sqlalchemy.Integer),
    sqlalchemy.Column("volume_value", sqlalchemy.Integer),
    sqlalchemy.Column("volume_bin_id", sqlalchemy.Integer),
    sqlalchemy.Column("volume_year", sqlalchemy.Integer),
    sqlalchemy.Column("volumes_bin_description", sqlalchemy.Text),
    sqlalchemy.Column("geo_json", sqlalchemy.Text),
)

# Duval table definition
duval_table = sqlalchemy.Table(
    "duval",
    metadata,
    sqlalchemy.Column("link_id", sqlalchemy.BigInteger, sqlalchemy.ForeignKey("link_info.link_id")),
    sqlalchemy.Column("date_time", sqlalchemy.DateTime),
    sqlalchemy.Column("freeflow", sqlalchemy.Numeric),
    sqlalchemy.Column("count", sqlalchemy.Integer),
    sqlalchemy.Column("std_dev", sqlalchemy.Numeric),
    sqlalchemy.Column("min", sqlalchemy.Numeric),
    sqlalchemy.Column("max", sqlalchemy.Numeric),
    sqlalchemy.Column("confidence", sqlalchemy.Integer),
    sqlalchemy.Column("average_speed", sqlalchemy.Numeric),
    sqlalchemy.Column("average_pct_85", sqlalchemy.Numeric),
    sqlalchemy.Column("average_pct_95", sqlalchemy.Numeric),
    sqlalchemy.Column("day_of_week", sqlalchemy.Integer),
    sqlalchemy.Column("period", sqlalchemy.Integer),
)


engine = sqlalchemy.create_engine(config.DATABASE_URL)
#engine = sqlalchemy.create_async_engine(config.DATABASE_URL)


metadata.create_all(engine)

#database = databases.Database(config.DATABASE_URL, force_rollback=config.DB_FORCE_ROLLBACK)
database = databases.Database(config.DATABASE_URL, force_rollback=False)

