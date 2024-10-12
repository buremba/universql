from mangum import Mangum

from universql.protocol.snowflake import app as snowflake_app

snowflake = Mangum(snowflake_app, lifespan="off")
