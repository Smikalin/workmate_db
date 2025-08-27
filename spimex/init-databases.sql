CREATE DATABASE spimex_sync OWNER spimex_user;
 
CREATE DATABASE spimex_async OWNER spimex_user;

GRANT ALL PRIVILEGES ON DATABASE spimex_sync TO spimex_user;
GRANT ALL PRIVILEGES ON DATABASE spimex_async TO spimex_user;
