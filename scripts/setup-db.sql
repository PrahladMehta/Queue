-- psql -U postgres -h localhost -f scripts/setup-db.sql
\prompt 'Password for role queuelab: ' app_password

CREATE ROLE queuelab LOGIN PASSWORD :'app_password';
CREATE DATABASE queuelab OWNER queuelab;
CREATE DATABASE queuelab_test OWNER queuelab;
