-- Runs once in database "poc" (POSTGRES_DB). Local only: password = user name.
create user order_svc password 'order_svc';
create schema orders authorization order_svc;

create user payment_svc password 'payment_svc';
create schema payments authorization payment_svc;
