-- Run once as "postgres" in Cloud SQL Studio (database poc) after `terraform apply`.
-- Replace <project> with the GCP project id. On Cloud SQL "postgres" is not a superuser,
-- so it joins each IAM role before creating a schema owned by it.
grant "order-svc@<project>.iam" to postgres;
create schema orders authorization "order-svc@<project>.iam";

grant "payment-svc@<project>.iam" to postgres;
create schema payments authorization "payment-svc@<project>.iam";
