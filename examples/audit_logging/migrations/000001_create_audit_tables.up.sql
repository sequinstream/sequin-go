create table if not exists user_permissions_log(
  id bigserial primary key,
  event_id text not null,
  user_id text not null,
  permission text not null,
  action text not null,
  old_values jsonb,
  new_values jsonb,
  created_at timestamp with time zone not null default now()
);

create unique index if not exists user_permissions_log_event_id_idx on user_permissions_log(event_id);

create index if not exists user_permissions_log_user_id_idx on user_permissions_log(user_id);

create table if not exists subscriptions_log(
  id bigserial primary key,
  event_id text not null,
  subscription_id text not null,
  customer_id text not null,
  status text not null,
  action text not null,
  old_values jsonb,
  new_values jsonb,
  created_at timestamp with time zone not null default now()
);

create unique index if not exists subscriptions_log_event_id_idx on subscriptions_log(event_id);

create index if not exists subscriptions_log_subscription_id_idx on subscriptions_log(subscription_id);

create index if not exists subscriptions_log_customer_id_idx on subscriptions_log(customer_id);

