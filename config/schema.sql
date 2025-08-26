-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE wh.bridge_customer_identity (
  master_customer_id bigint NOT NULL DEFAULT nextval('wh.bridge_customer_identity_master_customer_id_seq'::regclass),
  customer_sk bigint,
  confidence numeric,
  CONSTRAINT bridge_customer_identity_pkey PRIMARY KEY (master_customer_id),
  CONSTRAINT bridge_customer_identity_customer_sk_fkey FOREIGN KEY (customer_sk) REFERENCES wh.dim_customer(customer_sk)
);
CREATE TABLE wh.bridge_product_source (
  bridge_id bigint NOT NULL DEFAULT nextval('wh.bridge_product_source_bridge_id_seq'::regclass),
  product_sk bigint NOT NULL,
  source_channel text NOT NULL,
  source_product_id text NOT NULL,
  source_sku text,
  source_name text,
  cost_native numeric,
  currency_native text,
  updated_at timestamp with time zone,
  price_native numeric,
  CONSTRAINT bridge_product_source_pkey PRIMARY KEY (bridge_id),
  CONSTRAINT bridge_product_source_product_sk_fkey FOREIGN KEY (product_sk) REFERENCES wh.dim_product(product_sk)
);
CREATE TABLE wh.dim_campaign (
  campaign_sk bigint NOT NULL DEFAULT nextval('wh.dim_campaign_campaign_sk_seq'::regclass),
  source_campaign_id text UNIQUE,
  name text,
  channel_id smallint,
  start_at timestamp with time zone,
  end_at timestamp with time zone,
  budget_native numeric,
  currency_native text,
  CONSTRAINT dim_campaign_pkey PRIMARY KEY (campaign_sk),
  CONSTRAINT dim_campaign_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES wh.dim_channel(channel_id)
);
CREATE TABLE wh.dim_channel (
  channel_id smallint NOT NULL DEFAULT nextval('wh.dim_channel_channel_id_seq'::regclass),
  name text NOT NULL UNIQUE,
  CONSTRAINT dim_channel_pkey PRIMARY KEY (channel_id)
);
CREATE TABLE wh.dim_customer (
  customer_sk bigint NOT NULL DEFAULT nextval('wh.dim_customer_customer_sk_seq'::regclass),
  source_customer_id text NOT NULL,
  region text,
  first_seen_at timestamp with time zone,
  source_channel text NOT NULL,
  CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_sk)
);
CREATE TABLE wh.dim_date (
  date_key date NOT NULL,
  year integer,
  quarter integer,
  month integer,
  day integer,
  week_of_year integer,
  is_weekend boolean,
  CONSTRAINT dim_date_pkey PRIMARY KEY (date_key)
);
CREATE TABLE wh.dim_product (
  product_sk bigint NOT NULL DEFAULT nextval('wh.dim_product_product_sk_seq'::regclass),
  master_product_code text UNIQUE,
  name text,
  category text,
  brand text,
  is_active boolean DEFAULT true,
  created_at timestamp with time zone,
  starting_inventory numeric,
  CONSTRAINT dim_product_pkey PRIMARY KEY (product_sk)
);
CREATE TABLE wh.dim_store (
  store_sk bigint NOT NULL DEFAULT nextval('wh.dim_store_store_sk_seq'::regclass),
  store_id text UNIQUE,
  name text,
  region text,
  timezone text,
  CONSTRAINT dim_store_pkey PRIMARY KEY (store_sk)
);
CREATE TABLE wh.fact_ads_spend (
  campaign_sk bigint NOT NULL,
  date_key date NOT NULL,
  spend_native numeric,
  clicks bigint,
  impressions bigint,
  orders_attributed integer,
  revenue_attr_native numeric,
  CONSTRAINT fact_ads_spend_pkey PRIMARY KEY (campaign_sk, date_key),
  CONSTRAINT fact_ads_spend_campaign_sk_fkey FOREIGN KEY (campaign_sk) REFERENCES wh.dim_campaign(campaign_sk),
  CONSTRAINT fact_ads_spend_date_key_fkey FOREIGN KEY (date_key) REFERENCES wh.dim_date(date_key)
);
CREATE TABLE wh.fact_inventory (
  snapshot_date date NOT NULL,
  product_sk bigint NOT NULL,
  stock_qty numeric,
  CONSTRAINT fact_inventory_pkey PRIMARY KEY (snapshot_date, product_sk),
  CONSTRAINT fact_inventory_product_sk_fkey FOREIGN KEY (product_sk) REFERENCES wh.dim_product(product_sk)
);
CREATE TABLE wh.fact_order_items (
  order_sk bigint NOT NULL,
  product_sk bigint NOT NULL,
  qty numeric,
  price numeric,
  discount numeric,
  revenue_net numeric,
  cost numeric,
  margin numeric,
  CONSTRAINT fact_order_items_pkey PRIMARY KEY (order_sk, product_sk),
  CONSTRAINT fact_order_items_order_sk_fkey FOREIGN KEY (order_sk) REFERENCES wh.fact_orders(order_sk),
  CONSTRAINT fact_order_items_product_sk_fkey FOREIGN KEY (product_sk) REFERENCES wh.dim_product(product_sk)
);
CREATE TABLE wh.fact_orders (
  order_sk bigint NOT NULL DEFAULT nextval('wh.fact_orders_order_sk_seq'::regclass),
  order_id text NOT NULL UNIQUE,
  channel_id smallint,
  customer_sk bigint,
  store_sk bigint,
  order_ts timestamp with time zone,
  status text,
  currency_native text,
  order_total_gross numeric,
  order_total_net numeric,
  shipping_fee numeric,
  tax_total numeric,
  voucher_amount numeric,
  CONSTRAINT fact_orders_pkey PRIMARY KEY (order_sk),
  CONSTRAINT fact_orders_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES wh.dim_channel(channel_id),
  CONSTRAINT fact_orders_customer_sk_fkey FOREIGN KEY (customer_sk) REFERENCES wh.dim_customer(customer_sk),
  CONSTRAINT fact_orders_store_sk_fkey FOREIGN KEY (store_sk) REFERENCES wh.dim_store(store_sk)
);
CREATE TABLE wh.fact_refunds (
  refund_id text NOT NULL,
  order_sk bigint,
  product_sk bigint,
  amount_native numeric,
  amount_myr numeric,
  reason text,
  processed_ts timestamp with time zone,
  CONSTRAINT fact_refunds_pkey PRIMARY KEY (refund_id),
  CONSTRAINT fact_refunds_order_sk_fkey FOREIGN KEY (order_sk) REFERENCES wh.fact_orders(order_sk),
  CONSTRAINT fact_refunds_product_sk_fkey FOREIGN KEY (product_sk) REFERENCES wh.dim_product(product_sk)
);
CREATE TABLE wh.fx_rates (
  date_key date NOT NULL,
  currency text NOT NULL,
  to_myr numeric,
  CONSTRAINT fx_rates_pkey PRIMARY KEY (date_key, currency)
);