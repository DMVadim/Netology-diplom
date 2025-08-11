CREATE TABLE "dim_branches" (
  "branch_id" varchar PRIMARY KEY,
  "branch_name" varchar NOT NULL,
  "city_id" varchar,
  "city_name" varchar NOT NULL
);

CREATE TABLE "dim_cities" (
  "city_id" varchar PRIMARY KEY,
  "city_name" varchar NOT NULL,
  "timezone" varchar
);

CREATE TABLE "dim_customers" (
  "customer_id" varchar PRIMARY KEY,
  "customer_type" varchar NOT NULL,
  "gender" varchar NOT NULL
);

CREATE TABLE "dim_products" (
  "product_id" varchar PRIMARY KEY,
  "product_line" varchar NOT NULL
);

CREATE TABLE "dim_payment_methods" (
  "payment_id" varchar PRIMARY KEY,
  "payment_type" varchar NOT NULL
);

CREATE TABLE "dim_time" (
  "date_id" date PRIMARY KEY,
  "day_of_week" smallint NOT NULL,
  "is_weekend" boolean NOT NULL,
  "month" smallint NOT NULL,
  "quarter" smallint NOT NULL,
  "year" smallint NOT NULL,
  "is_holiday" boolean NOT NULL,
  "holiday_name" varchar
);

CREATE TABLE "fact_sales" (
  "sale_id" bigserial PRIMARY KEY,
  "branch_id" varchar,
  "customer_id" varchar,
  "product_id" varchar,
  "payment_id" varchar,
  "date_id" date,
  "invoice_id" varchar NOT NULL,
  "quantity" integer NOT NULL,
  "unit_price" decimal(10,2) NOT NULL,
  "tax_amount" decimal(10,2) NOT NULL,
  "total_amount" decimal(10,2) NOT NULL,
  "cogs" decimal(10,2) NOT NULL,
  "gross_margin_percent" decimal(5,2),
  "gross_income" decimal(10,2),
  "rating" decimal(3,1),
  "loaded_at" timestamp DEFAULT (now())
);

ALTER TABLE "dim_branches" ADD FOREIGN KEY ("city_id") REFERENCES "dim_cities" ("city_id");

ALTER TABLE "fact_sales" ADD FOREIGN KEY ("branch_id") REFERENCES "dim_branches" ("branch_id");

ALTER TABLE "fact_sales" ADD FOREIGN KEY ("customer_id") REFERENCES "dim_customers" ("customer_id");

ALTER TABLE "fact_sales" ADD FOREIGN KEY ("product_id") REFERENCES "dim_products" ("product_id");

ALTER TABLE "fact_sales" ADD FOREIGN KEY ("payment_id") REFERENCES "dim_payment_methods" ("payment_id");

ALTER TABLE "fact_sales" ADD FOREIGN KEY ("date_id") REFERENCES "dim_time" ("date_id");
