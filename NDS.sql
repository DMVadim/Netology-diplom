CREATE TABLE "branches" (
  "branch_id" varchar PRIMARY KEY,
  "branch_name" varchar,
  "city_id" varchar
);

CREATE TABLE "cities" (
  "city_id" varchar PRIMARY KEY,
  "city" varchar
);

CREATE TABLE "customer_types" (
  "customer_type_id" varchar PRIMARY KEY,
  "type_name" varchar
);

CREATE TABLE "genders" (
  "gender_id" varchar PRIMARY KEY,
  "gender_name" varchar
);

CREATE TABLE "product_lines" (
  "product_line_id" varchar PRIMARY KEY,
  "line_name" varchar
);

CREATE TABLE "payment_methods" (
  "payment_method_id" varchar PRIMARY KEY,
  "method_name" varchar
);

CREATE TABLE "customers" (
  "customer_id" serial PRIMARY KEY,
  "customer_type_id" varchar,
  "gender_id" varchar
);

CREATE TABLE "products" (
  "product_id" serial PRIMARY KEY,
  "product_line_id" varchar,
  "unit_price" decimal
);

CREATE TABLE "invoices" (
  "invoice_id" varchar PRIMARY KEY,
  "branch_id" varchar,
  "customer_id" integer,
  "date" date,
  "time" time,
  "payment_method_id" varchar,
  "rating" decimal
);

CREATE TABLE "invoice_items" (
  "item_id" serial PRIMARY KEY,
  "invoice_id" varchar,
  "product_id" integer,
  "quantity" integer,
  "tax_amount" decimal,
  "total" decimal,
  "cogs" decimal,
  "gross_margin_percentage" decimal,
  "gross_income" decimal
);

ALTER TABLE "branches" ADD FOREIGN KEY ("city_id") REFERENCES "cities" ("city_id");

ALTER TABLE "customers" ADD FOREIGN KEY ("customer_type_id") REFERENCES "customer_types" ("customer_type_id");

ALTER TABLE "customers" ADD FOREIGN KEY ("gender_id") REFERENCES "genders" ("gender_id");

ALTER TABLE "products" ADD FOREIGN KEY ("product_line_id") REFERENCES "product_lines" ("product_line_id");

ALTER TABLE "invoices" ADD FOREIGN KEY ("branch_id") REFERENCES "branches" ("branch_id");

ALTER TABLE "invoices" ADD FOREIGN KEY ("customer_id") REFERENCES "customers" ("customer_id");

ALTER TABLE "invoices" ADD FOREIGN KEY ("payment_method_id") REFERENCES "payment_methods" ("payment_method_id");

ALTER TABLE "invoice_items" ADD FOREIGN KEY ("invoice_id") REFERENCES "invoices" ("invoice_id");

ALTER TABLE "invoice_items" ADD FOREIGN KEY ("product_id") REFERENCES "products" ("product_id");
