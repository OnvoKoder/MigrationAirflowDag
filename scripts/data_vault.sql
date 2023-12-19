DROP DATABASE IF EXISTS Data_Vault; 

CREATE DATABASE Data_Vault 
  WITH 
    owner = hho
    encoding = 'UTF8'
    tablespace = pg_default 
    lc_collate = 'en_AU.UTF-8' 
    lc_ctype = 'en_AU.UTF-8'
    connection LIMIT = -1 
    template template0; 

create table HUB_product(
       keyid bytea primary key,
       load_date timestamp
      );
create table SAT_product(
        product_id bytea references HUB_product(keyid),
        load_date timestamp,
        name_product varchar(40),
        category varchar(15),
        price numeric(10, 2),
        supplier varchar(40)
      );
create table HUB_orders(
       keyid bytea primary key,
       load_date timestamp
        );
create table SAT_orders(
       order_id bytea references HUB_orders(keyid),
       load_date timestamp,
       order_date timestamp,
       freight numeric(10, 2)
        );
create table LINK_order_product(
        order_id bytea references HUB_orders(keyid),
        product_id bytea references HUB_product(keyid),
        load_date timestamp,
        qnty smallint,
        price numeric(10,2)
         );
create table HUB_customer(
       keyid bytea primary key,
       load_date timestamp
       );
create table SAT_customer(
       customer_id bytea references HUB_customer(keyid),
       firstname varchar(40),
       staff varchar(30),
       city varchar(15),
       country varchar(15),
       phone varchar(25),
       load_date timestamp
       );
create table LINK_order_customer(
         order_id bytea references HUB_orders(keyid),
         customer_id bytea references HUB_customer(keyid),
         load_date timestamp
        );