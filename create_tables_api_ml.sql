create table if not exists products_filtered(
    id varchar(50) not null,
    title varchar(256) not null,
    price float8 not null,
    seller_id varchar(50) not null,
    search_date date not null,
    load_date date not null,
    search_terms varchar(256) not null,
    product_category varchar(50) not null,
    product_name varchar(100) not null)
    distkey(search_terms)
    sortkey(search_terms, search_date)
;

create table if not exists price_analysis(
    search_date date not null,
    load_date date not null,
    product_category varchar(50) not null,
    product_name varchar(100) not null,
    count integer not null,
    mean float8 not null,
    median float8 not null,
    min float8 not null)
    distkey(product_name)
    sortkey(product_category, product_name)
;