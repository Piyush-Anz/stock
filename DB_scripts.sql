
##################################################################
#
#
#
# Comment: Below is the scripts to create tables in postgres database.
#
##################################################################

CREATE SEQUENCE seq_t_file_control START 1;


CREATE TABLE t_file_control (
    recordid        integer PRIMARY KEY DEFAULT nextval('seq_t_file_control'),
    filename       varchar(40) NOT NULL,
    date_loaded   date,
    download_status        varchar(10),
    preperation_status        varchar(10),
    db_table varchar(40)
);



CREATE TABLE t_mf_sch_type (
    FundHouse    varchar(100) NOT NULL,
    SchemeType   varchar(500) NOT NULL,
    SchemeCode   varchar(100) NOT NULL
);


CREATE SEQUENCE seq_t_stock_alphavantage START 1;

CREATE TABLE stg_stock_alphavantage (
    recordid integer PRIMARY KEY DEFAULT nextval('seq_t_stock_alphavantage'),
	timestamp DATE,
	open numeric,
	high numeric,
	low numeric,
	close numeric,
	volume numeric,
	Stock  varchar(10),
	Exchange varchar(10)
);


DROP table t_top10_200ASX_IdxWt;
CREATE TABLE t_top10_200ASX_IdxWt (
	Exchange varchar(10),
	Stock  varchar(10),
	Stock_Name varchar(100),
	Sector varchar(100),
	Market_Cap integer,
	Weight_per numeric,
	created_dt DATE default CURRENT_DATE
);


Drop TABLE t_stock_alphavantage;
CREATE TABLE t_stock_alphavantage (
	Exchange varchar(10)
	Stock  varchar(10),
	timestamp DATE,
	high numeric,
	low numeric,
    daily_range numeric,
	open numeric,
	close numeric,
	prev_close numeric,
	per_growth numeric,
	log_return numeric,
	volume numeric,
	avg_volume numeric
);



drop table t_all_ASX_stock;
CREATE TABLE t_all_ASX_stock (
	Exchange varchar(10),
	Stock  varchar(10),
	Stock_Name varchar(100),
	Sector varchar(100),
	Market_Cap numeric,
	Weight_per numeric,
	created_dt DATE default CURRENT_DATE
);




COPY t_all_ASX_stock(Exchange,Stock,Stock_Name,Sector,Market_Cap,Weight_per) FROM '/Users/piyushbijwal/Documents/Project/stock_analysis/MyScripts/stock/all_stock.csv' DELIMITER ',' CSV HEADER;



