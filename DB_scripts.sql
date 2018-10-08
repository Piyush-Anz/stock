
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

