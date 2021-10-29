BEGIN
  DBMS_CLOUD.DROP_CREDENTIAL(
    credential_name => 'OBJ_STORE_CRED'
  );
END;
/

begin
    DBMS_CLOUD.create_credential(
    credential_name => 'OBJ_STORE_CRED',
    username => 'oracleidentitycloudservice/skrzyp1984@o2.pl',
    password => 'TgM4);Tk+n[1RX7)KmHg'--ocid1.credential.oc1..aaaaaaaam7renppgdkcxvz43dprkwqgb4ov2imr7txegsz6j3vw6q6fqfeia
);
end;


select * from all_credentials;

--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

define uri_root = 'https://objectstorage.eu-frankfurt-1.oraclecloud.com/n/oractdemeaoci/b/TOMEK_DWH1/o'
define obj_credential = 'OBJ_STORE_CRED'

select * from dbms_cloud.list_objects('&obj_credential', '&uri_root/');

exec dbms_cloud_admin.enable_resource_principal();

define dcat_credential ='OCI$RESOURCE_PRINCIPAL'

select * from dbms_cloud.list_objects('&dcat_credential', '&uri_root/');


--------------------------------------------------------------------------------------------------------------------------------------
--manual create table external 
--------------------------------------------------------------------------------------------------------------------------------------

drop table ORDERS_PRODUCTS2;

BEGIN
    DBMS_CLOUD.CREATE_EXTERNAL_TABLE (
    table_name =>'ORDERS_PRODUCTS2',
    credential_name =>'OBJ_STORE_CRED',
    file_uri_list =>'https://objectstorage.eu-frankfurt-1.oraclecloud.com/n/oractdemeaoci/b/Tomek_DWH1/o/SALES%2F2017_Data.parquet',
    format => json_object('type' value 'parquet', 'schema' value 'first'));
END;

--------------------------------------------------------------------------------------------------------------------------------------
-- create external table from storage for files csv, parquet
--------------------------------------------------------------------------------------------------------------------------------------

define dcat_region='eu-frankfurt-1'
define dcat_ocid = 'ocid1.datacatalog.oc1.eu-frankfurt-1.amaaaaaa7ratcziavxsfqazaelwx5kqp7yrewqmn6tchgkzwgaz35efe6iya'
define dcat_credential = 'OCI$RESOURCE_PRINCIPAL'
define obj_credential = 'OBJ_STORE_CRED'
define uri_root = 'https://objectstorage.eu-frankfurt-1.oraclecloud.com/n/oractdemeaoci/b/Tomek_DWH1/o'

select * from dbms_cloud.list_objects('&obj_credential', '&uri_root/');

exec dbms_dcat.set_data_catalog_credential(credential_name =>'&dcat_credential');
exec dbms_dcat.set_object_store_credential(credential_name =>'&obj_credential');

begin dbms_dcat.set_data_catalog_conn (region => '&dcat_region',catalog_id => '&dcat_ocid');end;

select * from all_dcat_connections;

select * from all_dcat_assets;

select * from all_dcat_folders;

select * from all_dcat_entities;


begin
    dbms_dcat.run_sync(synced_objects =>'{"asset_list": [{"asset_id":"87e09807-e991-4198-b4f1-b0363ac7dd90","folder_list":["9e53aedd-61b8-40a8-b923-3c0ada6a92ca"]}]}');
end;

select type, start_time, status, logfile_table from user_load_operations;

--oractdemeaoci

select * from DBMS_DCAT$7_LOG order by log_timestamp desc;

select oracle_schema_name, oracle_table_name from dcat_entities;

select sum (FINAL_PRICE) from DCAT$ANALYTICS_WORKSHOP_ASSET_TOMEK_DWH1.TOMEK_SALES;


-- create external table from storage for files csv, parquet

drop table EXT_SALES_PART;
BEGIN
    DBMS_CLOUD.CREATE_EXTERNAL_PART_TABLE(
    table_name =>'EXT_SALES_PART',
    credential_name =>'OCI$RESOURCE_PRINCIPAL',
    format => json_object('type' value 'parquet', 'schema' value 'first') ,
    column_list => 'ORDERS_PRODUCTS_ID NUMBER, 
    ORDERS_ID NUMBER,
    PRODUCTS_ID NUMBER, 
    PRODUCTS_MODEL VARCHAR2(50), 
    PRODUCTS_NAME
    VARCHAR2(100), 
    PRODUCTS_PRICE NUMBER,
    FINAL_PRICE NUMBER, 
    PRODUCTS_TAX NUMBER,
    PRODUCTS_QUANTITY NUMBER, 
    LAST_MODIFIED timestamp, 
    DATE_PURCHASED timestamp',
    partitioning_clause => 'partition by range (DATE_PURCHASED)
    (
        partition p2017 values less than (to_timestamp(''2018-01-01 00:00:00'',''YYYY-MM-DD HH24:MI:SS'')) location(''https://objectstorage.eu-frankfurt-1.oraclecloud.com/n/oractdemeaoci/b/Tomek_DWH1/o/SALES%2F2017_Data.parquet'') ,
        partition p2018 values less than (to_timestamp(''2019-01-01 00:00:00'',''YYYY-MM-DD HH24:MI:SS'')) location(''https://objectstorage.eu-frankfurt-1.oraclecloud.com/n/oractdemeaoci/b/Tomek_DWH1/o/SALES%2F2018_Data.parquet''))
    '
    );
END;
/

select * from EXT_SALES_PART partition(p2018);

--------------------------------------------------------------------------------------------------------------------------------------
-- create view for data from storage for files json
--------------------------------------------------------------------------------------------------------------------------------------

select * from DCAT$ANALYTICS_WORKSHOP_ASSET_TOMEK_DWH1.TOMEK_categories;

CREATE or replace VIEW EX_CATEGORYVIEW AS
    SELECT
    JSON_VALUE(json_document, '$.categories_name') AS CATEGORIES_NAME,
    JSON_VALUE(json_document, '$.categories_id') AS CATEGORIES_ID
    FROM DCAT$ANALYTICS_WORKSHOP_ASSET_TOMEK_DWH1.TOMEK_categories;

SELECT * FROM EX_CATEGORYVIEW;