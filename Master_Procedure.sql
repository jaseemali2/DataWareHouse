-- Master Procedure to load from physical layer to dimension layer

CREATE OR REPLACE
PROCEDURE bl_cl.master_procedure()
LANGUAGE plpgsql
AS $$
BEGIN
	
	--CALL bl_cl.load_src_latvia();
	--CALL bl_cl.load_src_lithuania();
	      
		-- create view to load data to ce_stores from first data source sa_latvia
	CREATE OR REPLACE
	VIEW bl_cl.incriment_latvia_products_mv AS 
	SELECT
		*
	FROM
		sa_sales_latvia.src_latvia_sales sls
	WHERE
		insert_dt > (
		SELECT
			previous_loaded_date
		FROM
			bl_cl.prm_mta_incrimental_load
		WHERE
			source_table_name = 'sa_sales_latvia.src_latvia_sales'
			AND procedure_name = 'bl_cl.load_products_data_3nf()'
			AND target_table_name = 'bl_3nf.ce_products' );
	
	CREATE OR REPLACE
	VIEW bl_cl.incriment_lithuania_products_mv AS 
	SELECT
		*
	FROM
		sa_sales_lithuania.src_lithuania_sales sls
	WHERE
		insert_dt > (
		SELECT
			previous_loaded_date
		FROM
			bl_cl.prm_mta_incrimental_load
		WHERE
			source_table_name = 'sa_sales_lithuana.src_lithuania_sales'
			AND procedure_name = 'bl_cl.load_products_data_3nf()'
			AND target_table_name = 'bl_3nf.ce_products' );

		
	-- create view to load data to ce_sales from data source sa_latvia
		CREATE OR REPLACE
		VIEW bl_cl.incriment_latvia_sales_mv AS 
		SELECT
			*
	FROM
			sa_sales_latvia.src_latvia_sales sls
	WHERE
			insert_dt > (
		SELECT
				previous_loaded_date
		FROM
				bl_cl.prm_mta_incrimental_load
		WHERE
				source_table_name = 'sa_sales_latvia.src_latvia_sales'
			AND procedure_name = 'bl_cl.load_sales_data_3nf()'
			AND target_table_name = 'bl_3nf.ce_sales' );
	-- create view to load data to ce_sales from data source sa_lithuania
		CREATE OR REPLACE
		VIEW bl_cl.incriment_lithuania_sales_mv AS 
		SELECT
			*
	FROM
			sa_sales_lithuania.src_lithuania_sales
	WHERE
			insert_dt > (
		SELECT
				previous_loaded_date
		FROM
				bl_cl.prm_mta_incrimental_load
		WHERE
				source_table_name = 'sa_sales_lithuana.src_lithuania_sales'
			AND procedure_name = 'bl_cl.load_sales_data_3nf()'
			AND target_table_name = 'bl_3nf.ce_sales' );
		
	-- Load to 3NF layer
	CALL bl_cl.load_3nf_sequences();
	
	CALL bl_cl.load_sequences();
	
	CALL bl_cl.load_category_data_3nf();
	
	CALL bl_cl.load_product_data_3nf() ;
	
	CALL bl_cl.load_shippers_data_3nf() ;
	
	CALL bl_cl.load_regions_data_3nf() ;
	
	CALL bl_cl.load_countries_data_3nf();
	
	CALL bl_cl.load_cities_data_3nf();
	
	CALL bl_cl.load_addresses_data_3nf() ;
	
	CALL bl_cl.load_customers_data_3nf();
	
	CALL bl_cl.load_departments_data_3nf() ;
	
	CALL bl_cl.load_payments_data_3nf() ;
	
	CALL bl_cl.load_channels_data_3nf() ;
	
	CALL bl_cl.load_stores_data_3nf() ;
	
	CALL bl_cl.load_employees_data_3nf();
	
	CALL bl_cl.load_sales_data_3nf() ;
	-- Load to dimension Layer
	CALL bl_cl.load_products_bl_dm();
	
	CALL bl_cl.load_customers_bl_dm();
	
	CALL bl_cl.load_shippers_dm();
	
	CALL bl_cl.load_employees_bl_dm();
	
	CALL bl_cl.load_stores_bl_dm();
	
	CALL bl_cl.load_channel_payment_types_bl_dm();
	
	CALL bl_cl.load_to_fct_sales();

END ;

$$;

CALL  bl_cl.master_procedure();

SELECT * FROM bl_3nf.ce_products_scd cps ;
SELECT * FROM bl_dm.dim_products_scd dps ;

SELECT * FROM bl_3nf.ce_employees ce ;
SELECT cs.sale_dt FROM bl_3nf.ce_sales cs ;
SELECT * FROM bl_3nf.ce_sales cs 
ORDER BY sale_dt  desc;

SELECT * FROM bl_cl.logging_table  ;

UPDATE bl_3nf.ce_stores 
SET manager_first_name  = 'Jennifer',
	manager_last_name  = 'Stewart'
	WHERE store_id = 154;

SELECT * FROM bl_3nf.ce_stores cs ;

SELECT * FROM bl_dm.dim_stores ds ;

SELECT count(*)  FROM bl_dm.fct_sales;
SELECT count(*) FROM bl_dm.dim_products_scd dps ;










SELECT sls .manager_first_name , sls.manager_last_name , sls.store_id  FROM sa_sales_latvia.src_latvia_sales sls 
WHERE store_id = 4::varchar


