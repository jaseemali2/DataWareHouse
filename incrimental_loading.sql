-- Create meta table to incrimental loading process
CREATE TABLE IF NOT EXISTS bl_cl.prm_mta_incrimental_load(
	source_table_name varchar,
	target_table_name varchar,
	procedure_name varchar,
	previous_loaded_date timestamp
);

-- Initial inserts to meta table
INSERT INTO bl_cl.prm_mta_incrimental_load (source_table_name, target_table_name, procedure_name, previous_loaded_date)
VALUES 	('sa_sales_latvia.src_latvia_sales' , 'bl_3nf.ce_products', 'bl_cl.load_products_data_3nf()', '1900-01-01'),
		('sa_sales_lithuana.src_lithuania_sales' , 'bl_3nf.ce_products', 'bl_cl.load_products_data_3nf()', '1900-01-01'),
		('sa_sales_latvia.src_latvia_sales' , 'bl_3nf.ce_sales', 'bl_cl.load_sales_data_3nf()', '1900-01-01'),
		('sa_sales_lithuana.src_lithuania_sales' , 'bl_3nf.ce_sales', 'bl_cl.load_sales_data_3nf()', '1900-01-01');



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




