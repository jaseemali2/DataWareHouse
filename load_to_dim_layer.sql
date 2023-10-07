

--Create precedure for all seequances to assign values for surrogate keys
 CREATE OR REPLACE PROCEDURE bl_cl.load_sequences()
 LANGUAGE plpgsql
 AS $$
 BEGIN 

	CREATE SEQUENCE IF NOT EXISTS prod_surr_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	-- Create seequnce for primary key values in customer dimension tables.

	CREATE SEQUENCE IF NOT EXISTS cust_surr_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 -- Create seequnce for primary key values of shippers dimension tables.

	CREATE SEQUENCE IF NOT EXISTS shippers_surr_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 -- Create seequnce for primary key values in employees dimension tables.

	CREATE SEQUENCE IF NOT EXISTS emp_surr_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 -- Create seequnce for primary key values in stores dimension tables.

	CREATE SEQUENCE IF NOT EXISTS store_surr_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 -- Create seequnce for primary key values of junks dimension tables.

	CREATE SEQUENCE IF NOT EXISTS paymnet_channel_surr_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
END;
$$;



--Create function to load data into products table from 3nf layer
CREATE OR REPLACE PROCEDURE  bl_cl.load_products_bl_dm()
LANGUAGE plpgsql
AS $$
DECLARE
	v_rows_count INT;
	v_inserted_count INT;
	v_table_name VARCHAR := 'dim_products_scd';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message varchar;
BEGIN
	
	-- Update start date , end date of rows wich is updated in 3nf layer.
	UPDATE bl_dm.dim_products_scd dimp 
	SET update_dt = current_date, 
		end_dt = (SELECT end_dt  FROM bl_3nf.ce_products_scd p WHERE dimp.product_src_id = p.product_id::varchar
		AND p.is_active = FALSE),
		is_active = (SELECT is_active FROM bl_3nf.ce_products_scd p2 WHERE  dimp.product_src_id = p2.product_id::varchar
		AND is_active = FALSE)
		WHERE dimp.is_active = TRUE AND 
		EXISTS (SELECT 1 FROM bl_3nf.ce_products_scd p WHERE 
						dimp.product_src_id = p.product_id::varchar 
						AND dimp.start_dt = p.start_dt 
						AND p.is_active = FALSE );
	--Insert values to product table
	INSERT
		INTO
		bl_dm.dim_products_scd (product_surr_id,
		product_src_id,
		product_name,
		brand,
		package_type,
		category,
		start_dt,
		end_dt,
		is_active,
		insert_dt,
		update_dt)
		SELECT
		nextval('prod_surr_key_value'),
		COALESCE(cps.product_id::VARCHAR, 'n.a'),
		COALESCE(cps.product_name, 'n.a'),
		COALESCE(cps.brand, 'n.a'), 
		COALESCE(cps.package_type, 'n.a'),
		COALESCE(ca.category_name, 'n.a'),
		COALESCE (cps.start_dt , '1900-01-01'),
		COALESCE (cps.end_dt ,  '1900-01-01'),
		COALESCE (cps.is_active , TRUE ),
		CURRENT_DATE,
		TO_DATE('1990-01-01', 'YYYY-MM-DD')
	FROM
		bl_3nf.ce_products_scd cps
	LEFT JOIN bl_3nf.ce_categories ca 
		ON
		cps.category_id = ca.category_id
	WHERE
--To avoid dupilcate rows.
	NOT EXISTS (
	SELECT
		1
	FROM
		bl_dm.dim_products_scd AS dp
	WHERE
		dp.product_src_id = COALESCE(cps.product_id::varchar ,'n.a')
		AND dp.product_name = COALESCE(cps.product_name,'n.a')
		AND dp.brand = COALESCE(cps.brand ,'n.a')
		AND dp.package_type = COALESCE(cps.package_type ,'n.a')
		AND dp.category = COALESCE(ca.category_name ,'n.a'));
	-- Get number of inserted rows
	GET DIAGNOSTICS v_rows_count = ROW_COUNT;
	v_text_message := 'No. of rows inserted: ' || v_rows_count::VARCHAR;
	-- Print total number of insered rows.
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_text_message, v_inserted_date);
 	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
	
END;
$$;





-- Create procedure to insert values to customers dimension table from 3nf layer.
CREATE OR REPLACE PROCEDURE bl_cl.load_customers_bl_dm()
LANGUAGE plpgsql
AS $$
DECLARE 
	v_rows_count INT;
	v_inserted_count INT;
	v_table_name VARCHAR := 'dim_customers';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR ;
BEGIN 
	--Insert rows to customers table.
	INSERT
	INTO
	bl_dm.dim_customers (customer_surr_id,
	customer_src_id,
	first_name,
	last_name,
	email,
	phone,
	gender,
	date_of_birth,
	address,
	zipcode,
	city,
	country,
	region,
	insert_dt,
	update_dt)
SELECT
	NEXTVAL('cust_surr_key_value'),
	COALESCE(c.customer_id::varchar,'n.a'),
	COALESCE(c.first_name ,'n.a'),
	COALESCE(c.last_name , 'n.a'),
	COALESCE(c.email ,'n.a'),
	COALESCE(c.phone , 'n.a'),
	COALESCE(c.gender ,'n.a'),
	-- Convert varchar to date format from 3nf layer
	COALESCE(TO_DATE( c.date_of_birth , 'YYYY-MM-DD'),'1-1-1900'),
	COALESCE(a.address , 'n.a'),
	COALESCE(a.zipcode ,'n.a'),
	COALESCE(c2.city_name , 'n.a'),
	COALESCE(c3.country_name ,'n.a'),
	COALESCE(r.region_name ,'n.a'),
	current_date,
	TO_DATE( '1990-1-1' , 'YYYY-MM-DD')
	
FROM
	bl_3nf.ce_customers AS c
LEFT JOIN bl_3nf.ce_addresses AS a 
ON
	c.address_id = a.address_id
LEFT JOIN bl_3nf.ce_cities AS c2 
ON
	a.city_id = c2.city_id
LEFT JOIN bl_3nf.ce_countries AS c3 
ON
	c2.country_id = c3.country_id
LEFT JOIN bl_3nf.ce_regions AS r 
ON
	c3.region_id = r.region_id
--To avoid duplicate rows.
WHERE
	NOT EXISTS (
	SELECT
		1
	FROM
		bl_dm.dim_customers AS dc
	WHERE
		dc.customer_src_id = COALESCE(c.customer_id::varchar,'n.a')
		AND dc.first_name = COALESCE(c.first_name ,'n.a')
		AND dc.last_name = COALESCE(c.last_name , 'n.a')
		AND dc.email = COALESCE(c.email ,'n.a')
		AND dc.phone = COALESCE(c.phone , 'n.a')
		AND dc.gender = COALESCE(c.gender ,'n.a')
		-- Convert varchar to date format from 3nf layer
		AND dc.date_of_birth = COALESCE(TO_DATE( c.date_of_birth , 'YYYY-MM-DD'),'1-1-1900')
			AND dc.address = COALESCE(a.address , 'n.a')
			AND dc.zipcode = COALESCE(a.zipcode ,'n.a')
			AND dc.city = COALESCE(c2.city_name , 'n.a')
			AND dc.country = COALESCE(c3.country_name ,'n.a')
			AND dc.region = COALESCE(r.region_name ,'n.a') )
		-- Apply concept of SCD1, update columns if there is any change in 3nf layer												
			ON
		CONFLICT (customer_src_id) DO
	UPDATE
	SET
				first_name = excluded.first_name ,
				last_name = excluded.last_name,
				email = excluded.email,
				phone = excluded.phone,
				gender = excluded.gender,
				date_of_birth = excluded.date_of_birth,
				address = excluded.address,
				zipcode = excluded.zipcode,
				city = excluded.city,
				country = excluded.country,
				region = excluded.region,
				update_dt = CURRENT_DATE ;
	--Folllowing opearations will return total number of new inserted rows.
	GET DIAGNOSTICS v_rows_count = ROW_COUNT;
	v_text_message := 'No. of rows inserted: ' || v_rows_count::VARCHAR;
	-- Print total number of insered rows.
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_text_message, v_inserted_date);

EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
END;
$$;



--Create function to insert values to shippers dimension table.
CREATE OR REPLACE PROCEDURE bl_cl.load_shippers_dm()
LANGUAGE plpgsql
AS $$
DECLARE 
	v_rows_count INT;
	v_inserted_count INT;
	v_table_name VARCHAR := 'dim_shippers';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR ;
BEGIN
		INSERT
	INTO
	bl_dm.dim_shippers(shipper_surr_id,
	shipper_src_id,
	shipper_name,
	email,
	phone,
	insert_dt,
	update_dt)
SELECT
	NEXTVAL('shippers_surr_key_value'),
	COALESCE(cs.shipper_id::varchar, 'n.a'),
	COALESCE(cs.shipper_name, 'n.a'),
	COALESCE(cs.email, 'n.a'),
	COALESCE (cs.phone,
	'n.a'),
	current_date,
	TO_DATE( '1990-1-1' , 'YYYY-MM-DD')
FROM
	bl_3nf.ce_shippers AS cs
-- To avoid duplicate entries.
WHERE
	NOT EXISTS (
	SELECT
		1
	FROM
		bl_dm.dim_shippers AS ds
	WHERE
		ds.shipper_src_id = COALESCE(cs.shipper_id::varchar, 'n.a')
			AND ds.shipper_name = COALESCE(cs.shipper_name, 'n.a')
				AND ds.email = COALESCE(cs.email, 'n.a')
					AND ds.phone = COALESCE (cs.phone,
					'n.a'))
				ON CONFLICT (shipper_src_id) DO UPDATE 
				SET shipper_name = excluded.shipper_name,
					email = excluded.email,
					phone = excluded.phone;
	--Folllowing opearations will return total number of new inserted rows.
	GET DIAGNOSTICS v_rows_count = ROW_COUNT;
	v_text_message := 'No. of rows inserted: ' || v_rows_count::VARCHAR;
	-- Print total number of insered rows.
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_text_message, v_inserted_date);

	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
	
END;
$$;



--Create procedure to insert values to employees dimension table from 3nf layer.
CREATE OR REPLACE PROCEDURE bl_cl.load_employees_bl_dm()
LANGUAGE plpgsql
AS $$
DECLARE 
	v_rows_count INT;
	v_inserted_count INT;
	v_table_name VARCHAR := 'dim_employees';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR ;
BEGIN 
		INSERT
		INTO
		bl_dm.dim_employees (employee_surr_id,
		employee_src_id,
		first_name,
		last_name,
		email,
		phone,
		gender,
		date_of_birth,
		department ,
		address,
		zipcode,
		city,
		country,
		region,
		salary,
		insert_dt,
		update_dt)
	SELECT
		NEXTVAL('emp_surr_key_value'),
		COALESCE(e.employee_id::varchar,'n.a'),
		COALESCE(e.first_name ,'n.a'),
		COALESCE(e.last_name ,'n.a'),
		COALESCE(e.email ,'n.a'),
		COALESCE(e.phone ,'n.a'),
		COALESCE(e.gender ,'n.a'),
		-- Set to '1900-1-1' if select statement returns null value.
		COALESCE(TO_DATE( e.date_of_birth , 'YYYY-MM-DD') , '1900-1-1'),
		COALESCE(d.department_name ,'n.a'),
		COALESCE(a2.address,'n.a'),
		COALESCE(a2.zipcode ,'n.a'),
		COALESCE(cc.city_name ,'n.a'),
		COALESCE(cc2.country_name ,'n.a'),
		COALESCE(r2.region_name ,'n.a'),
		COALESCE(e.salary::decimal(8,2), -1),
		current_date,
		TO_DATE( '1990-1-1' , 'YYYY-MM-DD')
	FROM
		bl_3nf.ce_employees AS e
	LEFT JOIN bl_3nf.ce_departments AS d 
	ON
		e.department_id = d.department_id
	LEFT JOIN bl_3nf.ce_addresses AS a2
	ON
		e.address_id = a2.address_id
	LEFT JOIN bl_3nf.ce_cities AS cc 
	ON
		a2.city_id = cc.city_id
	LEFT JOIN bl_3nf.ce_countries AS cc2
	ON
		cc.country_id = cc2.country_id
	LEFT JOIN bl_3nf.ce_regions AS r2 
	ON
		cc2.region_id = r2.region_id
	WHERE
		NOT EXISTS (
		SELECT
			1
		FROM
			bl_dm.dim_employees AS de
		WHERE
			de.employee_src_id = COALESCE(e.employee_id::varchar,'n.a')
			AND de.first_name = COALESCE(e.first_name ,'n.a')
			AND de.last_name = COALESCE(e.last_name ,'n.a')
			AND de.email = COALESCE(e.email ,'n.a')
			AND de.phone = COALESCE(e.phone ,'n.a')
			AND de.gender = COALESCE(e.gender ,'n.a')
								-- Convert varchar to date format from 3nf layer
			AND de.date_of_birth = COALESCE(TO_DATE( e.date_of_birth , 'YYYY-MM-DD') , '1900-1-1')
				AND de.department = COALESCE(d.department_name ,'n.a')
				AND de.address = COALESCE(a2.address,'n.a')
				AND de.zipcode = COALESCE(a2.zipcode ,'n.a')
				AND de.city = COALESCE(cc.city_name ,'n.a')
				AND de.country = COALESCE(cc2.country_name ,'n.a')
				AND de.region = COALESCE(r2.region_name ,'n.a')
				AND de.salary = COALESCE(e.salary::decimal(8,2), -1))
				ON CONFLICT (employee_src_id) DO UPDATE 
				SET first_name = excluded.first_name ,
					last_name = excluded.last_name, 
					email = excluded.email,
					phone = excluded.phone,
					department = excluded.department,
					address = excluded.address,
					salary = excluded.salary;
	--Folllowing opearations will return total number of new inserted rows.
	GET DIAGNOSTICS v_rows_count = ROW_COUNT;
	v_text_message := 'No. of rows inserted: ' || v_rows_count::VARCHAR;
	-- Print total number of insered rows.
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_text_message, v_inserted_date);
	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
END;
$$;



--Create procedure to insert values to stores dimension table from 3nf layer.
CREATE OR REPLACE PROCEDURE bl_cl.load_stores_bl_dm()
LANGUAGE plpgsql
AS $$
DECLARE 
	v_rows_count INT;
	v_inserted_count INT;
	v_table_name VARCHAR := 'dim_stores';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR;
BEGIN 
	INSERT INTO bl_dm.dim_stores (store_surr_id ,store_src_id, store_name, manager_first_name, manager_last_name, email, address, zipcode,
		city , country , region , insert_dt , update_dt 
		)
		
		SELECT  NEXTVAL('store_surr_key_value'),
		COALESCE(cs.store_id::varchar, 'n.a'),
		COALESCE(cs.store_name, 'n.a'),
		COALESCE (cs.manager_first_name, 'n.a'),
		COALESCE (cs.manager_last_name, 'n.a'),
		COALESCE (cs.store_email, 'n.a'),
		COALESCE (ca.address, 'n.a'),
		COALESCE (ca.zipcode, 'n.a'),
		COALESCE (cc.city_name, 'n.a'),
		COALESCE (cc2.country_name, 'n.a'),
		COALESCE (cr.region_name, 'n.a'),
		current_date,
		TO_DATE( '1990-1-1' , 'YYYY-MM-DD')
		FROM bl_3nf.ce_stores cs  
		INNER JOIN bl_3nf.ce_addresses ca 
		ON cs.address_id = ca.address_id 
		INNER JOIN bl_3nf.ce_cities cc 
		ON ca.city_id = cc.city_id 
		INNER JOIN bl_3nf.ce_countries cc2 
		ON cc.country_id = cc2.country_id 
		INNER JOIN bl_3nf.ce_regions cr 
		ON cc2.region_id = cr.region_id 
		WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_stores ds
		WHERE ds.store_src_id = COALESCE(cs.store_id::varchar, 'n.a')
		AND ds.store_name = COALESCE(cs.store_name, 'n.a')
		AND ds.manager_first_name = COALESCE (cs.manager_first_name, 'n.a')
		AND ds.manager_last_name = COALESCE (cs.manager_last_name, 'n.a')
		AND ds.email = COALESCE (cs.store_email, 'n.a')
		AND ds.address = COALESCE (ca.address, 'n.a')
		AND ds.zipcode = COALESCE (ca.zipcode, 'n.a')
		AND ds.city = COALESCE (cc.city_name, 'n.a')
		AND ds.country = COALESCE (cc2.country_name, 'n.a')
		AND ds.region = COALESCE (cr.region_name, 'n.a')
		)
	-- Update columns if there is any change in 3nf layer.
	ON CONFLICT (store_src_id) DO UPDATE 
	SET store_name = excluded.store_name,
		manager_first_name = excluded.manager_first_name,
		manager_last_name = excluded.manager_last_name,
		email  = excluded.email,
		address = excluded.address,
		zipcode = excluded.zipcode,
		city = excluded.city,
		update_dt  = CURRENT_DATE;
	--Folllowing opearations will return total number of new inserted rows.
	GET DIAGNOSTICS v_rows_count = ROW_COUNT;
	v_text_message := 'No. of rows inserted: ' || v_rows_count::VARCHAR;
	-- Print total number of insered rows.
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_text_message, v_inserted_date);
	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
END;
$$;




--Create procedure to insert values to payment_chanel_type dimesnion table.
CREATE OR REPLACE PROCEDURE bl_cl.load_channel_payment_types_bl_dm()
LANGUAGE plpgsql
AS $$
DECLARE 
	v_rows_count INT;
	v_inserted_count INT;
	v_table_name VARCHAR := 'dim_channel_payment_types';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR;
BEGIN 
	INSERT
	INTO
	bl_dm.dim_payment_type_chanel_type (dim_payment_type_chanel_type_surr_id,
	payment_type_src_id,
	channel_type_src_id,
	-- Create unique id for upsert opearion.
	unique_id ,
	payment_type,
	channel_type,
	insert_dt,
	update_dt)
SELECT
--Incriment surrogate key value.
	NEXTVAL('paymnet_channel_surr_key_value'),
	COALESCE(cpt.payment_type_id::varchar, 'n.a'),
	COALESCE(cct.channel_type_id::varchar , 'n.a'),
	-- Creating unique id by concatinating payment and channel types.
	COALESCE(CONCAT(cpt.payment_type_id::varchar,'_', cct.channel_type_id::varchar), 'n.a'),
	COALESCE(cpt.payment_type_name , 'n.a'),
	COALESCE(cct.channel_type_name , 'n.a'),
	current_date,
	TO_DATE( '1990-1-1' , 'YYYY-MM-DD')
FROM
	bl_3nf.ce_payment_types cpt
-- In this case we need cartition product of payment_type and channel_type tables to get all combinations from both data sources. 
-- So need to use cross join of those two tables from 3nf layer.
CROSS JOIN bl_3nf.ce_channel_types cct
-- To avoid duplicate rows.
WHERE
	NOT EXISTS (
	SELECT
		1
	FROM
		bl_dm.dim_payment_type_chanel_type AS pc
	WHERE
		pc.payment_type_src_id = COALESCE(cpt.payment_type_id::varchar, 'n.a')
			AND pc.channel_type_src_id = COALESCE(cct.channel_type_id::varchar , 'n.a')
				AND pc.payment_type = COALESCE(cpt.payment_type_name , 'n.a')
					AND pc.channel_type = COALESCE(cct.channel_type_name , 'n.a'))
	ON CONFLICT (unique_id) DO UPDATE 
	SET payment_type = excluded.payment_type,
		channel_type = excluded.channel_type,
		update_dt = CURRENT_DATE;
		--Folllowing opearations will return total number of new inserted rows.
	GET DIAGNOSTICS v_rows_count = ROW_COUNT;
	v_text_message := 'No. of rows inserted: ' || v_rows_count::VARCHAR;
	-- Print total number of insered rows.
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_text_message, v_inserted_date);
	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);

END;
$$;





-- Procedure to insert values to fact table fct_sales

CREATE OR REPLACE PROCEDURE  bl_cl.load_to_fct_sales()
LANGUAGE plpgsql
AS $$
DECLARE 
	v_rec record;
	
	v_start_date date;
	
	v_end_date date;
	-- To find last day of month
	v_to_range date := (
	SELECT
		(date_trunc('MONTH', (SELECT min(cs.sale_dt))) + INTERVAL '1 MONTH - 1 DAY')::DATE AS last_date_of_month
	FROM
		bl_3nf.ce_sales cs) ;
	
	partition_name TEXT;

	v_count bigint := 0;

	v_text_message VARCHAR;

	v_user_info VARCHAR := current_user;
	
	partition_suffix TEXT;
	
	partition_prefix TEXT;
	
	v_attach_date_from date;
	
	v_attach_date_to date;
	
	v_attach_suffix TEXT ;
	
	v_attach_prefix TEXT ;
	
	to_dt date;
	
	v_fct_sale_max_sale_dt date := (
	SELECT
		max(sale_dt)
	FROM
		bl_dm.fct_sales fs2);
	
	last_month date := (
	SELECT
		date_trunc('MONTH', max(CS.sale_dt::date))::date
	FROM
		bl_3nf.ce_sales cs ) ;
	
	v_attach_name TEXT := '';
	
	v_max_3nf_sale_dt date = (
	SELECT
		max(cs.sale_dt::date)
	FROM
		bl_3nf.ce_sales cs);
BEGIN 
		FOR v_rec IN 
			SELECT
		date_trunc('month', cs.sale_dt)::date AS start_date,
		((date_trunc('month', cs.sale_dt)::date + INTERVAL '1 month')::date - INTERVAL '1 day')::date AS end_date, 
		*
	FROM
		bl_3nf.ce_sales cs
		-- To get dates more than last inserted one.
	WHERE
		cs.sale_dt > (
		SELECT
			max(sale_dt)
		FROM
			bl_dm.fct_sales fs2)
	ORDER BY
		cs.sale_dt 
		LOOP 
			v_start_date := v_rec.start_date;
	
	v_end_date := v_rec.end_date;
	
	partition_suffix := to_char(v_start_date, 'YYYYMMDD');
	
	partition_prefix := to_char(v_end_date, 'YYYYMMDD');
	
	partition_name := 'table_partition' || '_' || partition_suffix || '_' || partition_prefix;
	-- If current date is greater last day of previous month then attach inserted partition.
		IF v_rec.sale_dt > v_to_range
	AND v_rec.sale_dt != v_max_3nf_sale_dt THEN
	--To get start date of last month and then attach previous month partition.
	v_attach_date_from := (v_start_date - INTERVAL '1 month')::date;
	--To get end date of last month and then attach previous month partition.
	v_attach_date_to := (
	SELECT
		(DATE_TRUNC('MONTH', v_rec.sale_dt ) - INTERVAL '1 day')::date );
	
	v_attach_suffix := to_char(v_attach_date_from, 'YYYYMMDD');
	
	v_attach_prefix := to_char(v_attach_date_to, 'YYYYMMDD');
	
	v_attach_name := 'table_partition' || '_' || v_attach_suffix || '_' || v_attach_prefix;
	-- Attach last inserted partition to parent table. 
		EXECUTE 'ALTER TABLE bl_dm.fct_sales ATTACH PARTITION ' || v_attach_name || ' FOR VALUES FROM 
		     (' || quote_literal(v_attach_date_from) || ') TO (' || quote_literal(to_dt) || ')';
	
	v_to_range := v_end_date;
	END IF ;
	-- Check the partition is already present.
		IF NOT EXISTS (
	SELECT
		1
	FROM
		pg_tables
	WHERE
		schemaname = 'public'
		AND tablename = partition_name) THEN
	-- Date range of partition. 
	to_dt := (v_end_date + INTERVAL '1 day')::date ;
	
	EXECUTE 'CREATE TABLE IF NOT EXISTS ' || partition_name || ' PARTITION OF bl_dm.fct_sales FOR VALUES FROM 
			(' || quote_literal(v_start_date) || ') TO (' || quote_literal(to_dt) || ')';
	--Detach partition 
		EXECUTE 'ALTER TABLE bl_dm.fct_sales DETACH PARTITION ' || partition_name;
	END IF ;
	
		-- Insert rows to partition
			EXECUTE 'INSERT INTO ' || partition_name || 
			        ' (product_surr_id, store_surr_id, employee_surr_id, shipper_surr_id, customer_surr_id, 
			        dim_payment_type_chanel_type_surr_id, date_id, sale_dt, shipping_cost, product_price_kg , quantity , discount , 
			        amount,	insert_dt) 
			SELECT
				COALESCE(a.product_surr_id , -1) as product_surr_id ,
				COALESCE(a.store_surr_id, -1) as store_surr_id,
				COALESCE(a.employee_surr_id, -1) as employee_surr_id,
				COALESCE(a.shipper_surr_id, -1) as shipper_surr_id,
				COALESCE(a.customer_surr_id, -1) as customer_surr_id,
				COALESCE(a.dim_payment_type_chanel_type_surr_id , -1) as dim_payment_type_chanel_type_surr_id,
				a.time_id,
				$1,
				$2::NUMERIC(8,2),
				$3::NUMERIC(8,2),
				$4::NUMERIC(8,2),
				$5::NUMERIC(8,2),
				$6::NUMERIC(10,2),
				CURRENT_DATE
				FROM (
					SELECT
					s.sale_id AS sale_id,
					COALESCE(dp.product_surr_id , -1) AS product_surr_id,
					COALESCE(ds.store_surr_id, -1) AS store_surr_id,
					COALESCE(de.employee_surr_id , -1) AS employee_surr_id,
					COALESCE(ds2.shipper_surr_id, -1) AS shipper_surr_id,
					COALESCE(dc.customer_surr_id, -1) AS customer_surr_id,
					COALESCE(dcp.dim_payment_type_chanel_type_surr_id , -1) AS dim_payment_type_chanel_type_surr_id,
					dd.time_id  ,
					s.sale_dt AS sale_dt,
					s.shipping_cost::NUMERIC(8,2),
					s.product_price_kg::NUMERIC(8,2),
					s.quantity::NUMERIC(8,2),
					s.discount::NUMERIC(8,2),
					s.amount::NUMERIC(10,2),
					CURRENT_DATE AS insert_dt 
				FROM bl_3nf.ce_sales AS s
				LEFT JOIN bl_dm.dim_products_scd AS dp ON s.product_id::varchar = dp.product_src_id
				LEFT JOIN bl_dm.dim_stores AS ds ON s.store_id::varchar = ds.store_src_id
				LEFT JOIN bl_dm.dim_employees de ON s.employee_id::varchar = de.employee_src_id
				LEFT JOIN bl_dm.dim_shippers ds2 ON s.shipper_id::varchar = ds2.shipper_src_id
				LEFT JOIN bl_dm.dim_customers dc ON s.customer_id::varchar = dc.customer_src_id
				LEFT JOIN bl_dm.dim_payment_type_chanel_type dcp ON s.payment_type_id::varchar = dcp.payment_type_src_id
				AND s.channel_type_id::varchar = dcp.channel_type_src_id
				LEFT JOIN bl_dm.dim_dates AS dd ON s.sale_dt::date = dd.time_id  
				order by s.sale_dt 
				) AS a
			WHERE a.sale_id = $7 '
		USING 
				v_rec.sale_dt, 
				v_rec.shipping_cost::NUMERIC(8,
	2),
				v_rec.product_price_kg::NUMERIC(8,
	2),
				v_rec.quantity::NUMERIC(8,
	2),
				v_rec.discount::NUMERIC(8,
	2),
				v_rec.amount::NUMERIC(10,
	2),
				v_rec.sale_id;
	
	RAISE NOTICE 'insert date: %',
	v_rec.sale_dt;

	v_count = v_count + 1;

	END LOOP;
	--Atach last partiton to fact table.
	IF partition_name IS NOT NULL THEN 
	
	RAISE NOTICE 'Last partition: %',  partition_name;

	EXECUTE 'ALTER TABLE bl_dm.fct_sales ATTACH PARTITION ' || partition_name || ' FOR VALUES FROM 
		(' || quote_literal(v_start_date) || ') TO (' || quote_literal(to_dt) || ')';
	END IF ;

	v_text_message := 'No. of rows inserted: ' || v_count::VARCHAR;
	-- Print total number of insered rows.
	CALL bl_cl.logging_info(v_user_info, 'fct_sales', 'sa_sales_latvia' || 'sa_sales_lithuania', v_text_message, current_date);
END;
$$;

























