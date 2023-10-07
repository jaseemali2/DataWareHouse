-- Create schema BL_CL (Cleansing Layer)
CREATE SCHEMA IF NOT EXISTS BL_CL;

-- Create table for logging data storing
CREATE TABLE IF NOT EXISTS bl_cl.logging_table(
	user_info VARCHAR,
	inserted_table VARCHAR,
	data_source VARCHAR,
	text_message VARCHAR,
	inserted_date DATE	
);


-- Create procedure to insert appropriate logging information
CREATE OR REPLACE PROCEDURE bl_cl.logging_info(p_user_info VARCHAR, p_table_name VARCHAR, p_data_source VARCHAR, p_text_message VARCHAR, p_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN 
	INSERT INTO bl_cl.logging_table (user_info, inserted_table, data_source, text_message, inserted_date)
	VALUES (p_user_info,  p_table_name, p_data_source, p_text_message,  p_date);
END;
$$;

--SEEQUNCE PROCEEDURE 
CREATE OR REPLACE PROCEDURE bl_cl.load_3nf_sequences()
 LANGUAGE plpgsql
 AS $$
 BEGIN 

	CREATE SEQUENCE IF NOT EXISTS prod_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	-- Create seequnce for primary key values in customer dimension tables.

	CREATE SEQUENCE IF NOT EXISTS cust_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 -- Create seequnce for primary key values of shippers dimension tables.

	CREATE SEQUENCE IF NOT EXISTS shippers_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 -- Create seequnce for primary key values in employees dimension tables.

	CREATE SEQUENCE IF NOT EXISTS emp_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 -- Create seequnce for primary key values in stores dimension tables.

	CREATE SEQUENCE IF NOT EXISTS store_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 -- Create seequnce for primary values of payments.

	CREATE SEQUENCE IF NOT EXISTS paymnet_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 
	 -- Create seequnce for channel primary key

	CREATE SEQUENCE IF NOT EXISTS channel_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 
	 	 
	 -- Create seequnce for categories primary key

	CREATE SEQUENCE IF NOT EXISTS category_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 
	 -- Create seequnce for regions primary key

	CREATE SEQUENCE IF NOT EXISTS region_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 
	-- Create seequnce for regions primary key

	CREATE SEQUENCE IF NOT EXISTS countries_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 
	
	CREATE SEQUENCE IF NOT EXISTS cities_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 

	CREATE SEQUENCE IF NOT EXISTS addresses_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 
	
	CREATE SEQUENCE IF NOT EXISTS departments_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
	 

	CREATE SEQUENCE IF NOT EXISTS sales_id_key_value
	  START WITH 1
	  INCREMENT BY 1
	  MINVALUE 1
	  NO MAXVALUE
	  NO CYCLE;
END;
$$;




--Create function to load data from source tables to caregory table in 3nf layer
CREATE OR REPLACE PROCEDURE  bl_cl.load_category_data_3nf() 
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_categories';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;	
	v_error_message VARCHAR;
BEGIN 
	INSERT
		INTO
		bl_3nf.ce_categories(
		category_id,
		category_name,
		category_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		category_src_unique_id)
	SELECT
		nextval('category_id_key_value'), s1.category_name , s1.category_src_id, s1.source_system, s1.source_system_entity,
		s1.insert_dt, s1.update_dt, s1.category_src_unique_id 
	FROM 
	(SELECT DISTINCT COALESCE(ss.category , 'n.a') AS category_name ,
		COALESCE(ss.category_id , '-1') AS category_src_id ,
		'sa_sales_latvia'  AS source_system,
		 'src_latvia_sales' AS source_system_entity,
		CURRENT_DATE AS insert_dt,
		CURRENT_DATE AS update_dt,
		CONCAT(ss.category_id,'_' , 'sa_sales_latvia', '_', 'src_latvia_sales') AS category_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss ) AS s1
	-- To stop entering duplicate rows.
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_categories AS bc
		WHERE
			bc.category_name = COALESCE(s1.category_name  , 'n.a')
			AND bc.category_src_id = COALESCE(s1.category_src_id  , '-1') 
			AND bc.source_system = 'sa_sales_latvia'
			AND bc.source_system_entity = 'src_latvia_sales') 
			
			ON CONFLICT (category_src_unique_id) DO UPDATE 
				SET category_name  = excluded.category_name ,
					update_dt = CURRENT_DATE;
		-- To get number rows inserted
		GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
		-- text message for loggining table			
		v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load data to logging table.
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia', v_text_message, v_inserted_date);

	---Load from second data source
		INSERT
		INTO
		bl_3nf.ce_categories(
		category_id, 
		category_name,
		category_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt, 
		category_src_unique_id)
		SELECT 
		nextval('category_id_key_value'), s2.category_name , s2.category_src_id, s2.source_system, s2.source_system_entity,
		s2.insert_dt, s2.update_dt, s2.category_src_unique_id 
	FROM 
		(
		SELECT
		DISTINCT COALESCE(li.category , 'n.a') AS category_name ,
		COALESCE(li.category_id , '-1') AS category_src_id ,
		'sa_sales_lithuania'  AS source_system,
		 'src_lithuania_sales' AS source_system_entity,
		CURRENT_DATE AS insert_dt ,
		CURRENT_DATE AS update_dt ,
		CONCAT(li.category_id,'_' , 'sa_sales_lithuania', '_', 'src_lithuania_sales' ) AS category_src_unique_id
	FROM
		sa_sales_lithuania.src_lithuania_sales AS li ) AS s2  
	-- To stop entering duplicate rows.
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_categories AS bc
		WHERE
			bc.category_name = COALESCE(s2.category_name  , 'n.a')
			AND bc.category_src_id = COALESCE(s2.category_src_id , '-1') 
			AND bc.source_system = 'sa_sales_lithuania'
			AND bc.source_system_entity = 'src_lithuania_sales' ) 
			
			ON CONFLICT (category_src_unique_id) DO UPDATE 
				SET category_name  = excluded.category_name ,
					update_dt = CURRENT_DATE;		
	-- To get number rows inserted
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	--Load data to logging table 
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_lithuania', v_text_message, v_inserted_date);

	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
		
END;
$$;





-- Procedure to insert values to product table from datasource
CREATE OR REPLACE PROCEDURE bl_cl.load_product_data_3nf() 
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_products';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_prod_id int := NULL ;
	v_new_start_dt DATE ;
	v_src_id VARCHAR;
	v_src_source VARCHAR ;
	v_error_message VARCHAR;
	
BEGIN 
	-- To impliment scd2 logic
	--create CTE to update products from source table and then get the updated row src id and poduct id to insert to new row.
	WITH update_products AS 
	(
			UPDATE   bl_3nf.ce_products_scd p     
			SET is_active = FALSE , 
				end_dt = CURRENT_DATE,
				update_dt  = CURRENT_DATE 
			WHERE EXISTS (SELECT 1 FROM bl_cl.incriment_latvia_products_mv s1
			WHERE s1.product_id  = p.product_src_id  AND (s1.product_name  != p.product_name  OR s1.brand  != p.brand 
			OR s1.package_type  != p.package_type)
			AND p.is_active = TRUE AND p.source_system = 'sa_sales_latvia' )
			RETURNING *
	)
	SELECT product_id , start_dt, product_src_id , source_system  INTO v_prod_id, v_new_start_dt, v_src_id, v_src_source FROM update_products;

	-- v_prod_id will not be null if update occuar. In that case need to insert new row.
	IF  v_prod_id IS  NOT NULL THEN 
	RAISE NOTICE 'Row updated';
		INSERT INTO bl_3nf.ce_products_scd (product_id, product_name , brand, package_type, category_id , product_src_id , 
		source_system , source_system_entity , is_active, start_dt, end_dt, insert_dt, update_dt) 
		SELECT v_prod_id, sls.product_name , sls.brand , sls.package_type ,cc.category_id , 
		sls.product_id::varchar, 'sa_sales_latvia', 'src_latvia_sales', TRUE, current_date, '9999-12-31'::date,
		current_date, '1900-01-01'::date 
		FROM bl_cl.incriment_latvia_products_mv sls 
		LEFT JOIN bl_3nf.ce_categories cc 
			ON sls.category_id  = cc.category_src_id 
			AND cc.source_system = 'sa_sales_latvia'
		--Get the new updated product id.
		WHERE sls.product_id  = v_src_id  AND 
		-- To update only changed product.
		 NOT  EXISTS (SELECT 1 FROM  bl_3nf.ce_products_scd cps 
			WHERE cps.product_src_id  = sls.product_id::varchar
			AND cps.is_active = FALSE 
			AND cps.product_name  = sls.product_name  
			AND cps.brand = sls.brand 
			AND cps.package_type = sls.package_type 
			AND cps.source_system =  v_src_source 
			) ;
		END IF ;
	-- Insert values in to products table for initial load and if added new products.
		INSERT INTO bl_3nf.ce_products_scd (product_id, product_name, brand, package_type, category_id, product_src_id, source_system, 
		source_system_entity, start_dt, end_dt, is_active, insert_dt, update_dt)
		SELECT nextval('prod_id_key_value'), s1.product_name, s1.brand, s1.package_type, s1.category_id, s1.product_src_id,
		s1.source_system, source_system_entity, start_dt, end_dt, is_active, insert_dt, update_dt
		FROM 
		(
			SELECT DISTINCT  COALESCE(lv.product_name, 'n.a') AS product_name, 
			COALESCE (lv.brand, 'n.a') AS brand,
			COALESCE (lv.package_type, 'n.a') AS package_type,
			COALESCE (cc.category_id, -1) AS category_id,
			COALESCE (lv.product_id, '-1') AS product_src_id,
			 'sa_sales_latvia' AS source_system,
			 'src_latvia_sales' AS source_system_entity,
			 CURRENT_DATE AS start_dt,
			 '9999-12-31'::date  AS end_dt ,
			 TRUE AS is_active,
			 CURRENT_DATE AS insert_dt,
			 '1900-01-01'::date AS update_dt
			FROM bl_cl.incriment_latvia_products_mv lv  
			LEFT JOIN bl_3nf.ce_categories cc 
			ON lv.category_id = cc.category_src_id 
			AND cc.source_system = 'sa_sales_latvia'
		) AS s1 
		WHERE NOT EXISTS ( SELECT 1 FROM bl_3nf.ce_products_scd  p1 
		WHERE  p1.product_src_id = s1.product_src_id
		AND p1.source_system = 'sa_sales_latvia'
		AND p1.source_system_entity = 'src_latvia_sales'
		);
	
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;

	-- Insert information to meta table
	UPDATE bl_cl.prm_mta_incrimental_load 
	SET previous_loaded_date = current_timestamp 
	WHERE source_table_name = 'sa_sales_latvia.src_latvia_sales'
	AND target_table_name = 'bl_3nf.ce_products'
	AND procedure_name = 'bl_cl.load_products_data_3nf()'  ;
	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia', v_text_message, v_inserted_date);

-- Load data from second data source 
	WITH update_products_2 AS 
	(
			UPDATE   bl_3nf.ce_products_scd p 
			SET is_active = FALSE , 
				end_dt = CURRENT_DATE,
				update_dt  = CURRENT_DATE 
			WHERE EXISTS (SELECT 1 FROM bl_cl.incriment_lithuania_products_mv s2
			WHERE s2.product_id  = p.product_src_id  AND (s2.product_name  != p.product_name  OR s2.brand  != p.brand 
			OR s2.package_type  != p.package_type)
			AND p.is_active = TRUE AND p.source_system = 'sa_sales_lithuania' )
			RETURNING * 
	)
SELECT product_id , start_dt, product_src_id ,source_system INTO v_prod_id, v_new_start_dt, v_src_id, v_src_source FROM update_products_2;

	-- v_prod_id will not be null if update occuar. In that case need to insert new row.
	IF  v_prod_id IS  NOT NULL THEN 
	RAISE NOTICE 'Row updated';
		INSERT INTO bl_3nf.ce_products_scd (product_id, product_name , brand, package_type,category_id , product_src_id , 
		source_system , source_system_entity ,
		is_active, start_dt, end_dt, insert_dt, update_dt) 
		SELECT v_prod_id, sls2.product_name , sls2.brand , sls2.package_type ,cc2.category_id ,
		sls2.product_id::varchar, 'sa_sales_lithuania', 'src_lithuania_sales',  TRUE, current_date, '9999-12-31'::date,
		current_date, '1900-01-01'::date 
		FROM bl_cl.incriment_lithuania_products_mv  sls2
		LEFT JOIN bl_3nf.ce_categories cc2
			ON sls2.category_id  = cc2.category_src_id 
			AND cc2.source_system = 'sa_sales_lithuania'
		--Get the new updated product id.
		WHERE sls2.product_id  = v_src_id AND  
		-- To update only changed product.
		 NOT  EXISTS (SELECT 1 FROM  bl_3nf.ce_products_scd cps2 
			WHERE cps2.product_src_id  = sls2.product_id::varchar
			AND cps2.is_active = FALSE 
			AND cps2.product_name  = sls2.product_name 
			AND cps2.brand = sls2.brand 
			AND cps2.package_type = sls2.package_type 
			AND cps2.source_system = v_src_source 
			) ;
		END IF ;
		
	-- Insert values in to products table for initial load and if added new products.
		INSERT INTO bl_3nf.ce_products_scd (product_id, product_name, brand, package_type, category_id, product_src_id, source_system, 
		source_system_entity, start_dt, end_dt, is_active, insert_dt, update_dt)
		SELECT nextval('prod_id_key_value'), s2.product_name, s2.brand, s2.package_type, s2.category_id, s2.product_src_id,
		s2.source_system, s2.source_system_entity, s2.start_dt, s2.end_dt, s2.is_active, s2.insert_dt, s2.update_dt
		FROM 
		( 
			SELECT DISTINCT  COALESCE(lt.product_name, 'n.a') AS product_name, 
			COALESCE (lt.brand, 'n.a') AS brand,
			COALESCE (lt.package_type, 'n.a') AS package_type,
			COALESCE (cc2.category_id, -1) AS category_id,
			COALESCE (lt.product_id, '-1') AS product_src_id,
			 'sa_sales_lithuania' AS source_system,
			 'src_lithuania_sales' AS source_system_entity,
			 CURRENT_DATE AS start_dt,
			 '9999-12-31'::date  AS end_dt ,
			 TRUE AS is_active,
			 CURRENT_DATE AS insert_dt,
			 '1900-01-01'::date AS update_dt
			FROM bl_cl.incriment_lithuania_products_mv  lt 
			LEFT JOIN bl_3nf.ce_categories cc2
			ON lt.category_id = cc2.category_src_id 
			AND cc2.source_system = 'sa_sales_lithuania'
		) AS s2 
		WHERE NOT EXISTS ( SELECT 1 FROM bl_3nf.ce_products_scd  p3 
		WHERE p3.product_src_id = s2.product_src_id
		AND p3.source_system = 'sa_sales_lithuania'     
		AND p3.source_system_entity = 'src_lithuania_sales'
		);

	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;

	-- Insert information to meta table
	UPDATE bl_cl.prm_mta_incrimental_load 
	SET previous_loaded_date = current_timestamp 
	WHERE source_table_name = 'sa_sales_lithuana.src_lithuania_sales'
	AND target_table_name = 'bl_3nf.ce_products'
	AND procedure_name = 'bl_cl.load_products_data_3nf()';
	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_lithuania', v_text_message, v_inserted_date);

	
	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
END;
$$;



--Create function to load data to shippers table from datasource.
CREATE OR REPLACE PROCEDURE  bl_cl.load_shippers_data_3nf() 
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_shippers';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR;
BEGIN 
		INSERT
		INTO
		bl_3nf.ce_shippers(
		shipper_id, 
		shipper_name,
		email,
		phone,
		shipper_src_id ,
		source_system ,
		source_system_entity ,
		insert_dt,
		update_dt,
		shipper_src_unique_id )
	SELECT
		nextval('shippers_id_key_value'), s1.shipper_name , s1.email , s1.phone , s1.shipper_src_id, s1.source_system, s1.source_system_entity,
		s1.insert_dt, s1.update_dt, s1.shipper_src_unique_id
		FROM 	
	(
	SELECT 	
		DISTINCT COALESCE(ss.shipper_name , 'n.a') AS shipper_name ,
		COALESCE(ss.email ,'n.a') AS email ,
		COALESCE(ss.phone , 'n.a') AS phone ,
		COALESCE(ss.shipper_id ,'-1') AS shipper_src_id ,
		'sa_sales_latvia' AS source_system,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt,
		'1900-01-01'::date AS update_dt,
		concat(ss.shipper_id, '_','sa_sales_latvia', '_', 'src_latvia_sales' ) AS shipper_src_unique_id
	FROM
		sa_sales_latvia.src_latvia_sales AS ss ) AS s1 
	-- To avoid duplicate entries
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_shippers AS bc
		WHERE
			bc.shipper_name = COALESCE(s1.shipper_name , 'n.a')
			AND  bc.email = COALESCE(s1.email ,'n.a')
			AND bc.phone = COALESCE(s1.phone , 'n.a')
			AND bc.shipper_src_id = COALESCE(s1.shipper_src_id ,'-1') 
			AND bc.source_system = 'sa_sales_latvia')
			-- Apply scd1 logic in case any updates in source file.
			ON CONFLICT (shipper_src_unique_id) DO UPDATE 
			SET shipper_name = excluded.shipper_name ,
			email = excluded.email, 
			phone = excluded.phone,
			update_dt = current_date;
		
			-- To get number rows inserted
			GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
			-- text message for loggining table			
			v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
		
		CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia', v_text_message, v_inserted_date);
		
	-- Insert values to shippers table from second datasource.
		INSERT
		INTO
		bl_3nf.ce_shippers(
		shipper_id, 
		shipper_name,
		email,
		phone,
		shipper_src_id ,
		source_system ,
		source_system_entity ,
		insert_dt,
		update_dt,
		shipper_src_unique_id )
	SELECT
		nextval('shippers_id_key_value'), s2.shipper_name , s2.email , s2.phone , s2.shipper_src_id, s2.source_system, s2.source_system_entity,
		s2.insert_dt, s2.update_dt, s2.shipper_src_unique_id
		FROM 	
	(
	SELECT 	
		DISTINCT COALESCE(ss.shipper_name , 'n.a') AS shipper_name ,   --- -
		COALESCE(ss.email ,'n.a') AS email ,
		COALESCE(ss.phone , 'n.a') AS phone ,
		COALESCE(ss.shipper_id ,'-1') AS shipper_src_id ,
		'sa_sales_lithuania' AS source_system,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt,
		'1900-01-01'::date AS update_dt,
		concat(ss.shipper_id, '_', 'sa_sales_lithuania' , '_', 'src_lithuania_sales') AS shipper_src_unique_id
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss ) AS s2
	-- To avoid duplicate entries
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_shippers AS bc
		WHERE
			bc.shipper_name = COALESCE(s2.shipper_name , 'n.a')
			AND  bc.email = COALESCE(s2.email ,'n.a')
			AND bc.phone = COALESCE(s2.phone , 'n.a')
			AND bc.shipper_src_id = COALESCE(s2.shipper_src_id ,'-1') 
			AND bc.source_system = 'sa_sales_lithuania')
			ON CONFLICT (shipper_src_unique_id) DO UPDATE 
			SET shipper_name = excluded.shipper_name ,
			email = excluded.email, 
			phone = excluded.phone,
			update_dt = current_date;
			-- To get number rows inserted
			GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
			-- text message for loggining table			
			v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
		
		CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_lithuania', v_text_message, v_inserted_date);	
		EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
END;
$$;




-- Function insert values to regions table from datasource and returns inserted table. 
CREATE OR REPLACE PROCEDURE  bl_cl.load_regions_data_3nf() 
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_regions';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR;
BEGIN 
	
	-- Region details in data set are spread among employees, customers, stores. So we nned to extarct distinct values of regions from 
	-- those each columns
	INSERT
		INTO
		bl_3nf.ce_regions(
		region_id, 
		region_name,
		region_src_id,
		source_system,
		source_system_entity,
		insert_dt, 
		update_dt,
		region_src_unique_id)
	SELECT
		nextval('region_id_key_value'), s1.region_name , s1.region_src_id , s1.source_system , s1.source_system_entity , s1.insert_dt ,
		s1.update_dt , s1.region_src_unique_id 
		FROM 
		(
		SELECT DISTINCT COALESCE(ss.cust_region , 'n.a') AS region_name , 
		COALESCE (ss.cust_region_id , '-1') AS region_src_id , 
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.cust_region_id,'_', 'sa_sales_latvia','_', 'src_latvia_sales' ) AS region_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss  
	-- To avoid duplicate entries.
	WHERE
		NOT EXISTS (
		SELECT
			1
		FROM
			bl_3nf.ce_regions AS cr
		WHERE
			cr.region_name = COALESCE(ss.cust_region  , 'n.a')
			AND cr.region_src_id = COALESCE (ss.cust_region_id  , '-1')
			AND cr.source_system = 'sa_sales_latvia'
			AND cr.source_system_entity = 'src_latvia_sales'
			)
	UNION  
	SELECT
		DISTINCT COALESCE(ss.emp_region, 'n.a') AS  region_name ,
		COALESCE(ss.emp_region_id , '-1') AS region_src_id ,
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.emp_region_id, '_', 'sa_sales_latvia', '_', 'src_latvia_sales') AS region_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	WHERE
		NOT EXISTS (
		SELECT
			1
		FROM
			bl_3nf.ce_regions AS cr
		WHERE
			cr.region_name = COALESCE(ss.emp_region, 'n.a')
			AND cr.region_src_id = COALESCE(ss.emp_region_id , '-1')
			AND cr.source_system = 'sa_sales_latvia'
			AND cr.source_system_entity = 'src_latvia_sales')
	UNION 
	SELECT
		DISTINCT COALESCE(ss.store_region , 'n.a') AS region_name ,
		COALESCE(ss.store_region_id , '-1') AS region_src_id , 
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.store_region_id,'_', 'sa_sales_latvia', '_',  'src_latvia_sales') AS region_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	WHERE
		NOT EXISTS (
		SELECT
			1
		FROM
			bl_3nf.ce_regions AS cr
		WHERE
			cr.region_name = COALESCE(ss.store_region , 'n.a')
			AND cr.region_src_id = COALESCE(ss.store_region_id , '-1')
			AND cr.source_system = 'sa_sales_latvia'
			AND cr.source_system_entity = 'src_latvia_sales')) s1 
			ON CONFLICT (region_src_unique_id) DO UPDATE 
			SET region_name = excluded.region_name,
				update_dt = current_date;
		-- To get number rows inserted
		GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
			-- text message for loggining table			
		v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
		
		CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia', v_text_message, v_inserted_date);
	
	-- Insert values to regions table from second datasource.
		INSERT
		INTO
		bl_3nf.ce_regions(
		region_id, 
		region_name,
		region_src_id,
		source_system,
		source_system_entity,
		insert_dt, 
		update_dt,
		region_src_unique_id)
	SELECT
		nextval('region_id_key_value'), s2.region_name , s2.region_src_id , s2.source_system , s2.source_system_entity , s2.insert_dt ,
		s2.update_dt , s2.region_src_unique_id 
		FROM 
		(
		SELECT DISTINCT COALESCE(ss.cust_region , 'n.a') AS region_name , 
		COALESCE (ss.cust_region_id , '-1') AS region_src_id , 
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.cust_region_id,'_', 'sa_sales_lithuania','_', 'src_lithuania_sales' ) AS region_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss  
	-- To avoid duplicate entries.
	WHERE
		NOT EXISTS (
		SELECT
			1
		FROM
			bl_3nf.ce_regions AS cr
		WHERE
			cr.region_name = COALESCE(ss.cust_region  , 'n.a')
			AND cr.region_src_id = COALESCE (ss.cust_region_id  , '-1')
			AND cr.source_system = 'sa_sales_lithuania'
			AND cr.source_system_entity = 'src_lithuania_sales'
			)
	UNION  
	SELECT
		DISTINCT COALESCE(ss.emp_region, 'n.a') AS  region_name ,
		COALESCE(ss.emp_region_id , '-1') AS region_src_id ,
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.emp_region_id, '_', 'sa_sales_lithuania', '_', 'src_lithuania_sales') AS region_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	WHERE
		NOT EXISTS (
		SELECT
			1
		FROM
			bl_3nf.ce_regions AS cr
		WHERE
			cr.region_name = COALESCE(ss.emp_region, 'n.a')
			AND cr.region_src_id = COALESCE(ss.emp_region_id , '-1')
			AND cr.source_system = 'sa_sales_lithuania'
			AND cr.source_system_entity = 'src_lithuania_sales')
	UNION 
	SELECT
		DISTINCT COALESCE(ss.store_region , 'n.a') AS region_name ,
		COALESCE(ss.store_region_id , '-1') AS region_src_id , 
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.store_region_id,'_', 'sa_sales_lithuania', '_',  'src_lithuania_sales') AS region_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	WHERE
		NOT EXISTS (
		SELECT
			1
		FROM
			bl_3nf.ce_regions AS cr
		WHERE
			cr.region_name = COALESCE(ss.store_region , 'n.a')
			AND cr.region_src_id = COALESCE(ss.store_region_id , '-1')
			AND cr.source_system = 'sa_sales_lithuania'
			AND cr.source_system_entity = 'src_lithuania_sales')) s2
			ON CONFLICT (region_src_unique_id) DO UPDATE 
			SET region_name = excluded.region_name,
				update_dt = current_date;
		-- To get number rows inserted
		GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
			-- text message for loggining table			
		v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
		
		CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_lithuania', v_text_message, v_inserted_date);
	
		EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
END;
$$;



-- procedure to insert values to country table from data source.
CREATE OR REPLACE PROCEDURE  bl_cl.load_countries_data_3nf() 
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_countries';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR;
BEGIN 
	
	-- Country details in data set are spread among employees, customers, stores. So we nned to extarct distinct values of countries from 
	-- those each columns
	INSERT
		INTO
		bl_3nf.ce_countries(
		country_id ,
		country_name,
		region_id,
		country_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		country_src_unique_id)
	SELECT
		nextval('countries_id_key_value'), s1.country_name , s1.region_id , s1.country_src_id , s1.source_system , s1.source_system_entity ,
		s1.insert_dt , s1.update_dt , s1.country_src_unique_id 
		FROM 
	(  SELECT 	DISTINCT COALESCE(ss.emp_country, 'n.a') AS country_name ,
		COALESCE (bc.region_id , -1) AS region_id ,
		COALESCE (ss.emp_country_id , 'n.a') AS country_src_id ,
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt,
		concat(ss.emp_country_id, '_', 'sa_sales_latvia', '_', 'src_latvia_sales') AS country_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	-- Join with region table to get region_ids of latvia_sale data(first data source)
	LEFT JOIN bl_3nf.ce_regions AS bc 
	ON
		ss.emp_region_id = bc.region_src_id
		AND bc.source_system = 'sa_sales_latvia'
	--To avoid duplicate entries.
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_countries AS ec
		WHERE
			ec.country_name = COALESCE(ss.emp_country, 'n.a')
			AND ec.country_src_id = COALESCE (ss.emp_country_id , 'n.a')
			AND ec.region_id = COALESCE (bc.region_id , -1)
			AND ec.source_system = 'sa_sales_latvia'
			AND ec.source_system_entity = 'src_latvia_sales')
	UNION 
	SELECT
		DISTINCT COALESCE(ss.cust_country , 'n.a') AS country_name ,
		COALESCE (bc.region_id , -1) AS region_id ,
		COALESCE (ss.cust_country_id , '-1') AS country_src_id ,
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.cust_country_id, '_', 'sa_sales_latvia', '_', 'src_latvia_sales') AS country_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	LEFT JOIN bl_3nf.ce_regions AS bc 
	ON
		ss.cust_region_id = bc.region_src_id
		AND bc.source_system = 'sa_sales_latvia'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_countries AS ec
		WHERE
			ec.country_name =  COALESCE(ss.cust_country , 'n.a')
			AND ec.country_src_id = COALESCE (ss.cust_country_id , '-1')
			AND ec.region_id = COALESCE (bc.region_id , -1)
			AND ec.source_system = 'sa_sales_latvia'
			AND ec.source_system_entity = 'src_latvia_sales')
	UNION 
	SELECT
		DISTINCT COALESCE(ss.store_country , 'n.a') AS country_name ,
		COALESCE(bc.region_id , -1) AS region_id , 
		COALESCE(ss.store_country_id , '-1') AS country_src_id ,
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.store_country_id, '_', 'sa_sales_latvia', '_', 'src_latvia_sales') AS country_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	INNER JOIN bl_3nf.ce_regions AS bc 
	ON
		ss.store_region_id = bc.region_src_id
		AND bc.source_system = 'sa_sales_latvia'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_countries AS ec
		WHERE
			ec.country_name = COALESCE(ss.store_country , 'n.a')
			AND ec.country_src_id = COALESCE(ss.store_country_id , '-1')
			AND ec.region_id = COALESCE(bc.region_id , -1)
			AND ec.source_system = 'sa_sales_latvia'
			AND ec.source_system_entity = 'src_latvia_sales' )) s1 
			ON CONFLICT (country_src_unique_id) DO UPDATE 
			SET country_name = excluded.country_name ,
				update_dt = current_date ;
	-- To get number rows inserted
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
				-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia', v_text_message, v_inserted_date);

	-- Insert values to country table from second datasource.
	INSERT
		INTO
		bl_3nf.ce_countries(
		country_id ,
		country_name,
		region_id,
		country_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		country_src_unique_id)
	SELECT
		nextval('countries_id_key_value'), s2.country_name , s2.region_id , s2.country_src_id , s2.source_system , s2.source_system_entity ,
		s2.insert_dt , s2.update_dt , s2.country_src_unique_id 
		FROM 
	(  SELECT 	DISTINCT COALESCE(ss.emp_country, 'n.a') AS country_name ,
		COALESCE (bc.region_id , -1) AS region_id ,
		COALESCE (ss.emp_country_id , 'n.a') AS country_src_id ,
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt,
		concat(ss.emp_country_id, '_', 'sa_sales_lithuania', '_', 'src_lithuania_sales') AS country_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	-- Join with region table to get region_ids of latvia_sale data(first data source)
	LEFT JOIN bl_3nf.ce_regions AS bc 
	ON
		ss.emp_region_id = bc.region_src_id
		AND bc.source_system = 'sa_sales_lithuania'
	--To avoid duplicate entries.
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_countries AS ec
		WHERE
			ec.country_name = COALESCE(ss.emp_country, 'n.a')
			AND ec.country_src_id = COALESCE (ss.emp_country_id , 'n.a')
			AND ec.region_id = COALESCE (bc.region_id , -1)
			AND ec.source_system = 'sa_sales_lithuania'
			AND ec.source_system_entity = 'src_lithuania_sales')
	UNION 
	SELECT
		DISTINCT COALESCE(ss.cust_country , 'n.a') AS country_name ,
		COALESCE (bc.region_id , -1) AS region_id ,
		COALESCE (ss.cust_country_id , '-1') AS country_src_id ,
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.cust_country_id, '_', 'sa_sales_lithuania', '_', 'src_lithuania_sales') AS country_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	LEFT JOIN bl_3nf.ce_regions AS bc 
	ON
		ss.cust_region_id = bc.region_src_id
		AND bc.source_system = 'sa_sales_lithuania'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_countries AS ec
		WHERE
			ec.country_name =  COALESCE(ss.cust_country , 'n.a')
			AND ec.country_src_id = COALESCE (ss.cust_country_id , '-1')
			AND ec.region_id = COALESCE (bc.region_id , -1)
			AND ec.source_system = 'sa_sales_lithuania'
			AND ec.source_system_entity = 'src_lithuania_sales')
	UNION 
	SELECT
		DISTINCT COALESCE(ss.store_country , 'n.a') AS country_name ,
		COALESCE(bc.region_id , -1) AS region_id , 
		COALESCE(ss.store_country_id , '-1') AS country_src_id ,
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.store_country_id, '_', 'sa_sales_lithuania', '_', 'src_lithuania_sales') AS country_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	INNER JOIN bl_3nf.ce_regions AS bc 
	ON
		ss.store_region_id = bc.region_src_id
		AND bc.source_system = 'sa_sales_lithuania'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_countries AS ec
		WHERE
			ec.country_name = COALESCE(ss.store_country , 'n.a')
			AND ec.country_src_id = COALESCE(ss.store_country_id , '-1')
			AND ec.region_id = COALESCE(bc.region_id , -1)
			AND ec.source_system = 'sa_sales_lithuania'
			AND ec.source_system_entity = 'src_lithuania_sales' )) s2 
			ON CONFLICT (country_src_unique_id) DO UPDATE 
			SET country_name = excluded.country_name ,
				update_dt = current_date ;
	-- To get number rows inserted
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
				-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_lithuania', v_text_message, v_inserted_date);
	
	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
END;
$$;



-- Function to insert into city table from data source
CREATE OR REPLACE PROCEDURE  bl_cl.load_cities_data_3nf() 
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_cities';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR;
BEGIN 
		INSERT
		INTO
		bl_3nf.ce_cities(
		city_id, 
		city_name,
		country_id,
		city_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		city_src_unique_id)
	SELECT
		nextval('cities_id_key_value'),s1.city_name , s1.country_id , s1.city_src_id , s1.source_system , s1.source_system_entity , s1.insert_dt ,
		s1.update_dt , s1.city_src_unique_id 
		FROM 
		( 
		SELECT DISTINCT COALESCE (ss.cust_city , 'n.a') AS city_name , 
		COALESCE(cc.country_id , -1) AS country_id ,
		COALESCE (ss.cust_city_id , '-1') AS city_src_id ,
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.cust_city_id, '_', 'sa_sales_latvia', '_', 'src_latvia_sales' ) AS city_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	LEFT JOIN bl_3nf.ce_countries AS cc 
	ON
		ss.cust_country_id = cc.country_src_id
		AND cc.source_system = 'sa_sales_latvia'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_cities AS cc2
		WHERE
			cc2.city_name =  COALESCE (ss.cust_city , 'n.a')
			--AND cc2.city_src_id = COALESCE (ss.cust_city_id , '-1')
			--AND cc2.country_id = COALESCE(cc.country_id , -1)
			AND cc2.source_system = 'sa_sales_latvia'
			AND cc2.source_system_entity = 'src_latvia_sales' )
	UNION 
	SELECT
		DISTINCT COALESCE (ss.emp_city , 'n.a') city_name ,
		COALESCE (cc.country_id , -1) AS country_id ,
		COALESCE (ss.emp_city_id , '-1') AS city_src_id ,
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.emp_city_id,'_', 'sa_sales_latvia', '_', 'src_latvia_sales' ) AS city_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	LEFT JOIN bl_3nf.ce_countries AS cc 
	ON
		ss.emp_country_id = cc.country_src_id
		AND cc.source_system = 'sa_sales_latvia'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_cities AS cc2
		WHERE
			cc2.city_name = COALESCE (ss.emp_city , 'n.a')
			--AND cc2.city_src_id = COALESCE (ss.emp_city_id , '-1')
			--AND cc2.country_id = COALESCE (cc.country_id , -1)
			AND cc2.source_system = 'sa_sales_latvia'
			AND cc2.source_system_entity = 'src_latvia_sales' )
	UNION 
	SELECT
		DISTINCT COALESCE (ss.store_city , 'n.a') AS city_name ,
		COALESCE (cc.country_id , -1) AS country_id , 
		COALESCE (ss.store_city_id , '-1') AS city_src_id , 
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.store_city_id, 'sa_sales_latvia', '_', 'src_latvia_sales') AS city_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	LEFT JOIN bl_3nf.ce_countries AS cc 
	ON
		ss.store_country_id = cc.country_src_id
		AND cc.source_system = 'sa_sales_latvia'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_cities AS cc2
		WHERE
			cc2.city_name =  COALESCE (ss.store_city , 'n.a')
			--AND cc2.city_src_id = COALESCE (ss.store_city_id , '-1')
			--AND cc2.country_id = COALESCE (cc.country_id , -1)
			AND cc2.source_system = 'sa_sales_latvia'
			AND cc2.source_system_entity = 'src_latvia_sales' )) s1 
			ON CONFLICT (city_src_unique_id) DO UPDATE 
			SET city_name = excluded.city_name,
				update_dt = current_date ;
		-- To get number rows inserted
		GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
					-- text message for loggining table			
		v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
		-- Load to logging table	
		CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia', v_text_message, v_inserted_date);

	-- Insert into city table from second data source
		INSERT
		INTO
		bl_3nf.ce_cities(
		city_id, 
		city_name,
		country_id,
		city_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		city_src_unique_id)
	SELECT
		nextval('cities_id_key_value'),s2.city_name , s2.country_id , s2.city_src_id , s2.source_system , s2.source_system_entity , s2.insert_dt ,
		s2.update_dt , s2.city_src_unique_id 
		FROM 
		( 
		SELECT DISTINCT COALESCE (ss.cust_city , 'n.a') AS city_name , 
		COALESCE(cc.country_id , -1) AS country_id ,
		COALESCE (ss.cust_city_id , '-1') AS city_src_id ,
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.cust_city_id, '_',  'sa_sales_lithuania', '_', 'src_lithuania_sales' ) AS city_src_unique_id 
	FROM
	 sa_sales_lithuania.src_lithuania_sales  AS ss
	LEFT JOIN bl_3nf.ce_countries AS cc 
	ON
		ss.cust_country_id = cc.country_src_id
		AND cc.source_system = 'sa_sales_lithuania'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_cities AS cc2
		WHERE
			cc2.city_name =  COALESCE (ss.cust_city , 'n.a')
			 AND cc2.city_src_id = COALESCE (ss.cust_city_id , '-1')
			AND cc2.country_id = COALESCE(cc.country_id , -1)
			AND cc2.source_system = 'sa_sales_lithuania'
			AND cc2.source_system_entity = 'src_lithuania_sales' )
	UNION 
	SELECT
		DISTINCT COALESCE (ss.emp_city , 'n.a') city_name ,
		COALESCE (cc.country_id , -1) AS country_id ,
		COALESCE (ss.emp_city_id , '-1') AS city_src_id ,
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.emp_city_id,'_', 'sa_sales_lithuania', '_', 'src_lithuania_sales') AS city_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	LEFT JOIN bl_3nf.ce_countries AS cc 
	ON
		ss.emp_country_id = cc.country_src_id
		AND cc.source_system = 'sa_sales_lithuania'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_cities AS cc2
		WHERE
			cc2.city_name = COALESCE (ss.emp_city , 'n.a')
			AND cc2.city_src_id = COALESCE (ss.emp_city_id , '-1')
			--AND cc2.country_id = COALESCE (cc.country_id , -1)
			AND cc2.source_system = 'sa_sales_lithuania'
			AND cc2.source_system_entity = 'src_lithuania_sales' )
	UNION 
	SELECT
		DISTINCT COALESCE (ss.store_city , 'n.a') AS city_name ,
		COALESCE (cc.country_id , -1) AS country_id , 
		COALESCE (ss.store_city_id , '-1') AS city_src_id , 
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.store_city_id, 'sa_sales_lithuania', '_', 'src_lithuania_sales') AS city_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	LEFT JOIN bl_3nf.ce_countries AS cc 
	ON
		ss.store_country_id = cc.country_src_id
		AND cc.source_system = 'sa_sales_lithuania'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_cities AS cc2
		WHERE
			cc2.city_name =  COALESCE (ss.store_city , 'n.a')
			 AND cc2.city_src_id = COALESCE (ss.store_city_id , '-1')
			--AND cc2.country_id = COALESCE (cc.country_id , -1)
			AND cc2.source_system = 'sa_sales_lithuania'
			AND cc2.source_system_entity = 'src_lithuania_sales' )) s2
						ON CONFLICT (city_src_unique_id) DO UPDATE 
			SET city_name = excluded.city_name,
				update_dt = current_date ;
		-- To get number rows inserted
		GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
					-- text message for loggining table			
		v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
		-- Load to logging table	
		CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_lithuania', v_text_message, v_inserted_date);
	
		EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
END;
$$;




-- Function to insert values to addresses column from data source
CREATE OR REPLACE PROCEDURE  bl_cl.load_addresses_data_3nf() 
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_addresses';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR;
BEGIN 
	
		INSERT
		INTO
		bl_3nf.ce_addresses(
		address_id ,
		address,
		zipcode,
		city_id,
		address_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt, 
		address_src_unique_id)	
	SELECT
		nextval('addresses_id_key_value'),s1.address , s1.zipcode , s1.city_id , s1.address_src_id , s1.source_system , s1.source_system_entity ,
		s1.insert_dt , s1.update_dt , s1.address_src_unique_id 
		FROM 
	(
	SELECT 	
		DISTINCT COALESCE (ss.emp_address , 'n.a') AS address ,
		COALESCE (ss.emp_zipcode , 'n.a') AS zipcode ,
		COALESCE (bcc.city_id , -1) AS city_id ,
		COALESCE (ss.emp_address_id ,'-1') AS address_src_id ,
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.emp_address_id,'_',bcc.city_id , 'sa_sales_latvia','_', 'src_latvia_sales') AS address_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	LEFT  JOIN bl_3nf.ce_cities AS bcc 
	ON
		ss.emp_city_id = bcc.city_src_id
		AND bcc.source_system = 'sa_sales_latvia'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_addresses AS ca
		WHERE
			ca.address = COALESCE (ss.emp_address , 'n.a')
			AND ca.address_src_id = COALESCE (ss.emp_address_id ,'-1')
			AND ca.zipcode = COALESCE (ss.emp_zipcode , 'n.a')
			AND ca.city_id = COALESCE (bcc.city_id , -1)
			AND ca.source_system = 'sa_sales_latvia'
			AND ca.source_system_entity = 'src_latvia_sales'
	)
	UNION 
	SELECT
		DISTINCT COALESCE (ss.cust_address , 'n.a') AS address ,
		COALESCE (ss.cust_zipcode , 'n.a') AS zipcode ,
		COALESCE (bcc.city_id , -1) AS city_id ,
		COALESCE (ss.cust_address_id ,'-1') AS address_src_id , 
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt, 
		'1900-01-01'::date AS update_dt ,
		concat(ss.cust_address_id,'_',bcc.city_id , 'sa_sales_latvia','_', 'src_latvia_sales') AS address_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	LEFT JOIN bl_3nf.ce_cities AS bcc 
	ON
		ss.cust_city_id = bcc.city_src_id
		AND bcc.source_system = 'sa_sales_latvia'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_addresses AS ca
		WHERE
			ca.address =  COALESCE (ss.cust_address , 'n.a')
			AND ca.address_src_id = COALESCE (ss.cust_address_id ,'-1')
			AND ca.zipcode = COALESCE (ss.cust_zipcode , 'n.a')
			AND ca.city_id = COALESCE (bcc.city_id , -1) 
			AND ca.source_system = 'sa_sales_latvia'
			AND ca.source_system_entity = 'src_latvia_sales'
	)
	UNION 
	SELECT
		DISTINCT COALESCE (ss.store_address , 'n.a') AS address ,
		COALESCE (ss.store_zipcode , 'n.a') AS zipcode ,
		COALESCE (bcc.city_id , -1) AS city_id ,
		COALESCE (ss.store_address_id , '-1') AS address_src_id ,
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt, 
		'1900-01-01'::date AS update_dt ,
		concat(ss.store_address_id,'_',bcc.city_id, 'sa_sales_latvia','_', 'src_latvia_sales') AS address_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	LEFT  JOIN bl_3nf.ce_cities AS bcc 
	ON
		ss.store_city_id = bcc.city_src_id
		AND bcc.source_system = 'sa_sales_latvia'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_addresses AS ca
		WHERE
			ca.address = COALESCE (ss.store_address , 'n.a')
			AND ca.address_src_id = COALESCE (ss.store_address_id , '-1')
			AND ca.zipcode = COALESCE (ss.store_zipcode , 'n.a')
			AND ca.city_id = COALESCE (bcc.city_id , -1) 
			AND ca.source_system = 'sa_sales_latvia'
			AND ca.source_system_entity = 'src_latvia_sales')) s1 
			ON CONFLICT (address_src_unique_id) DO UPDATE 
			SET address = excluded.address,
				zipcode = excluded.zipcode,
				update_dt = current_date ;
				

		GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
					-- text message for loggining table			
		v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
		-- Load to logging table	
		CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia', v_text_message, v_inserted_date);
	
	
--Insert values to addresses column from second data source
		INSERT
		INTO
		bl_3nf.ce_addresses(
		address_id ,
		address,
		zipcode,
		city_id,
		address_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt, 
		address_src_unique_id)	
	SELECT
		nextval('addresses_id_key_value'),s2.address , s2.zipcode , s2.city_id , s2.address_src_id , s2.source_system , s2.source_system_entity ,
		s2.insert_dt , s2.update_dt , s2.address_src_unique_id 
		FROM 
	(
	SELECT 	
		DISTINCT COALESCE (ss.emp_address , 'n.a') AS address ,
		COALESCE (ss.emp_zipcode , 'n.a') AS zipcode ,
		COALESCE (bcc.city_id , -1) AS city_id ,
		COALESCE (ss.emp_address_id ,'-1') AS address_src_id ,
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.emp_address_id,'_',bcc.city_id , 'sa_sales_lithuania','_', 'src_lithuania_sales') AS address_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	LEFT  JOIN bl_3nf.ce_cities AS bcc 
	ON
		ss.emp_city_id = bcc.city_src_id
		AND bcc.source_system = 'sa_sales_lithuania'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_addresses AS ca
		WHERE
			ca.address = COALESCE (ss.emp_address , 'n.a')
			AND ca.address_src_id = COALESCE (ss.emp_address_id ,'-1')
			AND ca.zipcode = COALESCE (ss.emp_zipcode , 'n.a')
			AND ca.city_id = COALESCE (bcc.city_id , -1)
			AND ca.source_system = 'sa_sales_lithuania'
			AND ca.source_system_entity = 'src_lithuania_sales'
	)
	UNION 
	SELECT
		DISTINCT COALESCE (ss.cust_address , 'n.a') AS address ,
		COALESCE (ss.cust_zipcode , 'n.a') AS zipcode ,
		COALESCE (bcc.city_id , -1) AS city_id ,
		COALESCE (ss.cust_address_id ,'-1') AS address_src_id , 
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.cust_address_id,'_',bcc.city_id , 'sa_sales_lithuania','_', 'src_lithuania_sales') AS address_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	LEFT JOIN bl_3nf.ce_cities AS bcc 
	ON
		ss.cust_city_id = bcc.city_src_id
		AND bcc.source_system = 'sa_sales_lithuania'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_addresses AS ca
		WHERE
			ca.address =  COALESCE (ss.cust_address , 'n.a')
			AND ca.address_src_id = COALESCE (ss.cust_address_id ,'-1')
			AND ca.zipcode = COALESCE (ss.cust_zipcode , 'n.a')
			AND ca.city_id = COALESCE (bcc.city_id , -1) 
			AND ca.source_system = 'sa_sales_lithuania'
			AND ca.source_system_entity = 'src_lithuania_sales'
	)
	UNION 
	SELECT
		DISTINCT COALESCE (ss.store_address , 'n.a') AS address ,
		COALESCE (ss.store_zipcode , 'n.a') AS zipcode ,
		COALESCE (bcc.city_id , -1) AS city_id ,
		COALESCE (ss.store_address_id , '-1') AS address_src_id ,
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.store_address_id,'_',bcc.city_id , 'sa_sales_lithuania','_', 'src_lithuania_sales') AS address_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	LEFT  JOIN bl_3nf.ce_cities AS bcc 
	ON
		ss.store_city_id = bcc.city_src_id
		AND bcc.source_system = 'sa_sales_lithuania'
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_addresses AS ca
		WHERE
			ca.address = COALESCE (ss.store_address , 'n.a')
			AND ca.address_src_id = COALESCE (ss.store_address_id , '-1')
			AND ca.zipcode = COALESCE (ss.store_zipcode , 'n.a')
			AND ca.city_id = COALESCE (bcc.city_id , -1) 
			AND ca.source_system = 'sa_sales_lithuania'
			AND ca.source_system_entity = 'src_lithuania_sales')) s2
			ON CONFLICT (address_src_unique_id) DO UPDATE 
			SET address = excluded.address,
				zipcode = excluded.zipcode,
				update_dt = current_date ;
				

		GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
					-- text message for loggining table			
		v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
		-- Load to logging table	
		CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_lithuania', v_text_message, v_inserted_date);	
	
		
		EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
END;
$$;



-- Function to insert values into customers table fromdata source
CREATE OR REPLACE PROCEDURE  bl_cl.load_customers_data_3nf() 
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_shippers';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR;
BEGIN 
		INSERT
		INTO
		bl_3nf.ce_customers(
		customer_id, 
		first_name,
		last_name,
		email,
		phone,
		gender,
		address_id,
		date_of_birth,
		customer_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		customer_src_unique_id)
	SELECT
	nextval('cust_id_key_value'), s1.first_name , s1.last_name , s1.email , s1.phone , s1.gender , s1.address_id , s1.date_of_birth ,
	s1.customer_src_id , s1.source_system , s1.source_system_entity , s1.insert_dt , s1.update_dt , s1.customer_src_unique_id 
	FROM 
	( SELECT DISTINCT COALESCE (ss.cust_first_name , 'n.a') AS first_name ,
		COALESCE (ss.cust_last_name , 'n.a') AS last_name ,
		COALESCE (ss.cust_email , 'n.a') AS email ,
		COALESCE (ss.cust_phone , 'n.a') AS phone ,
		COALESCE (ss.cust_gender, 'n.a') AS gender ,
		COALESCE (ca2.address_id , -1) AS address_id ,
		COALESCE (ss.cust_date_of_birth , '1900-1-1') AS date_of_birth ,
		COALESCE (ss.cust_id , '-1') AS customer_src_id , 
		'sa_sales_latvia' source_system ,
		'src_latvia_sales' source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.cust_id ,'_', 'sa_sales_latvia' ,'_', 'src_latvia_sales' ) AS customer_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	LEFT JOIN bl_3nf.ce_addresses AS ca2
	ON
		ss.cust_address_id = ca2.address_src_id
		AND ca2.source_system = 'sa_sales_latvia'
	-- To avoid duplicate entries
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_customers AS ccu
		WHERE
			ccu.first_name = COALESCE (ss.cust_first_name , 'n.a')
			AND ccu.last_name = COALESCE (ss.cust_last_name , 'n.a')
			AND ccu.email = COALESCE (ss.cust_email , 'n.a')
			AND ccu.phone = COALESCE (ss.cust_phone , 'n.a')
			AND ccu.gender = COALESCE (ss.cust_gender, 'n.a')
			AND ccu.date_of_birth = COALESCE (ss.cust_date_of_birth , '1900-1-1')
			AND ccu.customer_src_id = COALESCE (ss.cust_id , '-1')
			AND ccu.source_system = 'sa_sales_latvia'
			AND ccu.source_system_entity = 'src_latvia_sales')) s1 
			ON CONFLICT (customer_src_unique_id) DO UPDATE 
			SET first_name = excluded.first_name,
				last_name = excluded.last_name,
				email = excluded.email,
				phone = excluded.phone,
				gender = excluded.gender,
				update_dt = current_date;
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia', v_text_message, v_inserted_date);
	
	-- Insert values into customers table from second data source
	INSERT
		INTO
		bl_3nf.ce_customers(
		customer_id, 
		first_name,
		last_name,
		email,
		phone,
		gender,
		address_id,
		date_of_birth,
		customer_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		customer_src_unique_id)
	SELECT
	nextval('cust_id_key_value'), s2.first_name , s2.last_name , s2.email , s2.phone , s2.gender , s2.address_id , s2.date_of_birth ,
	s2.customer_src_id , s2.source_system , s2.source_system_entity , s2.insert_dt , s2.update_dt , s2.customer_src_unique_id 
	FROM 
	( SELECT DISTINCT COALESCE (ss.cust_first_name , 'n.a') AS first_name ,
		COALESCE (ss.cust_last_name , 'n.a') AS last_name ,
		COALESCE (ss.cust_email , 'n.a') AS email ,
		COALESCE (ss.cust_phone , 'n.a') AS phone ,
		COALESCE (ss.cust_gender, 'n.a') AS gender ,
		COALESCE (ca2.address_id , -1) AS address_id ,
		COALESCE (ss.cust_date_of_birth , '1900-1-1') AS date_of_birth ,
		COALESCE (ss.cust_id , '-1') AS customer_src_id , 
		'sa_sales_lithuania' source_system ,
		'src_lithuania_sales' source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.cust_id ,'_', 'sa_sales_lithuania' ,'_', 'src_lithuania_sales' ) AS customer_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	LEFT JOIN bl_3nf.ce_addresses AS ca2
	ON
		ss.cust_address_id = ca2.address_src_id
		AND ca2.source_system = 'sa_sales_lithuania'
	-- To avoid duplicate entries
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_customers AS ccu
		WHERE
			ccu.first_name = COALESCE (ss.cust_first_name , 'n.a')
			AND ccu.last_name = COALESCE (ss.cust_last_name , 'n.a')
			AND ccu.email = COALESCE (ss.cust_email , 'n.a')
			AND ccu.phone = COALESCE (ss.cust_phone , 'n.a')
			AND ccu.gender = COALESCE (ss.cust_gender, 'n.a')
			AND ccu.date_of_birth = COALESCE (ss.cust_date_of_birth , '1900-1-1')
			AND ccu.customer_src_id = COALESCE (ss.cust_id , '-1')
			AND ccu.source_system = 'sa_sales_lithuania'
			AND ccu.source_system_entity = 'src_lithuania_sales')) s2
			ON CONFLICT (customer_src_unique_id) DO UPDATE 
			SET first_name = excluded.first_name,
				last_name = excluded.last_name,
				email = excluded.email,
				phone = excluded.phone,
				gender = excluded.gender,
				update_dt = current_date; 
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_lithuania', v_text_message, v_inserted_date);

		
	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
		
END;
$$;
   


-- Function to insert into departments table from data source
CREATE OR REPLACE PROCEDURE  bl_cl.load_departments_data_3nf()  
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_departments';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR;
BEGIN
		INSERT
		INTO
		bl_3nf.ce_departments(
		department_id, 
		department_name,
		department_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		department_src_unique_id)
	SELECT
	nextval('departments_id_key_value'), s1.department_name , s1.department_src_id , s1.source_system , s1.source_system_entity ,
	s1.insert_dt , s1.update_dt , s1.department_src_unique_id 
	FROM 
	( SELECT
		DISTINCT COALESCE (ss.emp_department ,'n.a') AS department_name ,
		COALESCE (ss.emp_department_id, '-1') AS department_src_id ,
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.emp_department_id, '_' , 'sa_sales_latvia', '_', 'src_latvia_sales') AS department_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_departments AS cd
		WHERE
			cd.department_name = COALESCE (ss.emp_department ,'n.a')
			AND cd.department_src_id = COALESCE (ss.emp_department_id, '-1')
			AND cd.source_system = 'sa_sales_latvia'
			AND cd.source_system_entity = 'src_latvia_sales'))s1
			ON CONFLICT (department_src_unique_id) DO UPDATE 
			SET department_name = excluded.department_name,
				update_dt = current_date;
	-- Get number of inserted rows
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia', v_text_message, v_inserted_date);

	--Insert into departments table from second data source
			INSERT
		INTO
		bl_3nf.ce_departments(
		department_id, 
		department_name,
		department_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		department_src_unique_id)
	SELECT
	nextval('departments_id_key_value'), s2.department_name , s2.department_src_id , s2.source_system , s2.source_system_entity ,
	s2.insert_dt , s2.update_dt , s2.department_src_unique_id 
	FROM 
	( SELECT
		DISTINCT COALESCE (ss.emp_department ,'n.a') AS department_name ,  
		COALESCE (ss.emp_department_id, '-1') AS department_src_id ,
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.emp_department_id, '_' , 'sa_sales_lithuania', '_', 'src_lithuania_sales') AS department_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_departments AS cd
		WHERE
			cd.department_name = COALESCE (ss.emp_department ,'n.a')
			AND cd.department_src_id = COALESCE (ss.emp_department_id, '-1')
			AND cd.source_system = 'sa_sales_lithuania'
			AND cd.source_system_entity = 'src_lithuania_sales'))s2
			ON CONFLICT (department_src_unique_id) DO UPDATE 
			SET department_name = excluded.department_name,
				update_dt = current_date;
	-- Get number of inserted rows
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_lithuania', v_text_message, v_inserted_date);

	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
END ;
$$;



-- Function to insert values into payments table from data saource
CREATE OR REPLACE PROCEDURE  bl_cl.load_payments_data_3nf() 
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_payments';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR;
BEGIN
		INSERT
		INTO
		bl_3nf.ce_payment_types(
		payment_type_id ,
		payment_type_name,
		payment_type_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		paymentsrc_src_unique_id)
	SELECT
		nextval('paymnet_id_key_value'), s1.payment_type_name , s1.payment_type_src_id , s1.source_system ,
		s1.source_system_entity , s1.insert_dt , s1.update_dt , s1.paymentsrc_src_unique_id 
		FROM 
	(	
		SELECT 
		DISTINCT COALESCE (ss.payment_type, 'n.a') AS payment_type_name ,
		COALESCE (ss.payment_type_id, 'n.a') AS payment_type_src_id ,
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.payment_type_id, '_', 'sa_sales_latvia', '_', 'src_latvia_sales') AS paymentsrc_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_payment_types AS cpt
		WHERE
			cpt.payment_type_name = COALESCE (ss.payment_type, 'n.a')
			AND cpt.payment_type_src_id = COALESCE (ss.payment_type_id, 'n.a')
			AND cpt.source_system = 'sa_sales_latvia'
			AND cpt.source_system_entity = 'src_latvia_sales' )) s1 
			ON CONFLICT (paymentsrc_src_unique_id) DO UPDATE 
			SET payment_type_name = excluded.payment_type_name,
				update_dt = current_date;
			-- Get number of inserted rows
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia', v_text_message, v_inserted_date);
	
	--Insert values into payments table from second data saource
		INSERT
		INTO
		bl_3nf.ce_payment_types(
		payment_type_id ,
		payment_type_name,
		payment_type_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		paymentsrc_src_unique_id)
	SELECT
		nextval('paymnet_id_key_value'), s2.payment_type_name , s2.payment_type_src_id , s2.source_system ,
		s2.source_system_entity , s2.insert_dt , s2.update_dt , s2.paymentsrc_src_unique_id 
		FROM 
	(	
		SELECT 
		DISTINCT COALESCE (ss.payment_type, 'n.a') AS payment_type_name ,
		COALESCE (ss.payment_type_id, 'n.a') AS payment_type_src_id ,
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.payment_type_id, '_', 'sa_sales_lithuania', '_', 'src_lithuania_sales') AS paymentsrc_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_payment_types AS cpt
		WHERE
			cpt.payment_type_name = COALESCE (ss.payment_type, 'n.a')
			AND cpt.payment_type_src_id = COALESCE (ss.payment_type_id, 'n.a')
			AND cpt.source_system = 'sa_sales_lithuania'
			AND cpt.source_system_entity = 'src_lithuania_sales' )) s2 
			ON CONFLICT (paymentsrc_src_unique_id) DO UPDATE 
			SET payment_type_name = excluded.payment_type_name,
				update_dt = current_date;
			-- Get number of inserted rows
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_lithuania', v_text_message, v_inserted_date);

	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
END;
$$;


-- Procedure to insert values to channel types table from data source
CREATE OR REPLACE PROCEDURE  bl_cl.load_channels_data_3nf() 
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_channels';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR ;
BEGIN
	INSERT
		INTO
		bl_3nf.ce_channel_types(
		channel_type_id,
		channel_type_name,
		channel_type_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		channel_src_unique_id)
	SELECT
		nextval('channel_id_key_value'), s1.channel_type_name , s1.channel_type_src_id , s1.source_system , s1.source_system_entity ,
		s1.insert_dt , s1.update_dt , s1.channel_src_unique_id 
		FROM 
	(SELECT 	
		DISTINCT COALESCE (ss.channel_type , 'n.a') AS channel_type_name ,
		COALESCE (ss.channel_type_id , 'n.a') AS channel_type_src_id ,
		'sa_sales_latvia' AS  source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		 concat(ss.channel_type_id, '_', 'sa_sales_latvia', '_', 'src_latvia_sales')   AS channel_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_channel_types AS cct
		WHERE
			cct.channel_type_name = COALESCE (ss.channel_type , 'n.a')
			AND cct.channel_type_src_id = COALESCE (ss.channel_type_id , 'n.a')
			AND cct.source_system = 'sa_sales_latvia'
			AND cct.source_system_entity = 'src_latvia_sales')) s1 
			ON CONFLICT (channel_src_unique_id) DO UPDATE 
			SET channel_type_name = excluded.channel_type_name,
				update_dt = current_date ;
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia', v_text_message, v_inserted_date);

	-- Load from second data source 
	INSERT
		INTO
		bl_3nf.ce_channel_types(
		channel_type_id,
		channel_type_name,
		channel_type_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		channel_src_unique_id)
	SELECT
		nextval('channel_id_key_value'), s2.channel_type_name , s2.channel_type_src_id , s2.source_system , s2.source_system_entity ,
		s2.insert_dt , s2.update_dt , s2.channel_src_unique_id 
		FROM 
	(SELECT 	
		DISTINCT COALESCE (ss.channel_type , 'n.a') AS channel_type_name ,
		COALESCE (ss.channel_type_id , 'n.a') AS channel_type_src_id ,
		'sa_sales_lithuania' AS  source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		 concat(ss.channel_type_id, '_', 'sa_sales_lithuania', '_', 'src_lithuania_sales')   AS channel_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_channel_types AS cct
		WHERE
			cct.channel_type_name = COALESCE (ss.channel_type , 'n.a')
			AND cct.channel_type_src_id = COALESCE (ss.channel_type_id , 'n.a')
			AND cct.source_system = 'sa_sales_lithuania'
			AND cct.source_system_entity = 'src_lithuania_sales')) s2
			ON CONFLICT (channel_src_unique_id) DO UPDATE 
			SET channel_type_name = excluded.channel_type_name,
				update_dt = current_date ;
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_lithuania', v_text_message, v_inserted_date);

	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);

	END;
$$;



--Procedure to insert values into stores from data source
CREATE OR REPLACE PROCEDURE  bl_cl.load_stores_data_3nf() 
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_stores';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR;
BEGIN	
		INSERT
		INTO
		bl_3nf.ce_stores(
		store_id,
		store_name,
		manager_first_name,
		manager_last_name,
		store_email ,
		address_id,
		store_src_id ,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		store_src_unique_id)
	SELECT
		nextval('store_id_key_value'), s1.store_name , s1.manager_first_name , s1.manager_last_name , s1.store_email , s1.address_id ,
		s1.store_src_id , s1.source_system , s1.source_system_entity , s1.insert_dt , s1.update_dt , s1.store_src_unique_id 
		FROM 
		(
		SELECT
		DISTINCT COALESCE (ss.store_name , 'n.a') AS store_name ,
		COALESCE (ss.manager_first_name , 'n.a') AS manager_first_name ,
		COALESCE (ss.manager_last_name , 'n.a') AS manager_last_name ,
		COALESCE (ss.store_email, 'n.a') AS store_email ,
		COALESCE (ced.address_id , -1) AS address_id ,
		COALESCE (ss.store_id , '-1') AS store_src_id ,
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.store_id , '_', 'sa_sales_latvia', '_', 'src_latvia_sales' ) AS store_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales  AS ss
	LEFT JOIN bl_3nf.ce_addresses AS ced 
	ON
		ced.address_src_id = ss.store_address_id
		AND ced.source_system = 'sa_sales_latvia'
		AND ced.source_system_entity = 'src_latvia_sales'
	WHERE
		NOT EXISTS (
		SELECT
			1
		FROM
			bl_3nf.ce_stores AS ces1
		WHERE
			ces1.store_name = COALESCE (ss.store_name , 'n.a')
			AND ces1.store_src_id = COALESCE (ss.store_id , '-1')
			AND ces1.manager_first_name = COALESCE (ss.manager_first_name , 'n.a')
			AND ces1.manager_last_name = COALESCE (ss.manager_last_name , 'n.a')
			AND ces1.store_email = COALESCE (ss.store_email, 'n.a')
			AND ces1.address_id = COALESCE (ced.address_id , -1)
			AND ces1.source_system = 'sa_sales_latvia'
			AND ces1.source_system_entity = 'src_latvia_sales')) s1 
			ON CONFLICT (store_src_unique_id) DO UPDATE 
			SET store_name = excluded.store_name,
				manager_first_name = excluded.manager_first_name,
				manager_last_name = excluded.manager_last_name,
				store_email = excluded.store_email,
				update_dt = current_date;
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name,  'sa_sales_latvia', v_text_message, v_inserted_date);


	--Load from second data source
		INSERT
		INTO
		bl_3nf.ce_stores(
		store_id,
		store_name,
		manager_first_name,
		manager_last_name,
		store_email ,
		address_id,
		store_src_id ,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		store_src_unique_id)
	SELECT
		nextval('store_id_key_value'), s2.store_name , s2.manager_first_name , s2.manager_last_name , s2.store_email , s2.address_id ,
		s2.store_src_id , s2.source_system , s2.source_system_entity , s2.insert_dt , s2.update_dt , s2.store_src_unique_id 
		FROM 
		(SELECT
		DISTINCT COALESCE (ss2.store_name , 'n.a') AS store_name ,
		COALESCE (ss2.manager_first_name , 'n.a') AS manager_first_name ,
		COALESCE (ss2.manager_last_name , 'n.a') AS manager_last_name ,
		COALESCE (ss2.store_email, 'n.a') AS store_email ,
		COALESCE (ced.address_id , -1) AS address_id ,
		COALESCE (ss2.store_id , '-1') AS store_src_id ,
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss2.store_id , '_', 'sa_sales_lithuania', '_', 'src_lithuania_sales' ) AS store_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss2
	LEFT JOIN bl_3nf.ce_addresses AS ced 
	ON
		ced.address_src_id = ss2.store_address_id
		AND ced.source_system = 'sa_sales_lithuania'
		AND ced.source_system_entity = 'src_lithuania_sales'
	WHERE
		NOT EXISTS (
		SELECT
			1
		FROM
			bl_3nf.ce_stores AS ces1
		WHERE
			ces1.store_name = COALESCE (ss2.store_name , 'n.a')
			AND ces1.store_src_id = COALESCE (ss2.store_id , '-1')
			AND ces1.manager_first_name = COALESCE (ss2.manager_first_name , 'n.a')
			AND ces1.manager_last_name = COALESCE (ss2.manager_last_name , 'n.a')
			AND ces1.store_email = COALESCE (ss2.store_email, 'n.a')
			AND ces1.address_id = COALESCE (ced.address_id , -1)
			AND ces1.source_system = 'sa_sales_lithuania'
			AND ces1.source_system_entity = 'src_lithuania_sales')) s2
			ON CONFLICT (store_src_unique_id) DO UPDATE 
			SET store_name = excluded.store_name,
				manager_first_name = excluded.manager_first_name,
				manager_last_name = excluded.manager_last_name,
				store_email = excluded.store_email,
				update_dt = current_date;
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name,  'sa_sales_lithuania', v_text_message, v_inserted_date);


	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
END;
$$;



-- Procedure to insert data into employees table from data source 
CREATE OR REPLACE PROCEDURE  bl_cl.load_employees_data_3nf() 
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_employees';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	v_error_message VARCHAR;
BEGIN
		
		INSERT
		INTO
		bl_3nf.ce_employees(
		employee_id,
		first_name,
		last_name,
		email,
		phone,
		gender,
		address_id,
		date_of_birth,
		department_id,
		salary,
		employee_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		employee_src_unique_id)
	SELECT
		nextval('emp_id_key_value'), s1.first_name , s1.last_name , s1.email , s1.phone ,s1.gender , s1.address_id , s1.date_of_birth ,
		s1.department_id , s1.salary , s1.employee_src_id , s1.source_system , s1.source_system_entity , s1.insert_dt , s1.update_dt ,
		s1.employee_src_unique_id 
		FROM 
		(SELECT 	
		DISTINCT COALESCE (ss.emp_first_name, 'n.a') AS first_name ,
		COALESCE (ss.emp_last_name , 'n.a') AS last_name ,
		COALESCE (ss.emp_email , 'n.a') AS email ,
		COALESCE (ss.emp_phone, 'n.a') AS phone ,
		COALESCE (ss.emp_gender , 'n.a') AS gender ,
		COALESCE (cad.address_id , -1) AS address_id ,
		COALESCE (ss.emp_date_of_birth , '1900-1-1') AS date_of_birth ,
		COALESCE (cde.department_id , -1) AS department_id ,
		COALESCE (ss.emp_salary, '-1') AS salary ,
		COALESCE (ss.emp_id , '-1') AS employee_src_id ,
		'sa_sales_latvia' AS source_system ,
		'src_latvia_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.emp_id , '_' , 'sa_sales_latvia', '_', 'src_latvia_sales') AS employee_src_unique_id 
	FROM
		sa_sales_latvia.src_latvia_sales AS ss
	LEFT JOIN bl_3nf.ce_addresses AS cad 
	ON
		cad.address_src_id = ss.emp_address_id
		AND cad.source_system = 'sa_sales_latvia'
	LEFT JOIN bl_3nf.ce_departments AS cde 
	ON
		cde.department_src_id = ss.emp_department_id
		AND cde.source_system = 'sa_sales_latvia'
	-- To avoid duplicate entries.
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_employees AS cem
		WHERE
			cem.first_name = COALESCE (ss.emp_first_name, 'n.a')
			AND cem.last_name = COALESCE (ss.emp_last_name , 'n.a')
			AND cem.email = COALESCE (ss.emp_email , 'n.a')
			AND cem.phone = COALESCE (ss.emp_phone, 'n.a')
			AND cem.gender = COALESCE (ss.emp_gender , 'n.a')
			AND cem.date_of_birth = COALESCE (ss.emp_date_of_birth , '1900-1-1')
			AND cem.salary = COALESCE (ss.emp_salary, '-1')
			AND cem.employee_src_id = COALESCE (ss.emp_id , '-1')
			AND cem.department_id = COALESCE (cde.department_id , -1)
			AND cem.address_id = COALESCE (cad.address_id , -1)
			AND cem.source_system = 'sa_sales_latvia'
			AND cem.source_system_entity = 'src_latvia_sales')) s1 
			ON CONFLICT (employee_src_unique_id) DO UPDATE 
			SET first_name = excluded.first_name,
				last_name = excluded.last_name,
				email = excluded.email,
				phone = excluded.phone,
				salary = excluded.salary,
				update_dt = current_date;	
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia', v_text_message, v_inserted_date);

	-- Load from second data source
		
		INSERT
		INTO
		bl_3nf.ce_employees(
		employee_id,
		first_name,
		last_name,
		email,
		phone,
		gender,
		address_id,
		date_of_birth,
		department_id,
		salary,
		employee_src_id,
		source_system,
		source_system_entity,
		insert_dt,
		update_dt,
		employee_src_unique_id)
	SELECT
		nextval('emp_id_key_value'), s2.first_name , s2.last_name , s2.email , s2.phone ,s2.gender , s2.address_id , s2.date_of_birth ,
		s2.department_id , s2.salary , s2.employee_src_id , s2.source_system , s2.source_system_entity , s2.insert_dt , s2.update_dt ,
		s2.employee_src_unique_id 
		FROM 
		(SELECT 	
		DISTINCT COALESCE (ss.emp_first_name, 'n.a') AS first_name ,
		COALESCE (ss.emp_last_name , 'n.a') AS last_name ,
		COALESCE (ss.emp_email , 'n.a') AS email ,
		COALESCE (ss.emp_phone, 'n.a') AS phone ,
		COALESCE (ss.emp_gender , 'n.a') AS gender ,
		COALESCE (cad.address_id , -1) AS address_id ,
		COALESCE (ss.emp_date_of_birth , '1900-1-1') AS date_of_birth ,
		COALESCE (cde.department_id , -1) AS department_id ,
		COALESCE (ss.emp_salary, '-1') AS salary ,
		COALESCE (ss.emp_id , '-1') AS employee_src_id ,
		'sa_sales_lithuania' AS source_system ,
		'src_lithuania_sales' AS source_system_entity ,
		current_date AS insert_dt ,
		'1900-01-01'::date AS update_dt ,
		concat(ss.emp_id , '_' , 'sa_sales_lithuania', '_', 'src_lithuania_sales') AS employee_src_unique_id 
	FROM
		sa_sales_lithuania.src_lithuania_sales  AS ss
	LEFT JOIN bl_3nf.ce_addresses AS cad 
	ON
		cad.address_src_id = ss.emp_address_id
		AND cad.source_system = 'sa_sales_lithuania'
	LEFT JOIN bl_3nf.ce_departments AS cde 
	ON
		cde.department_src_id = ss.emp_department_id
		AND cde.source_system = 'sa_sales_lithuania'
	-- To avoid duplicate entries.
	WHERE
		NOT EXISTS(
		SELECT
			1
		FROM
			bl_3nf.ce_employees AS cem
		WHERE
			cem.first_name = COALESCE (ss.emp_first_name, 'n.a')
			AND cem.last_name = COALESCE (ss.emp_last_name , 'n.a')
			AND cem.email = COALESCE (ss.emp_email , 'n.a')
			AND cem.phone = COALESCE (ss.emp_phone, 'n.a')
			AND cem.gender = COALESCE (ss.emp_gender , 'n.a')
			AND cem.date_of_birth = COALESCE (ss.emp_date_of_birth , '1900-1-1')
			AND cem.salary = COALESCE (ss.emp_salary, '-1')
			AND cem.employee_src_id = COALESCE (ss.emp_id , '-1')
			AND cem.department_id = COALESCE (cde.department_id , -1)
			AND cem.address_id = COALESCE (cad.address_id , -1)
			AND cem.source_system = 'sa_sales_lithuania'
			AND cem.source_system_entity = 'src_lithuania_sales')) s2 
			ON CONFLICT (employee_src_unique_id) DO UPDATE 
			SET first_name = excluded.first_name,
				last_name = excluded.last_name,
				email = excluded.email,
				phone = excluded.phone,
				salary = excluded.salary,
				update_dt = current_date;	
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_lithuania', v_text_message, v_inserted_date);

	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);

END;
$$;


--Function to insert values into sales table from data source
CREATE OR REPLACE PROCEDURE  bl_cl.load_sales_data_3nf() 
LANGUAGE plpgsql
AS $$
DECLARE 
	v_inserted_count INT;
	v_table_name VARCHAR := 'ce_sales';
	v_inserted_date DATE := CURRENT_DATE;
	v_user_info VARCHAR := CURRENT_USER;
	v_text_message VARCHAR;
	 v_error_message VARCHAR ;
BEGIN
		INSERT
		INTO
		bl_3nf.ce_sales(
		sale_id,
		product_id,
		employee_id,
		customer_id,
		store_id,
		payment_type_id,
		channel_type_id,
		shipper_id,
		sale_dt , 
		product_price_kg,
		shipping_cost,
		quantity,
		amount,
		sale_src_id,
		source_system,
		source_system_entity,
		insert_dt)
	SELECT
		nextval('sales_id_key_value'),
		COALESCE (cep.product_id , -1),
		COALESCE (cee.employee_id , -1),
		COALESCE (cec.customer_id , -1),
		COALESCE (ces2.store_id , -1),
		COALESCE (cep2.payment_type_id, -1),
		COALESCE (cec2.channel_type_id , -1),
		COALESCE (cs.shipper_id , -1),
		COALESCE (ss.sale_date::date , '1900-01-01') AS sale_dt ,
		COALESCE (ss.product_price::NUMERIC(8,2) , -1),
		COALESCE (ss.shipping_cost::NUMERIC(8,2) , -1),
		COALESCE (ss.quantity_kg::NUMERIC(8,2) , -1),
		COALESCE (ss.amount::NUMERIC(10,2) , -1),
		COALESCE (ss.sale_id , '-1'),
		'sa_sales_latvia',
		'src_latvia_sales',
		current_date
	
	FROM
		bl_cl.incriment_latvia_sales_mv AS ss
	LEFT JOIN bl_3nf.ce_products_scd AS cep 
	ON
		cep.product_src_id = ss.product_id
	AND cep.source_system = 'sa_sales_latvia'
	LEFT JOIN bl_3nf.ce_employees AS cee
	ON
		cee.employee_src_id = ss.emp_id
	AND cee.source_system = 'sa_sales_latvia'
	LEFT JOIN bl_3nf.ce_customers AS cec  
	ON
		cec.customer_src_id = ss.cust_id
	AND cec.source_system = 'sa_sales_latvia'
	LEFT JOIN bl_3nf.ce_stores AS ces2
	ON
		ces2.store_src_id = ss.store_id
	AND ces2.source_system = 'sa_sales_latvia'
	LEFT JOIN bl_3nf.ce_payment_types AS cep2
	ON
		cep2.payment_type_src_id = ss.payment_type_id
	AND cep2.source_system = 'sa_sales_latvia'
	LEFT JOIN bl_3nf.ce_channel_types AS cec2
	ON
		cec2.channel_type_src_id = ss.channel_type_id
	AND cec2.source_system = 'sa_sales_latvia'
	LEFT JOIN bl_3nf.ce_shippers cs 
	ON
		cs.shipper_src_id = ss.shipper_id
	AND cs.source_system = 'sa_sales_latvia'
	-- To avoid duplicates

WHERE NOT EXISTS (
SELECT
	1
FROM
	bl_3nf.ce_sales cs1
WHERE
	cs1.product_id = COALESCE (cep.product_id ,
	-1)
		AND cs1.employee_id = COALESCE (COALESCE (cee.employee_id ,
		-1))
			AND cs1.customer_id = COALESCE (cec.customer_id ,
			-1)
				AND cs1.store_id = COALESCE (ces2.store_id ,
				-1)
					AND cs1.payment_type_id = COALESCE (cep2.payment_type_id,
					-1)
						AND cs1.channel_type_id = COALESCE (cec2.channel_type_id ,
						-1)
							AND cs1.shipper_id = COALESCE (cs.shipper_id ,
							-1)
								AND cs1.sale_dt  = COALESCE (TO_DATE(ss.sale_date , 'YYYY-MM-DD'),
								'1900-1-1')
									AND cs1.product_price_kg::NUMERIC(8,
									2) = COALESCE (ss.product_price::NUMERIC(8,
									2) ,
									-1)
										AND cs1.shipping_cost::NUMERIC(8,
										2) = COALESCE (ss.shipping_cost::NUMERIC(8,
										2) ,
										-1)
											AND cs1.quantity::NUMERIC(8,
											2) = COALESCE (ss.quantity_kg::NUMERIC(8,
											2) ,
											-1)
												AND cs1.amount::NUMERIC(10,
												2) = COALESCE (ss.amount::NUMERIC(10,
												2) ,
												-1)
													AND cs1.sale_src_id = COALESCE (ss.sale_id,
													'-1')
														AND cs1.source_system = 'sa_sales_latvia'
														AND cs1.source_system_entity = 'src_latvia_sales'
	);
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;

	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia', v_text_message, v_inserted_date);

	UPDATE bl_cl.prm_mta_incrimental_load 
	SET previous_loaded_date = current_timestamp 
	WHERE source_table_name = 'sa_sales_latvia.src_latvia_sales'  
	AND target_table_name = 'bl_3nf.ce_sales' 
	AND procedure_name = 'bl_cl.load_sales_data_3nf()';

	--Insert into sales table from second data source
	INSERT
		INTO
		bl_3nf.ce_sales(
		sale_id,
		product_id,
		employee_id,
		customer_id,
		store_id,
		payment_type_id,
		channel_type_id,
		shipper_id,
		sale_dt ,
		product_price_kg,
		shipping_cost,
		quantity,
		amount,
		sale_src_id,
		source_system,
		source_system_entity,
		insert_dt)
	SELECT
		nextval('sales_id_key_value'),
		COALESCE (li_cep.product_id , -1),
		COALESCE (li_cee.employee_id , -1),
		COALESCE (li_cec.customer_id , -1),
		COALESCE (li_ces2.store_id , -1),
		COALESCE (li_cep2.payment_type_id, -1),
		COALESCE (li_cec2.channel_type_id , -1),
		COALESCE (li_cs.shipper_id , -1),
		COALESCE (li.sale_date::date ,'1900-1-1'),
		COALESCE (li.product_price::NUMERIC(8,2) , -1),
		COALESCE (li.shipping_cost::NUMERIC(8,2) , -1),
		COALESCE (li.quantity_kg::NUMERIC(8,2) , -1),
		COALESCE (li.amount::NUMERIC(10,2) , -1),
		COALESCE (li.sale_id,'-1'),
		'sa_sales_lithuania',
		'src_lithuania_sales',
		current_date
	
	FROM
		bl_cl.incriment_lithuania_sales_mv AS li
	LEFT JOIN bl_3nf.ce_products_scd AS li_cep
	ON
		li_cep.product_src_id = li.product_id
	AND li_cep.source_system = 'sa_sales_lithuania'
	LEFT JOIN bl_3nf.ce_employees AS li_cee
	ON
		li_cee.employee_src_id = li.emp_id
	AND li_cee.source_system = 'sa_sales_lithuania'
	LEFT JOIN bl_3nf.ce_customers AS li_cec 
	ON
		li_cec.customer_src_id = li.cust_id
	AND li_cec.source_system = 'sa_sales_lithuania'
	LEFT JOIN bl_3nf.ce_stores AS li_ces2
	ON
		li_ces2.store_src_id = li.store_id
	AND li_ces2.source_system = 'sa_sales_lithuania'
	LEFT JOIN bl_3nf.ce_payment_types AS li_cep2
	ON
		li_cep2.payment_type_src_id = li.payment_type_id
	AND li_cep2.source_system = 'sa_sales_lithuania'
	LEFT JOIN bl_3nf.ce_channel_types AS li_cec2
	ON
		li_cec2.channel_type_src_id = li.channel_type_id
	AND li_cec2.source_system = 'sa_sales_lithuania'
	LEFT JOIN bl_3nf.ce_shippers li_cs 
	ON
		li_cs.shipper_src_id = li.shipper_id
	AND li_cs.source_system = 'sa_sales_lithuania'
	-- To avoid duplicates

WHERE NOT EXISTS (
SELECT
	1
FROM
	bl_3nf.ce_sales cs2
WHERE
	cs2.product_id = COALESCE (li_cep.product_id ,
	-1)
		AND cs2.employee_id = COALESCE (COALESCE (li_cee.employee_id ,
		-1))
			AND cs2.customer_id = COALESCE (li_cec.customer_id ,
			-1)
				AND cs2.store_id = COALESCE (li_ces2.store_id ,
				-1)
					AND cs2.payment_type_id = COALESCE (li_cep2.payment_type_id,
					-1)
						AND cs2.channel_type_id = COALESCE (li_cec2.channel_type_id ,
						-1)
							AND cs2.shipper_id = COALESCE (li_cs.shipper_id ,
							-1)
								AND cs2.sale_dt  = COALESCE (TO_DATE(li.sale_date , 'YYYY-MM-DD'),
								'1900-1-1')
									AND cs2.product_price_kg::NUMERIC(8,
									2) = COALESCE (li.product_price::NUMERIC(8,
									2) ,
									-1)
										AND cs2.shipping_cost::NUMERIC(8,
										2) = COALESCE (li.shipping_cost::NUMERIC(8,
										2) ,
										-1)
											AND cs2.quantity::NUMERIC(8,
											2) = COALESCE (li.quantity_kg::NUMERIC(8,
											2) ,
											-1)
												AND cs2.amount::NUMERIC(10,
												2) = COALESCE (li.amount::NUMERIC(10,
												2) ,
												-1)
													AND cs2.sale_src_id = COALESCE (li.sale_id,
													'-1')
														AND cs2.source_system = 'sa_sales_lithuania'
														AND cs2.source_system_entity = 'src_lithuania_sales'
	);
	GET DIAGNOSTICS v_inserted_count =  ROW_COUNT;
	-- text message for loggining table			
	v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
	-- Load to logging table	
	CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_lithuania', v_text_message, v_inserted_date);
	--Update meta table
	UPDATE bl_cl.prm_mta_incrimental_load 
	SET previous_loaded_date = current_timestamp 
	WHERE source_table_name = 'sa_sales_lithuana.src_lithuania_sales'  
	AND target_table_name = 'bl_3nf.ce_sales' 
	AND procedure_name = 'bl_cl.load_sales_data_3nf()';
	EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, v_table_name, 'sa_sales_latvia' || 'sa_sales_lithuania', v_error_message , v_inserted_date);
END;
$$;



















 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 