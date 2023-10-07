--Procedure for loading data into src_latvia_sales
CREATE OR REPLACE PROCEDURE bl_cl.load_src_latvia()
LANGUAGE plpgsql
AS $$
DECLARE 
	v_user_info VARCHAR := CURRENT_USER;
	v_inserted_date date := current_date;
	v_text_message VARCHAR;
	v_rows_count int;
	v_error_message VARCHAR;
BEGIN 
		--Insert to sales_latvia source table.
		INSERT INTO sa_sales_latvia.src_latvia_sales(
		PRODUCT_ID,
		PRODUCT_NAME ,
		BRAND ,
		PACKAGE_TYPE,
		CATEGORY , 
		CATEGORY_ID , 
		PRODUCT_PRICE  ,
		CUST_ID , 
		CUST_FIRST_NAME ,
		CUST_LAST_NAME , 
		CUST_EMAIL ,
		CUST_PHONE , 
		CUST_GENDER , 
		CUST_DATE_OF_BIRTH  , 
		CUST_ADDRESS ,
		CUST_ADDRESS_ID , 
		CUST_ZIPCODE , 
		CUST_CITY_ID , 
		CUST_CITY  ,
		CUST_COUNTRY  , 
		CUST_REGION  , 
		CUST_COUNTRY_ID  , 
		CUST_REGION_ID ,
		EMP_ID , 
		EMP_FIRST_NAME  , 
		EMP_LAST_NAME , 
		EMP_EMAIL  , 
		EMP_PHONE ,
		EMP_GENDER , 
		EMP_DATE_OF_BIRTH , 
		EMP_ADDRESS , 
		EMP_ADDRESS_ID ,
		EMP_ZIPCODE  , 
		EMP_DEPARTMENT , 
		EMP_DEPARTMENT_ID , 
		EMP_JOB_TITLE ,
		EMP_SALARY  , 
		EMP_CITY_ID , 
		EMP_CITY , 
		EMP_COUNTRY  , 
		EMP_REGION ,
		EMP_COUNTRY_ID , 
		EMP_REGION_ID , 
		STORE_ID , 
		STORE_NAME  ,
		MANAGER_FIRST_NAME , 
		MANAGER_LAST_NAME  , 
		STORE_EMAIL ,
		STORE_ADDRESS  , 
		STORE_CITY , 
		STORE_COUNTRY , 
		STORE_REGION ,
		STORE_ZIPCODE , 
		STORE_ADDRESS_ID, 
		STORE_CITY_ID ,
		STORE_COUNTRY_ID, 
		STORE_REGION_ID , 
		CHANNEL_TYPE_ID,
		CHANNEL_TYPE , 
		PAYMENT_TYPE_ID , 
		PAYMENT_TYPE , 
		SHIPPER_ID ,
		SHIPPER_NAME  , 
		EMAIL , 
		PHONE  , 
		SHIPPING_COST, 
		SALE_ID ,
		SALE_DATE, 
		QUANTITY_KG, 
		AMOUNT ,
		INSERT_DT
	)  
	-- Convert all column data types to varchar and store to table. 
	SELECT 
		PRODUCT_ID::varchar ,
		PRODUCT_NAME::varchar ,
		BRAND:: varchar ,
		PACKAGE_TYPE:: varchar ,
		CATEGORY:: varchar , 
		CATEGORY_ID:: varchar, 
		PRODUCT_PRICE:: varchar ,
		CUST_ID:: varchar, 
		CUST_FIRST_NAME:: varchar ,
		CUST_LAST_NAME:: varchar , 
		CUST_EMAIL:: varchar ,
		CUST_PHONE:: varchar , 
		CUST_GENDER ::varchar , 
		CUST_DATE_OF_BIRTH:: varchar , 
		CUST_ADDRESS ::varchar  ,
		CUST_ADDRESS_ID ::varchar, 
		CUST_ZIPCODE:: varchar , 
		CUST_CITY_ID:: varchar, 
		CUST_CITY:: varchar ,
		CUST_COUNTRY:: varchar , 
		CUST_REGION ::varchar , 
		CUST_COUNTRY_ID ::varchar , 
		CUST_REGION_ID ::varchar,
		EMP_ID ::varchar, 
		EMP_FIRST_NAME:: varchar , 
		EMP_LAST_NAME ::varchar , 
		EMP_EMAIL ::varchar , 
		EMP_PHONE ::varchar ,
		EMP_GENDER:: varchar , 
		EMP_DATE_OF_BIRTH:: varchar, 
		EMP_ADDRESS ::varchar, 
		EMP_ADDRESS_ID ::varchar,
		EMP_ZIPCODE:: varchar , 
		EMP_DEPARTMENT:: varchar, 
		EMP_DEPARTMENT_ID ::varchar, 
		EMP_JOB_TITLE:: varchar ,
		EMP_SALARY:: varchar , 
		EMP_CITY_ID:: varchar , 
		EMP_CITY ::varchar , 
		EMP_COUNTRY:: varchar , 
		EMP_REGION ::varchar,
		EMP_COUNTRY_ID ::varchar, 
		EMP_REGION_ID ::varchar , 
		STORE_ID:: varchar, 
		STORE_NAME ::varchar ,
		MANAGER_FIRST_NAME:: varchar , 
		MANAGER_LAST_NAME:: varchar , 
		STORE_EMAIL ::varchar ,
		STORE_ADDRESS:: varchar , 
		STORE_CITY:: varchar, 
		STORE_COUNTRY:: varchar , 
		STORE_REGION ::varchar ,
		STORE_ZIPCODE ::varchar , 
		STORE_ADDRESS_ID ::varchar , 
		STORE_CITY_ID ::varchar ,
		STORE_COUNTRY_ID ::varchar , 
		STORE_REGION_ID ::varchar , 
		CHANNEL_TYPE_ID ::varchar,
		CHANNEL_TYPE:: varchar , 
		PAYMENT_TYPE_ID ::varchar , 
		PAYMENT_TYPE ::varchar , 
		SHIPPER_ID ::varchar,
		SHIPPER_NAME ::varchar , 
		EMAIL ::varchar , 
		PHONE:: varchar , 
		SHIPPING_COST ::varchar, 
		SALE_ID ::varchar,
		SALE_DATE:: varchar, 
		QUANTITY_KG ::varchar, 
		AMOUNT ::varchar,
		CURRENT_TIMESTAMP	
	FROM sa_sales_latvia.ext_latvia_sales AS e
WHERE NOT EXISTS (
	SELECT 1 FROM sa_sales_latvia.src_latvia_sales s 
		WHERE s.product_id  = e.product_id::varchar 
		AND s.category = e.category_id::varchar
		AND s.cust_id = e.cust_id::varchar
		AND s.emp_id = e.emp_id::varchar
		AND s.store_id = e.store_id::varchar
		AND s.channel_type_id = e.channel_type_id::varchar
		AND s.payment_type_id = e.payment_type_id::varchar
		AND s.shipper_id = e.shipper_id::varchar
		AND s.sale_id = e.sale_id::varchar
		AND s.sale_date::date = e.sale_date 

);

	GET DIAGNOSTICS v_rows_count = ROW_COUNT;
	v_text_message := 'No. of rows inserted: ' || v_rows_count::VARCHAR;
	-- Print total number of insered rows.
	CALL bl_cl.logging_info(v_user_info, 'src_latvia_sales', 'sa_sales_latvia', v_text_message, v_inserted_date);

		EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, 'src_latvia', 'sa_sales_latvia', v_error_message , v_inserted_date);
END ;
$$;




--Procedure for loading data into src_lithuania_sales
CREATE OR REPLACE PROCEDURE bl_cl.load_src_lithuania()
LANGUAGE plpgsql
AS $$
DECLARE 
	v_user_info VARCHAR := CURRENT_USER;
	v_inserted_date date := current_date;
	v_text_message VARCHAR;
	v_rows_count int;
	v_error_message VARCHAR;
BEGIN 
		--Insert to sales_latvia source table.
	--Insert to sales_latvia source table.
		INSERT INTO  sa_sales_lithuania.src_lithuania_sales(
		PRODUCT_ID,
		PRODUCT_NAME ,
		BRAND ,
		PACKAGE_TYPE,
		CATEGORY , 
		CATEGORY_ID , 
		PRODUCT_PRICE  ,
		CUST_ID , 
		CUST_FIRST_NAME ,
		CUST_LAST_NAME , 
		CUST_EMAIL ,
		CUST_PHONE , 
		CUST_GENDER , 
		CUST_DATE_OF_BIRTH  , 
		CUST_ADDRESS ,
		CUST_ADDRESS_ID , 
		CUST_ZIPCODE , 
		CUST_CITY_ID , 
		CUST_CITY  ,
		CUST_COUNTRY  , 
		CUST_REGION  , 
		CUST_COUNTRY_ID  , 
		CUST_REGION_ID ,
		EMP_ID , 
		EMP_FIRST_NAME  , 
		EMP_LAST_NAME , 
		EMP_EMAIL  , 
		EMP_PHONE ,
		EMP_GENDER , 
		EMP_DATE_OF_BIRTH , 
		EMP_ADDRESS , 
		EMP_ADDRESS_ID ,
		EMP_ZIPCODE  , 
		EMP_DEPARTMENT , 
		EMP_DEPARTMENT_ID , 
		EMP_JOB_TITLE ,
		EMP_SALARY  , 
		EMP_CITY_ID , 
		EMP_CITY , 
		EMP_COUNTRY  , 
		EMP_REGION ,
		EMP_COUNTRY_ID , 
		EMP_REGION_ID , 
		STORE_ID , 
		STORE_NAME  ,
		MANAGER_FIRST_NAME , 
		MANAGER_LAST_NAME  , 
		STORE_EMAIL ,
		STORE_ADDRESS  , 
		STORE_CITY , 
		STORE_COUNTRY , 
		STORE_REGION ,
		STORE_ZIPCODE , 
		STORE_ADDRESS_ID, 
		STORE_CITY_ID ,
		STORE_COUNTRY_ID, 
		STORE_REGION_ID , 
		CHANNEL_TYPE_ID,
		CHANNEL_TYPE , 
		PAYMENT_TYPE_ID , 
		PAYMENT_TYPE , 
		SHIPPER_ID ,
		SHIPPER_NAME  , 
		EMAIL , 
		PHONE  , 
		SHIPPING_COST, 
		SALE_ID ,
		SALE_DATE, 
		QUANTITY_KG, 
		DISCOUNT,
		AMOUNT ,
		INSERT_DT
	)  
	-- Convert all column data types to varchar and store to table. 
	SELECT 
		PRODUCT_ID::varchar ,
		PRODUCT_NAME::varchar ,
		BRAND:: varchar ,
		PACKAGE_TYPE:: varchar ,
		CATEGORY:: varchar , 
		CATEGORY_ID:: varchar, 
		PRODUCT_PRICE:: varchar ,
		CUST_ID:: varchar, 
		CUST_FIRST_NAME:: varchar ,
		CUST_LAST_NAME:: varchar , 
		CUST_EMAIL:: varchar ,
		CUST_PHONE:: varchar , 
		CUST_GENDER ::varchar , 
		CUST_DATE_OF_BIRTH:: varchar , 
		CUST_ADDRESS ::varchar  ,
		CUST_ADDRESS_ID ::varchar, 
		CUST_ZIPCODE:: varchar , 
		CUST_CITY_ID:: varchar, 
		CUST_CITY:: varchar ,
		CUST_COUNTRY:: varchar , 
		CUST_REGION ::varchar , 
		CUST_COUNTRY_ID ::varchar , 
		CUST_REGION_ID ::varchar,
		EMP_ID ::varchar, 
		EMP_FIRST_NAME:: varchar , 
		EMP_LAST_NAME ::varchar , 
		EMP_EMAIL ::varchar , 
		EMP_PHONE ::varchar ,
		EMP_GENDER:: varchar , 
		EMP_DATE_OF_BIRTH:: varchar, 
		EMP_ADDRESS ::varchar, 
		EMP_ADDRESS_ID ::varchar,
		EMP_ZIPCODE:: varchar , 
		EMP_DEPARTMENT:: varchar, 
		EMP_DEPARTMENT_ID ::varchar, 
		EMP_JOB_TITLE:: varchar ,
		EMP_SALARY:: varchar , 
		EMP_CITY_ID:: varchar , 
		EMP_CITY ::varchar , 
		EMP_COUNTRY:: varchar , 
		EMP_REGION ::varchar,
		EMP_COUNTRY_ID ::varchar, 
		EMP_REGION_ID ::varchar , 
		STORE_ID:: varchar, 
		STORE_NAME ::varchar ,
		MANAGER_FIRST_NAME:: varchar , 
		MANAGER_LAST_NAME:: varchar , 
		STORE_EMAIL ::varchar ,
		STORE_ADDRESS:: varchar , 
		STORE_CITY:: varchar, 
		STORE_COUNTRY:: varchar , 
		STORE_REGION ::varchar ,
		STORE_ZIPCODE ::varchar , 
		STORE_ADDRESS_ID ::varchar , 
		STORE_CITY_ID ::varchar ,
		STORE_COUNTRY_ID ::varchar , 
		STORE_REGION_ID ::varchar , 
		CHANNEL_TYPE_ID ::varchar,
		CHANNEL_TYPE:: varchar , 
		PAYMENT_TYPE_ID ::varchar , 
		PAYMENT_TYPE ::varchar , 
		SHIPPER_ID ::varchar,
		SHIPPER_NAME ::varchar , 
		EMAIL ::varchar , 
		PHONE:: varchar , 
		SHIPPING_COST ::varchar, 
		SALE_ID ::varchar,
		SALE_DATE:: varchar, 
		QUANTITY_KG ::varchar, 
		DISCOUNT::varchar, 
		AMOUNT ::varchar,
		CURRENT_TIMESTAMP
	FROM sa_sales_lithuania.ext_lithuania_sales AS e 
	WHERE NOT EXISTS (
		SELECT 1 FROM sa_sales_lithuania.src_lithuania_sales AS s 
		WHERE s.product_id  = e.product_id::varchar 
		AND s.category = e.category_id::varchar
		AND s.cust_id = e.cust_id::varchar
		AND s.emp_id = e.emp_id::varchar
		AND s.store_id = e.store_id::varchar
		AND s.channel_type_id = e.channel_type_id::varchar
		AND s.payment_type_id = e.payment_type_id::varchar
		AND s.shipper_id = e.shipper_id::varchar
		AND s.sale_id = e.sale_id::varchar
		AND s.sale_date::date = e.sale_date 
	)   ;


	GET DIAGNOSTICS v_rows_count = ROW_COUNT;
	v_text_message := 'No. of rows inserted: ' || v_rows_count::VARCHAR;
	-- Print total number of insered rows.
	CALL bl_cl.logging_info(v_user_info, 'src_lithuania_sales', 'sa_sales_lithuania', v_text_message, v_inserted_date);

		EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, 'src_lithuania', 'sa_sales_lithuania', v_error_message , v_inserted_date);
END ;
$$;






