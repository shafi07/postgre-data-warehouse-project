CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    -- Set the start time for the batch process
    batch_start_time := clock_timestamp();

    RAISE NOTICE '===============================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '===============================================';

    -- CRM Table: crm_cust_info
    RAISE NOTICE '>> Trunning Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    start_time := clock_timestamp();
    RAISE NOTICE '>> Inserting Data into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info FROM '/path/to/your/csv/crm_cust_info.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconsds', EXTRACT(SECOND FROM (end_time - start_time));

    -- CRM Table: crm_prd_info
    RAISE NOTICE '>> Trunning Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    start_time := clock_timestamp();
    RAISE NOTICE '>> Inserting Data into: bronze.crm_prd_info';
    COPY bronze.crm_cust_info FROM '/path/to/your/csv/crm_prd_info.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconsds', EXTRACT(SECOND FROM (end_time - start_time));

    -- CRM Table: crm_sales_details
    RAISE NOTICE '>> Trunning Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    start_time := clock_timestamp();
    RAISE NOTICE '>> Inserting Data into: bronze.crm_sales_details';
    COPY bronze.crm_cust_info FROM '/path/to/your/csv/crm_sales_details.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconsds', EXTRACT(SECOND FROM (end_time - start_time));

    -- ERP Table: erp_loc_a101
    TRUNCATE TABLE bronze.erp_loc_a101;
    start_time := clock_timestamp();
    COPY bronze.erp_loc_a101 FROM 'C:/sql/dwh_project/datasets/source_erp/loc_a101.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- ERP Table: erp_cust_az12
    TRUNCATE TABLE bronze.erp_cust_az12;
    start_time := clock_timestamp();
    COPY bronze.erp_cust_az12 FROM 'C:/sql/dwh_project/datasets/source_erp/cust_az12.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- ERP Table: erp_px_cat_g1v2
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    start_time := clock_timestamp();
    COPY bronze.erp_px_cat_g1v2 FROM 'C:/sql/dwh_project/datasets/source_erp/px_cat_g1v2.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(SECOND FROM batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPT WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE '==========================================';
END;
$$;