/*
==============================================================================
    STORED PROCEDURE: bronze.load_bronze
    PURPOSE:
        - Loads data into the Bronze layer from various source CSV files.
        - Truncates existing tables before inserting new data.
        - Tracks load duration for performance monitoring.
        - Handles errors using TRY...CATCH.

    WARNING:
        - **This procedure permanently deletes existing data in the Bronze layer** 
          before loading fresh data.
        - Ensure the source file paths are correct before execution.
        - Requires appropriate permissions for BULK INSERT operations.

    TABLES AFFECTED:
        - bronze.crm_cust_info
        - bronze.crm_prd_info
        - bronze.crm_sales_details
        - bronze.erp_cust_az12
        - bronze.erp_loc_a101
        - bronze.erp_px_cat_g1v2

    ERROR HANDLING:
        - Captures and logs error messages during execution.
==============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();
	BEGIN TRY
		--CRM DATA LOAD
		PRINT '============================================================================';
		PRINT 'Load BRONZE Layer Data';
		PRINT '============================================================================';

		PRINT '::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::';
		PRINT 'CRM Data Load';
		PRINT '::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::';


		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Truncate Table: crm_cust_info';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		TRUNCATE TABLE bronze.crm_cust_info; -- delete any data inside the table which pre exists
		PRINT '----------------------------------------------------------------------------';
		PRINT 'Loading Data in Table: crm_cust_info';
		PRINT '----------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\komal\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, --This indicates that the insert must be made starting from second row (because first row is header)
			FIELDTERMINATOR = ',', -- delimited used in the source file
			TABLOCK -- locks the table when bulk loading data
		);
		SET @end_time = GETDATE();

		PRINT '::> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------------------------------------------------';


		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Truncate Table: crm_prd_info';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		TRUNCATE TABLE bronze.crm_prd_info; -- delete any data inside the table which pre exists
		PRINT '----------------------------------------------------------------------------';
		PRINT 'Loading Data in Table: crm_prd_info';
		PRINT '----------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\komal\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, --This indicates that the insert must be made starting from second row (because first row is header)
			FIELDTERMINATOR = ',', -- delimited used in the source file
			TABLOCK -- locks the table when bulk loading data
		);
		SET @end_time = GETDATE();
		PRINT '::> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------------------------------------------------';

		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Truncate Table: crm_sales_details';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		TRUNCATE TABLE bronze.crm_sales_details; -- delete any data inside the table which pre exists
		PRINT '----------------------------------------------------------------------------';
		PRINT 'Loading Data in Table: crm_sales_details';
		PRINT '----------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\komal\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, --This indicates that the insert must be made starting from second row (because first row is header)
			FIELDTERMINATOR = ',', -- delimited used in the source file
			TABLOCK -- locks the table when bulk loading data
		);
		SET @end_time = GETDATE();
		PRINT '::> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------------------------------------------------';

		--ERP DATA LOAD
		PRINT '::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::';
		PRINT 'ERP Data Load';
		PRINT '::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::';

		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Truncate Table: erp_cust_az12';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		TRUNCATE TABLE bronze.erp_cust_az12; -- delete any data inside the table which pre exists
		PRINT '----------------------------------------------------------------------------';
		PRINT 'Loading Data in Table: erp_cust_az12';
		PRINT '----------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\komal\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, --This indicates that the insert must be made starting from second row (because first row is header)
			FIELDTERMINATOR = ',', -- delimited used in the source file
			TABLOCK -- locks the table when bulk loading data
		);
		SET @end_time = GETDATE();
		PRINT '::> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------------------------------------------------';



		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Truncate Table: erp_loc_a101';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		TRUNCATE TABLE bronze.erp_loc_a101; -- delete any data inside the table which pre exists
		PRINT '----------------------------------------------------------------------------';
		PRINT 'Loading Data in Table: erp_loc_a101';
		PRINT '----------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\komal\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, --This indicates that the insert must be made starting from second row (because first row is header)
			FIELDTERMINATOR = ',', -- delimited used in the source file
			TABLOCK -- locks the table when bulk loading data
		);
		SET @end_time = GETDATE();
		PRINT '::> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------------------------------------------------';


		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Truncate Table: erp_px_cat_g1v2';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2; -- delete any data inside the table which pre exists
		PRINT '----------------------------------------------------------------------------';
		PRINT 'Loading Data in Table: erp_px_cat_g1v2';
		PRINT '----------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\komal\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, --This indicates that the insert must be made starting from second row (because first row is header)
			FIELDTERMINATOR = ',', -- delimited used in the source file
			TABLOCK -- locks the table when bulk loading data
		);
		SET @end_time = GETDATE();
		PRINT '::> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------------------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '============================================================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';

	END TRY
	BEGIN CATCH

		PRINT '============================================================================';
		PRINT 'Error Occured while Loading BRONZE Layer';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '============================================================================';

	END CATCH

END
