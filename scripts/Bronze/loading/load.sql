CREATE PROCEDURE loading_bronze_layer
as
begin
--load crm_cust_info table from cust_info.csv file
	BEGIN TRY
		truncate table bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\ITI_DATA_ENGINNERING\udemy\Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FORMAT = 'CSV',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n'
		)
		--load crm_cust_info table from prd_info.csv file

		truncate table bronze.crm_prd_info
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\ITI_DATA_ENGINNERING\udemy\Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FORMAT = 'CSV',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			KEEPNULLS
		);

		--load crm_cust_info table from sales_details.csv file
		truncate table bronze.crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\ITI_DATA_ENGINNERING\udemy\Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FORMAT = 'CSV',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			KEEPNULLS
		);

		 --load erp_CUST_AZ12 table from CUST_AZ12.csv file
		truncate table bronze.erp_CUST_AZ12
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'E:\ITI_DATA_ENGINNERING\udemy\Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FORMAT = 'CSV',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			KEEPNULLS
		);


		 --load erp_LOC_A101 table from LOC_A101.csv file
		truncate table bronze.erp_LOC_A101
		BULK INSERT bronze.erp_LOC_A101
		FROM 'E:\ITI_DATA_ENGINNERING\udemy\Baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FORMAT = 'CSV',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			KEEPNULLS
		);


		--load erp_PX_CAT_G1V2 table from PX_CAT_G1V2.csv file
		truncate table bronze.erp_PX_CAT_G1V2
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'E:\ITI_DATA_ENGINNERING\udemy\Baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FORMAT = 'CSV',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			KEEPNULLS
		);

	END TRY
		BEGIN CATCH
				PRINT '=========================================='
				PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
				PRINT 'Error Message' + ERROR_MESSAGE();
				PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
				PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
				PRINT '=========================================='
		END CATCH
end
