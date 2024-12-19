/* 

This code illustrates the whole ETL process of the mandate from the creation of the tables, to the transformation and the loading of the data into the dimensional and fact tables.

First of all, I will create a new database called TIA for this project.


-- Drop the database if it already exists and recreate it with the correct collation.
*/
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'TIA')
	DROP DATABASE TIA;
GO

CREATE DATABASE TIA COLLATE SQL_Latin1_General_CP1_CI_AS;
GO

USE TIA;
GO

/*
 * The DDL statements below create the "dimension" tables first, followed by the "fact" table.
 */

CREATE TABLE dim_Date (
	NoDate					INT IDENTITY(1,1)	PRIMARY KEY,
	[Date]					DATETIME		NOT NULL,
	Month					TINYINT			NOT NULL CONSTRAINT CK_Month CHECK (Month BETWEEN 1 AND 12),
	Quarter					TINYINT			NOT NULL CONSTRAINT CK_Quarter CHECK (Quarter BETWEEN 1 AND 4),
	Year					SMALLINT		NOT NULL,
	FiscalYear				SMALLINT		NOT NULL,
	QuarterFiscalYear		TINYINT			NOT NULL CONSTRAINT CK_QuarterFiscalYr CHECK (QuarterFiscalYear BETWEEN 1 AND 4),
	StrategyCycle			TINYINT			NOT NULL,
		CONSTRAINT CK_YearCheck CHECK (Year >= FiscalYear));

CREATE TABLE dim_Product (
	ProductID				INT IDENTITY(1,1)	PRIMARY KEY,
	ProductName				NVARCHAR(50)	NOT NULL,
	ProductNumber			NVARCHAR(25)	NOT NULL,
	DaysOnTheMarket			SMALLINT		NOT NULL CONSTRAINT CK_DaysOnTheMarket CHECK (DaysOnTheMarket >= 0),
	AverageRating			DECIMAL(4,2)	CONSTRAINT CK_AverageRating CHECK (AverageRating BETWEEN 0.00 AND 5.00),
	ProfitMargin			MONEY			NOT NULL,
	ProductStyle			NVARCHAR(11)	NOT NULL CONSTRAINT CK_ProductStyle CHECK (ProductStyle IN ('Women','Men','Universal','Unavailable')),
	EffectiveDate			DATETIME		NOT NULL,
	ExpirationDate			DATETIME		NOT NULL DEFAULT ('9999-12-31'),
	CurrentStatus			VARCHAR(7)		NOT NULL DEFAULT ('Current'),
		CONSTRAINT CK_DateCheckProduct CHECK (ExpirationDate > EffectiveDate));

CREATE INDEX AK_dim_Product_ProfitMargin ON dim_Product (ProfitMargin);
/* Adding a non-clustered index on the ProfitMargin column can be justified if:
   1) The product inventory is or will become extensive.
   2) Users frequently query detailed data based on the ProfitMargin attribute.
*/

EXEC sys.sp_addextendedproperty 
	'MS_Description', 'Product style (Women, Men, Universal, Unavailable – if NULL).',
	'schema', 'dbo',
	'table', 'dim_Product',
	'column', 'ProductStyle';

CREATE TABLE dim_SalesAssociate (
	SalesAssociateID		INT IDENTITY(1,1)	PRIMARY KEY,
	LastName				NVARCHAR(50)	NOT NULL,
	FirstName				NVARCHAR(50)	NOT NULL,
	Commission				NVARCHAR(10)	NOT NULL,
	SalesYear				SMALLINT		NOT NULL,
	TotalSales				MONEY			NOT NULL,
	TotalSalesPreviousYear	MONEY,
	ChangeInSales			MONEY,
	ChangeInSalesPercentage	NVARCHAR(10),
	[Difference]			NVARCHAR(3),
	EffectiveDate			DATETIME		NOT NULL,
	ExpirationDate			DATETIME		NOT NULL DEFAULT ('9999-12-31'),
	CurrentStatus			VARCHAR(7)		NOT NULL DEFAULT ('Current'),
		CONSTRAINT CK_DateCheckSA CHECK (ExpirationDate > EffectiveDate));

CREATE TABLE fact_Sale (
	SaleID					INT IDENTITY(1,1)	PRIMARY KEY,
	OrderQty				SMALLINT		NOT NULL,
	GrossDiscount			MONEY			NOT NULL,
	ProfitMargin			MONEY			NOT NULL,
	DateID					INT				NOT NULL FOREIGN KEY REFERENCES dim_Date (NoDate),
	ProductID				INT				NOT NULL FOREIGN KEY REFERENCES dim_Product (ProductID),
	SalesAssociateID		INT				FOREIGN KEY REFERENCES dim_SalesAssociate (SalesAssociateID));

/* 
After creating all the tables of the model, I will now proceed with the transformation and the loading phase of the ETL process for each dimensional tables and the fact table. 
*/

-- 1. Dim_Date

CREATE OR ALTER PROCEDURE sp_ETL_Date
	@StartDate DATETIME,	-- Expected format: yyyy-mm-dd, e.g., @StartDate = '2010-01-01', @EndDate = '2015-12-31'
	@EndDate DATETIME
AS
BEGIN TRANSACTION
BEGIN TRY
	-- Common Table Expression to generate dates from @StartDate to @EndDate
	WITH MyCTE([CurrentDate]) AS (
		SELECT @StartDate AS [CurrentDate]
		UNION ALL
		SELECT DATEADD(DAY, 1, [CurrentDate]) AS [CurrentDate]
		FROM MyCTE 
		WHERE [CurrentDate] < @EndDate
	)

	-- Insert generated dates into dim_Date table with calculated attributes
	INSERT INTO dim_Date ([Date], Month, Quarter, Year, FiscalYear, QuarterFiscalYear, StrategyCycle)
	SELECT
		MyCTE.[CurrentDate],
		MONTH(MyCTE.[CurrentDate]),
		DATEPART(QUARTER, MyCTE.[CurrentDate]),
		YEAR(MyCTE.[CurrentDate]),
		CASE 
			WHEN MONTH(MyCTE.[CurrentDate]) >= 6 THEN YEAR(MyCTE.[CurrentDate]) 
			ELSE YEAR(MyCTE.[CurrentDate]) - 1 
		END,
		CASE 
			WHEN MONTH(MyCTE.[CurrentDate]) BETWEEN 3 AND 5 THEN '4'
			WHEN MONTH(MyCTE.[CurrentDate]) BETWEEN 6 AND 8 THEN '1'
			WHEN MONTH(MyCTE.[CurrentDate]) BETWEEN 9 AND 11 THEN '2'
			ELSE '3'
		END,
		FLOOR(DATEDIFF(YEAR, @StartDate, MyCTE.[CurrentDate]) / 3) + 1
	FROM MyCTE
	OPTION (MAXRECURSION 10000);
END TRY

BEGIN CATCH
	-- Rollback transaction in case of errors
	IF @@TRANCOUNT > 0 
	BEGIN
		ROLLBACK TRANSACTION;
	END
	PRINT('An error occurred in sp_ETL_Date: ' + ERROR_MESSAGE());
END CATCH

-- Commit transaction if successful
IF @@TRANCOUNT > 0
BEGIN
	COMMIT TRANSACTION;
END
GO

-- Execution and verification
EXEC sp_ETL_Date @StartDate = '2010-01-01', @EndDate = '2015-12-31';

SELECT * FROM dbo.dim_Date;

-- 2. Dim_Produit

CREATE OR ALTER PROCEDURE sp_ETL_Product
AS
BEGIN TRANSACTION
BEGIN TRY
	-- If the staging table exists, delete it
	IF EXISTS
		(SELECT * FROM information_schema.tables WHERE table_schema = CURRENT_USER AND table_name = 'ProductStagingTable')
	BEGIN
		DROP TABLE ProductStagingTable;
	END

	-- Create the staging table, ProductStagingTable
	CREATE TABLE ProductStagingTable (
		ProductID	INT	NOT NULL,
		Rating		DECIMAL(2,1) NOT NULL
	);

	-- Extract data from Production.ProductReview into the staging table
	INSERT INTO ProductStagingTable
	SELECT
		ProductID,
		Rating
	FROM AdventureWorks2022.Production.ProductReview;

	-- Load .csv contents into the staging table
	BULK INSERT ProductStagingTable
	FROM 'd:\11133347\Desktop\contenu_TIA\2024_2025\A2024\seance_9\TP3\produits_evaluations.csv'
	WITH (
		FIRSTROW = 3,
		FIELDTERMINATOR = ';',	-- Field delimiter
		ROWTERMINATOR = '\n',	-- Shift control to the next row
		MAXERRORS = 0,
		ERRORFILE = 'd:\11133347\Desktop\contenu_TIA\2024_2025\A2024\seance_9\TP3\ProductStagingTableErrorRows.csv',
		TABLOCK
	);

	-- Load .json contents into the staging table
	DECLARE @json NVARCHAR(MAX);

	SELECT @json = BulkColumn 
	FROM OPENROWSET (BULK 'd:\11133347\Desktop\contenu_TIA\2024_2025\A2024\seance_9\TP3\produits_evaluations.json', SINGLE_CLOB) AS j;

	INSERT INTO ProductStagingTable
	SELECT
		ProductID,
		Rating
	FROM OPENJSON(@json)
	WITH (
		ProductID INT '$.ProductID',
		Rating DECIMAL(2,1) '$.Evaluation.Score'
	);

	-- Insert data into the Product dimension table
	INSERT INTO dim_Product (ProductName, ProductNumber, DaysOnTheMarket, AverageRating, ProfitMargin, ProductStyle, EffectiveDate)
	SELECT
		p.[Name],
		p.ProductNumber,
		DATEDIFF(DAY, SellStartDate, COALESCE(SellEndDate, GETDATE())),
		AVG(pst.Rating),
		p.ListPrice - p.StandardCost,
		CASE p.Style
			WHEN 'W' THEN 'Women'
			WHEN 'M' THEN 'Men'
			WHEN 'U' THEN 'Universal'
			ELSE 'Unavailable'
		END,
		GETDATE()
	FROM AdventureWorks2022.Production.[Product] p
	LEFT JOIN ProductStagingTable pst ON p.ProductID = pst.ProductID
	GROUP BY
		p.[Name],
		p.ProductNumber,
		DATEDIFF(DAY, SellStartDate, COALESCE(SellEndDate, GETDATE())),
		p.ListPrice - p.StandardCost,
		CASE p.Style
			WHEN 'W' THEN 'Women'
			WHEN 'M' THEN 'Men'
			WHEN 'U' THEN 'Universal'
			ELSE 'Unavailable'
		END;
END TRY

BEGIN CATCH
	-- Rollback the transaction in case of errors
	IF @@TRANCOUNT > 0 
	BEGIN
		ROLLBACK TRANSACTION;
	END
	PRINT('An error occurred in sp_ETL_Product: ' + ERROR_MESSAGE());
END CATCH

-- Commit the transaction if successful
IF @@TRANCOUNT > 0
BEGIN
	COMMIT TRANSACTION;
END
GO

-- Execution and verification

EXEC sp_ETL_Produit;

SELECT * FROM dbo.ProductStagingTable;

SELECT * FROM dbo.ProductStagingTable WHERE ProductID = 921;

SELECT * FROM dbo.dim_Produit;


-- 3. Dim_Sales_Associate

CREATE OR ALTER PROCEDURE sp_ETL_SalesAssociate
AS
BEGIN TRANSACTION
BEGIN TRY
	-- Common Table Expression (CTE) to aggregate Sales Associate data
	WITH SalesAssociateCTE (LastName, FirstName, Commission, SalesYear, TotalSales) AS (
		SELECT
			p.LastName,
			p.FirstName,
			sp.CommissionPct,
			DATEPART(YEAR, soh.OrderDate),
			SUM(soh.SubTotal)
		FROM AdventureWorks2022.Sales.SalesOrderHeader soh
		INNER JOIN AdventureWorks2022.Sales.SalesPerson sp ON soh.SalesPersonID = sp.BusinessEntityID
		INNER JOIN AdventureWorks2022.Person.Person p ON sp.BusinessEntityID = p.BusinessEntityID
		GROUP BY
			p.LastName,
			p.FirstName,
			sp.CommissionPct,
			DATEPART(YEAR, soh.OrderDate)
	)

	-- Insert aggregated data into the SalesAssociate dimension table
	INSERT INTO dim_SalesAssociate (
		LastName, 
		FirstName, 
		Commission, 
		SalesYear, 
		TotalSales, 
		TotalSalesPreviousYear, 
		ChangeInSales, 
		ChangeInSalesPercentage, 
		[Difference], 
		EffectiveDate
	)
	SELECT 
		SACTE.LastName,
		SACTE.FirstName,
		FORMAT(SACTE.Commission, 'p'),
		SACTE.SalesYear,
		SACTE.TotalSales,
		LAG(SACTE.TotalSales, 1) OVER (PARTITION BY SACTE.LastName, SACTE.FirstName ORDER BY SACTE.SalesYear) AS TotalSalesPreviousYear,
		SACTE.TotalSales - LAG(SACTE.TotalSales, 1) OVER (PARTITION BY SACTE.LastName, SACTE.FirstName ORDER BY SACTE.SalesYear) AS ChangeInSales,
		FORMAT(
			(SACTE.TotalSales - LAG(SACTE.TotalSales, 1) OVER (PARTITION BY SACTE.LastName, SACTE.FirstName ORDER BY SACTE.SalesYear)) 
			/ NULLIF(LAG(SACTE.TotalSales, 1) OVER (PARTITION BY SACTE.LastName, SACTE.FirstName ORDER BY SACTE.SalesYear), 0),
			'p'
		) AS ChangeInSalesPercentage,
		CASE
			WHEN SACTE.TotalSales - LAG(SACTE.TotalSales, 1) OVER (PARTITION BY SACTE.LastName, SACTE.FirstName ORDER BY SACTE.SalesYear) IS NULL THEN 'N/A'
			WHEN SACTE.TotalSales - LAG(SACTE.TotalSales, 1) OVER (PARTITION BY SACTE.LastName, SACTE.FirstName ORDER BY SACTE.SalesYear) > 0 THEN '+'
			WHEN SACTE.TotalSales - LAG(SACTE.TotalSales, 1) OVER (PARTITION BY SACTE.LastName, SACTE.FirstName ORDER BY SACTE.SalesYear) < 0 THEN '-'
			ELSE '='
		END AS [Difference],
		GETDATE()
	FROM SalesAssociateCTE AS SACTE;
END TRY

BEGIN CATCH
	-- Rollback the transaction in case of errors
	IF @@TRANCOUNT > 0 
	BEGIN
		ROLLBACK TRANSACTION;
	END
	PRINT('An error occurred in sp_ETL_SalesAssociate: ' + ERROR_MESSAGE());
END CATCH

-- Commit the transaction if successful
IF @@TRANCOUNT > 0
BEGIN
	COMMIT TRANSACTION;
END
GO

-- Execution and verification

EXEC sp_ETL_SalesAssociate;

SELECT * FROM dbo.dim_SalesAssociate;


-- 4. Fact_Sale

CREATE OR ALTER PROCEDURE sp_ETL_Fact_Sale
AS
BEGIN TRANSACTION
BEGIN TRY
	-- Insert data into the fact table (fait_Vente)
	INSERT INTO fait_Vente (OrderQty, GrossDiscount, ProfitMargin, NoDate, NoProduit, NoSalesAssociate)
	SELECT
		SUM(sod.OrderQty), -- Total quantity ordered
		SUM(sod.UnitPrice * sod.UnitPriceDiscount * sod.OrderQty), -- Total gross discount
		SUM(sod.LineTotal) - SUM(sod.OrderQty * p1.StandardCost), -- Total profit margin
		dim_Date.NoDate, -- Date foreign key
		dim_Produit.NoProduit, -- Product foreign key
		dim_SalesAssociate.NoSalesAssociate -- Sales Associate foreign key
	FROM AdventureWorks2022.Sales.SalesOrderDetail sod
	INNER JOIN AdventureWorks2022.Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
	INNER JOIN dim_Date ON dim_Date.[Date] = soh.OrderDate
	INNER JOIN AdventureWorks2022.Production.[Product] p1 ON sod.ProductID = p1.ProductID
	INNER JOIN dim_Produit ON dim_Produit.NumeroProduit = p1.ProductNumber
	INNER JOIN AdventureWorks2022.Sales.SalesPerson sp ON soh.SalesPersonID = sp.BusinessEntityID
	INNER JOIN AdventureWorks2022.Person.Person p2 ON sp.BusinessEntityID = p2.BusinessEntityID
	INNER JOIN dim_SalesAssociate ON dim_SalesAssociate.LastName = p2.LastName
		AND dim_SalesAssociate.FirstName = p2.FirstName
		AND DATEPART(YEAR, soh.OrderDate) = dim_SalesAssociate.SalesYear
	GROUP BY dim_Date.NoDate, dim_Produit.NoProduit, dim_SalesAssociate.NoSalesAssociate;
END TRY

BEGIN CATCH
	-- Rollback the transaction in case of errors
	IF @@TRANCOUNT > 0 
	BEGIN
		ROLLBACK TRANSACTION;
	END
	PRINT('An error occurred in sp_ETL_Fact_Sale: ' + ERROR_MESSAGE());
END CATCH

-- Commit the transaction if successful
IF @@TRANCOUNT > 0
BEGIN
	COMMIT TRANSACTION;
END
GO

--Execution and verification

EXEC sp_ETL_fait_Vente;

SELECT * FROM dbo.fait_Vente;

-- Joining all dimensions with the fact table
SELECT *
FROM dbo.fait_Vente
INNER JOIN dbo.dim_Date ON dbo.fait_Vente.NoDate = dbo.dim_Date.NoDate
INNER JOIN dbo.dim_Produit ON dbo.fait_Vente.NoProduit = dbo.dim_Produit.NoProduit
INNER JOIN dbo.dim_SalesAssociate ON dbo.fait_Vente.NoSalesAssociate = dbo.dim_SalesAssociate.NoSalesAssociate;

