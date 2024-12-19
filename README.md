# AdventureWorks Star Schema Data Warehouse

This project focuses on designing and implementing the first star schema for AdventureWorks' data warehouse to facilitate the analysis of sales. The solution emphasizes creating dimensions and fact tables that support straightforward queries, enabling business users to extract actionable insights with ease.

---

## 📊 **Star Schema Overview**

### Fact Table:
- **`fait_Vente`**: The sales fact table that aggregates sales metrics by product, date, and sales associate.

### Dimensions:
- **`dim_Date`**: Provides detailed date attributes, including fiscal and calendar data.
- **`dim_Product`**: Contains detailed product attributes, including profit margins and customer evaluations.
- **`dim_SalesAssociate`**: Contains sales associate attributes and performance metrics.

---

## 📋 **Key Requirements and Features**

### 1. **Date Dimension (`dim_Date`)**

- **Attributes:**
  - Date in `datetime` format.
  - Calendar month (1–12) with constraints ensuring valid values.
  - Calendar quarter (1–4) with constraints ensuring valid values.
  - Calendar year.
  - Fiscal year (AdventureWorks’ fiscal year starts on June 1 and ends on May 31).
  - Fiscal quarter (1–4) with constraints ensuring valid values.
  - Strategic planning cycle (3-year intervals, calculated dynamically).

- **Highlights:**
  - Dynamic handling of fiscal year and strategic cycle calculations.
  - Includes all dates between a user-specified `StartDate` and `EndDate`.

---

### 2. **Product Dimension (`dim_Product`)**

- **Attributes:**
  - Product name.
  - Product number.
  - Days on the market (calculated as the difference between today and the product's start date, ensuring values are ≥ 0).
  - Average customer evaluations (calculated from CSV, JSON files, and database tables).
  - Profit margin (indexed for optimized queries).
  - Product style (`Women`, `Men`, `Universal`, `Unavailable` with constraints ensuring valid values).

- **Highlights:**
  - Integrates multiple sources (CSV, JSON, and transactional data) for calculating product evaluations.
  - Indexed on profit margin to optimize queries on high-demand attributes.
  - Metadata added for better documentation of product styles.

---

### 3. **Sales Associate Dimension (`dim_SalesAssociate`)**

- **Attributes:**
  - Sales associate's last and first name.
  - Commission percentage (formatted as `%`).
  - Sales year.
  - Total sales for the year.
  - Total sales for the previous year.
  - Sales difference (current year vs. previous year).
  - Percentage difference in sales.
  - Indicator for performance (`+`, `-`, `=`, or `N/A` if `NULL`).

- **Highlights:**
  - Enables tracking and comparing yearly sales performance.
  - Simplifies querying key sales metrics for associates.

---

### 4. **Fact Table (`fait_Vente`)**

- **Attributes:**
  - Quantity sold.
  - Total discount value.
  - Perceived profit (calculated using the standard cost of the product).

- **Highlights:**
  - Aggregates data from all dimensions to provide comprehensive sales insights.
  - Directly links to `dim_Date`, `dim_Product`, and `dim_SalesAssociate` for seamless analysis.

---

## 🚀 **Purpose and Use Cases**

The designed schema empowers AdventureWorks to:

- Analyze sales performance by product, date, and sales associate.
- Track and optimize product profitability and customer satisfaction.
- Monitor sales associate performance and reward top contributors.
- Generate actionable insights through simple, no-calculation queries.

---

## 🛠 **Technologies Used**

- **SQL Server** for database design and ETL processes.
- **ETL Scripts** to load data into dimensions and fact tables dynamically.
- **Constraints and Indexing** for ensuring data quality and optimizing query performance.
- **Metadata** to enhance data documentation and usability.

---

This project demonstrates my expertise in designing efficient data warehouse solutions, creating ETL processes, and ensuring data quality and performance optimization. It is ideal for companies prioritizing data-driven decision-making! ✨

