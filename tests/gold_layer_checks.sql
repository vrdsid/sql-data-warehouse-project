/*
===============================================================================
                                QUALITY CHECKS
===============================================================================

Script Purpose:
    This script performs a series of quality checks to ensure the integrity, 
    consistency, and accuracy of the Gold Layer data warehouse. These checks 
    help maintain data reliability for analytical and reporting purposes by 
    validating key constraints and relationships.

Key Checks Performed:
    1. **Uniqueness Constraints**:
       - Ensures that surrogate keys in dimension tables (e.g., `customer_key`, 
         `product_key`) are unique and do not contain duplicates.
    
    2. **Referential Integrity**:
       - Verifies that all foreign key references in fact tables correctly map 
         to existing records in the associated dimension tables.
    
    3. **Data Model Connectivity**:
       - Ensures that relationships between fact and dimension tables are intact, 
         confirming that every fact record has a valid reference in the dimensions.

Usage Notes:
    - Any discrepancies found in these checks should be investigated and resolved 
      promptly to maintain data integrity.
    - Ensure that corrections align with business logic and data governance 
      standards before making updates.

===============================================================================
*/


SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  
