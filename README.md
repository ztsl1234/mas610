# mas610
mas610

## Exercise 2: Regulatory Data Model Transformation (MAS 610 / Basel III)
## Objective: Assess SQL/data-modeling logic and financial domain reasoning.

### Setup: Provide three CSV tables:
accounts, loans, and collateral.

### Task Description:
• Write SQL/PySpark to produce a normalized output table aligned to a simplified MAS 610 “Balance Sheet – Loans and Advances” schema.
• Include logic to handle missing collateral values (default risk weight = 100%).
• Output both JSON and Parquet formats.

### Evaluation Focus:
• Join logic and data type discipline
• Modular, readable SQL or PySpark
• Business-rule translation to data logic
• Testing for edge cases / null handling

### Defining a Simplified MAS 610 Loans and Advances Schema

Based on MAS 610 guidelines (https://www.mas.gov.sg/~/media/MAS/Regulations%20and%20Financial%20Stability/Regulations%20Guidance%20and%20Licensing/Commercial%20Banks/Regulations%20Guidance%20and%20Licensing/Notices/MAS%20Notice%20610%20Appendix%201.pdf) and to provide a granular view of loan exposure, a simplified, normalized schema for our target table, `mas_610_loans_and_advances`, can be defined as follows:

| Column Name | Data Type | Description |
|---|---|---|
| `loan_id` | STRING | Unique identifier for each loan facility. |
| `customer_id` | STRING | Unique identifier for the borrower. |
| `loan_type` | STRING | Type of loan (e.g., Term Loan, Revolving Credit). |
| `currency` | STRING | The currency of the loan. |
| `principal_amount` | DECIMAL(18, 2) | The total outstanding principal of the loan. |
| `collateral_id` | STRING | Unique identifier for the associated collateral. |
| `collateral_value` | DECIMAL(18, 2) | The market value of the collateral. |
| `secured_portion` | DECIMAL(18, 2) | The portion of the loan covered by collateral. |
| `unsecured_portion` | DECIMAL(18, 2) | The portion of the loan not covered by collateral. |
| `risk_weight` | INTEGER | The risk weight assigned to the unsecured portion. |
| `reporting_date` | DATE | The date of the reporting period. |

