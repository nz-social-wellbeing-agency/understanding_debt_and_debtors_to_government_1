/**************************************************************************************************
Title: Debt to IRD
Author: Simon Anastasiadis
Reviewer: Freya Li

Inputs & Dependencies:
- [IDI_Clean].[security].[concordance]
- [IDI_Adhoc].[clean_read_DEBT].[ir_debt_transactions]
- [IDI_Adhoc].[clean_read_DEBT].[ir_debt_all_cases]
Outputs:
- [IDI_Sandpit].[DL-MAA2020-01].[d2g_ird_debt_cases]
- [IDI_Sandpit].[DL-MAA2020-01].[d2g_ird_debt_by_quarter]
- [IDI_UserCode].[DL-MAA2020-01].[d2g_ird_debt_type]

Description:
Debt, debt balances, and replayment for debtors owing money to IRD.

Intended purpose:
Identifying debtors.
Calculating number of debts and total value of debts.
Calculating change in debts - due to borrowing or repayment.

Notes:
1) As tables are adhoc they need to be linked to IDI_Clean via agency specific uid's.
   Both IRD tables exhibit excellent correspondence to one-another and both link
   very well to the spine.
2) Date range for dataset is 2010Q1 to 2018Q4. About one-tenth of debt cases arise
   prior to 2009. Existing balances are created as [dbt_running_bal] amounts without
   new assessments. This is consistent with debt cases beginning pre-2010.
3) Numbers represent changes in debt owing. So principle is positive, repayments are
   negative, interest is positive when charges and negative if reversed/waived.
4) Each identity can have multiple cases, of different tax types, and case numbers are
   reused between tax types and individuals. Hence for a unique join three columns must be used:
   snz_ird_uid, snz_dbt_case_uid, and dbt_tax_type .
5) These is no requirement for debt balances to be reported quarter-by-quarter. A row/record
   only appears in the dataset if there were transactions that quarter. Hence where a balance is
   non-zero in the absence of future records, the balance should be considered unchanged.
6) The delta values are consistently the sum of their constituent parts (assessment, interest,
   penalty, repayment, remission). However, the running balances are not consistently the
   cumulative sum of the deltas. Hence the running balances must be recalculated.
7) Outliers:
   Very large positive and negative balances and delta values are observed in the data.
   Defining 'large values' as magnitudes in excess of $1 million, then a tiny fraction
   of people <0.1% have very large balances at any given time, however these balances
   can account for up to 10% of all debt owed at that time.
8) Values approximately zero that should be treated as zero are of minimal concern.
   More than 80% of the values under $10 are exactly $0.
   Inspection of individual records suggests that small positive balances (e.g. $4) are
   adjusted away to zero using negative account maintenance, with the account closed at $0.
   The account is also treated as closed on a small negative balance when there are no further
   transactions (e.g. the payment is not reversed, or penalty/interest is not applied).
9) Spikes in balances do occur (e.g. assessed for debt of $100k one quarter, and the assessment
   is reversed the next quarter). About 20% of people had a positive change in debt 2017Q1
   followed by a negative change in debt 2017Q2 (or the reverse) this was just under 20% of total
   debt balances.
   However, it is difficult to distinguish these from people who are assessed one quarter and
   repay the next, or penalties that are applied and then reversed (as an incentive for repayment).

Parameters & Present values:
  Current refresh = 20200120
  Prefix = d2g_
  Project schema = [DL-MAA2020-01]
 
Issues:
1) There may be data missing from the table.
   - Total debt is much lower than expected. An MSD & IRD debt comparison from 30 September 2016
     reports total IRD debt of $6.3 billion. A similar estimate for 2019 was given in the cross-
	 agency working group. However, simple calculations of debt from this data give values
	 closer to $4.5 billion.
   - A significant proportion of debt cases have a negative balance (-$100 or less) while the case
     is open. This suggests overpayment of debt.
   - Many of the cases with negative balances are open at the start of 2010 when the dataset begins.
     And hence might be missing data pre-2010 or are longer duration debts with more chance to miss
	 information.
   - Many negative balances occur at the end of a debt case where there continue to be repayments
     even after the debt balance goes negative.
   - The number of debtors is comparable between the Sept 2016 report (305k) and the number observed
     in the IDI data.
2) It seems unlikely that such a large proportion of people would overpay their debts, or would keep
   overpaying having already overpaid. We make the following adjustment:
   - Find the largest negative value for each debt case
   - If this value is more negative than -$10 add it back on to the debt case
   - E.g. balances of $400, $100, -$200, -$500
          would become $900, $600, $300, $0
   This adjustment makes close to $1 billion difference to the total observed debt. Hence, even if
   it is incorrect, it is clear that the many negative values are a significant concern.
3) The adhoc table is not indexed. This means that analyses can be very slow.
4) Recalculating running balance is slow (3 hours in some cases).
 
History (reverse order):
2020-11-25 FL QA
2020-07-02 SA update following input from SME
2020-06-23 SA v1
**************************************************************************************************/

/**************************************************************************************************
Prior to use, copy to sandpit and index
As learned from working with MSD debt, unindexed takes much longer to run
(runtime 2 minutes)
**************************************************************************************************/
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions];
GO

SELECT *
	,DATEFROMPARTS(
		2000 + CAST(RIGHT(dbt_date_processed,2) AS INT)
		,CASE LEFT(dbt_date_processed,3) WHEN 'JAN' THEN 1 WHEN 'APR' THEN 4 WHEN 'JUL' THEN 7 WHEN 'OCT' THEN 10 END
		,1
	) AS debt_date
INTO [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions]
FROM [IDI_Adhoc].[clean_read_DEBT].[ir_debt_transactions]
GO

/* Add index */
/* Several indexes tried, final index matches subsequent partition by.
Using other indexes performed significantly worse. */
CREATE CLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions] ([snz_ird_uid], [snz_dbt_case_uid], [dbt_tax_type]);
GO

/**************************************************************************************************
recalculate running balances
(runtime 4-13 minutes OR 3 hours)
**************************************************************************************************/
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_corrected]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_corrected];
GO

SELECT *
	,SUM(dbt_delta) OVER (PARTITION BY [snz_ird_uid], [snz_dbt_case_uid], [dbt_tax_type] ORDER BY debt_date) AS dbt_running_bal_corrected
INTO [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_corrected]
FROM [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions]
GO

/* Add index */
CREATE CLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_corrected] ([snz_ird_uid], [snz_dbt_case_uid], [dbt_tax_type]);
GO

/**************************************************************************************************
add large negative balances back in to each case
(runtime 3 minutes)
**************************************************************************************************/
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_neg_adj]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_neg_adj];
GO

SELECT a.*
	,b.min_balance
	,dbt_running_bal_corrected + COALESCE(ABS(b.min_balance), 0) AS dbt_running_bal_neg_adj
INTO [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_neg_adj]
FROM [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_corrected] a
LEFT JOIN (
	/* minimum balance for each person-case-tax_type */
	SELECT snz_ird_uid
		,snz_dbt_case_uid
		,dbt_tax_type
		,MIN(dbt_running_bal_corrected) AS min_balance
	FROM [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_corrected]
	GROUP BY snz_ird_uid, snz_dbt_case_uid, dbt_tax_type
	HAVING MIN(dbt_running_bal_corrected) < -10
) b
ON a.snz_ird_uid = b.snz_ird_uid
AND a.snz_dbt_case_uid = b.snz_dbt_case_uid
AND a.dbt_tax_type = b.dbt_tax_type
GO

/* Add index */
CREATE CLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_neg_adj] ([snz_ird_uid], [snz_dbt_case_uid], [dbt_tax_type]);
GO

/**************************************************************************************************
2017 & 2018 debt by quarters
**************************************************************************************************/
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_ird_balances]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_ird_balances];
GO

SELECT snz_ird_uid
	,dbt_tax_type
	,SUM(IIF('2017-01-01' <= end_date AND debt_date <= '2017-03-31', dbt_running_bal_neg_adj, 0)) AS Y2017Q1
	,SUM(IIF('2017-04-01' <= end_date AND debt_date <= '2017-06-30', dbt_running_bal_neg_adj, 0)) AS Y2017Q2
	,SUM(IIF('2017-07-01' <= end_date AND debt_date <= '2017-09-30', dbt_running_bal_neg_adj, 0)) AS Y2017Q3
	,SUM(IIF('2017-10-01' <= end_date AND debt_date <= '2017-12-31', dbt_running_bal_neg_adj, 0)) AS Y2017Q4
	,SUM(IIF('2018-01-01' <= end_date AND debt_date <= '2018-03-31', dbt_running_bal_neg_adj, 0)) AS Y2018Q1
	,SUM(IIF('2018-04-01' <= end_date AND debt_date <= '2018-06-30', dbt_running_bal_neg_adj, 0)) AS Y2018Q2
	,SUM(IIF('2018-07-01' <= end_date AND debt_date <= '2018-09-30', dbt_running_bal_neg_adj, 0)) AS Y2018Q3
	,SUM(IIF('2018-10-01' <= end_date AND debt_date <= '2018-12-31', dbt_running_bal_neg_adj, 0)) AS Y2018Q4
	,SUM(IIF(YEAR(debt_date) = 2018, dbt_assessment, 0)) AS incurred_2018
	,SUM(IIF(YEAR(debt_date) = 2018, dbt_payment, 0)) AS repaid_2018
INTO [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_ird_balances]
FROM (

SELECT *
	  ,CASE
		/* account has open balance and no future record */
		WHEN dbt_running_bal_neg_adj > 10 AND lead_date IS NULL THEN '9999-01-01'
		/* account has open balance */
		WHEN dbt_running_bal_neg_adj > 10 THEN DATEADD(DAY, -1, lead_date)
		/* otherwise account has a record next quarter OR account balance is too low and account is closed */
		ELSE NULL END AS end_date
FROM (
	SELECT *
		,LAG(dbt_running_bal_neg_adj, 1) OVER(PARTITION BY [snz_ird_uid],[snz_dbt_case_uid],[dbt_tax_type] ORDER BY debt_date) AS lag_balance
		,LEAD(dbt_running_bal_neg_adj, 1) OVER(PARTITION BY [snz_ird_uid],[snz_dbt_case_uid],[dbt_tax_type] ORDER BY debt_date) AS lead_balance
		,LAG(debt_date, 1) OVER(PARTITION BY [snz_ird_uid],[snz_dbt_case_uid],[dbt_tax_type] ORDER BY debt_date) AS lag_date
		,LEAD(debt_date, 1) OVER(PARTITION BY [snz_ird_uid],[snz_dbt_case_uid],[dbt_tax_type] ORDER BY debt_date) AS lead_date
	FROM [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_neg_adj]
) k1

) k2
GROUP BY snz_ird_uid, dbt_tax_type

/* Add index */
CREATE CLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_ird_balances] (snz_ird_uid);
GO

/**************************************************************************************************
join on snz_uid
**************************************************************************************************/
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[d2g_ird_debt_cases]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[d2g_ird_debt_cases];
GO
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[d2g_ird_debt_by_quarter]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[d2g_ird_debt_by_quarter];
GO

SELECT b.snz_uid
	  ,a.*
INTO [IDI_Sandpit].[DL-MAA2020-01].[d2g_ird_debt_cases]
FROM [IDI_Adhoc].[clean_read_DEBT].[ir_debt_all_cases] a
LEFT JOIN [IDI_Clean_20200120].[security].[concordance] b
ON a.snz_ird_uid = b.snz_ird_uid

SELECT b.snz_uid
	  ,a.*
INTO [IDI_Sandpit].[DL-MAA2020-01].[d2g_ird_debt_by_quarter]
FROM [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_ird_balances] a
LEFT JOIN [IDI_Clean_20200120].[security].[concordance] b
ON a.snz_ird_uid = b.snz_ird_uid

/* Add index */
CREATE CLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[d2g_ird_debt_cases] (snz_uid);
GO
CREATE CLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[d2g_ird_debt_by_quarter] (snz_uid);
GO

/**************************************************************************************************
remove temporary table
**************************************************************************************************/
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions];
GO
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_corrected]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_corrected];
GO
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_neg_adj]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[ir_debt_transactions_neg_adj];
GO
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_ird_balances]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_ird_balances];
GO

/**************************************************************************************************
view for each debt type
**************************************************************************************************/

USE [IDI_UserCode]
GO

IF OBJECT_ID('[DL-MAA2020-01].[d2g_ird_debt_type]','V') IS NOT NULL
DROP VIEW [DL-MAA2020-01].[d2g_ird_debt_type];
GO

CREATE VIEW [DL-MAA2020-01].[d2g_ird_debt_type] AS
SELECT *
	,IIF(dbt_tax_type = 'Child Support', Y2017Q1, 0) AS Y2017Q1_child_support
	,IIF(dbt_tax_type = 'Child Support', Y2017Q2, 0) AS Y2017Q2_child_support
	,IIF(dbt_tax_type = 'Child Support', Y2017Q3, 0) AS Y2017Q3_child_support
	,IIF(dbt_tax_type = 'Child Support', Y2017Q4, 0) AS Y2017Q4_child_support
	,IIF(dbt_tax_type = 'Child Support', Y2018Q1, 0) AS Y2018Q1_child_support
	,IIF(dbt_tax_type = 'Child Support', Y2018Q2, 0) AS Y2018Q2_child_support
	,IIF(dbt_tax_type = 'Child Support', Y2018Q3, 0) AS Y2018Q3_child_support
	,IIF(dbt_tax_type = 'Child Support', Y2018Q4, 0) AS Y2018Q4_child_support

	,IIF(dbt_tax_type = 'Families', Y2017Q1, 0) AS Y2017Q1_families
	,IIF(dbt_tax_type = 'Families', Y2017Q2, 0) AS Y2017Q2_families
	,IIF(dbt_tax_type = 'Families', Y2017Q3, 0) AS Y2017Q3_families
	,IIF(dbt_tax_type = 'Families', Y2017Q4, 0) AS Y2017Q4_families
	,IIF(dbt_tax_type = 'Families', Y2018Q1, 0) AS Y2018Q1_families
	,IIF(dbt_tax_type = 'Families', Y2018Q2, 0) AS Y2018Q2_families
	,IIF(dbt_tax_type = 'Families', Y2018Q3, 0) AS Y2018Q3_families
	,IIF(dbt_tax_type = 'Families', Y2018Q4, 0) AS Y2018Q4_families

	,IIF(dbt_tax_type = 'GST', Y2017Q1, 0) AS Y2017Q1_GST
	,IIF(dbt_tax_type = 'GST', Y2017Q2, 0) AS Y2017Q2_GST
	,IIF(dbt_tax_type = 'GST', Y2017Q3, 0) AS Y2017Q3_GST
	,IIF(dbt_tax_type = 'GST', Y2017Q4, 0) AS Y2017Q4_GST
	,IIF(dbt_tax_type = 'GST', Y2018Q1, 0) AS Y2018Q1_GST
	,IIF(dbt_tax_type = 'GST', Y2018Q2, 0) AS Y2018Q2_GST
	,IIF(dbt_tax_type = 'GST', Y2018Q3, 0) AS Y2018Q3_GST
	,IIF(dbt_tax_type = 'GST', Y2018Q4, 0) AS Y2018Q4_GST

	,IIF(dbt_tax_type = 'Income Tax', Y2017Q1, 0) AS Y2017Q1_income_tax
	,IIF(dbt_tax_type = 'Income Tax', Y2017Q2, 0) AS Y2017Q2_income_tax
	,IIF(dbt_tax_type = 'Income Tax', Y2017Q3, 0) AS Y2017Q3_income_tax
	,IIF(dbt_tax_type = 'Income Tax', Y2017Q4, 0) AS Y2017Q4_income_tax
	,IIF(dbt_tax_type = 'Income Tax', Y2018Q1, 0) AS Y2018Q1_income_tax
	,IIF(dbt_tax_type = 'Income Tax', Y2018Q2, 0) AS Y2018Q2_income_tax
	,IIF(dbt_tax_type = 'Income Tax', Y2018Q3, 0) AS Y2018Q3_income_tax
	,IIF(dbt_tax_type = 'Income Tax', Y2018Q4, 0) AS Y2018Q4_income_tax

	,IIF(dbt_tax_type = 'Other', Y2017Q1, 0) AS Y2017Q1_other
	,IIF(dbt_tax_type = 'Other', Y2017Q2, 0) AS Y2017Q2_other
	,IIF(dbt_tax_type = 'Other', Y2017Q3, 0) AS Y2017Q3_other
	,IIF(dbt_tax_type = 'Other', Y2017Q4, 0) AS Y2017Q4_other
	,IIF(dbt_tax_type = 'Other', Y2018Q1, 0) AS Y2018Q1_other
	,IIF(dbt_tax_type = 'Other', Y2018Q2, 0) AS Y2018Q2_other
	,IIF(dbt_tax_type = 'Other', Y2018Q3, 0) AS Y2018Q3_other
	,IIF(dbt_tax_type = 'Other', Y2018Q4, 0) AS Y2018Q4_other

	,IIF(dbt_tax_type = 'PAYE', Y2017Q1, 0) AS Y2017Q1_PAYE
	,IIF(dbt_tax_type = 'PAYE', Y2017Q2, 0) AS Y2017Q2_PAYE
	,IIF(dbt_tax_type = 'PAYE', Y2017Q3, 0) AS Y2017Q3_PAYE
	,IIF(dbt_tax_type = 'PAYE', Y2017Q4, 0) AS Y2017Q4_PAYE
	,IIF(dbt_tax_type = 'PAYE', Y2018Q1, 0) AS Y2018Q1_PAYE
	,IIF(dbt_tax_type = 'PAYE', Y2018Q2, 0) AS Y2018Q2_PAYE
	,IIF(dbt_tax_type = 'PAYE', Y2018Q3, 0) AS Y2018Q3_PAYE
	,IIF(dbt_tax_type = 'PAYE', Y2018Q4, 0) AS Y2018Q4_PAYE

	,IIF(dbt_tax_type = 'Student Loan', Y2017Q1, 0) AS Y2017Q1_student_loan
	,IIF(dbt_tax_type = 'Student Loan', Y2017Q2, 0) AS Y2017Q2_student_loan
	,IIF(dbt_tax_type = 'Student Loan', Y2017Q3, 0) AS Y2017Q3_student_loan
	,IIF(dbt_tax_type = 'Student Loan', Y2017Q4, 0) AS Y2017Q4_student_loan
	,IIF(dbt_tax_type = 'Student Loan', Y2018Q1, 0) AS Y2018Q1_student_loan
	,IIF(dbt_tax_type = 'Student Loan', Y2018Q2, 0) AS Y2018Q2_student_loan
	,IIF(dbt_tax_type = 'Student Loan', Y2018Q3, 0) AS Y2018Q3_student_loan
	,IIF(dbt_tax_type = 'Student Loan', Y2018Q4, 0) AS Y2018Q4_student_loan

FROM [IDI_Sandpit].[DL-MAA2020-01].[d2g_ird_debt_by_quarter]
GO
