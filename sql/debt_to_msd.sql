/**************************************************************************************************
Title: Debt to MSD
Author: Simon Anastasiadis
Reviewer: Freya Li

Inputs & Dependencies:
- [IDI_Clean].[security].[concordance]
- [IDI_Adhoc].[clean_read_DEBT].[msd_debt]
Outputs:
- [IDI_Sandpit].[DL-MAA2020-01].[d2g_msd_debt_cases]
- [IDI_Sandpit].[DL-MAA2020-01].[d2g_msd_debt_by_quarter]

Description:
Debt, debt balances, and replayment for debtors owing money to MSD.

Intended purpose:
Identifying debtors.
Calculating number of debts and total value of debts.
Calculating change in debts - due to borrowing or repayment.

Notes:
1) As table is adhoc it needs to be linked to IDI_Clean via agency specific uid's.
   Tables exhibits excellence linking to the spine.
2) Date range for dataset is 2009-01-01 to 2019-12-31. Existing balances are created as
   new principle (amount incurred) on the openning date.
3) Total outstanding debt appears to be consistent with MSD & IRD debt comparison that
   looked at debt balances on 30 September 2016.
4) Numbers represent changes in debt owing. So principle is positive, repayments and
   write-offs are negative. A small number of repayments and write-offs are positive
   we assume these are reversals - and they increase debt in the same way as principle.
5) Outlier values
   Some principle amounts > $10k, which is unlikely to be recoverable assistance or overpayment.
   Large transactions (>$10k) make up a tiny proportion of transactions (0.1%) and 
   effect a small number of people (3%) but are a significant amount of total amounts incurred (23%).
   Current hypothesis is that such amounts are fraud (which is part of the dataset) or 
   receipt of more than one form of recoverable assistance (e.g. one for clothes, another for heating).
   Conversation with MSD data experts suggests these amounts are most likely related to fraud.
6) Values approx 0 that should be zero is of minimal concern.
   As values are dollars, all numbers should be rounded to 2 decimal places.
   Less than 0.5% of people ever have an absolute debt balance of 1-5 cents.
7) Spikes - Yes there are occurrences where people's balances change in a spike pattern
   (suddent, large change, immediately reversed) and the value of the change exceeds $1k.
   There are less than 23,000 instances of this in the dataset, about 0.05% of records
   The total amount of money involved is less than 0.5% of total debt.
   Hence this pattern is not of concern.
8) Recoverable assistance - Looking at third tier expenditure that is recoverable:
   - Amounts less than $2k are common
   - Amounts of $3-5k are uncommon
   - Amounts exceeding $5k occur but are rare
   So if we are concerned about spikes, $5k is a reasonable threshold for identifying spikes
   because people could plausably borrow and repay $2000 in recoverable assistance in a single month.
9) We would expect that a debtor's balance is always non-negative. So if we sum amounts
   incurred less repayments and write offs, then debt balances should always be >= 0.
   However, some identities have dates on which their net amount owing is negative.
   About 6000 people have negative amounts owing at some point.
   Inspection of the data suggests that this happens when repayments exceed debt, and
   rather than withdraw the excess, the amount is of debt is left negative until the individual
   borrows again (either overpayment or recoverable assistance).
   It is common to observe negative balances that later on become zero balances.
   We would not observe this if the negative balances implied that some debt incurred was not
   recorded in the dataset.
   Total amount of negative debt is small: $1.5 million (<0.1% of total debt).

Parameters & Present values:
  Current refresh = 20200120
  Prefix = d2g_
  Project schema = [DL-MAA2020-01]
 
Issues:
1) The table is not indexed. This means that analyses can be very slow. Hence this script first
   makes a copy of the table in the Sandpit and indexes it. The copy is deleted at the end.
   Without indexing cumulative-sum took 9.5 hours, with indexing it took 3 minutes.
2) Slow. Even with indexing, runtime 35 minutes.
 
History (reverse order):
2020-11-24 FL QA
2020-06-22 SA v1
2020-03-19 SA work begun
**************************************************************************************************/

/*
Prior to use, copy to sandpit and index
Unindexed code took 9.5 hours to run cumulative-sum
*/
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[msd_debt]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[msd_debt];
GO

SELECT *
INTO [IDI_Sandpit].[DL-MAA2020-01].[msd_debt]
FROM [IDI_Adhoc].[clean_read_DEBT].[msd_debt]

/* Add index */
CREATE CLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[msd_debt] (snz_msd_uid, [debt_as_at_date]);
GO

/*********** Active debt cases ***********/
/*
An incurred debt is considered active from the date it is incurred, until
the sum of all repayments (ever) is greater than the sum of the incurred debt
and all previous (ever) debts.

E.g. Monday I incurred $40 debt, Tuesday I repay $5, Wednesday I incurred $20 debt.
Then Wednesday's debt will be repaid once $60 total has been repaid.

As very small amounts outstanding are often not worth collecting and are treated as closed,
we use a cut off of $10. And so consider debt repaid when a debt has been repaid to within $10.
This avoids people having a single outstanding debt of <$10 for a long time when in practice
MSD is not seeking to collect this debt.

This calculation has some errors where multiple debts are established on the same day
due to cross-joining. However this effects less than 5% of all records so we ignore it
as it will only have a small effect on the definition of active debt cases.
*/
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_cases]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_cases];
GO

WITH
owed_to_date AS (
	SELECT a.[snz_msd_uid]
		  ,a.[debt_as_at_date]
		  ,SUM(COALESCE(b.[amount_incurred], 0)) AS total_incurred_to_date
	FROM [IDI_Sandpit].[DL-MAA2020-01].[msd_debt] a
	LEFT JOIN [IDI_Sandpit].[DL-MAA2020-01].[msd_debt] b
	ON a.snz_msd_uid = b.snz_msd_uid
	AND a.[debt_as_at_date] >= b.[debt_as_at_date]
	WHERE a.[amount_incurred] IS NOT NULL
	AND b.[amount_incurred] IS NOT NULL
	GROUP BY a.[snz_msd_uid]
		  ,a.[debt_as_at_date]
),
paid_to_date AS (
	SELECT a.[snz_msd_uid]
		  ,a.[debt_as_at_date]
		  ,SUM(COALESCE(b.[amount_repaid], 0) + COALESCE(b.[amount_written_off], 0)) AS total_paid_to_date
	FROM [IDI_Sandpit].[DL-MAA2020-01].[msd_debt] a
	LEFT JOIN [IDI_Sandpit].[DL-MAA2020-01].[msd_debt] b
	ON a.snz_msd_uid = b.snz_msd_uid
	AND a.[debt_as_at_date] >= b.[debt_as_at_date]
	GROUP BY a.[snz_msd_uid]
		  ,a.[debt_as_at_date]
)
SELECT owe.snz_msd_uid
	  ,owe.debt_as_at_date AS debt_start_date
	  ,COALESCE(MIN(paid.debt_as_at_date), '9999-01-01') AS debt_end_date
INTO [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_cases]
FROM owed_to_date AS owe
LEFT JOIN paid_to_date AS paid
ON owe.snz_msd_uid = paid.snz_msd_uid
AND owe.debt_as_at_date <= paid.debt_as_at_date
AND owe.total_incurred_to_date + paid.total_paid_to_date <= 10 -- Using $10 instead of zero as very small balances are treated as closed
GROUP BY owe.snz_msd_uid
	  ,owe.debt_as_at_date

/* Add index */
CREATE CLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_cases] (snz_msd_uid);
GO

/*********** 2017 & 2018 total debt at the end of each quarter AND 2018 principle & repayments ***********/
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_by_quarter]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_by_quarter];
GO

SELECT [snz_msd_uid]
	  ,ROUND(SUM(for_2017Q1_value), 2) AS value_2017Q1
	  ,ROUND(SUM(for_2017Q2_value), 2) AS value_2017Q2
	  ,ROUND(SUM(for_2017Q3_value), 2) AS value_2017Q3
	  ,ROUND(SUM(for_2017Q4_value), 2) AS value_2017Q4
	  ,ROUND(SUM(for_2018Q1_value), 2) AS value_2018Q1
	  ,ROUND(SUM(for_2018Q2_value), 2) AS value_2018Q2
	  ,ROUND(SUM(for_2018Q3_value), 2) AS value_2018Q3
	  ,ROUND(SUM(for_2018Q4_value), 2) AS value_2018Q4
	  ,ROUND(SUM(for_2018_incurred), 2) AS incurred_2018
	  ,ROUND(SUM(for_2018_repaid), 2) AS repaid_2018
INTO [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_by_quarter]
FROM (
	SELECT [snz_msd_uid]
		  ,[debt_as_at_date]
		  ,[amount_incurred]
		  ,[amount_repaid]
		  ,[amount_written_off]
		  /* 2017 quarters setup */
		  ,IIF([debt_as_at_date] <= '2017-03-31',
			COALESCE([amount_incurred], 0)
			+ COALESCE([amount_repaid], 0)
			+ COALESCE([amount_written_off], 0), 0) AS for_2017Q1_value
		  ,IIF([debt_as_at_date] <= '2017-06-30',
			COALESCE([amount_incurred], 0)
			+ COALESCE([amount_repaid], 0)
			+ COALESCE([amount_written_off], 0), 0) AS for_2017Q2_value
		  ,IIF([debt_as_at_date] <= '2017-09-30',
			COALESCE([amount_incurred], 0)
			+ COALESCE([amount_repaid], 0)
			+ COALESCE([amount_written_off], 0), 0) AS for_2017Q3_value
		  ,IIF([debt_as_at_date] <= '2017-12-31',
			COALESCE([amount_incurred], 0)
			+ COALESCE([amount_repaid], 0)
			+ COALESCE([amount_written_off], 0), 0) AS for_2017Q4_value
		  /* 2018 quarters setup */
		  ,IIF([debt_as_at_date] <= '2018-03-31',
			COALESCE([amount_incurred], 0)
			+ COALESCE([amount_repaid], 0)
			+ COALESCE([amount_written_off], 0), 0) AS for_2018Q1_value
		  ,IIF([debt_as_at_date] <= '2018-06-30',
			COALESCE([amount_incurred], 0)
			+ COALESCE([amount_repaid], 0)
			+ COALESCE([amount_written_off], 0), 0) AS for_2018Q2_value
		  ,IIF([debt_as_at_date] <= '2018-09-30',
			COALESCE([amount_incurred], 0)
			+ COALESCE([amount_repaid], 0)
			+ COALESCE([amount_written_off], 0), 0) AS for_2018Q3_value
		  ,IIF([debt_as_at_date] <= '2018-12-31',
			COALESCE([amount_incurred], 0)
			+ COALESCE([amount_repaid], 0)
			+ COALESCE([amount_written_off], 0), 0) AS for_2018Q4_value
		  /* incurred debt setup */
		  ,IIF(YEAR([debt_as_at_date]) = 2018, COALESCE([amount_incurred], 0), 0) AS for_2018_incurred
		  ,IIF(YEAR([debt_as_at_date]) = 2018, COALESCE([amount_repaid], 0), 0) AS for_2018_repaid
	FROM [IDI_Sandpit].[DL-MAA2020-01].[msd_debt]
) k
GROUP BY snz_msd_uid
HAVING NOT (ABS(SUM(for_2018Q1_value)) < 1
AND ABS(SUM(for_2018Q2_value)) < 1
AND ABS(SUM(for_2018Q3_value)) < 1
AND ABS(SUM(for_2018Q4_value)) < 1
AND ABS(SUM(for_2017Q1_value)) < 1
AND ABS(SUM(for_2017Q2_value)) < 1
AND ABS(SUM(for_2017Q3_value)) < 1
AND ABS(SUM(for_2017Q4_value)) < 1)

/* Add index */
CREATE CLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_by_quarter] (snz_msd_uid);
GO

/*********** join on snz_uid ***********/
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[d2g_msd_debt_cases]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[d2g_msd_debt_cases];
GO
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[d2g_msd_debt_by_quarter]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[d2g_msd_debt_by_quarter];
GO

SELECT b.snz_uid
	  ,a.*
INTO [IDI_Sandpit].[DL-MAA2020-01].[d2g_msd_debt_cases]
FROM [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_cases] a
LEFT JOIN [IDI_Clean_20200120].[security].[concordance] b
ON a.snz_msd_uid = b.snz_msd_uid

SELECT b.snz_uid
	  ,a.*
INTO [IDI_Sandpit].[DL-MAA2020-01].[d2g_msd_debt_by_quarter]
FROM [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_by_quarter] a
LEFT JOIN [IDI_Clean_20200120].[security].[concordance] b
ON a.snz_msd_uid = b.snz_msd_uid

/* Add index */
CREATE CLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[d2g_msd_debt_cases] (snz_uid);
GO
CREATE CLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[d2g_msd_debt_by_quarter] (snz_uid);
GO

/*********** remove temporary table ***********/
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[msd_debt]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[msd_debt];
GO
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_cases]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_cases];
GO
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_by_quarter]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[d2g_tmp_msd_debt_by_quarter];
GO

/*********** Useful starting reference (warning - slow without indexing) ***********/
/*
SELECT [snz_msd_uid]
      ,[amount_incurred]
      ,[debt_as_at_date]
      ,[debt_id]
      ,[amount_repaid]
      ,[amount_written_off]
	  ,SUM(
		IIF([amount_incurred] IS NULL, 0, [amount_incurred]) + 
		IIF([amount_repaid] IS NULL, 0, [amount_repaid]) + 
		IIF([amount_written_off] IS NULL, 0, [amount_written_off])) OVER(PARTITION BY [snz_msd_uid] ORDER BY [debt_as_at_date]) AS running_total
FROM [IDI_Sandpit].[DL-MAA2020-01].[msd_debt]
*/