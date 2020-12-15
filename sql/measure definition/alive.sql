/**************************************************************************************************
Title: Period a person is alive
Author: Simon Anastasiadis
Reviewer: Marianna Pekar, Joel Bancolita

Inputs & Dependencies:
- [IDI_Clean].[data].[personal_detail]
Outputs:
- [IDI_UserCode].[DL-MAA2020-01].[d2g_alive]

Description:
The period we can be confident that a person is alive.
This is birth to death where both birth and death are available.
If death is not available, we assume people do not live beyond 130 years.

Intended purpose:
Suitable for creating indicators for when/whether a person is alive.
Expected to use to filter datasets for whether people are alive.
 
Notes:
1) Only year and month of death are available in the IDI. Day of birth and day of death
   are considered identifying. Hence all births happen on the 15th of the month and
   all deaths happen on the 28th.
2) Future births and deaths are excluded. This is applied via a filter on the year of
   birth and year of death.

Parameters & Present values:
  Current refresh = 20200120
  Prefix = d2g_
  Project schema = [DL-MAA2020-01]
  Limit on future births & deaths = 2021

Issues:
 
History (reverse order):
2020-07-22 JB QA
2020-07-16 MP QA
2020-02-28 SA v1
**************************************************************************************************/

/* Set database for writing views */
USE IDI_UserCode
GO

/* Clear existing view */
IF OBJECT_ID('[DL-MAA2020-01].[d2g_alive]','V') IS NOT NULL
DROP VIEW [DL-MAA2020-01].[d2g_alive];
GO

/* Create view */
CREATE VIEW [DL-MAA2020-01].[d2g_alive] AS
SELECT *
FROM (
	SELECT snz_uid
		,DATEFROMPARTS([snz_birth_year_nbr], [snz_birth_month_nbr], 15) AS [start_date]
		,IIF( [snz_deceased_year_nbr] IS NOT NULL AND [snz_deceased_month_nbr] IS NOT NULL,
			DATEFROMPARTS([snz_deceased_year_nbr], [snz_deceased_month_nbr], 28),
			DATEFROMPARTS([snz_birth_year_nbr] + 130, [snz_birth_month_nbr], 28)) AS [end_date]
	FROM [IDI_Clean_20200120].[data].[personal_detail]
	WHERE [snz_person_ind] = 1
	AND [snz_birth_year_nbr] IS NOT NULL
	AND [snz_birth_month_nbr] IS NOT NULL
	AND [snz_birth_year_nbr] <= 2021		-- exclude people born in the future
	AND ([snz_birth_year_nbr] IS NULL OR [snz_deceased_year_nbr] < 2021) -- exclude people with death records for the future
) k
WHERE [start_date] <= [end_date];
GO
