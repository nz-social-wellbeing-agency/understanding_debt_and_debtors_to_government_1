# Understanding_debt_and_debtors_to_government_1
Initial work to understand debt owed to government agencies and the characteristicsw of people who owe debt.

## Overview
This analysis provides an overview of debt owed to the Ministry of Social Development (MSD) and Inland Revenue (IR) in New Zealand in 2018. It considers people who owe debt to only one, or to both, government agencies.
It considers the characteristics of people who owe debt, and how debt changed over the year.
This code should be read and used in alongside the accompanying report: **Understanding debt and debtors to government:
Focus on MSD and IR debt**.

## Dependencies
It is necessary to have an IDI project if you wish to run the code.
 Visit the Stats NZ website for more information about this. 
This analysis has been developed for the IDI_Clean_20200120 refresh of the IDI.
 As changes in database structure can occur between refreshes, the initial preparation
 of the input information may require updating to run the code in other refreshes.

The R code makes use of several publicly available R packages. Stats NZ who maintain the IDI
have already installed in the IDI the all the key packages that this analysis depends on. Should the version of 
these packages be important, this analysis was conducted using `odbc` version 1.2.3,
 `DBI` version 1.1.0, `dplyr` version 1.0.0, and `dbplyr` version 1.4.4.

If applying this code to another environment other than the IDI, several features of the environment
 are required: First, R and some database manager (such as SQL Server) must be installed. Second, these 
must be configured such that R can pass commands to, and retrieve results from, the database. Once the 
environment is configured correctly, then adjustments to the code in response to the new environment can
 be considered.

## Folder descriptions
This repositry contains all the core code to assemble the data and run the analysis.

* **sql:** All the definitions of the inputs are found here. This folder contains subfolders for population definitions and measure definitions. These definitions can be used independently of this project.
* **rprogs:** This folder contains all the R scripts for executing each stage of the analysis and constructing the resulting output table.

Note that as the Dataset Assembly Tool was developed during the course of this project, this project includes an early
version of the assembly tool. Researchers intersted in the tool itself are advised to see its separate GitHub project.
Documentation for the Dataset Assembly Tool can be found on our [website](https://swa.govt.nz/publications/guidance/). Interested readers are advised to view both the primer and the training presentation.
The GitHub page for the assembly tool is [here](https://github.com/nz-social-wellbeing-agency/dataset_assembly_tool).

## Instructions to run the project

Prior to running the project be sure to review the associated report and documentation.

1. Setup SQL inputs, population and measures.
	* Make any required changes to the SQL definition scripts.
	* Run all scripts inside the SQL folder
2. Test the performance of the assembly tool.
	* Enter the connection details at the top of dbplyr_helper_functions.R
	* Run automated_tests.R to confirm all tests run correctly
3. Assemble the data for analysis.
	* Run run_assembly.R
4. Clean the prepared table and create summary output.
	* Run tidy_variables.R
	* Run cross_tab_summaries.R and output_group_summaries.R

## Getting Help
If you have any questions email info@swa.govt.nz

