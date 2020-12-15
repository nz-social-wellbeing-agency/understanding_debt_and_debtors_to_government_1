###############################################################################
#' Description: SLA debt dataset creation
#'
#' Input: 
#'
#' Output: Dataset suitable for subsequent analysis
#' 
#' Author: Simon Anastasiadis
#' 
#' Dependencies: general_assembly_tool.R, dbplyr_helper_functions.R, utility_functions.R
#' 
#' Notes: runtime 10-15 minutes
#'    
#' Issues:
#' 
#' History (reverse order):
#' 2020-01-08 SA v0
#'#############################################################################

## setup ------------------------------------------------------------------------------------------

setwd("~/Network-Shares/DataLabNas/MAA/MAA2020-01/debt to government/rprogs")
source("utility_functions.R")
source("dbplyr_helper_functions.R")
source("general_assembly_tool_functions.R")
source("general_assembly_tool.R")

## user controls / parameters ---------------------------------------------------------------------

# inputs
POPULATION_FILE = "./population_and_period.xlsx"
MEASURES_FILE = "./measures.xlsx"
# outputs
OUTPUT_DATABASE = "[IDI_Sandpit]"
OUTPUT_SCHEMA = "[DL-MAA2020-01]"
ASSEMBLED_TABLE = "[d2g_long_thin]"
OUTPUT_TABLE = "[d2g_rectangular]"
# controls
DEVELOPMENT_MODE = FALSE
VERBOSE = "details" # {"all", "details", "heading", "none", "default"}

## create dataset ---------------------------------------------------------------------------------

# run assembly tool
general_data_assembly_tool(
  input_population_and_period_table = POPULATION_FILE,
  input_measures_to_assemble_table = MEASURES_FILE,
  output_database = OUTPUT_DATABASE,
  output_schema = OUTPUT_SCHEMA,
  output_table = ASSEMBLED_TABLE,
  control_development_mode = DEVELOPMENT_MODE,
  control_verbose = VERBOSE,
  control_overwrite_output_table = TRUE,
  control_run_checks_only = FALSE,
  control_skip_pre_checks = FALSE
)

# connect
db_con = create_database_connection(database = OUTPUT_DATABASE)
# compress assembled table
compress_table(db_con, OUTPUT_DATABASE, OUTPUT_SCHEMA, ASSEMBLED_TABLE)

# pivot table
pivoted_table = create_access_point(db_connection =  db_con, OUTPUT_DATABASE, OUTPUT_SCHEMA, tbl_name = ASSEMBLED_TABLE) %>%
  rename(snz_uid = identity_column) %>%
  pivot_table(label_column = "label_measure", value_column = "value_measure")

run_time_inform_user("pivoting and saving", context = "heading", print_level = VERBOSE)
written_tbl = write_to_database(pivoted_table, db_con, OUTPUT_DATABASE, OUTPUT_SCHEMA, OUTPUT_TABLE, OVERWRITE = TRUE)

# tidy
run_time_inform_user("compacting", context = "heading", print_level = VERBOSE)
compress_table(db_con, OUTPUT_DATABASE, OUTPUT_SCHEMA, OUTPUT_TABLE)

# close connection
close_database_connection(db_con)
run_time_inform_user("grand completion", context = "heading", print_level = VERBOSE)
