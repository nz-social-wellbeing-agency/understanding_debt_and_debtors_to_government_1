#####################################################################################################
#' Description: Define and summarise by groups for output
#'
#' Input: Tidied debt table for D2G project
#'
#' Output: Excel/CSV file of summaries by group
#' 
#' Author: Simon Anastasiadis
#' 
#' Dependencies: dbplyr_helper_functions.R, utility_functions.R, table_consistency_checks.R
#' 
#' Notes: Clustering is currently out of scope so this section has been commented out.
#' 
#' Issues:
#' W&S and WHP employment in IRD EMS is different from W&S and WHP in tax summary.
#' Hence 2020-07-10 we decide not to use the tax summary $ amounts from these sources.
#' 
#' History (reverse order):
#' 2020-06-24 SA v1
#####################################################################################################

## parameters -------------------------------------------------------------------------------------

# locations
PROJECT_FOLDER = "~/Network-Shares/DataLabNas/MAA/MAA2020-01/debt to government/rprogs"
SANDPIT = "[IDI_Sandpit]"
USERCODE = "[IDI_UserCode]"
OUR_SCHEMA = "[DL-MAA2020-01]"

# inputs
TIDY_TABLE = "[d2g_tidy_table]"

# outputs
GROUP_LIST = "[d2g_summary_group_list]"
RESULTS_FILE_COUNT = "d2g_group_summary_count.xlsx"
RESULTS_FILE_SUM = "d2g_group_summary_sum.xlsx"

# controls
DEVELOPMENT_MODE = FALSE
VERBOSE = "all" # {"all", "details", "heading", "none"}

## setup ------------------------------------------------------------------------------------------

setwd(PROJECT_FOLDER)
source("utility_functions.R")
source("dbplyr_helper_functions.R")
source("table_consistency_checks.R")

## access dataset ---------------------------------------------------------------------------------

run_time_inform_user("GRAND START", context = "heading", print_level = VERBOSE)

db_con = create_database_connection(database = "IDI_Sandpit")
working_table = create_access_point(db_con, SANDPIT, OUR_SCHEMA, TIDY_TABLE)

if(DEVELOPMENT_MODE)
  working_table = working_table %>% filter(snz_uid %% 100 == 0)

## define groups ----------------------------------------------------------------------------------

run_time_inform_user("defining groups", context = "heading", print_level = VERBOSE)

#### create table to store groups -------------------------------------------------------

output_columns = list(group_name = "[varchar](40) NOT NULL",
                      snz_uid = "[int] NOT NULL")

create_table(db_con, SANDPIT, OUR_SCHEMA, GROUP_LIST, output_columns, OVERWRITE = TRUE)

run_time_inform_user("table created", context = "details", print_level = VERBOSE)

#### list of group columns --------------------------------------------------------------
#
# the following is a list of data.frame columns/groups.
# Every column in this list defines a group as follows:
# - the name of the group is the name of the defining column
# - an identity is in the group if it has a 1 in the defining column

group_defining_columns = c(
  "debtor_MSD",        # any MSD debt
  "debtor_IRD",        # any IRD debt
  "any_debt",        # debt to one or more agencies
  "debtor_2_agencies",        # debt to two more agencies
  "entered_into_debt",        # people entering into debt
  "entered_into_debt_strict", # people entering into debt who do not exit in 2018
  "exited_from_debt",        # people exiting from debt
  "exited_from_debt_strict", # people exiting from debt who did not enter in 2018
  "went_bankrupt",        # people who go bankrupt
  "main_activity_employed",        # main activity = W&S or WHP
  "main_activity_study",        # main activity = study
  "main_activity_NEET",        # main activity = NEET
  "beneficiary_debtor",        # beneficiaries with debt
  "beneficiary_debtor_in_hhld_w_kids",        # beneficiaries with debt in hhlds with children
  "debtor_in_hhld_w_kids",        # people with debt in hhlds with children
  "whole_population",        # overview of whole population
  "beneficiary",        # whole population beneficiaries
  "beneficiary_in_hhld_w_kids",        # whole population beneficiaries in hhlds with children
  "in_hhld_w_kids",        # whole population hhlds with children
  "debtor_IRD_child_support",        # debtors owing child support
  "debtor_IRD_families",        # debotrs owing families payments
  "debtor_IRD_GST",        # debtors owing GST
  "debtor_IRD_income_tax",        # debtors owing income tax
  "debtor_IRD_PAYE",        # debtors owing PAYE
  "debtor_IRD_student_loan",        # debtors owing student loan
  "debtor_msd_JS",        # debtors receiving Jobseeker
  "debtor_msd_SPS",        # debtors receiving sole parent support
  "debtor_msd_SLP",        # debtors receiving supported living payment
  "debtor_msd_NZS",        # debtors receiving NZ super
  "long_term_MSD_debtor",        # debtors who have had debt to MSD for an extended time
  "short_term_MSD_debtor",        # debtors who only have short term debt to MSD
  "long_term_IRD_debtor",        # debtors who have had debt to IRD for an extended time
  "short_term_IRD_debtor"        # debtors who only have short term debt to IRD
)

#### define groups from list ------------------------------------------------------------

run_time_inform_user("starting appending groups to table", context = "details", print_level = VERBOSE)

for(col in group_defining_columns){
  
  assert(col %in% colnames(working_table), "group defining column missing from input table")
  
  table_to_append = working_table %>%
    filter(!!sym(col) == 1) %>%
    mutate(group_name = col) %>%
    select(group_name, snz_uid)
  
  run_time_inform_user(glue::glue("appending group {col}"), context = "all", print_level = VERBOSE)
  append_database_table(db_con, SANDPIT, OUR_SCHEMA, GROUP_LIST,
                        list_of_columns = names(output_columns), table_to_append)
}

run_time_inform_user("finished appending groups to table", context = "details", print_level = VERBOSE)

#### clustering -------------------------------------------------------------------------
#
# out of scope for present deadline

# local_table = working_table %>%
#   filter(!is.na(dbt_2018Q4_r)) %>%
#   select("snz_uid",
#          ...
#          "ird_2018Q2",
#          ...
#          "msd_2018Q1",
#          ...) %>%
#   collect() %>%
#   # debt balance changes
#   mutate(...
#          ird_2018Q3_c = coalesce(ird_2018Q3, 0) - coalesce(ird_2018Q2, 0),
#          msd_2018Q1_c = coalesce(msd_2018Q1, 0) - coalesce(msd_2017Q4, 0),
#          ...
#   )%>%
#   # log all financial variables
#   mutate(...
#          msd_2018Q4 = sign(msd_2018Q4) * log(1 + abs(msd_2018Q4)),
#          ...
#   ) %>%
#   # replace missings with zero
#   mutate_if(is.double, ~{coalesce(.x, 0)})

# determining optionmal choice of number of clusters
# for(k in 2:30){
#   cluster_results = clara(local_table[,-1], k = k, samples = 5, sampsize = 1000, pamLike = TRUE)
#   print(glue::glue("k = {k}, silloette = {cluster_results$silinfo$avg.width}"))
# }
# results 2 is best - but 2 will be IRD and MSD separately.
# 8 is competitive, so we use this to allow for variation between IRD and MSD debtors

# run_time_inform_user("begin clustering", context = "details", print_level = VERBOSE)
# cluster_results = clara(local_table[,-1], k = 8, samples = NUM_CLUSTERING_SAMPLES, sampsize = 2000, pamLike = TRUE)
# run_time_inform_user("finished clustering", context = "details", print_level = VERBOSE)
# 
# local_cluster_groups = local_table %>%
#   cbind(cluster = cluster_results$clustering) %>%
#   mutate(group_name = paste0('cluster', cluster)) %>%
#   select(group_name, snz_uid)
# 
# run_time_inform_user("copying clusters to sql", context = "all", print_level = VERBOSE)
# remote_cluster_groups = copy_r_to_sql(db_con, SANDPIT, OUR_SCHEMA, CLUSTER_LIST, local_cluster_groups, OVERWRITE = TRUE)
# 
# run_time_inform_user("appending clusters to list of groups", context = "all", print_level = VERBOSE)
# append_database_table(db_con, SANDPIT, OUR_SCHEMA, GROUP_LIST,
#                       list_of_columns = names(output_columns), remote_cluster_groups)

#### tidy up ----------------------------------------------------------------------------

# index
run_time_inform_user("indexing", context = "details", print_level = VERBOSE)
result = create_clustered_index(db_con, SANDPIT, OUR_SCHEMA, GROUP_LIST, "snz_uid")

## join and summarise -----------------------------------------------------------------------------

run_time_inform_user("loading summary into R", context = "heading", print_level = VERBOSE)

# connect to group table
groups_members = create_access_point(db_con, SANDPIT, OUR_SCHEMA, GROUP_LIST)

#### list of columns to summarise -------------------------------------------------------

columns_to_count = c(
  "area_type=Large urban area",
  "area_type=Major urban area",
  "area_type=Medium urban area",
  "area_type=Rural other",
  "area_type=Rural settlement",
  "area_type=Small urban area",
  "audit_gss",
  "bankrupt",
  "bankruptcy_process",
  "ben_JS",
  "ben_SPS",
  "ben_SLP",
  "ben_NZS",
  "ben_OTH",
  "birth_year",
  "child_at_address",
  "death_in_family",
  "deprivation",
  "employment_end",
  "end_official_relationship",
  "eth_asian",
  "eth_european",
  "eth_maori",
  "eth_other",
  "eth_pacific",
  "family_violence_experience",
  "highest_qualification",
  "ird_long_term_debtor",
  "ird_num_debts",
  # "ird_num_debts_end",
  # "ird_num_debts_start",
  "is_alive",
  "life_satisfaction",
  "life_worthwhile",
  "msd_long_term_debtor",
  "msd_num_debts",
  # "msd_num_debts_end",
  # "msd_num_debts_start",
  "mwi_afford_300_item",
  "mwi_enough_income",
  "mwi_material_wellbeing_index",
  "mwi_not_pay_bills_on_time",
  "number_dependent_children",
  "recent_migrant",
  "recent_parent",
  "region_code=01",
  "region_code=02",
  "region_code=03",
  "region_code=04",
  "region_code=05",
  "region_code=06",
  "region_code=07",
  "region_code=08",
  "region_code=09",
  "region_code=12",
  "region_code=13",
  "region_code=14",
  "region_code=15",
  "region_code=16",
  "region_code=17",
  "region_code=18",
  "residential_population_indicator",
  "sex_code=1", # male
  "sex_code=2", # female
  "sf12_mental_health",
  "sf12_physical_health",
  "sickness_benefit_start",
  "spine_indicator",
  "start_official_relationship",
  "main_activity_employed",
  "main_activity_study",
  "main_activity_NEET",
  "days_benefit_r",
  "days_corrections_any_r",
  "days_corrections_prison_r",
  "days_dead_r",
  "days_employed_r",
  "days_neet_r",
  "days_overseas_r",
  "days_sickness_benefit_r",
  "days_social_housing_r",
  "days_study_r",
  "days_benefit_180",
  "days_corrections_any_180",
  "days_corrections_prison_180",
  "days_dead_180",
  "days_employed_180",
  "days_neet_180",
  "days_overseas_180",
  "days_sickness_benefit_180",
  "days_social_housing_180",
  "days_study_180",
  "entry_Q1",
  "entry_Q2",
  "entry_Q3",
  "entry_Q4",
  "entry_2018",
  "exit_Q1",
  "exit_Q2",
  "exit_Q3",
  "exit_Q4",
  "exit_2018",
  "total_income_r",
  # "WAS_income_r",
  # "WHP_income_r",
  "BEN_income_r",
  "ird_2018Q4_r",
  "msd_2018Q4_r",
  "dbt_2018Q4_r",
  "ird_change_r",
  "msd_change_r",
  "dbt_change_r",
  "msd_2018_new_debt_r",
  "ird_2018_new_debt_r",
  "msd_2018_repaid_r",
  "ird_2018_repaid_r"
)

columns_to_sum = c(
  "BEN_income",
  "ird_2017Q1",
  "ird_2017Q2",
  "ird_2017Q3",
  "ird_2017Q4",
  "ird_2018Q1",
  "ird_2018Q2",
  "ird_2018Q3",
  "ird_2018Q4",
  "ird_Y2017Q1_child_support",
  "ird_Y2017Q1_families",
  "ird_Y2017Q1_GST",
  "ird_Y2017Q1_income_tax",
  "ird_Y2017Q1_other",
  "ird_Y2017Q1_PAYE",
  "ird_Y2017Q1_student_loan",
  "ird_Y2017Q2_child_support",
  "ird_Y2017Q2_families",
  "ird_Y2017Q2_GST",
  "ird_Y2017Q2_income_tax",
  "ird_Y2017Q2_other",
  "ird_Y2017Q2_PAYE",
  "ird_Y2017Q2_student_loan",
  "ird_Y2017Q3_child_support",
  "ird_Y2017Q3_families",
  "ird_Y2017Q3_GST",
  "ird_Y2017Q3_income_tax",
  "ird_Y2017Q3_other",
  "ird_Y2017Q3_PAYE",
  "ird_Y2017Q3_student_loan",
  "ird_Y2017Q4_child_support",
  "ird_Y2017Q4_families",
  "ird_Y2017Q4_GST",
  "ird_Y2017Q4_income_tax",
  "ird_Y2017Q4_other",
  "ird_Y2017Q4_PAYE",
  "ird_Y2017Q4_student_loan",
  "ird_Y2018Q1_child_support",
  "ird_Y2018Q1_families",
  "ird_Y2018Q1_GST",
  "ird_Y2018Q1_income_tax",
  "ird_Y2018Q1_other",
  "ird_Y2018Q1_PAYE",
  "ird_Y2018Q1_student_loan",
  "ird_Y2018Q2_child_support",
  "ird_Y2018Q2_families",
  "ird_Y2018Q2_GST",
  "ird_Y2018Q2_income_tax",
  "ird_Y2018Q2_other",
  "ird_Y2018Q2_PAYE",
  "ird_Y2018Q2_student_loan",
  "ird_Y2018Q3_child_support",
  "ird_Y2018Q3_families",
  "ird_Y2018Q3_GST",
  "ird_Y2018Q3_income_tax",
  "ird_Y2018Q3_other",
  "ird_Y2018Q3_PAYE",
  "ird_Y2018Q3_student_loan",
  "ird_Y2018Q4_child_support",
  "ird_Y2018Q4_families",
  "ird_Y2018Q4_GST",
  "ird_Y2018Q4_income_tax",
  "ird_Y2018Q4_other",
  "ird_Y2018Q4_PAYE",
  "ird_Y2018Q4_student_loan",
  "msd_2017Q1",
  "msd_2017Q2",
  "msd_2017Q3",
  "msd_2017Q4",
  "msd_2018Q1",
  "msd_2018Q2",
  "msd_2018Q3",
  "msd_2018Q4",
  "total_income",
  # "WAS_income",
  # "WHP_income",
  "msd_JS_2018Q1",
  "msd_JS_2018Q2",
  "msd_JS_2018Q3",
  "msd_JS_2018Q4",
  "msd_SPS_2018Q1",
  "msd_SPS_2018Q2",
  "msd_SPS_2018Q3",
  "msd_SPS_2018Q4",
  "msd_SLP_2018Q1",
  "msd_SLP_2018Q2",
  "msd_SLP_2018Q3",
  "msd_SLP_2018Q4",
  "msd_NZS_2018Q1",
  "msd_NZS_2018Q2",
  "msd_NZS_2018Q3",
  "msd_NZS_2018Q4",
  "msd_OTH_2018Q1",
  "msd_OTH_2018Q2",
  "msd_OTH_2018Q3",
  "msd_OTH_2018Q4",
  "msd_2018_new_debt",
  "ird_2018_new_debt",
  "msd_2018_repaid",
  "ird_2018_repaid"
)

#### summarise distribution for each column - count -------------------------------------

distribution_summary_function = function(working_table, group_table, column_name){
  results = working_table %>%
    left_join(groups_members, by = "snz_uid") %>%
    group_by(group_name, !!sym(column_name)) %>%
    summarise(count = n()) %>%
    mutate(measure = column_name) %>%
    select(group_name, measure, value = !!sym(column_name), count) %>%
    collect()
}

output = list()

for(col in columns_to_count){
  assert(col %in% colnames(working_table), glue::glue("column name {col} is not in table"))
  
  output[[col]] = distribution_summary_function(working_table, groups_members, col)
  
  run_time_inform_user(glue::glue("summarising measure {col}"), context = "all", print_level = VERBOSE)
}

results_count = data.table::rbindlist(output)

run_time_inform_user("counting complete", context = "details", print_level = VERBOSE)


#### summarise distribution for each column - sum ---------------------------------------

totals_summary_function = function(working_table, group_table, column_name){
  results = working_table %>%
    left_join(groups_members, by = "snz_uid") %>%
    group_by(group_name) %>%
    summarise(total = sum(!!sym(column_name), na.rm = TRUE),
              count = sum(ifelse(is.na(!!sym(column_name)), 0, 1), na.rm = TRUE)) %>%
    mutate(measure = column_name) %>%
    select(group_name, measure, total, count) %>%
    collect()
}

output = list()

for(col in columns_to_sum){
  assert(col %in% colnames(working_table), glue::glue("column name {col} is not in table"))
  
  output[[col]] = totals_summary_function(working_table, groups_members, col)
  
  run_time_inform_user(glue::glue("summarising measure {col}"), context = "all", print_level = VERBOSE)
}

results_sum = data.table::rbindlist(output)

run_time_inform_user("summing complete", context = "details", print_level = VERBOSE)

## write output to excel --------------------------------------------------------------------------

# close connection
close_database_connection(db_con)

run_time_inform_user("saving output table for excel", context = "heading", print_level = VERBOSE)

results_count = tidyr::spread(results_count, "group_name", "count")

xlsx::write.xlsx(results_count, file = RESULTS_FILE_COUNT, row.names = FALSE, sheetName = "count")
xlsx::write.xlsx(results_sum, file = RESULTS_FILE_SUM, row.names = FALSE, sheetName = "sum")

run_time_inform_user("grand completion", context = "heading", print_level = VERBOSE)
