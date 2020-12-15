#####################################################################################################
#' Description: Tidy assembled data
#'
#' Input: Rectangular debt table produced by run_assembly for D2G project
#'
#' Output: Tidied debt table for D2G project
#' 
#' Author: Simon Anastasiadis
#' 
#' Dependencies: dbplyr_helper_functions.R, utility_functions.R, table_consistency_checks.R
#' 
#' Notes: 
#' 
#' Issues:
#' This script / dataset commonly records zeros as NAs. This was done for ease of counting:
#' counting a 0/1 variable will return the number of rows in the entire dataset, but counting
#' a NA/1 variable will return only the number of 1's.
#' However this can create confusion between true NAs and true zeros.
#' We recommend using a more sophisticated counting function rather than recoding NAs as zero
#' for all future projects.
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
ASSEMBLED_TABLE = "[d2g_rectangular]"

# outputs
TIDY_TABLE = "[d2g_tidy_table]"

# controls
DEVELOPMENT_MODE = FALSE
VERBOSE = "details" # {"all", "details", "heading", "none"}

## setup ------------------------------------------------------------------------------------------

setwd(PROJECT_FOLDER)
source("utility_functions.R")
source("dbplyr_helper_functions.R")
source("table_consistency_checks.R")

## access dataset ---------------------------------------------------------------------------------

run_time_inform_user("GRAND START", context = "heading", print_level = VERBOSE)

db_con = create_database_connection(database = "IDI_Sandpit")

working_table = create_access_point(db_con, SANDPIT, OUR_SCHEMA, ASSEMBLED_TABLE)

if(DEVELOPMENT_MODE)
  working_table = working_table %>% filter(snz_uid %% 100 == 0)

## error checking ---------------------------------------------------------------------------------

run_time_inform_user("error checks begun", context = "heading", print_level = VERBOSE)

# one person per period
assert_all_unique(working_table, c('snz_uid', 'label_summary_period'))
# at least 1000 rows
assert_size(working_table, ">", 1000)

run_time_inform_user("error checks complete", context = "heading", print_level = VERBOSE)

## tidy dataset variables -------------------------------------------------------------------------

# during development inspect state of table
# explore::explore_shiny(collect(working_table))

#### tidy non-zeros and missing -----------------------------------------------
working_table = working_table %>%
  mutate(family_violence_experience = ifelse(family_violence_experience >= 1, 1, NA),
         child_at_address = ifelse(child_at_address >= 1, 1, NA),
         # remove zeros, so counting records will only count 1's
         eth_asian = ifelse(eth_asian > 0, 1, NA),
         eth_european = ifelse(eth_european > 0, 1, NA),
         eth_maori = ifelse(eth_maori > 0, 1, NA),
         eth_other = ifelse(eth_other + eth_MELAA > 0, 1, NA),
         eth_pacific = ifelse(eth_pacific > 0, 1, NA),
         # 75 is chosen as an upper bound as non-response codes are all larger than 75
         # e.g. SF12 have a max value of 71, and MWI uses 77 as non-response indicator.
         life_satisfaction = ifelse(life_satisfaction >= 75, NA, life_satisfaction),
         life_worthwhile = ifelse(life_worthwhile >= 75, NA, life_worthwhile),
         mwi_afford_300_item = ifelse(mwi_afford_300_item >= 75, NA, mwi_afford_300_item),
         mwi_enough_income = ifelse(mwi_enough_income >= 75, NA, mwi_enough_income),
         mwi_material_wellbeing_index = ifelse(mwi_material_wellbeing_index >= 75, NA, mwi_material_wellbeing_index),
         mwi_not_pay_bills_on_time = ifelse(mwi_not_pay_bills_on_time >= 75, NA, mwi_not_pay_bills_on_time),
         sf12_mental_health = ifelse(sf12_mental_health >= 75, NA, sf12_mental_health),
         sf12_physical_health = ifelse(sf12_physical_health >= 75, NA, sf12_physical_health),
         # highest qualification
         highest_qualification = ifelse(highest_qualification %in% c(1,2,3,4,5,6,7,8,9,10), highest_qualification, NA),
         highest_qualification = ifelse(16 <= 2018 - birth_year & 2018 - birth_year <= 65, highest_qualification, NA),
         # zero & negative debt balances
         ird_2017Q1 = ifelse(ird_2017Q1 < 1, NA, ird_2017Q1),
         ird_2017Q2 = ifelse(ird_2017Q2 < 1, NA, ird_2017Q2),
         ird_2017Q3 = ifelse(ird_2017Q3 < 1, NA, ird_2017Q3),
         ird_2017Q4 = ifelse(ird_2017Q4 < 1, NA, ird_2017Q4),
         ird_2018Q1 = ifelse(ird_2018Q1 < 1, NA, ird_2018Q1),
         ird_2018Q2 = ifelse(ird_2018Q2 < 1, NA, ird_2018Q2),
         ird_2018Q3 = ifelse(ird_2018Q3 < 1, NA, ird_2018Q3),
         ird_2018Q4 = ifelse(ird_2018Q4 < 1, NA, ird_2018Q4),
         msd_2017Q1 = ifelse(msd_2017Q1 < 1, NA, msd_2017Q1),
         msd_2017Q2 = ifelse(msd_2017Q2 < 1, NA, msd_2017Q2),
         msd_2017Q3 = ifelse(msd_2017Q3 < 1, NA, msd_2017Q3),
         msd_2017Q4 = ifelse(msd_2017Q4 < 1, NA, msd_2017Q4),
         msd_2018Q1 = ifelse(msd_2018Q1 < 1, NA, msd_2018Q1),
         msd_2018Q2 = ifelse(msd_2018Q2 < 1, NA, msd_2018Q2),
         msd_2018Q3 = ifelse(msd_2018Q3 < 1, NA, msd_2018Q3),
         msd_2018Q4 = ifelse(msd_2018Q4 < 1, NA, msd_2018Q4),
         # zero & negative debt balances IRD components
         ird_Y2017Q1_child_support = ifelse(ird_Y2017Q1_child_support < 1, NA, ird_Y2017Q1_child_support),
         ird_Y2017Q2_child_support = ifelse(ird_Y2017Q2_child_support < 1, NA, ird_Y2017Q2_child_support),
         ird_Y2017Q3_child_support = ifelse(ird_Y2017Q3_child_support < 1, NA, ird_Y2017Q3_child_support),
         ird_Y2017Q4_child_support = ifelse(ird_Y2017Q4_child_support < 1, NA, ird_Y2017Q4_child_support),
         ird_Y2018Q1_child_support = ifelse(ird_Y2018Q1_child_support < 1, NA, ird_Y2018Q1_child_support),
         ird_Y2018Q2_child_support = ifelse(ird_Y2018Q2_child_support < 1, NA, ird_Y2018Q2_child_support),
         ird_Y2018Q3_child_support = ifelse(ird_Y2018Q3_child_support < 1, NA, ird_Y2018Q3_child_support),
         ird_Y2018Q4_child_support = ifelse(ird_Y2018Q4_child_support < 1, NA, ird_Y2018Q4_child_support),
         ird_Y2017Q1_families = ifelse(ird_Y2017Q1_families < 1, NA, ird_Y2017Q1_families),
         ird_Y2017Q2_families = ifelse(ird_Y2017Q2_families < 1, NA, ird_Y2017Q2_families),
         ird_Y2017Q3_families = ifelse(ird_Y2017Q3_families < 1, NA, ird_Y2017Q3_families),
         ird_Y2017Q4_families = ifelse(ird_Y2017Q4_families < 1, NA, ird_Y2017Q4_families),
         ird_Y2018Q1_families = ifelse(ird_Y2018Q1_families < 1, NA, ird_Y2018Q1_families),
         ird_Y2018Q2_families = ifelse(ird_Y2018Q2_families < 1, NA, ird_Y2018Q2_families),
         ird_Y2018Q3_families = ifelse(ird_Y2018Q3_families < 1, NA, ird_Y2018Q3_families),
         ird_Y2018Q4_families = ifelse(ird_Y2018Q4_families < 1, NA, ird_Y2018Q4_families),
         ird_Y2017Q1_GST = ifelse(ird_Y2017Q1_GST < 1, NA, ird_Y2017Q1_GST),
         ird_Y2017Q2_GST = ifelse(ird_Y2017Q2_GST < 1, NA, ird_Y2017Q2_GST),
         ird_Y2017Q3_GST = ifelse(ird_Y2017Q3_GST < 1, NA, ird_Y2017Q3_GST),
         ird_Y2017Q4_GST = ifelse(ird_Y2017Q4_GST < 1, NA, ird_Y2017Q4_GST),
         ird_Y2018Q1_GST = ifelse(ird_Y2018Q1_GST < 1, NA, ird_Y2018Q1_GST),
         ird_Y2018Q2_GST = ifelse(ird_Y2018Q2_GST < 1, NA, ird_Y2018Q2_GST),
         ird_Y2018Q3_GST = ifelse(ird_Y2018Q3_GST < 1, NA, ird_Y2018Q3_GST),
         ird_Y2018Q4_GST = ifelse(ird_Y2018Q4_GST < 1, NA, ird_Y2018Q4_GST),
         ird_Y2017Q1_income_tax = ifelse(ird_Y2017Q1_income_tax < 1, NA, ird_Y2017Q1_income_tax),
         ird_Y2017Q2_income_tax = ifelse(ird_Y2017Q2_income_tax < 1, NA, ird_Y2017Q2_income_tax),
         ird_Y2017Q3_income_tax = ifelse(ird_Y2017Q3_income_tax < 1, NA, ird_Y2017Q3_income_tax),
         ird_Y2017Q4_income_tax = ifelse(ird_Y2017Q4_income_tax < 1, NA, ird_Y2017Q4_income_tax),
         ird_Y2018Q1_income_tax = ifelse(ird_Y2018Q1_income_tax < 1, NA, ird_Y2018Q1_income_tax),
         ird_Y2018Q2_income_tax = ifelse(ird_Y2018Q2_income_tax < 1, NA, ird_Y2018Q2_income_tax),
         ird_Y2018Q3_income_tax = ifelse(ird_Y2018Q3_income_tax < 1, NA, ird_Y2018Q3_income_tax),
         ird_Y2018Q4_income_tax = ifelse(ird_Y2018Q4_income_tax < 1, NA, ird_Y2018Q4_income_tax),
         ird_Y2017Q1_other = ifelse(ird_Y2017Q1_other < 1, NA, ird_Y2017Q1_other),
         ird_Y2017Q2_other = ifelse(ird_Y2017Q2_other < 1, NA, ird_Y2017Q2_other),
         ird_Y2017Q3_other = ifelse(ird_Y2017Q3_other < 1, NA, ird_Y2017Q3_other),
         ird_Y2017Q4_other = ifelse(ird_Y2017Q4_other < 1, NA, ird_Y2017Q4_other),
         ird_Y2018Q1_other = ifelse(ird_Y2018Q1_other < 1, NA, ird_Y2018Q1_other),
         ird_Y2018Q2_other = ifelse(ird_Y2018Q2_other < 1, NA, ird_Y2018Q2_other),
         ird_Y2018Q3_other = ifelse(ird_Y2018Q3_other < 1, NA, ird_Y2018Q3_other),
         ird_Y2018Q4_other = ifelse(ird_Y2018Q4_other < 1, NA, ird_Y2018Q4_other),
         ird_Y2017Q1_PAYE = ifelse(ird_Y2017Q1_PAYE < 1, NA, ird_Y2017Q1_PAYE),
         ird_Y2017Q2_PAYE = ifelse(ird_Y2017Q2_PAYE < 1, NA, ird_Y2017Q2_PAYE),
         ird_Y2017Q3_PAYE = ifelse(ird_Y2017Q3_PAYE < 1, NA, ird_Y2017Q3_PAYE),
         ird_Y2017Q4_PAYE = ifelse(ird_Y2017Q4_PAYE < 1, NA, ird_Y2017Q4_PAYE),
         ird_Y2018Q1_PAYE = ifelse(ird_Y2018Q1_PAYE < 1, NA, ird_Y2018Q1_PAYE),
         ird_Y2018Q2_PAYE = ifelse(ird_Y2018Q2_PAYE < 1, NA, ird_Y2018Q2_PAYE),
         ird_Y2018Q3_PAYE = ifelse(ird_Y2018Q3_PAYE < 1, NA, ird_Y2018Q3_PAYE),
         ird_Y2018Q4_PAYE = ifelse(ird_Y2018Q4_PAYE < 1, NA, ird_Y2018Q4_PAYE),
         ird_Y2017Q1_student_loan = ifelse(ird_Y2017Q1_student_loan < 1, NA, ird_Y2017Q1_student_loan),
         ird_Y2017Q2_student_loan = ifelse(ird_Y2017Q2_student_loan < 1, NA, ird_Y2017Q2_student_loan),
         ird_Y2017Q3_student_loan = ifelse(ird_Y2017Q3_student_loan < 1, NA, ird_Y2017Q3_student_loan),
         ird_Y2017Q4_student_loan = ifelse(ird_Y2017Q4_student_loan < 1, NA, ird_Y2017Q4_student_loan),
         ird_Y2018Q1_student_loan = ifelse(ird_Y2018Q1_student_loan < 1, NA, ird_Y2018Q1_student_loan),
         ird_Y2018Q2_student_loan = ifelse(ird_Y2018Q2_student_loan < 1, NA, ird_Y2018Q2_student_loan),
         ird_Y2018Q3_student_loan = ifelse(ird_Y2018Q3_student_loan < 1, NA, ird_Y2018Q3_student_loan),
         ird_Y2018Q4_student_loan = ifelse(ird_Y2018Q4_student_loan < 1, NA, ird_Y2018Q4_student_loan),
         # number of dependent children
         number_dependent_children = ifelse(number_dependent_children >= 3, 3, number_dependent_children),
         # zero & negative incomes
         WAS_income = ifelse(WAS_income < 1, NA, WAS_income),
         WHP_income = ifelse(WHP_income < 1, NA, WHP_income),
         BEN_income = ifelse(BEN_income < 1, NA, BEN_income),
         total_income = ifelse(total_income < 1, NA, total_income),
         # new debt and repayments
         msd_2018_new_debt = ifelse(msd_2018_new_debt < 1, NA, msd_2018_new_debt),
         ird_2018_new_debt = ifelse(ird_2018_new_debt < 1, NA, ird_2018_new_debt),
         msd_2018_repaid = ifelse(msd_2018_repaid > -1, NA, abs(msd_2018_repaid)),
         ird_2018_repaid = ifelse(ird_2018_repaid > -1, NA, abs(ird_2018_repaid))
  )

#### define new variables -----------------------------------------------------
working_table = working_table %>%
  mutate(
    age_cat = ifelse(2018 - birth_year > 65, 'r',
                     ifelse(2018 - birth_year < 16, 'c', 'a')), # c(hild), a(dult), r(retiree)
    # support receipt type
    ben_JS = ifelse(`ben_type=Job Seeker` > 0, 1, 0),
    ben_SPS = ifelse(`ben_type=Sole Parent Support` > 0
                     | `ben_type=Young Parent Payment` > 0, 1, 0),
    ben_SLP = ifelse(`ben_type=Sickness` > 0
                     | `ben_type=Invalids` > 0, 1, 0),
    ben_NZS = ifelse(`ben_type=NZSuper` > 0, 1, 0),
    ben_OTH = ifelse(`ben_type=Student` > 0
                     | `ben_type=Youth` > 0
                     | `ben_type=Caring For Sick Or Infirm` > 0, 1, 0),
    # main activity
    main_activity_employed = ifelse(coalesce(days_employed,0) > coalesce(days_study,0)
                                    & coalesce(days_employed,0) > coalesce(days_neet,0)
                                    & coalesce(days_employed,0) >= 180, 1, NA),
    main_activity_study = ifelse(coalesce(days_study,0) > coalesce(days_employed,0)
                                 & coalesce(days_study,0) > coalesce(days_neet,0)
                                 & coalesce(days_study,0) >= 180, 1, NA),
    main_activity_NEET = ifelse(coalesce(days_neet,0) > coalesce(days_study,0)
                                & coalesce(days_neet,0) > coalesce(days_employed ,0)
                                & coalesce(days_neet,0) >= 180, 1, NA),
    # rounded number of days in activities
    days_benefit_r = 30 * round(days_benefit / 30),
    days_corrections_any_r = 30 * round(days_corrections_any / 30),
    days_corrections_prison_r = 30 * round(days_corrections_prison / 30),
    days_dead_r = 30 * round(days_dead / 30),
    days_employed_r = 30 * round(days_employed / 30),
    days_neet_r = 30 * round(days_neet / 30),
    days_overseas_r = 30 * round(days_overseas / 30),
    days_sickness_benefit_r = 30 * round(days_sickness_benefit / 30),
    days_social_housing_r = 30 * round(days_social_housing / 30),
    days_study_r = 30 * round(days_study / 30),
    # did you spend at least half the year in activity
    days_benefit_180 = ifelse(days_benefit >= 180, 1, NA),
    days_corrections_any_180 = ifelse(days_corrections_any >= 180, 1, NA),
    days_corrections_prison_180 = ifelse(days_corrections_prison >= 180, 1, NA),
    days_dead_180 = ifelse(days_dead >= 180, 1, NA),
    days_employed_180 = ifelse(days_employed >= 180, 1, NA),
    days_neet_180 = ifelse(days_neet >= 180, 1, NA),
    days_overseas_180 = ifelse(days_overseas >= 180, 1, NA),
    days_sickness_benefit_180 = ifelse(days_sickness_benefit >= 180, 1, NA),
    days_social_housing_180 = ifelse(days_social_housing >= 180, 1, NA),
    days_study_180 = ifelse(days_study >= 180, 1, NA),
    # income distribution
    # round each value to the nearest 2000
    # *_r prefix denotes 'rounded'
    total_income_r = 2000 * round(total_income / 2000),
    WAS_income_r = 2000 * round(WAS_income / 2000),
    WHP_income_r = 2000 * round(WHP_income / 2000),
    BEN_income_r = 2000 * round(BEN_income / 2000),
    # debt distribution
    # round each value to the nearest 2000
    # *_r prefix denotes 'rounded'
    ird_2018Q4_r = 2000 * round(ird_2018Q4 / 2000),
    msd_2018Q4_r = 2000 * round(msd_2018Q4 / 2000),
    dbt_2018Q4_r = 2000 * round((coalesce(msd_2018Q4,0) + coalesce(ird_2018Q4,0)) / 2000),
    # change in debt distribution
    # In buckets of width 200
    # Simple rounded would give buckets [-300,-100],[-100,100],[100,300] labeled -200,0,200
    # But we want to keep zero separate and bucket widths consistent
    # hence floor & adding half the bucket width gives buckets [-200,-1],[0],[1,200]
    # labeled -100,0,100
    ird_change_r = sign(coalesce(ird_2018Q4, 0) - coalesce(ird_2017Q4,0))
    * (100 + 200 * floor(abs((coalesce(ird_2018Q4, 0) - coalesce(ird_2017Q4,0)) / 200))),
    msd_change_r = sign(coalesce(msd_2018Q4, 0) - coalesce(msd_2017Q4,0))
    * (100 + 200 * floor(abs((coalesce(msd_2018Q4, 0) - coalesce(msd_2017Q4,0)) / 200))),
    dbt_change_r = sign(coalesce(ird_2018Q4, 0) - coalesce(ird_2017Q4,0)
                        + coalesce(msd_2018Q4, 0) - coalesce(msd_2017Q4,0))
    * (100 + 200 * floor(abs((coalesce(ird_2018Q4, 0) - coalesce(ird_2017Q4,0)) / 200
                         + (coalesce(msd_2018Q4, 0) - coalesce(msd_2017Q4,0)) / 200))),
    msd_2018_new_debt_r = 100 + 200 * floor(msd_2018_new_debt / 200),
    ird_2018_new_debt_r = 100 + 200 * floor(ird_2018_new_debt / 200),
    msd_2018_repaid_r = 100 + 200 * floor(msd_2018_repaid / 200),
    ird_2018_repaid_r = 100 + 200 * floor(ird_2018_repaid / 200),
    # rescale GSS results
    # round to the nearest 2 or 5
    mwi_tmp = 2 * floor(coalesce(mwi_material_wellbeing_index, 0) / 2),
    sf12_ph = 5 * floor(coalesce(sf12_physical_health, 0) / 5),
    sf12_mh = 5 * floor(coalesce(sf12_mental_health, 0) / 5),
    # entry into debt
    entry_Q1 = ifelse(is.na(ird_2017Q4) & is.na(msd_2017Q4)
                      & (!is.na(ird_2018Q1) | !is.na(msd_2018Q1)), 1, NA),
    entry_Q2 = ifelse(is.na(ird_2018Q1) & is.na(msd_2018Q1)
                      & (!is.na(ird_2018Q2) | !is.na(msd_2018Q2)), 1, NA),
    entry_Q3 = ifelse(is.na(ird_2018Q2) & is.na(msd_2018Q2)
                      & (!is.na(ird_2018Q3) | !is.na(msd_2018Q3)), 1, NA),
    entry_Q4 = ifelse(is.na(ird_2018Q3) & is.na(msd_2018Q3)
                      & (!is.na(ird_2018Q4) | !is.na(msd_2018Q4)), 1, NA),
    # exit from debt
    exit_Q1 = ifelse((!is.na(ird_2017Q4) | !is.na(msd_2017Q4))
                     & is.na(ird_2018Q1) & is.na(msd_2018Q1), 1, NA),
    exit_Q2 = ifelse((!is.na(ird_2018Q1) | !is.na(msd_2018Q1))
                     & is.na(ird_2018Q2) & is.na(msd_2018Q2), 1, NA),
    exit_Q3 = ifelse((!is.na(ird_2018Q2) | !is.na(msd_2018Q2))
                     & is.na(ird_2018Q3) & is.na(msd_2018Q3), 1, NA),
    exit_Q4 = ifelse((!is.na(ird_2018Q3) | !is.na(msd_2018Q3))
                     & is.na(ird_2018Q4) & is.na(msd_2018Q4), 1, NA)
  ) %>%
  # these defn depend on previous mutate, so to ensure new cols exist in SQL we run in separate mutate
  mutate(
    # no value where original value is zero
    dbt_2018Q4_r = ifelse(is.na(msd_2018Q4) & is.na(ird_2018Q4), NA, dbt_2018Q4_r),
    ird_change_r = ifelse(is.na(ird_2018Q4) & is.na(ird_2018Q3), NA, ird_change_r),
    msd_change_r = ifelse(is.na(msd_2018Q4) & is.na(msd_2018Q3), NA, msd_change_r),
    dbt_change_r = ifelse(is.na(ird_2018Q4) & is.na(ird_2018Q3)
                          & is.na(msd_2018Q4) & is.na(msd_2018Q3), NA, dbt_change_r),
    msd_2018_new_debt_r = ifelse(is.na(msd_2018_new_debt), NA, msd_2018_new_debt_r),
    ird_2018_new_debt_r = ifelse(is.na(ird_2018_new_debt), NA, ird_2018_new_debt_r),
    msd_2018_repaid_r = ifelse(is.na(msd_2018_repaid), NA, msd_2018_repaid_r),
    ird_2018_repaid_r = ifelse(is.na(ird_2018_repaid), NA, ird_2018_repaid_r),
    # any entry or exit
    entry_2018 = ifelse(is.na(entry_Q1) & is.na(entry_Q2) & is.na(entry_Q3) & is.na(entry_Q4), NA, 1),
    exit_2018 = ifelse(is.na(exit_Q1) & is.na(exit_Q2) & is.na(exit_Q3) & is.na(exit_Q4), NA, 1),
    # debt held by support type
    msd_JS_2018Q1 = ifelse(ben_JS == 1, msd_2018Q1, NA),
    msd_JS_2018Q2 = ifelse(ben_JS == 1, msd_2018Q2, NA),
    msd_JS_2018Q3 = ifelse(ben_JS == 1, msd_2018Q3, NA),
    msd_JS_2018Q4 = ifelse(ben_JS == 1, msd_2018Q4, NA),
    msd_SPS_2018Q1 = ifelse(ben_SPS == 1, msd_2018Q1, NA),
    msd_SPS_2018Q2 = ifelse(ben_SPS == 1, msd_2018Q2, NA),
    msd_SPS_2018Q3 = ifelse(ben_SPS == 1, msd_2018Q3, NA),
    msd_SPS_2018Q4 = ifelse(ben_SPS == 1, msd_2018Q4, NA),
    msd_SLP_2018Q1 = ifelse(ben_SLP == 1, msd_2018Q1, NA),
    msd_SLP_2018Q2 = ifelse(ben_SLP == 1, msd_2018Q2, NA),
    msd_SLP_2018Q3 = ifelse(ben_SLP == 1, msd_2018Q3, NA),
    msd_SLP_2018Q4 = ifelse(ben_SLP == 1, msd_2018Q4, NA),
    msd_NZS_2018Q1 = ifelse(ben_NZS == 1, msd_2018Q1, NA),
    msd_NZS_2018Q2 = ifelse(ben_NZS == 1, msd_2018Q2, NA),
    msd_NZS_2018Q3 = ifelse(ben_NZS == 1, msd_2018Q3, NA),
    msd_NZS_2018Q4 = ifelse(ben_NZS == 1, msd_2018Q4, NA),
    msd_OTH_2018Q1 = ifelse(ben_OTH == 1, msd_2018Q1, NA),
    msd_OTH_2018Q2 = ifelse(ben_OTH == 1, msd_2018Q2, NA),
    msd_OTH_2018Q3 = ifelse(ben_OTH == 1, msd_2018Q3, NA),
    msd_OTH_2018Q4 = ifelse(ben_OTH == 1, msd_2018Q4, NA),
    # rescaling of GSS results
    mwi_material_wellbeing_index = ifelse(is.na(mwi_material_wellbeing_index), NA, mwi_tmp),
    sf12_physical_health = ifelse(is.na(sf12_physical_health), NA, sf12_ph),
    sf12_mental_health = ifelse(is.na(sf12_mental_health), NA, sf12_mh)
  )

#### define group belonging indicators ----------------------------------------
working_table = working_table %>%
  mutate(
    # debtor populations
    debtor_MSD = ifelse(msd_2018Q4 > 0, 1, NA),
    debtor_IRD = ifelse(ird_2018Q4 > 0, 1, NA),
    debtor_2_agencies = ifelse(msd_2018Q4 > 0 & ird_2018Q4 > 0, 1, NA),
    any_debt = ifelse(msd_2018Q4 > 0 | ird_2018Q4 > 0, 1, NA),
    entered_into_debt = entry_2018,
    entered_into_debt_strict = ifelse(!is.na(entry_2018) & is.na(exit_2018), 1, NA),
    exited_from_debt = exit_2018,
    exited_from_debt_strict = ifelse(!is.na(exit_2018) & is.na(entry_2018), 1, NA),
    went_bankrupt = ifelse(bankrupt == 1, 1, NA),
    beneficiary_debtor = ifelse(days_benefit >= 180
                                & (msd_2018Q4 > 0 | ird_2018Q4 > 0), 1, NA),
    beneficiary_debtor_in_hhld_w_kids = ifelse(days_benefit >= 180
                                               & child_at_address >= 1
                                               & (msd_2018Q4 > 0 | ird_2018Q4 > 0), 1, NA),
    debtor_in_hhld_w_kids = ifelse(child_at_address >= 1
                                   & (msd_2018Q4 > 0 | ird_2018Q4 > 0), 1, NA),
    long_term_MSD_debtor = ifelse(msd_long_term_debtor >= 1, 1, NA),
    short_term_MSD_debtor = ifelse(is.na(msd_long_term_debtor) & msd_2018Q4 > 0, 1, NA),
    long_term_IRD_debtor = ifelse(ird_long_term_debtor >= 1, 1, NA),
    short_term_IRD_debtor = ifelse(is.na(ird_long_term_debtor) & ird_2018Q4 > 0, 1, NA),
    # specific debt populations
    debtor_IRD_child_support = ifelse(ird_Y2018Q4_child_support > 0, 1, NA),
    debtor_IRD_families = ifelse(ird_Y2018Q4_families > 0, 1, NA),
    debtor_IRD_GST = ifelse(ird_Y2018Q4_GST > 0, 1, NA),
    debtor_IRD_income_tax = ifelse(ird_Y2018Q4_income_tax > 0, 1, NA),
    debtor_IRD_PAYE = ifelse(ird_Y2018Q4_PAYE > 0, 1, NA),
    debtor_IRD_student_loan = ifelse(ird_Y2018Q4_student_loan > 0, 1, NA),
    debtor_msd_JS = ifelse(msd_JS_2018Q4 > 0, 1, NA),
    debtor_msd_SPS = ifelse(msd_SPS_2018Q4 > 0, 1, NA),
    debtor_msd_SLP = ifelse(msd_SLP_2018Q4 > 0, 1, NA),
    debtor_msd_NZS = ifelse(msd_NZS_2018Q4 > 0, 1, NA),
    # non-debtor populaitons
    whole_population = residential_population_indicator,
    beneficiary = ifelse(days_benefit >= 180, 1, NA),
    beneficiary_in_hhld_w_kids = ifelse(days_benefit >= 180
                                        & child_at_address >= 1, 1, NA),
    in_hhld_w_kids = ifelse(child_at_address >= 1, 1, NA)
  )

# during development inspect state of table
# explore::explore_shiny(collect(working_table))

## write for output -------------------------------------------------------------------------------

run_time_inform_user("saving output table", context = "heading", print_level = VERBOSE)
written_tbl = write_to_database(working_table, db_con, SANDPIT, OUR_SCHEMA, TIDY_TABLE, OVERWRITE = TRUE)

# index
run_time_inform_user("indexing", context = "details", print_level = VERBOSE)
create_clustered_index(db_con, SANDPIT, OUR_SCHEMA, TIDY_TABLE, "snz_uid")
# compress
run_time_inform_user("compressing", context = "details", print_level = VERBOSE)
compress_table(db_con, SANDPIT, OUR_SCHEMA, TIDY_TABLE)

# close connection
close_database_connection(db_con)
run_time_inform_user("grand completion", context = "heading", print_level = VERBOSE)
