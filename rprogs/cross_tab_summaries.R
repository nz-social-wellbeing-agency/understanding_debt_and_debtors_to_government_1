#####################################################################################################
#' Description: Transition analysis for output
#'
#' Input: 
#'
#' Output: 
#' 
#' Author: Simon Anastasiadis
#' 
#' Dependencies:
#' 
#' Notes: 
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2020-04-16 SA v0
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
AVG_RESULTS_FILE = "d2g_avg_cross_tabs.xlsx"
COUNT_RESULTS_FILE = "d2g_cnt_cross_tabs.xlsx"

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

working_table = create_access_point(db_con, SANDPIT, OUR_SCHEMA, TIDY_TABLE)

if(DEVELOPMENT_MODE)
  working_table = working_table %>% filter(snz_uid %% 100 == 0)

## load and prep dataset --------------------------------------------------------------------------

local_table = working_table %>%
  filter(2018 - birth_year >= 16) %>%
  select(birth_year,
         deprivation,
         eth_asian,
         eth_european,
         eth_maori,
         eth_other,
         eth_pacific,
         highest_qualification,
         life_satisfaction,
         `sex_code=1`, # male
         `sex_code=2`, # female
         ird_2018Q4,
         msd_2018Q4,
         total_income,
         BEN_income,
         WAS_income) %>%
  collect() %>%
  # resolve missings
  mutate(eth_asian = coalesce(eth_asian, 0),
         eth_european = coalesce(eth_european, 0),
         eth_maori = coalesce(eth_maori, 0),
         eth_other = coalesce(eth_other, 0),
         eth_pacific = coalesce(eth_pacific, 0),
         `sex_code=1` = coalesce(`sex_code=1`, 0),
         `sex_code=2` = coalesce(`sex_code=2`, 0),
         total_income = coalesce(total_income, 0),
         BEN_income = coalesce(BEN_income, 0),
         WAS_income = coalesce(WAS_income, 0)) %>%
  mutate(sex = ifelse(`sex_code=1` >= 1, "m", "f"),
         has_WAS = ifelse(WAS_income >= 1, 1, 0),
         has_OTH = ifelse(total_income - WAS_income - BEN_income >= 10000, 1, 0),
         total_income_band = case_when(0 < total_income & total_income <= 5000 ~ "0_5k",
                                       5000 < total_income & total_income <= 20000 ~ "5k_20k",
                                       20000 < total_income & total_income <= 50000 ~ "20k_50k",
                                       50000 < total_income & total_income <= 100000 ~ "50k_100k",
                                       100000 < total_income ~ "100k_up",
                                       TRUE ~ "none"),
         owes_ird = ifelse(coalesce(ird_2018Q4,0) >= 1, 1, 0),
         owes_msd = ifelse(coalesce(msd_2018Q4,0) >= 1, 1, 0),
         ird_2018_trim = ifelse(ird_2018Q4 > 50000, 50000, ird_2018Q4),
         msd_2018_trim = ifelse(msd_2018Q4 > 50000, 50000, msd_2018Q4),
         highest_qual = case_when(highest_qualification %in% c(1,2,3,4) ~ 'cert',
                                  highest_qualification %in% c(5,6,7) ~ 'grad',
                                  highest_qualification %in% c(8,9,10) ~ 'post',
                                  is.na(highest_qualification) & 2018 - birth_year <= 65 ~ 'none')
  )
                                      
run_time_inform_user("data loaded to R memory", context = "details", print_level = VERBOSE)

# close connection
close_database_connection(db_con)

## cross-tabs for averages ------------------------------------------------------------------------
#### average cross-tab functions --------------------------------------------------------

# Function to create an adjusted mean, such that the mean for each group is equal.
#
# Consider comparing ethnicity & debt - we are concerned this is an unfair comparison
# because debt and deprivation are correlated, and ethnicity and depriavation are also
# correlated. But if every deprivation had the same average debt then a comparison of
# debt and ethnicity would not be confounded by the effect of deprivation.
normalise_means = function(data, col, adjusters){
  data = data %>%
    filter(!is.na(!!sym(col))) %>%
    mutate(adj = !!sym(col))
  
  overall_mean = mean(data[[col]])
  
  for(aa in adjusters){
    # get means for each category
    category_means = data %>%
      filter(!is.na(!!sym(aa))) %>%
      group_by(!!sym(aa)) %>%
      summarise(avg = mean(!!sym(col)))
    
    # make adjustment
    data = data %>%
      inner_join(category_means, by = aa) %>%
      mutate(adj = adj - avg + overall_mean) %>%
      select(-"avg") # remove avg so it can be remade
    # now mean(col) for each group of 'aa' is equal to the mean for the whole dataset
  }
  
  return(data)
}

# Function to summarise adj for each subgroup
# Outputs df: col | subgroup | label | mean | std.dev | total | count
tabulate_means = function(data, col, label){
  
  result = data %>%
    filter(!is.na(!!sym(col))) %>%
    group_by(!!sym(col)) %>%
    summarise(mean = mean(adj, na.rm = TRUE),
              sdev = sd(adj, na.rm = TRUE),
              total = sum(adj, na.rm = TRUE),
              num = n()) %>%
    mutate(col = col,
           label = label) %>%
    select(col, subgroup = !!sym(col), label, mean, sdev, num)
  
  return(result)
}

# wrapper function for ease of execution
wrapper = function(data, col_to_group, col_to_summarise, adjusters){
  if(exists("adjusters", inherits = FALSE) & length(adjusters) >= 1){
    label = paste0(col_to_summarise,"_adj_",paste0(adjusters, collapse = "_"))
    
    data %>%
      normalise_means(col_to_summarise, adjusters) %>%
      tabulate_means(col_to_group, label)
    
  } else {
    label = paste0(col_to_summarise,"_raw")
    
    data %>%
      normalise_means(col_to_summarise, NULL) %>%
      tabulate_means(col_to_group, label)
  }
}

#### average cross-tab comparison -------------------------------------------------------

output_df = data.frame(stringsAsFactors = FALSE)

cols_to_group = c("eth_asian",
                  "eth_european",
                  "eth_maori",
                  "eth_other",
                  "eth_pacific",
                  "sex",
                  "life_satisfaction")
cols_to_summarise = c("owes_ird",
                      "owes_msd",
                      "ird_2018Q4",
                      "msd_2018Q4",
                      "ird_2018_trim",
                      "msd_2018_trim")

for(col_to_group in cols_to_group)
  for(col_to_summarise in cols_to_summarise)
    output_df = rbind(output_df, wrapper(local_table, col_to_group, col_to_summarise, c()))

cols_to_adjust = c("deprivation",
                   "has_WAS",
                   "has_OTH",
                   "total_income_band",
                   "highest_qual")

for(col_to_group in cols_to_group)
  for(col_to_summarise in cols_to_summarise)
    for(col_to_adjust in cols_to_adjust)
      output_df = rbind(output_df, wrapper(local_table, col_to_group, col_to_summarise, col_to_adjust))

for(col_to_group in cols_to_group)
  for(col_to_summarise in cols_to_summarise)
    output_df = rbind(output_df, wrapper(local_table, col_to_group, col_to_summarise, c("deprivation", "total_income_band")))

for(col_to_group in cols_to_group)
  for(col_to_summarise in cols_to_summarise)
    output_df = rbind(output_df, wrapper(local_table, col_to_group, col_to_summarise, c("deprivation", "highest_qual")))

for(col_to_group in cols_to_group)
  for(col_to_summarise in cols_to_summarise)
    output_df = rbind(output_df, wrapper(local_table, col_to_group, col_to_summarise, c("deprivation", "highest_qual", "total_income_band")))

run_time_inform_user("tabulating complete", context = "details", print_level = VERBOSE)

#### write average output to excel ------------------------------------------------------

run_time_inform_user("saving average output table for excel", context = "heading", print_level = VERBOSE)

openxlsx::write.xlsx(as.data.frame(output_df), AVG_RESULTS_FILE, row.names = FALSE)

## cross-tabs for counts --------------------------------------------------------------------------
#### count cross-tab functions ----------------------------------------------------------

# Function to summarise counts for two groups
# Outputs df: col1 | group1 | col2 | group2 | number
tabulate_counts = function(data, col1, col2){
  
  result = data %>%
    filter(!is.na(!!sym(col1)),
           !is.na(!!sym(col2))) %>%
    group_by(!!sym(col1), !!sym(col2)) %>%
    summarise(num = n()) %>%
    ungroup() %>%
    mutate(col1 = col1,
           group1 = !!sym(col1),
           col2 = col2,
           group2 = !!sym(col2)) %>%
    select(col1, group1, col2, group2, num)
  
  return(result)
}

#### count cross-tab comparison ---------------------------------------------------------

output_df = data.frame(stringsAsFactors = FALSE)

col_list = c("eth_asian",
             "eth_european",
             "eth_maori",
             "eth_other",
             "eth_pacific",
             "sex",
             "life_satisfaction",
             "owes_ird",
             "owes_msd",
             "deprivation",
             "has_WAS",
             "has_OTH",
             "total_income_band",
             "highest_qual")

for(c1 in 1:(length(col_list)-1))
  for(c2 in (c1+1):length(col_list))
    output_df = rbind(output_df, tabulate_counts(local_table, col_list[c1], col_list[c2]))

#### write count output to excel --------------------------------------------------------

run_time_inform_user("saving count output table for excel", context = "heading", print_level = VERBOSE)

openxlsx::write.xlsx(as.data.frame(output_df), COUNT_RESULTS_FILE, row.names = FALSE)

## conclude ---------------------------------------------------------------------------------------

run_time_inform_user("grand completion", context = "heading", print_level = VERBOSE)
