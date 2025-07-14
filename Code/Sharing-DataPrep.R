library(dplyr)
library(stringr)
library(purrr)
library(readr)
library(glue)

#============================================================
# Import data and tidy
#============================================================

# Define file paths
base_path <- "C:/Research/NCAA BHE Change/data/"
conferences_dir <- "C:/Research/NCAA BHE Change/data/Conferences"

# Create a function to read and mutate data
read_and_mutate <- function(season, division, sex, type) {
  file <- paste0(base_path, "NCAA_", division, sex, "_", season, "_", type, ".csv")
  read.csv(file) %>% mutate(season = season, sex = sex)
}

# Seasons, Divisions, and Sexes
seasons <- c("2023-24", "2024-25")
divisions <- c("D1", "D2", "D3")
sexes <- c("W", "M")
types <- c("matches", "team", "players")

# Initialize empty lists to store data
matches_list <- list()
teams_list <- list()
players_list <- list()

# Loop through the combinations of season, division, and sex to load and combine data
for (season in seasons) {
  for (division in divisions) {
    for (sex in sexes) {
      # Skip D2 for Men's teams
      if (division == "D2" && sex == "M") next
      for (type in types) {
        if (type == "matches") {
          matches_list[[paste0(season, "_", division, "_", sex)]] <- read_and_mutate(season, division, sex, type)
        } else if (type == "team") {
          teams_list[[paste0(season, "_", division, "_", sex)]] <- read_and_mutate(season, division, sex, type)
        } else if (type == "players") {
          players_list[[paste0(season, "_", division, "_", sex)]] <- read_and_mutate(season, division, sex, type)
        }
      }
    }
  }
}

# Combine data
matches <- bind_rows(matches_list)
teams <- bind_rows(teams_list)
players <- bind_rows(players_list)

# Standardize schools with naming variations
matches <- matches %>%
  mutate(
    across(
      c(home, away),
      ~ str_replace(.x, "^Saint Francis \\(PA\\)$", "Saint Francis")
    )
  )

teams <- teams %>%
  mutate(
    Name = str_replace(Name, "^Saint Francis \\(PA\\)$", "Saint Francis")
  )

players <- players %>%
  mutate(
    team_name = str_replace(team_name, "^Saint Francis \\(PA\\)$", "Saint Francis")
  )


# Remove duplicate matches that result from cross-division play
matches <- matches %>%
  distinct(match_id, .keep_all = TRUE)

teams <- teams %>%
  distinct(match_id, Name, .keep_all = TRUE)

players <- players %>%
  distinct(match_id, X., team_name, .keep_all = TRUE)


#============================================================
# Load conference data
#============================================================
conference_data <- list.files(conferences_dir, pattern = "\\.csv$", full.names = TRUE) |>
  map_dfr(function(path) {
    
    # ----------- parse info from filename ----------------------------------
    fname <- basename(path)  # e.g. "2025-M1.csv"
    
    year <- str_extract(fname, "^[0-9]{4}") |> as.integer()   # First 4 digits
    sex_div <- str_extract(fname, "(?<=-)[WM][123]")           # Character after "-" and digit
    
    sex_flag <- str_sub(sex_div, 1, 1)                        # "W" or "M"
    division_num <- str_sub(sex_div, 2, 2)                    # "1", "2", or "3"
    
    division <- paste0("D", division_num)                    # e.g., "D1", "D2", "D3"
    
    sex <- ifelse(sex_flag == "W", "W", "M")                  # Keep as W or M (matching your main data)
    
    season <- ifelse(
      sex == "W",
      glue("{year}-{substr(year + 1, 3, 4)}"),  # Women's: year + next year
      glue("{year - 1}-{substr(year, 3, 4)}")   # Men's: prior year + current year
    )
    
    # ----------- read the CSV ----------------------------------------------
    read_csv(path,
             col_types = cols(.default = col_character()),
             show_col_types = FALSE) |>
      mutate(
        division = division,
        sex      = sex,
        season   = season
      )
  })

# Safely clean team names
teams <- teams %>%
  mutate(Name = str_trim(Name) %>% str_squish())

players <- players %>%
  mutate(Name = str_trim(team_name) %>% str_squish())

conference_data <- conference_data %>%
  mutate(Team = str_trim(Team) %>% str_squish())


# Merge conference data with teams data
teams <- teams %>%
  left_join(
    conference_data %>% select(Team, division, sex, season, Conference),
    by = c("Name" = "Team", "sex", "season")
  )

players <- players %>%
  left_join(
    conference_data %>% select(Team, division, sex, season, Conference),
    by = c("team_name" = "Team", "sex", "season")
  )

#============================================================
# Data Integrity Checks
#============================================================

# 1. Derive sets played and identify sets < 3 
matches <- matches %>%
  mutate(
    sets_played = rowSums(!is.na(select(., home_set1, home_set2, home_set3, home_set4, home_set5)))
  )
#Remove matches (Found 9)
remove_match_ids <- matches %>%
  filter(sets_played < 3) %>%
  pull(match_id)
matches <- matches %>% filter(!match_id %in% remove_match_ids)
teams <- teams %>% filter(!match_id %in% remove_match_ids)
players <- players %>% filter(!match_id %in% remove_match_ids)


# 2. Identify matches with inconsistent set scores
# Flag matches where the winning team did not win by at least 2 points
inconsistent_scores <- matches %>% filter(
  (home_set1 > away_set1 & home_set1 - away_set1 < 2 & home_set1 >= 25) |
    (home_set2 > away_set2 & home_set2 - away_set2 < 2 & home_set2 >= 25) |
    (home_set3 > away_set3 & home_set3 - away_set3 < 2 & home_set3 >= 25) |
    (home_set4 > away_set4 & home_set4 - away_set4 < 2 & home_set4 >= 25) |
    (home_set5 > away_set5 & home_set5 - away_set5 < 2 & home_set5 >= 15) |
    (away_set1 > home_set1 & away_set1 - home_set1 < 2 & away_set1 >= 25) |
    (away_set2 > home_set2 & away_set2 - home_set2 < 2 & away_set2 >= 25) |
    (away_set3 > home_set3 & away_set3 - home_set3 < 2 & away_set3 >= 25) |
    (away_set4 > home_set4 & away_set4 - home_set4 < 2 & away_set4 >= 25) |
    (away_set5 > home_set5 & away_set5 - home_set5 < 2 & away_set5 >= 15)
)

if (nrow(inconsistent_scores) > 0) {
  cat("Warning: Matches with inconsistent set scores detected. Review required.")
}
# Remove (Found 6)
matches <- matches %>%
  filter(!match_id %in% inconsistent_scores$match_id)
teams <- teams %>%
  filter(!match_id %in% inconsistent_scores$match_id)
players <- players %>%
  filter(!match_id %in% inconsistent_scores$match_id)


# Flag matches where the winning scores are unreasonable
inconsistent_scores <- matches %>% filter(
  (home_set1 > away_set1 & home_set1 - away_set1 > 2 & home_set1 > 25) |
    (home_set2 > away_set2 & home_set2 - away_set2 > 2 & home_set2 > 25) |
    (home_set3 > away_set3 & home_set3 - away_set3 > 2 & home_set3 > 25) |
    (home_set4 > away_set4 & home_set4 - away_set4 > 2 & home_set4 > 25) |
    (home_set5 > away_set5 & home_set5 - away_set5 > 2 & home_set5 > 15) |
    (away_set1 > home_set1 & away_set1 - home_set1 > 2 & away_set1 > 25) |
    (away_set2 > home_set2 & away_set2 - home_set2 > 2 & away_set2 > 25) |
    (away_set3 > home_set3 & away_set3 - home_set3 > 2 & away_set3 > 25) |
    (away_set4 > home_set4 & away_set4 - home_set4 > 2 & away_set4 > 25) |
    (away_set5 > home_set5 & away_set5 - home_set5 > 2 & away_set5 > 15)
)

if (nrow(inconsistent_scores) > 0) {
  cat("Warning: Matches with inconsistent set scores detected. Review required.")
}
# Remove (Found 33)
matches <- matches %>%
  filter(!match_id %in% inconsistent_scores$match_id)
teams <- teams %>%
  filter(!match_id %in% inconsistent_scores$match_id)
players <- players %>%
  filter(!match_id %in% inconsistent_scores$match_id)

# Flag matches where the winning scores are unreasonable - Part 2
inconsistent_scores <- matches %>% filter(
  (home_set1 < 25 & away_set1 < 25) |
    (home_set2 < 25 & away_set2 < 25) |
    (home_set3 < 25 & away_set3 < 25) |
    (home_set4 < 25 & away_set4 < 25) |
    (home_set5 < 15 & away_set5 < 15)
)

if (nrow(inconsistent_scores) > 0) {
  cat("Warning: Matches with inconsistent set scores detected. Review required.")
}
# Remove (Found 33)
matches <- matches %>%
  filter(!match_id %in% inconsistent_scores$match_id)
teams <- teams %>%
  filter(!match_id %in% inconsistent_scores$match_id)
players <- players %>%
  filter(!match_id %in% inconsistent_scores$match_id)


# Check for matches where the winner didn't win 3 sets
matches <- matches %>%
  mutate(
    home_sets_won = rowSums(
      cbind(
        home_set1 > away_set1,
        home_set2 > away_set2,
        home_set3 > away_set3,
        home_set4 > away_set4,
        home_set5 > away_set5
      ),
      na.rm = TRUE
    ),
    away_sets_won = rowSums(
      cbind(
        away_set1 > home_set1,
        away_set2 > home_set2,
        away_set3 > home_set3,
        away_set4 > home_set4,
        away_set5 > home_set5
      ),
      na.rm = TRUE
    )
  )

inconsistent_winners <- matches %>%
  filter(home_sets_won != 3 & away_sets_won != 3)

if (nrow(inconsistent_winners) > 0) {
  cat("Warning: Matches with inconsistent match scores detected. Review required.")
}
# Found 1
# Remove
matches <- matches %>%
  filter(!match_id %in% inconsistent_winners$match_id)
teams <- teams %>%
  filter(!match_id %in% inconsistent_winners$match_id)
players <- players %>%
  filter(!match_id %in% inconsistent_winners$match_id)


# Flag matches with negative or extremely high attendance
implausible_attendance <- matches %>% filter(attendance < 0 | attendance > 10000)
if (nrow(implausible_attendance) > 0) {
  cat("Warning: Matches with implausible attendance figures detected. Review required.
")
}
# No < 0 found, all > 10,000 observed to be legitimate


# 3. Check BHE counts
# Calculate statistical thresholds for BHE outlier detection
team_metrics <- teams %>% select(BHE)
metric_stats <- teams %>% summarize(
  BHE_mean = mean(BHE, na.rm = TRUE), BHE_sd = sd(BHE, na.rm = TRUE)
)

# Define thresholds as mean B1 3 * sd
thresholds <- metric_stats %>% mutate(
  BHE_max = BHE_mean + 3 * BHE_sd
)

# Flag teams with unrealistic performance metrics
thresholds <- team_metrics %>% summarize(
  BHE_max = mean(BHE, na.rm = TRUE) + 3 * sd(BHE, na.rm = TRUE)
)

implausible_teams <- teams %>% filter(
  BHE > thresholds$BHE_max
)
if (nrow(implausible_teams) > 0) {
  implausible_teams <- implausible_teams %>% mutate(
    trigger_variable = case_when(
      BHE > thresholds$BHE_max ~ "BHE",
      TRUE ~ "Unknown"
    )
  )
  cat("Warning: Teams with implausible performance metrics detected based on statistical thresholds. Review required.
")
}
# Remove 5 matches found to have excessive BHE (one or both 20+)
remove_match_ids <- c("5667887","3244477","3239776","3239546","3239169") # Specify match IDs to remove
matches <- matches %>% filter(!match_id %in% remove_match_ids)
teams <- teams %>% filter(!match_id %in% remove_match_ids)
players <- players %>% filter(!match_id %in% remove_match_ids)


# 4. Validate player data
# Check for inconsistencies between player stats and team totals (e.g., sum of player kills > team kills)
inconsistent_players <- players %>%
  group_by(match_id, team_name) %>%
  summarize(total_BHE = sum(BHE, na.rm = TRUE)) %>%
  left_join(teams, by = c("match_id" = "match_id", "team_name" = "Name")) %>%
  filter(total_BHE > BHE)
if (nrow(inconsistent_players) > 0) {
  cat("Warning: Player stats inconsistent with team totals detected. Review required.
")
}
# Remove 1 match found to have bad data
remove_match_ids <- c("3251998") # Specify match IDs to remove
matches <- matches %>% filter(!match_id %in% remove_match_ids)
teams <- teams %>% filter(!match_id %in% remove_match_ids)
players <- players %>% filter(!match_id %in% remove_match_ids)


# 4. Find match IDs where Division is empty (indicating non-NCAA)
empty_division_ids <- teams %>% filter(division == "" | is.na(division)) %>% pull(match_id)
# Filter out
matches <- matches %>% filter(!match_id %in% empty_division_ids)
teams <- teams %>% filter(!match_id %in% empty_division_ids)
players <- players %>% filter(!match_id %in% empty_division_ids)


#============================================================
# Additional filters for panel
#============================================================
# Filter to only include matches among teams involved in the same sex-division in both seasons
# 1. Identify teams present in both seasons per sex and division ----------
stable_teams <- teams %>%
  distinct(Name, sex, division, season) %>%
  group_by(Name, sex, division) %>%
  filter(n() == 2) %>%
  ungroup()

# 2. Keep only those teams in teams table ---------------------------------
teams <- teams %>%
  semi_join(stable_teams, by = c("Name","sex","division"))

# 3. Find matches where both home and away are in the filtered teams ------
valid_match_ids <- teams %>%
  count(match_id) %>%
  filter(n == 2) %>%
  pull(match_id)

# 4. Restrict matches and players to valid matches ------------------------
matches <- matches %>%
  filter(match_id %in% valid_match_ids)
players <- players %>%
  filter(match_id %in% valid_match_ids)
teams <- teams %>%
  filter(match_id %in% valid_match_ids)

#============================================================
# Prepare derived data for analysis
#============================================================

# Calculate total points per match
matches <- matches %>%
  mutate(
    total_points = rowSums(
      select(., matches("^(home|away)_set[1-5]$")),
      na.rm = TRUE
    )
  )

# Calculate BHE/Point
teams <- teams %>%
  left_join(matches %>% select(match_id, total_points), by = "match_id") %>%
  mutate(BHE_per_point = BHE / total_points) %>%
  select(-total_points) # if you don't want to keep it

# Match data prep
matches <- matches %>%
  mutate(
    match_date = parse_date_time(
      match_date,
      orders = c("m/d/Y I:M p", "m/d/Y H:M", "m/d/Y"),
      tz = "America/New_York"
    ),
    season = factor(season),
    sex    = factor(sex)
  )

# join match‑level fields, add flags & ITS variables
first_date <- min(matches$match_date, na.rm = TRUE)

teams <- teams %>%
  left_join(
    matches %>% select(match_id, home, away, attendance, match_date),
    by = "match_id"
  ) %>%
  mutate(
    home_away      = case_when(Name == home ~ "Home",
                               Name == away ~ "Away",
                               TRUE         ~ NA_character_),
    log_attendance = log1p(attendance),
    crowd_bracket  = factor(cut(attendance,
                                breaks = c(-Inf, 100, 1000, Inf),
                                labels = c("<100", "100-1000", ">1000"))),
    post           = as.integer(season == "2024-25"),
    time_index     = as.numeric(difftime(match_date, first_date, units = "days"))
  )

# standardize positions
players <- players %>%
  mutate(
    P = case_when(
      is.na(P) | P == "G"                ~ "Unknown",
      P == "RS"                          ~ "OPP",
      P %in% c("L", "DB", "DS", "LIBERO")~ "L/DS",
      P %in% c("MH", "MIDDLEBLOCKER")    ~ "MB",
      P %in% c("OH", "OUTSIDEHITTER")    ~ "OH",
      P == "SETTER"                      ~ "S",
      TRUE                               ~ P
    )
  )

#============================================================
# Save analysis-ready data
#============================================================
output_base_path <- "C:/Research/NCAA BHE Change/data/"
dir.create(output_base_path, showWarnings = FALSE)

write.csv(matches, file = paste0(output_base_path, "analysis_ready_matches.csv"), row.names = FALSE)
write.csv(teams, file = paste0(output_base_path, "analysis_ready_teams.csv"), row.names = FALSE)
write.csv(players, file = paste0(output_base_path, "analysis_ready_players.csv"), row.names = FALSE)

rm(list = setdiff(ls(), c("matches", "teams", "players")))