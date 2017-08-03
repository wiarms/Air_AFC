################################################################################
# AAFC: AIR Aggregation Forecast
#
# Written by Liu Tao (lt@ncic.ac.cn)
# Derived from work by Zhang KeWei
################################################################################

#
# Config params:
#   obs_mode:
#     T for generate base from observation data
#     F for generate base from mode forecast data
#   appending:
#     T for appending new data if base file exists
#     F for overwrite with all data if base file exists
#   skip_missing:
#     T for skip if data file missing on someday
#     F for stop if data file missing on someday
#   force_merge:
#     T forces using merge method in dispatching process
#     F try column copy method (i.e. rapid mode) when possible
#   parallel_write:
#     T for parallel processing when write base files
#     F for serial processing when write base files
#
AAFC_gen_base_daily <- function(src_dir, dst_dir, mode_str, SID_file,
                                start_day, end_day, obs_mode = FALSE,
                                appending = FALSE, skip_missing = FALSE,
                                force_merge = FALSE, parallel_write = T)
{
  if (!is.character(src_dir) || !is.character(dst_dir) ||
      !is.character(mode_str) || !is.character(SID_file) ||
      !is.character(start_day) || !is.character(end_day) ||
      !is.logical(obs_mode) || !is.logical(appending) ||
      !is.logical(skip_missing) || !is.logical(force_merge) ||
      !is.logical(parallel_write)) {
    message("Invalid parameter")
    return(-1)
  }

  # mode file definitions
  mod_banner <- "station_forcast_daily"
  mod_file_prefix <- paste(mod_banner, "_", mode_str, "_", sep = "")
  mod_col_names <- c("S_TIME", "F_TIME", "SID",
                     "PM2.5", "PM10", "CO", "NO2",
                     "SO2", "O3", "O3_8H")
  mod_col_classes <- c("character", "character", "character",
                       "numeric", "numeric", "numeric", "numeric",
                       "numeric", "numeric", "numeric")
  mod_var <- c("PM2.5", "PM10", "CO", "NO2", "SO2", "O3", "O3_8H")
  mod_prd <- c("0d", "1d", "2d", "3d", "4d", "5d", "6d", "7d")
  mod_sep <- ","

  # obs file definitions
  obs_banner <- "station_obs_daily"
  obs_file_prefix <- paste(obs_banner, "_", sep = "")
  obs_col_names <- c("SID", "LNG", "LAT",
                     "PM2.5", "PM10", "CO", "NO2",
                     "SO2", "O3", "O3_8H")
  obs_col_classes <- c("character", "numeric", "numeric",
                       "numeric", "numeric", "numeric", "numeric",
                       "numeric", "numeric", "numeric")
  obs_var <- c("PM2.5", "PM10", "CO", "NO2", "SO2", "O3", "O3_8H")
  obs_prd <- c("0d")
  obs_sep <- ""

  # src file definitions
  if (obs_mode) {
    src_file_prefix <- obs_file_prefix
    src_col_names <- obs_col_names
    src_col_classes <- obs_col_classes
    src_sep <- obs_sep
  } else {
    src_file_prefix <- mod_file_prefix
    src_col_names <- mod_col_names
    src_col_classes <- mod_col_classes
    src_sep <- mod_sep
  }

  # src file preparations
  sday <- as.Date(start_day)
  eday <- as.Date(end_day)
  if (sday <= eday) {
    nr_days <- difftime(eday, sday, units = "days") + 1
    day_seq_by <- "day"
  } else {
    nr_days <- difftime(sday, eday, units = "days") + 1
    day_seq_by <- "-1 day"
  }
  day_idx <- seq(sday, by = day_seq_by, length.out = nr_days)
  day_idx <- format(day_idx, "%Y%m%d")
  src_name <- sprintf("%s/%s%s.txt", src_dir, src_file_prefix, day_idx)

  # base file (the output) definitions
  if (obs_mode) {
    fc_var <- obs_var
    fc_prd <- obs_prd
  } else {
    fc_var <- mod_var
    fc_prd <- mod_prd
  }
  base_col_prefix <- "D"
  base_SID_names <- c("SID")
  base_SID_classes <- c("character")
  base_sep <- ","

  # base table preparations
  nr_var <- length(fc_var)
  nr_prd <- length(fc_prd)
  nr_bases <- (nr_var * nr_prd)
  base_name <- character(nr_bases)
  base <- list()
  base_SID_consistent <- TRUE

  # create base tables
  message("creating base tables")
  for (v_idx in 1:nr_var) {
    for (p_idx in 1:nr_prd) {
      b_idx <- (v_idx - 1) * nr_prd + p_idx
      base_name[b_idx] <- sprintf("%s/%s_%s_%s.csv", dst_dir,
                                  fc_var[v_idx], mode_str, fc_prd[p_idx])
      if (appending && file.exists(base_name[b_idx])) {
        f_name <- base_name[b_idx]
      } else {
        f_name <- SID_file
      }
      base[[b_idx]] <- read.table(f_name, header = TRUE, sep = base_sep,
                                  colClasses = base_SID_classes,
                                  strip.white = TRUE, stringsAsFactors = FALSE)
      if (!identical(base[[1]][ , 1], base[[b_idx]][ , 1])) {
        base_SID_consistent <- FALSE
      }
    }
  }
  message("base_SID_consistent = ", base_SID_consistent)

  # parse src files daily, dispatch data to base tables
  for (i in 1:nr_days) {
    # check for src file
    if (!file.exists(src_name[i])) {
      if (skip_missing) {
        message("file ", src_name[i], " is missing, skip")
        next()
      } else {
        message("file ", src_name[i], " is missing, stop")
        return(-1)
      }
    }

    # read in
    daily <- read.table(src_name[i], sep = src_sep,
                        col.names = src_col_names,
                        colClasses = src_col_classes,
                        strip.white = TRUE, stringsAsFactors = FALSE)

    # prepare for reshape
    if (obs_mode) {
      daily$LNG <- NULL
      daily$LAT <- NULL
      nc <- ncol(daily) + 1
      daily[ , nc] <- paste(base_col_prefix, day_idx[i], sep = "")
      names(daily)[nc] <- "F_TIME"
    } else {
      daily$S_TIME <- NULL
      daily$F_TIME <- paste(base_col_prefix, daily$F_TIME, sep = "")
    }

    # reshape
    library(reshape2)
    work <- melt(daily, id = c("F_TIME", "SID"))
    work <- dcast(work, variable + SID ~ F_TIME)

    # dispatch
    rapid_dispatch <- FALSE
    for (v_idx in 1:nr_var) {
      work_v <- subset(work, variable == fc_var[v_idx])
      # check for rapid dispatch
      if (v_idx == 1) {
        if (base_SID_consistent) {
          rapid_dispatch <- identical(base[[1]][ , 1], work_v[ , 2])
        }
        if (force_merge) {
          rapid_dispatch <- FALSE
        }
        message(src_name[i], " : dispatching, rapid mode = ", rapid_dispatch)
      }

      for (p_idx in 1:nr_prd) {
        # check for col duplicate
        b_idx <- (v_idx - 1) * nr_prd + p_idx
        p_name <- names(work_v)[2 + p_idx]
        p_match <- grep(p_name, names(base[[b_idx]]))
        p_clean <- (length(p_match) == 0)
        if (!p_clean) {
          if (appending) {
            message(base_name[b_idx], " : col ", p_name, " is duplicate, skip")
            next()
          } else {
            message(base_name[b_idx], " : col ", p_name, " is duplicate, stop")
            return(-1)
          }
        }

        # do dispatch
        if (rapid_dispatch) {
          nc <- ncol(base[[b_idx]]) + 1
          base[[b_idx]][ , nc] <- work_v[ , 2 + p_idx]
          names(base[[b_idx]])[nc] <- names(work_v)[2 + p_idx]
        } else {
          work_p <- work_v[ , c(2, 2 + p_idx)]
          base[[b_idx]] <- merge(base[[b_idx]], work_p, by = "SID", sort = F)
        }
      }
    }
  }

  # write base tables to files
  if (dir.exists(dst_dir) == FALSE) {
    dir.create(dst_dir)
  }
  if (parallel_write) {
    library(foreach)
    library(doParallel)
    message("starting parallel process for writing files in ", dst_dir, "/")
    cl <- makeCluster(4)
    registerDoParallel(cl)
    foreach (i = 1:nr_bases) %dopar% {
      write.table(base[[i]], base_name[i], sep = base_sep,
                  quote = FALSE, row.names = FALSE)
    }
    stopCluster(cl)
    message("stopping parallel process for writing files in ", dst_dir, "/")
  } else {
    for (i in 1:nr_bases) {
      message(base_name[i], " : writing file")
      write.table(base[[i]], base_name[i], sep = base_sep,
                  quote = FALSE, row.names = FALSE)
    }
  }

  return(0)
}

#
# Rearrange station info file Station_d01_xy.txt
#
AAFC_gen_SINFO_file <- function(src_file, dst_file)
{
  src_col_names <- c("SEQ", "SID", "GRID_X", "GRID_Y",
                     "LNG", "LAT", "NAME", "CITY")
  src_col_classes <- c("character", "character", "integer", "integer",
                       "numeric", "numeric", "character", "character")
  sinfo <- read.table(src_file, sep = "",
                      col.names = src_col_names,
                      colClasses = src_col_classes,
                      strip.white = TRUE,
                      stringsAsFactors = FALSE,
                      fileEncoding = "UTF8")
  sinfo$SEQ <- NULL
  grid_xy <- sprintf("G%03d%03d", sinfo$GRID_X, sinfo$GRID_Y)
  sinfo$GRID_XY <- grid_xy

  vars <- c("SID", "NAME", "CITY",
                "GRID_X", "GRID_Y", "GRID_XY",
                "LNG", "LAT")
  sinfo <- sinfo[vars]

  write.table(sinfo, dst_file, sep = ",",
              quote = FALSE, row.names = FALSE,
              fileEncoding = "GBK")
}

#
# generate station REGION file, to aggregate stations
#
# usage: AAFC_gen_REG_file(SINFO_file, REG_file, REG_KEY)
# REG_KEY is the variable as Region keys, normaly can be
#   "CITY": aggregate stations in same CITY
#   "SID": each station is a individal region
#   "GRID_XY": use the nearest grid to aggregate stations
#
AAFC_gen_REG_file <- function(src_file, dst_file, reg_key)
{
  src_col_names <- c("SID", "NAME", "CITY",
                     "GRID_X", "GRID_Y", "GRID_XY",
                     "LNG", "LAT")
  src_col_classes <- c("character", "character", "character",
                       "integer", "integer", "character",
                       "numeric", "numeric")
  sinfo <- read.table(src_file, sep = ",", header = TRUE,
                      colClasses = src_col_classes,
                      strip.white = TRUE,
                      stringsAsFactors = FALSE,
                      fileEncoding = "GBK")

  reg_col <- factor(sinfo[ , c(reg_key)])
  sinfo$REG <- as.integer(reg_col)
  sinfo <- sinfo[order(sinfo$REG), ]

  reg_info <- sinfo[ , c("REG", "SID")]
  write.table(reg_info, dst_file, sep = ",", quote = FALSE, row.names = FALSE,
              fileEncoding = "GBK")
}

#
# Generate W tables from model files
#   model file name: subject_model.csv, eg. O3_camx_d02_0d.csv
#                    subject = O3, model = camx_d02_0d
#   w_tbl file name: work_subject_variable.csv, eg m4_O3_D20150901.csv
#                    work = m4, subject = O3, variable = D20150901
#                    the variable is parsed from model file automaticlly
#
# xmodels_str/addons_str should be character vectors
#   xmodels_str: provides models (and/or obs) in this work
#   addons_str: specifies extra empty cols in W table (if needed)
#
# W table (for each day) -->
# | REG | SID | model1 | model2 | ... | obs | afc | res |
#
# . REG = region
# . SID = station ID
# . model 1-N
# . obs = observation
# . afc = aggratioin_forecast
# . res = residual
#
AAFC_gen_W_table <- function(src_dir, dst_dir, work_name, subject, xmodels_str,
                             addons_str = character(0), REG_file)
{
  # prepare for REG file
  REG_col_names <- c("REG", "SID")
  REG_col_classes <- c("integer", "character")

  # read in REG file
  REG_tbl <- read.table(REG_file, sep = ",", header = TRUE,
                        colClasses = REG_col_classes,
                        strip.white = TRUE,
                        stringsAsFactors = FALSE)

  # prepare for model tables
  nr_xmodels <- length(xmodels_str)
  model_file <- paste(src_dir, "/", subject, "_", xmodels_str, ".csv", sep = "")
  model_tbl <- list()

  # read in model table and merge with REG_tbl
  for (i in 1:nr_xmodels) {
    message("reading in ", model_file[i])
    tbl <- read.table(model_file[i], sep = ",", header = TRUE,
                                 colClasses = c("character"),
                                 strip.white = TRUE,
                                 stringsAsFactors = FALSE)
    model_tbl[[i]] <- merge(REG_tbl, tbl, by = "SID", sort = FALSE)
  }

  # check consistency of models
  for (i in 1:nr_xmodels) {
    if (i == 1) {
      model_var <- names(model_tbl[[i]])
      REG_sid <- REG_tbl[ , "SID"]
    }
    if (!identical(model_var, names(model_tbl[[i]]))) {
      stop("Inconsistent variables in models\n")
    }
    if (!identical(REG_sid, model_tbl[[i]][ , "SID"])) {
      stop("Inconsistent SID between REG_file and models\n")
    }
  }

  # prepare for W tables
  model_var <- names(model_tbl[[1]])
  w_var <- model_var[c(-1, -2)]   # remove "REG", "SID"
  nr_vars <- length(w_var)
  nr_addons <- length(addons_str)
  addon <- rep(-999, nrow(REG_tbl))
  w_file <- paste(dst_dir, "/", work_name, "_", subject, "_", w_var,
                     ".csv", sep = "")

  # generate W tables
  for (i in 1:nr_vars) {
    message("handling ", w_var[i])
    w_tbl <- REG_tbl
    for (j in 1:nr_xmodels) {
      nc <- ncol(w_tbl) + 1
      w_tbl[ , nc] <- model_tbl[[j]][ , 2 + i]
      names(w_tbl)[nc] <- xmodels_str[j]
    }

    if (nr_addons > 0) {
      for (j in 1:nr_addons) {
        w_tbl <- cbind(w_tbl, addon)
        names(w_tbl)[ncol(w_tbl)] <- addons_str[j]
      }
    }

    if (i == 1) {
      if (dir.exists(dst_dir) == FALSE) {
        dir.create(dst_dir)
      }
    }
    write.table(w_tbl, w_file[i], sep = ",", quote = FALSE, row.names = FALSE,
                fileEncoding = "GBK")
  }
}

#
# U table (for each day) -->
# | REG | snr | snr_v | avg_afc | avg_obs | avg_e | sum_se | lambda |
# | b0 | u1 | u2 | .. | A11 | A12 | ..
#
# REG               = region
# snr               = number of stations in the region
# snr_v             = valid stations (without data missing) in the region
# avg_afc           = average afc value of valid stations in the region
# avg_obs           = average obs value of valid stations in the region
# avg_e             = average error value of valid staions in the region
# sum_se            = summation of squared error of valid stations in the region
# lambda            = lambda used for ridge-regression
# b0                = constant term of regression
# u 1-N             = weight for models of regression
# A 11-(N+1)(N+1)   = the X'X matrix of regression
#
AAFC_gen_U_table <- function(dst_dir, dst_file, models_str, REG_file)
{
  # prepare for REG file
  REG_col_names <- c("REG", "SID")
  REG_col_classes <- c("integer", "character")

  # read in REG file
  REG_tbl <- read.table(REG_file, sep = ",", header = TRUE,
                        colClasses = REG_col_classes,
                        strip.white = TRUE,
                        stringsAsFactors = FALSE)

  # prepare for U_tbl
  nr_regions <- REG_tbl[nrow(REG_tbl), 1]
  nr_models <- length(models_str)
  if (nr_models < 1) {
    stop("Invalid models_str length")
  }
  nr_u <- nr_models
  nr_a <- nr_models + 1

  # generate U_tbl
  REG <- seq(1, nr_regions, 1)
  snr <- rep(0, nr_regions)
  tmp <- rep(0, nr_regions)

  U_tbl <- data.frame(REG, snr)
  for (i in 1:nr_regions) {
    REG_i <- subset(REG_tbl, REG == i)
    U_tbl[i, 2] <- nrow(REG_i)
  }

  U_tbl <- cbind(U_tbl, tmp)
  names(U_tbl)[ncol(U_tbl)] <- "snr_v"
  U_tbl <- cbind(U_tbl, tmp)
  names(U_tbl)[ncol(U_tbl)] <- "avg_afc"
  U_tbl <- cbind(U_tbl, tmp)
  names(U_tbl)[ncol(U_tbl)] <- "avg_obs"
  U_tbl <- cbind(U_tbl, tmp)
  names(U_tbl)[ncol(U_tbl)] <- "avg_e"
  U_tbl <- cbind(U_tbl, tmp)
  names(U_tbl)[ncol(U_tbl)] <- "sum_se"
  U_tbl <- cbind(U_tbl, tmp)
  names(U_tbl)[ncol(U_tbl)] <- "lambda"
  U_tbl <- cbind(U_tbl, tmp)
  names(U_tbl)[ncol(U_tbl)] <- "b0"

  for (i in 1:nr_u) {
    U_tbl <- cbind(U_tbl, tmp)
    names(U_tbl)[ncol(U_tbl)] <- paste("u_", models_str[i], sep = "")
  }

  for (i in 1:nr_a) {
    for (j in 1:nr_a) {
      U_tbl <- cbind(U_tbl, tmp)
      names(U_tbl)[ncol(U_tbl)] <- paste("A_", as.character(i),
                                         as.character(j), sep = "")
    }
  }

  if (dir.exists(dst_dir) == FALSE) {
    dir.create(dst_dir)
  }
  U_file <- paste(dst_dir, "/", dst_file, sep = "")
  write.table(U_tbl, U_file, sep = ",", quote = FALSE, row.names = FALSE,
              fileEncoding = "GBK")
}

#
# reset U table
#   1) set all values to 0, except for col "REG" and "snr"
#   2) set col "lambda" and related "Axx" to lambda
#
# U table (for each day) -->
# | REG | snr | snr_v | avg_afc | avg_obs | avg_e | sum_se | lambda |
# | b0 | u1 | u2 | .. | A11 | A12 | ..
#
AAFC_rst_U_table <- function(U_file, nr_models, lambda)
{
  U_fixed_names <- c("REG", "snr", "snr_v", "avg_afc", "avg_obs", "avg_e",
                     "sum_se", "lambda", "b0")
  U_fixed_cols <- length(U_fixed_names)
  U_ex_cols <- nr_models + ((nr_models + 1) ^ 2)
  U_cols <- U_fixed_cols + U_ex_cols
  U_col_classes <- c("integer", "integer", "integer", "numeric", "numeric",
                     "numeric", "numeric", "integer", "numeric",
                     rep("numeric", U_ex_cols))

  U_tbl <- read.table(U_file, sep = ",", header = TRUE,
                      colClasses = U_col_classes,
                      strip.white = TRUE,
                      stringsAsFactors = FALSE)

  if (!identical(names(U_tbl)[1:U_fixed_cols], U_fixed_names)) {
    stop("seems U_file is not a U table")
  }
  if (ncol(U_tbl) != U_cols) {
    stop("seems U_file is not a U table")
  }

  U_tbl[ , (U_fixed_cols + 1):U_cols] <- 0

  U_tbl[ , c("lambda")] <- lambda

  nr_a <- nr_models + 1
  idx_a <- U_fixed_cols + nr_models
  for (i in 1:nr_a) {
    for (j in 1:nr_a) {
      idx_a <- idx_a + 1
      if (i == j) {
        U_tbl[ , idx_a] <- lambda
      }
    }
  }

  write.table(U_tbl, U_file, sep = ",", quote = FALSE, row.names = FALSE,
              fileEncoding = "GBK")
}

#
# Prepare for work directory
#
AAFC_gen_work_dir <- function(models_dir, subject, models_str, obs_str,
                              work_dir, work_name, REG_file)
{
  xmodels_str <- c(models_str, obs_str)
  U_dir <- paste(work_dir, "/U", sep = "")
  U_name <- "U.csv"

  message("Generating work directory")
  message("Generating W tables...")
  AAFC_gen_W_table(models_dir, work_dir, work_name, subject, xmodels_str,
                   c("afc", "res"), REG_file)
  message("Generating U table...")
  AAFC_gen_U_table(U_dir, U_name, models_str, REG_file)
  message("Generating work directory OK")
}

#
# W file: workname_subject_date.csv
# U file: U/U_date.csv
#
# W table (for each day) -->
# | REG | SID | model1 | model2 | ... | obs | afc | res |
#
# U table (for each day) -->
# | REG | snr | snr_v | avg_afc | avg_obs | avg_e | sum_se | lambda |
# | b0 | u1 | u2 | .. | A11 | A12 | ..
#
# Note: in working process, data missing in W table is set to NA
# Sub_note: assumes in W table, obs column is named "obs_0d"
# Sub_note: assumes in W table, obs follows the last model column
#
# afc_lookahead: AFC forecast period (predicted day - observed day)
#
# to handle afc_lookahead, use copy of U_tbl for different operations
# UP_tbl for predict, saved to the day to be predicted
# UT_tbl for training, saved to the next day of current day
# US_tbl for training statistic convenience, saved to current day
#
AAFC_work_temporal <- function(work_dir, work_name, subject,
                               start_day, end_day,
                               nr_models, afc_lookahead = 1,
                               reset_work = FALSE, lambda = 0,
                               do_predict = TRUE,
                               do_observe = TRUE,
                               do_train = TRUE,
                               predict_handler = AAFC_do_predict_RR,
                               observe_handler = AAFC_do_observe_NORMAL,
                               train_handler = AAFC_do_train_RR,
                               show_missing = FALSE)
{
  # param check
  if (afc_lookahead < 1) {
    stop("Invalid afc_lookahead")
  }

  # day indexes
  sday <- as.Date(start_day)
  eday <- as.Date(end_day)
  if (sday > eday) {
    tmp <- sday
    sday <- eday
    eday <- tmp
  }

  # U_tbl for extra afc_lookahead days
  nr_days <- difftime(eday, sday, units = "days") + 1
  nr_idx_days <- nr_days + afc_lookahead
  day_seq_by <- "day"
  day_idx <- seq(sday, by = day_seq_by, length.out = nr_idx_days)
  day_idx <- format(day_idx, "%Y%m%d")
  day_prefix <- "D"
  day_prefix_idx <- paste(day_prefix, day_idx, sep = "")

  # file names
  W_name <- sprintf("%s/%s_%s_%s.csv", work_dir, work_name, subject,
                    day_prefix_idx)
  UP_name <- sprintf("%s/U/UP_%s.csv", work_dir, day_prefix_idx)
  UT_name <- sprintf("%s/U/UT_%s.csv", work_dir, day_prefix_idx)
  US_name <- sprintf("%s/U/US_%s.csv", work_dir, day_prefix_idx)
  U_default <- sprintf("%s/U/U.csv", work_dir)

  # prepare for W/U file
  W_col_names <- c("REG", "SID")
  W_ex_cols <- nr_models + 3
  W_col_classes <- c("integer", "character", rep("numeric", W_ex_cols))

  U_fixed_names <- c("REG", "snr", "snr_v", "avg_afc", "avg_obs", "avg_e",
                     "sum_se", "lambda", "b0")
  U_fixed_cols <- length(U_fixed_names)
  U_ex_cols <- nr_models + ((nr_models + 1) ^ 2)
  U_cols <- U_fixed_cols + U_ex_cols
  U_col_classes <- c("integer", "integer", "integer", "numeric", "numeric",
                     "numeric", "numeric", "integer", "numeric",
                     rep("numeric", U_ex_cols))

  # reset_work
  #   TRUE: start new sequence
  #   FALSE: continue with current sequence
  #
  if (reset_work) {
    AAFC_rst_U_table(U_default, nr_models, lambda)
    file.copy(U_default, UT_name[1], overwrite = T)
    for (i in 1:afc_lookahead) {
      file.copy(U_default, UP_name[i], overwrite = T)
    }
  }

  # work for each day
  UT_tbl <- read.table(UT_name[1], sep = ",", header = TRUE,
                          colClasses = U_col_classes,
                          strip.white = TRUE,
                          stringsAsFactors = FALSE)

  for (i in 1:nr_days) {
    message("Processing ", day_idx[i], " --->")

    if (afc_lookahead == 1) {
      UP_tbl <- UT_tbl
    } else {
      UP_tbl <- read.table(UP_name[i], sep = ",", header = TRUE,
                          colClasses = U_col_classes,
                          strip.white = TRUE,
                          stringsAsFactors = FALSE)
    }

    W_tbl <- read.table(W_name[i], sep = ",", header = TRUE,
                        colClasses = W_col_classes,
                        strip.white = TRUE,
                        stringsAsFactors = FALSE)
    W_tbl[W_tbl == -999] <- NA

    update_W <- FALSE
    update_U <- FALSE

    # data process
    if (do_predict) {
      result <- predict_handler(W_tbl, UP_tbl, nr_models, show_missing)
      W_tbl <- result$W_tbl
      update_W <- TRUE
    }
    if (do_observe) {
      result <- observe_handler(W_tbl, UP_tbl, nr_models, show_missing)
      W_tbl <- result$W_tbl
      update_W <- TRUE
    }
    if (do_train) {
      result <- train_handler(W_tbl, UT_tbl, nr_models, show_missing)
      UT_tbl <- result$U_tbl
      update_U <- TRUE
    }

    # file update
    if (update_W) {
      write.table(W_tbl, W_name[i], sep = ",", quote = FALSE,
                  row.names = FALSE, fileEncoding = "GBK")
    }
    if (update_U) {
      write.table(UT_tbl, UT_name[i + 1], sep = ",", quote = FALSE,
                  row.names = FALSE, fileEncoding = "GBK")
      file.copy(UT_name[i + 1], UP_name[i + afc_lookahead], overwrite = T)
      file.copy(UT_name[i + 1], US_name[i], overwrite = T)
    }
  }
}

#
# Note: assumes in W table, obs column is named "obs_0d"
#
AAFC_do_observe_NORMAL <- function(W_tbl, U_tbl, nr_models, show_missing = F)
{
  # add obs to W table
  # need to do

  # update res to W table
  afc <- W_tbl[ , "afc"]
  obs <- W_tbl[ , "obs_0d"]
  res <- afc - obs
  W_tbl[ , "res"] <- res

  ret_list <- list(W_tbl = W_tbl)
  return(ret_list)
}

#
# W table (for each day) -->
# | REG | SID | model1 | model2 | ... | obs | afc | res |
#
# U table (for each day) -->
# | REG | snr | snr_v | avg_afc | avg_obs | avg_e | sum_se | lambda |
# | b0 | u1 | u2 | .. | A11 | A12 | ..
#
AAFC_do_predict_RR <- function(W_tbl, U_tbl, nr_models, show_missing = FALSE)
{
  # col indexes
  W_col_names <- names(W_tbl)
  U_col_names <- names(U_tbl)
  idx_SID <- which(W_col_names == "SID")[1]
  idx_m <- idx_SID + 1
  idx_b0 <- which(U_col_names == "b0")[1]
  idx_u <- idx_b0 + 1
  idx_a <- which(U_col_names == "A_11")[1]

  regions <- U_tbl[ , c("REG")]

  for (i in regions) {
    # original data from data frame
    u <- U_tbl[i, idx_b0:(idx_b0 + nr_models), drop = F] # b0, u(1..nr_models)
    m <- W_tbl[W_tbl$REG == i, idx_m:(idx_m + nr_models - 1), drop = F]
    m <- cbind(rep(1.0, nrow(m)), m)  # add regression const col

    # set to matrix format
    u <- as.matrix(u)
    m <- as.matrix(m)

    # calculate AFC
    afc <- m %*% t(u)

    # update to W table
    W_tbl[W_tbl$REG == i, "afc"] <- as.vector(afc)
  }

  ret_list <- list(W_tbl = W_tbl)
  return(ret_list)
}

#
# W table (for each day) -->
# | REG | SID | model1 | model2 | ... | obs | afc | res |
#
# U table (for each day) -->
# | REG | snr | snr_v | avg_afc | avg_obs | avg_e | sum_se | lambda |
# | b0 | u1 | u2 | .. | A11 | A12 | ..
#
# Note: assumes in W table, obs follows the last model column
#
AAFC_do_train_RR <- function(W_tbl, U_tbl, nr_models, show_missing = FALSE)
{
  library("MASS")
  regions <- U_tbl[ , c("REG")]
  nr_a <- nr_models + 1
  missing_map <- rep(".", length(regions))

  # col indexes
  W_col_names <- names(W_tbl)
  U_col_names <- names(U_tbl)
  idx_SID <- which(W_col_names == "SID")[1]
  idx_m <- idx_SID + 1
  idx_b0 <- which(U_col_names == "b0")[1]
  idx_u <- idx_b0 + 1
  idx_a <- which(U_col_names == "A_11")[1]

  for (i in regions) {
    # original data from data frame
    Ut <- U_tbl[i, idx_b0:(idx_b0 + nr_models), drop = F] # b0, u(1..nr_models)
    At_line <- U_tbl[i, idx_a:(idx_a + nr_a ^ 2 - 1), drop = F]
    m_obs <- W_tbl[W_tbl$REG == i, idx_m:(idx_m + nr_models), drop = F] # m, obs
    m_obs <- cbind(rep(1.0, nrow(m_obs)), m_obs)  # add regression const col

    # omit NA
    m_obs_valid <- na.omit(m_obs)
    m <- m_obs_valid[1:(ncol(m_obs_valid) - 1)]
    obs <- m_obs_valid[ncol(m_obs_valid)]

    # set to matrix format
    Ut <- as.matrix(Ut)
    At <- matrix(as.vector(t(At_line)), nrow = nr_a, ncol = nr_a, byrow = T)
    m <- as.matrix(m)
    obs <- as.matrix(obs)

    # data missing check
    full_snr <- nrow(m_obs)
    valid_snr <- nrow(m_obs_valid)
    if (valid_snr == 0) {
      missing_map[i] <- "O"
    } else if (valid_snr != full_snr) {
      missing_map[i] <- "*"
    }

    # regression training
    if (valid_snr > 0) {
      # calculate RES
      afc <- Ut %*% t(m)
      res <- afc - t(obs)

      # calculate At1 and At1'
      At1 <- At + t(m) %*% m
      At1_inv <- ginv(At1)

      # calculate Ut1
      xy <- res %*% m
      Ut1 <- Ut - xy %*% t(At1_inv)
    } else {
      # keep old A/U
      afc <- NA
      obs <- NA
      res <- NA
      At1 <- At
      Ut1 <- Ut
    }

    # update to U table
    U_tbl[i, "snr_v"] <- valid_snr
    U_tbl[i, "avg_afc"] <- mean(afc)
    U_tbl[i, "avg_obs"] <- mean(obs)
    U_tbl[i, "avg_e"] <- mean(res)
    U_tbl[i, "sum_se"] <- sum(res ^ 2)
    U_tbl[i, idx_b0:(idx_b0 + nr_models)] <- Ut1
    At1_line <- matrix(t(At1), nrow = 1)
    U_tbl[i, idx_a:(idx_a + nr_a ^ 2 - 1)] <- At1_line
  }

  if (show_missing) {
    message("Missing_T: ", missing_map)
  }

  ret_list <- list(W_tbl = W_tbl, U_tbl = U_tbl)
  return(ret_list)
}

#
# hyper training process, find best param for regression
#
# currently use region RMSE to estimate lambda, see
# AAFC_analyse_U_temporal() for definition of region RMSE
#
AAFC_hyper_training <- function(work_dir, work_name, subject,
                                   start_day, end_day,
                                   nr_models, afc_lookahead = 1,
                                   reset_work = FALSE, lambda = 0,
                                   do_predict = TRUE,
                                   do_observe = TRUE,
                                   do_train = TRUE,
                                   predict_handler = AAFC_do_predict_RR,
                                   observe_handler = AAFC_do_observe_NORMAL,
                                   train_handler = AAFC_do_train_RR,
                                   show_missing = FALSE,
                                   lambda_start = 0,
                                   lambda_end = 10000,
                                   lambda_step = 100)
{
  # params
  lambda_seq <- seq(lambda_start, lambda_end, lambda_step)
  rmse_list <- list()
  mae_list <- list()

  # training & analyzing rounds
  for (lambda_x in lambda_seq) {
    message("\n\n[*] Hyper training with lambda = ", lambda_x)
    AAFC_work_temporal(work_dir = work_dir,
                       work_name = work_name,
                       subject = subject,
                       start_day = start_day,
                       end_day = end_day,
                       nr_models = nr_models,
                       afc_lookahead = afc_lookahead,
                       reset_work = reset_work,
                       lambda = lambda_x,
                       do_predict = do_predict,
                       do_observe = do_observe,
                       do_train = do_train,
                       predict_handler = predict_handler,
                       observe_handler = observe_handler,
                       train_handler = train_handler,
                       show_missing = show_missing)

    message("[*] Round result analyzing")
    ret <- AAFC_analyse_U_temporal(work_dir, start_day, end_day)
    rmse_reg <- ret$RMSE
    mae_reg <- ret$MAE

    # append to list
    rmse_list <- append(rmse_list, list(rmse_reg))
    mae_list <- append(mae_list, list(mae_reg))
  }

  message("\n\n[*] Hyper analyzing")

  # serializing test data
  sel <- function(x) x
  rmse_tbl <- as.data.frame(sapply(rmse_list, sel))
  mae_tbl <- as.data.frame(sapply(mae_list, sel))
  names(rmse_tbl) <- paste("lambda_", lambda_seq, sep = "")
  names(mae_tbl) <- paste("lambda_", lambda_seq, sep = "")

  # test data file name
  sday <- format(as.Date(start_day), "%Y%m%d")
  eday <- format(as.Date(end_day), "%Y%m%d")
  test_name <- sprintf("%s-%s", sday, eday)
  P_dir <- paste(work_dir, "/P", sep = "")
  RMSE_file <- paste(P_dir, "/", test_name, "_RMSE.csv", sep = "")
  MAE_file <- paste(P_dir, "/", test_name, "_MAE.csv", sep = "")
  lambda_v_file <- paste(P_dir, "/", test_name, "_lambda_v.csv", sep = "")

  if (dir.exists(P_dir) == FALSE) {
    dir.create(P_dir)
  }

  # merge test data with exist file
  if (file.exists(RMSE_file)) {
    message("Reading ", RMSE_file)
    rmse_old_tbl <- read.table(RMSE_file, sep = ",", header = TRUE,
                               stringsAsFactors = FALSE)
    for (i in names(rmse_tbl)) {
      rmse_old_tbl[i] <- rmse_tbl[i]
    }
    rmse_tbl <- rmse_old_tbl
  }

  if (file.exists(MAE_file)) {
    message("Reading ", MAE_file)
    mae_old_tbl <- read.table(MAE_file, sep = ",", header = TRUE,
                              stringsAsFactors = FALSE)
    for (i in names(mae_tbl)) {
      mae_old_tbl[i] <- mae_tbl[i]
    }
    mae_tbl <- mae_old_tbl
  }

  # save test data to file
  message("Writing ", RMSE_file)
  write.table(rmse_tbl, RMSE_file, sep = ",", quote = FALSE,
              row.names = FALSE, fileEncoding = "GBK")
  message("Writing ", MAE_file)
  write.table(mae_tbl, MAE_file, sep = ",", quote = FALSE,
              row.names = FALSE, fileEncoding = "GBK")

  # calculate best lambda_v
  sel2 <- function(x) x[2]
  rmse_tbl[is.na(rmse_tbl)] <- Inf
  idx_v <- apply(rmse_tbl, 1, which.min)
  str_v <- names(rmse_tbl)[idx_v]
  str_v <- strsplit(str_v, "_")
  str_v <- sapply(str_v, sel2)
  rmse_lambda_v <- as.numeric(str_v)

  mae_tbl[is.na(mae_tbl)] <- Inf
  idx_v <- apply(mae_tbl, 1, which.min)
  str_v <- names(mae_tbl)[idx_v]
  str_v <- strsplit(str_v, "_")
  str_v <- sapply(str_v, sel2)
  mae_lambda_v <- as.numeric(str_v)

  # save lambda_v_tbl to file
  message("Writing ", lambda_v_file)
  lambda_v_tbl <- data.frame(rmse_lambda_v, mae_lambda_v)
  write.table(lambda_v_tbl, lambda_v_file, sep = ",", quote = FALSE,
              row.names = FALSE, fileEncoding = "GBK")

  # use default lambda_v (here is rmse_lambda_v) to run work
  lambda_v <- rmse_lambda_v
  message("\n\n[*] Hyper training with lambda_v")
  AAFC_work_temporal(work_dir = work_dir,
                     work_name = work_name,
                     subject = subject,
                     start_day = start_day,
                     end_day = end_day,
                     nr_models = nr_models,
                     afc_lookahead = afc_lookahead,
                     reset_work = reset_work,
                     lambda = lambda_v,
                     do_predict = do_predict,
                     do_observe = do_observe,
                     do_train = do_train,
                     predict_handler = predict_handler,
                     observe_handler = observe_handler,
                     train_handler = train_handler,
                     show_missing = show_missing)

  message("[*] Round result analyzing")
  AAFC_analyse_U_temporal(work_dir, start_day, end_day)
}

#
# Analyse AAFC U results, targets include RMSE and MAE
#
# Note: RMSE of region is calculated by all valid stations in the region
#       individually, not related to any average results of that region
# Note: MAE of region is calculated based on average results of the region
#
AAFC_analyse_U_temporal <- function(work_dir, start_day, end_day)
{
  # data dir
  U_dir <- paste(work_dir, "/U", sep = "")
  S_dir <- paste(work_dir, "/U/S", sep = "")

  # serialize statitics files
  SS_prefix <- "US_"
  AAFC_table_serialize(U_dir, SS_prefix, S_dir, SS_prefix, start_day, end_day,
                       select = c("snr_v", "avg_e", "sum_se"))
  SNR_file <- paste(S_dir, "/", SS_prefix, "snr_v", ".csv", sep = "")
  SE_file <- paste(S_dir, "/", SS_prefix, "sum_se", ".csv", sep = "")
  AE_file <- paste(S_dir, "/", SS_prefix, "avg_e", ".csv", sep = "")

  # read in statistics tables
  SNR_tbl <- read.table(SNR_file, sep = ",", header = T, stringsAsFactors = F)
  SE_tbl <- read.table(SE_file, sep = ",", header = T, stringsAsFactors = F)
  AE_tbl <- read.table(AE_file, sep = ",", header = T, stringsAsFactors = F)

  SNR <- as.matrix(SNR_tbl)
  SE <- as.matrix(SE_tbl)
  AE <- abs(as.matrix(AE_tbl))

  AEV <- AE
  AEV[!is.na(AEV)] <- 1
  AEV[is.na(AEV)] <- 0

  # sum to region
  snr_reg <- apply(SNR, 1, sum)
  snr_reg[snr_reg == 0] <- NA
  se_reg <- apply(SE, 1, sum, na.rm = T)
  aev_reg <- apply(AEV, 1, sum)
  aev_reg[aev_reg == 0] <- NA
  ae_reg <- apply(AE, 1, sum, na.rm = T)

  # calculate RMSE/MAE
  RMSE <- sqrt(se_reg / snr_reg)
  MAE <- ae_reg / aev_reg

  # save analyse results
  analyse_tbl <- data.frame(RMSE, MAE)
  analyse_file <- paste(S_dir, "/", SS_prefix, "ANALYSE", ".csv", sep = "")
  message("Writing ", analyse_file)
  write.table(analyse_tbl, analyse_file, sep = ",", quote = FALSE,
              row.names = FALSE, fileEncoding = "GBK")

  # return RMSE/MAE
  return(list(RMSE = RMSE, MAE = MAE))
}

#
# Analyse AAFC W results, targets include RMSE and MAE
#
# serialized work data can be binded with different REGION table,
# so RMSE/MAE can be analysed by different region policies
#
# analyse result is saved in work_dir/U/S
#
AAFC_analyse_W_temporal <- function(work_dir, work_name, subject,
                                    start_day, end_day, reg_file)
{
  # result dir
  WS_prefix <- "WS_"
  WS_dir <- paste(work_dir, "/U/S", sep = "")

  # serialize statistics data in work files
  work_prefix <- paste(work_name, "_", subject, "_", sep = "")
  AAFC_table_serialize(work_dir, work_prefix, WS_dir, WS_prefix,
                       start_day, end_day,
                       select = c("obs_0d", "afc", "res"),
                       bind = reg_file)

  # WS file names
  OBS_file <- paste(WS_dir, "/", WS_prefix, "obs_0d.csv", sep = "")
  AFC_file <- paste(WS_dir, "/", WS_prefix, "afc.csv", sep = "")
  RES_file <- paste(WS_dir, "/", WS_prefix, "res.csv", sep = "")

  # REG file names
  REG_prefix <- "REG_"
  OBS_reg_file <- paste(WS_dir, "/", REG_prefix, "obs_0d.csv", sep = "")
  AFC_reg_file <- paste(WS_dir, "/", REG_prefix, "afc.csv", sep = "")
  RES_reg_file <- paste(WS_dir, "/", REG_prefix, "res.csv", sep = "")
  ANALYSE_file <- paste(WS_dir, "/", REG_prefix, "ANALYSE.csv", sep = "")

  # set correct col_classes
  sday <- as.Date(start_day)
  eday <- as.Date(end_day)
  WS_ex_cols <- abs(difftime(sday, eday, units = "days")) + 1
  WS_col_classes <- c("integer", "character", rep("numeric", WS_ex_cols))

  # read in statistics tables
  message("Reading ", OBS_file)
  OBS_tbl <- read.table(OBS_file, sep = ",", header = T,
                        colClasses = WS_col_classes,
                        stringsAsFactors = F)
  message("Reading ", AFC_file)
  AFC_tbl <- read.table(AFC_file, sep = ",", header = T,
                        colClasses = WS_col_classes,
                        stringsAsFactors = F)
  message("Reading ", RES_file)
  RES_tbl <- read.table(RES_file, sep = ",", header = T,
                        colClasses = WS_col_classes,
                        stringsAsFactors = F)

  # reshape to region mean table
  library("reshape2")
  message("Reshaping...")
  md <- melt(OBS_tbl, id = c("REG", "SID"))
  OBS_reg_tbl <- dcast(md, REG~variable, mean, na.rm = T)
  md <- melt(AFC_tbl, id = c("REG", "SID"))
  AFC_reg_tbl <- dcast(md, REG~variable, mean, na.rm = T)
  md <- melt(RES_tbl, id = c("REG", "SID"))
  RES_reg_tbl <- dcast(md, REG~variable, mean, na.rm = T)

  # calculate region RMSE/MAE
  message("Analyzing...")
  RES <- as.matrix(RES_reg_tbl[2:ncol(RES_reg_tbl)])
  AE <- abs(RES)
  SE <- AE ^ 2
  MAE <- apply(AE, 1, mean, na.rm = T)
  MSE <- apply(SE, 1, mean, na.rm = T)
  RMSE <- sqrt(MSE)

  # generate ANALYSE table
  ANALYSE_tbl <- data.frame(RES_reg_tbl[1], RMSE, MAE)
  avg <- apply(ANALYSE_tbl, 2, mean, na.rm = T)
  ANALYSE_tbl[, 1] <- as.character(ANALYSE_tbl[, 1])
  avg <- as.data.frame(t(avg))
  avg[, 1] <- "avg"
  ANALYSE_tbl <- rbind(ANALYSE_tbl, avg)

  # save tables to file
  message("Writing ", OBS_reg_file)
  write.table(OBS_reg_tbl, OBS_reg_file, sep = ",", quote = FALSE,
              row.names = FALSE, fileEncoding = "GBK")
  message("Writing ", AFC_reg_file)
  write.table(AFC_reg_tbl, AFC_reg_file, sep = ",", quote = FALSE,
              row.names = FALSE, fileEncoding = "GBK")
  message("Writing ", RES_reg_file)
  write.table(RES_reg_tbl, RES_reg_file, sep = ",", quote = FALSE,
              row.names = FALSE, fileEncoding = "GBK")
  message("Writing ", ANALYSE_file)
  write.table(ANALYSE_tbl, ANALYSE_file, sep = ",", quote = FALSE,
              row.names = FALSE, fileEncoding = "GBK")

  # return RMSE/MAE
  return(list(RMSE = RMSE, MAE = MAE))
}

#
# a common process to serialize day-divided tables with same format
#
# Note: data directly combined without merge, remember to check
# serialized keys to verify data consistency
#
AAFC_table_serialize <- function(src_dir, src_prefix, dst_dir, dst_prefix,
                                 start_day, end_day, select = "", bind = "")
{
  # day index
  sday <- as.Date(start_day)
  eday <- as.Date(end_day)
  if (sday <= eday) {
    nr_days <- difftime(eday, sday, units = "days") + 1
    day_seq_by <- "day"
  } else {
    nr_days <- difftime(sday, eday, units = "days") + 1
    day_seq_by <- "-1 day"
  }
  day_idx <- seq(sday, by = day_seq_by, length.out = nr_days)
  day_idx <- format(day_idx, "%Y%m%d")
  day_prefix <- "D"
  day_prefix_idx <- paste(day_prefix, day_idx, sep = "")

  # src_file names
  src_name <- sprintf("%s/%s%s.csv", src_dir, src_prefix, day_prefix_idx)

  # read in all src_file
  src_list <- list()
  for (i in 1:nr_days) {
    message("Reading ", src_name[i])
    tbl <- read.table(src_name[i], sep = ",", header = TRUE,
                        stringsAsFactors = FALSE)
    if (!identical(select, "")) {
      tbl <- tbl[select]
    }
    src_list <- append(src_list, list(tbl))
  }

  # col select function
  sel_col <- function(x, col_idx) x[ , col_idx]

  # serialize by column
  nr_col <- ncol(src_list[[1]])
  dst_var <- names(src_list[[1]])
  dst_name <- sprintf("%s/%s%s.csv", dst_dir, dst_prefix, dst_var)

  if (dir.exists(dst_dir) == FALSE) {
    dir.create(dst_dir)
  }

  for (i in 1:nr_col) {
    message("Writing ", dst_name[i])
    tbl <- as.data.frame(sapply(src_list, sel_col, i))
    names(tbl) <- day_prefix_idx
    if (!identical(bind, "")) {
      b_tbl <- read.csv(bind)
      tbl <- cbind(b_tbl, tbl)
    }
    write.table(tbl, dst_name[i], sep = ",", quote = FALSE,
                row.names = FALSE, fileEncoding = "GBK")
  }
}

