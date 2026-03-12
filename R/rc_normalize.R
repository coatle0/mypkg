#!/usr/bin/env R
# rc_normalize.R - normalization helpers for Risk Commander R client

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

# -------------------------
# Basic converters
# -------------------------

rc_int0 <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_integer_)
  x_chr <- trimws(as.character(x))
  x_chr[x_chr == ""] <- NA_character_
  suppressWarnings(as.integer(x_chr))
}

rc_num <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_real_)
  x_chr <- trimws(as.character(x))
  x_chr[x_chr == ""] <- NA_character_
  suppressWarnings(as.numeric(x_chr))
}

# -------------------------
# Detect first list field in response body
# -------------------------

rc_detect_first_list_field <- function(resp_body) {
  if (!is.list(resp_body) || length(resp_body) == 0) return(NULL)
  nms <- names(resp_body)
  if (is.null(nms)) return(NULL)
  for (nm in nms) {
    val <- resp_body[[nm]]
    if (is.list(val) && length(val) > 0) {
      return(list(name = nm, value = val))
    }
  }
  NULL
}

# -------------------------
# Account normalization
# -------------------------

rc_norm_acnt <- function(resp_body) {
  if (!is.list(resp_body)) {
    stop("resp_body must be a list (parsed JSON).")
  }

  # Positions list (if present)
  positions_list <- resp_body$stk_acnt_evlt_prst %||% list()
  if (!is.list(positions_list)) positions_list <- list()

  # Build summary data.frame from top-level numeric-like fields
  summary_fields <- c(
    "tot_est_amt",
    "aset_evlt_amt",
    "tot_pur_amt",
    "prsm_dpst_aset_amt"
  )

  summary_vals <- lapply(summary_fields, function(f) resp_body[[f]] %||% NA)
  names(summary_vals) <- summary_fields

  summary_df <- data.frame(
    tot_est_amt = rc_int0(summary_vals$tot_est_amt),
    aset_evlt_amt = rc_int0(summary_vals$aset_evlt_amt),
    tot_pur_amt = rc_int0(summary_vals$tot_pur_amt),
    prsm_dpst_aset_amt = rc_int0(summary_vals$prsm_dpst_aset_amt),
    stringsAsFactors = FALSE
  )

  # Positions to data.frame
  if (length(positions_list) == 0) {
    positions_df <- data.frame()
  } else {
    # Convert list-of-lists to data.frame with character columns first
    nms <- unique(unlist(lapply(positions_list, names)))
    mat <- lapply(nms, function(nm) vapply(positions_list, function(row) row[[nm]] %||% NA, character(1)))
    names(mat) <- nms
    positions_df <- as.data.frame(mat, stringsAsFactors = FALSE)

    # Numeric parsing for *_amt, *_qty, and typical numeric-looking fields
    for (col in names(positions_df)) {
      col_lower <- tolower(col)
      if (grepl("(_amt|_qty|_prc|_rate|_eval|_evlt)$", col_lower)) {
        positions_df[[col]] <- rc_num(positions_df[[col]])
      }
    }
  }

  list(summary = summary_df, positions = positions_df)
}

# -------------------------
# OHLC normalization
# -------------------------

rc_norm_ohlc <- function(resp_body) {
  if (!is.list(resp_body)) {
    stop("resp_body must be a list (parsed JSON).")
  }

  field <- rc_detect_first_list_field(resp_body)
  if (is.null(field)) return(data.frame())

  rows <- field$value
  if (!is.list(rows) || length(rows) == 0) return(data.frame())

  nms <- unique(unlist(lapply(rows, names)))
  mat <- lapply(nms, function(nm) vapply(rows, function(row) row[[nm]] %||% NA, character(1)))
  names(mat) <- nms
  df <- as.data.frame(mat, stringsAsFactors = FALSE)

  # Try to convert numeric-like columns
  for (col in names(df)) {
    # cntr_tm 은 항상 문자열로 유지
    if (identical(col, "cntr_tm")) next

    vals <- df[[col]]

    # 가격 필드: 앞의 +, - 기호 제거 후 숫자로 파싱
    if (col %in% c("cur_prc", "open_pric", "high_pric", "low_pric")) {
      cleaned <- gsub("^[+-]", "", trimws(as.character(vals)))
      df[[col]] <- rc_num(cleaned)
      next
    }

    # 그 외 컬럼은 heuristic 으로 숫자 여부 판단
    sample_vals <- na.omit(vals[seq_len(min(length(vals), 50))])
    if (length(sample_vals) == 0) next
    if (all(grepl("^[0-9\\-\\.]+$", sample_vals))) {
      df[[col]] <- rc_num(vals)
    }
  }

  df
}

