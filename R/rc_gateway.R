#!/usr/bin/env R
# rc_gateway.R - R client gateway for Risk Commander
# Reference style:
# - POST /kiwoom/{ohlc|ohlc_daily|acnt}?env=live|paper
# - POST /auto/*  : no env query (server uses account.env / strategy->account)
# - Request body: DIRECT JSON (no wrapper)
# - Paging: HTTP headers cont-yn / next-key
#
# Added for /auto/order (strategy_id SSOT): rc_post_no_env, rc_auto_order, rc_auto_order_safe,
#   rc_auto_buy_limit, rc_auto_buy_limit_ioc, rc_auto_order_sell, rc_auto_order_modify, rc_auto_order_cancel.

library(httr2)
library(jsonlite)

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

# -------------------------
# Gateway config
# -------------------------
rc_gateway <- function(host = "http://127.0.0.1:8000") {
  token <- Sys.getenv("RC_INTERNAL_TOKEN", "")
  if (identical(token, "")) {
    warning("RC_INTERNAL_TOKEN is empty; requests will fail until it is set.")
  }
  list(host = host, token = token)
}

# -------------------------
# Header helper (case-insensitive)
# -------------------------
rc_get_header <- function(headers, name) {
  if (is.null(headers) || length(headers) == 0) return(NA_character_)
  nms <- tolower(names(headers))
  idx <- match(tolower(name), nms)
  if (is.na(idx)) return(NA_character_)
  val <- headers[[idx]]
  if (length(val) == 0) return(NA_character_)
  as.character(val)[1]
}

# -------------------------
# Rate limit detector (HTTP 429 or return_code=5 message)
# -------------------------
rc_is_rate_limited <- function(res) {
  if (is.null(res)) return(FALSE)
  if (!is.null(res$status) && res$status == 429) return(TRUE)
  if (is.list(res$body)) {
    rc0 <- suppressWarnings(as.integer(res$body$return_code %||% NA))
    msg <- as.character(res$body$return_msg %||% "")
    if (!is.na(rc0) && rc0 == 5L && grepl("���� ��û", msg, fixed = TRUE)) return(TRUE)
  }
  FALSE
}

# -------------------------
# Low-level POST (no throw on 4xx/5xx)
# -------------------------
rc_post <- function(rc, path, body, env = "live",
                    cont_yn = NULL, next_key = NULL, query = list()) {
  if (rc$token == "") stop("RC_INTERNAL_TOKEN is not set in R session env")

  headers <- list(
    "Authorization" = paste("Bearer", rc$token),
    "Content-Type" = "application/json"
  )
  if (!is.null(cont_yn)) headers[["cont-yn"]] <- cont_yn
  if (!is.null(next_key)) headers[["next-key"]] <- next_key

  req <- request(paste0(rc$host, path)) |>
    req_url_query(env = env, !!!query) |>
    req_headers(!!!headers) |>
    req_body_json(body, auto_unbox = TRUE) |>
    req_error(is_error = function(resp) FALSE)

  resp <- req_perform(req)

  body_text <- resp_body_string(resp)
  body_json <- tryCatch(
    fromJSON(body_text, simplifyVector = FALSE),
    error = function(e) NULL
  )

  list(
    status = resp_status(resp),
    headers = resp_headers(resp),
    body_text = body_text,
    body = body_json
  )
}

# -------------------------
# Low-level POST for /auto/* (no env query; server uses account.env)
# -------------------------
rc_post_no_env <- function(rc, path, body, cont_yn = NULL, next_key = NULL, query = list()) {
  if (rc$token == "") stop("RC_INTERNAL_TOKEN is not set in R session env")

  headers <- list(
    "Authorization" = paste("Bearer", rc$token),
    "Content-Type" = "application/json"
  )
  if (!is.null(cont_yn)) headers[["cont-yn"]] <- cont_yn
  if (!is.null(next_key)) headers[["next-key"]] <- next_key

  req <- request(paste0(rc$host, path)) |>
    req_url_query(!!!query) |>
    req_headers(!!!headers) |>
    req_body_json(body, auto_unbox = TRUE) |>
    req_error(is_error = function(resp) FALSE)

  resp <- req_perform(req)

  body_text <- resp_body_string(resp)
  body_json <- tryCatch(
    fromJSON(body_text, simplifyVector = FALSE),
    error = function(e) NULL
  )

  list(
    status = resp_status(resp),
    headers = resp_headers(resp),
    body_text = body_text,
    body = body_json
  )
}

# -------------------------
# Generic paged POST using cont-yn / next-key headers
# -------------------------
rc_paged_post <- function(rc, path, body, env = "live", query = list(),
                          max_pages = 20, sleep_sec = 1.0,
                          max_retry_429 = 8, backoff_base = 1.0, backoff_cap = 30) {
  results <- list()
  cont_yn <- NULL
  next_key <- NULL
  page <- 1L

  while (page <= max_pages) {
    tries <- 0L

    repeat {
      res <- rc_post(rc, path, body, env = env, query = query, cont_yn = cont_yn, next_key = next_key)
      if (!rc_is_rate_limited(res)) break

      tries <- tries + 1L
      if (tries > max_retry_429) {
        stop("Rate limit persists. Increase sleep_sec or reduce max_pages.")
      }
      Sys.sleep(min(backoff_cap, backoff_base * (2^(tries - 1L))))
    }

    results[[length(results) + 1L]] <- res
    if (is.null(res$status) || res$status >= 400) break

    cont_h <- rc_get_header(res$headers, "cont-yn")
    next_h <- rc_get_header(res$headers, "next-key")
    if (is.na(cont_h) || toupper(cont_h) != "Y") break

    cont_yn <- "Y"
    next_key <- if (is.na(next_h)) "" else as.character(next_h)

    page <- page + 1L
    Sys.sleep(sleep_sec)
  }

  results
}

# -------------------------
# Endpoint helpers
# -------------------------

# OHLC minute (ka10080 via /kiwoom/ohlc)
rc_ohlc_minute <- function(rc, stk_cd, tic_scope = "1", upd_stkpc_tp = "1",
                           qry_cnt = NULL, base_dt = NULL,
                           env = "live", cont_yn = NULL, next_key = NULL) {
  body <- list(
    stk_cd = stk_cd,
    tic_scope = tic_scope,
    upd_stkpc_tp = upd_stkpc_tp
  )
  if (!is.null(qry_cnt)) body$qry_cnt <- qry_cnt
  if (!is.null(base_dt)) body$base_dt <- base_dt
  rc_post(rc, "/kiwoom/ohlc", body = body, env = env,
          cont_yn = cont_yn, next_key = next_key)
}

# OHLC daily (ka10081 via /kiwoom/ohlc_daily)
rc_ohlc_daily <- function(rc, stk_cd, base_dt, upd_stkpc_tp = "1",
                          env = "live", cont_yn = NULL, next_key = NULL) {
  body <- list(
    stk_cd = stk_cd,
    base_dt = base_dt,
    upd_stkpc_tp = upd_stkpc_tp
  )
  rc_post(rc, "/kiwoom/ohlc_daily", body = body, env = env,
          cont_yn = cont_yn, next_key = next_key)
}

# Account status (kt00004 via /kiwoom/acnt)
rc_acnt <- function(rc, qry_tp = "0", dmst_stex_tp = "KRX",
                    env = "live", cont_yn = NULL, next_key = NULL) {
  body <- list(
    qry_tp = qry_tp,
    dmst_stex_tp = dmst_stex_tp
  )
  rc_post(rc, "/kiwoom/acnt", body = body, env = env,
          cont_yn = cont_yn, next_key = next_key)
}

# Account order fills (kt00007 via /kiwoom/acnt, SSOT acnt_added.md)
rc_acnt_order_fills <- function(rc, qry_tp, stk_bond_tp, sell_tp, dmst_stex_tp,
                                ord_dt = "", stk_cd = "", fr_ord_no = "",
                                env = "live", cont_yn = NULL, next_key = NULL) {
  body <- list(
    qry_tp = qry_tp,
    stk_bond_tp = stk_bond_tp,
    sell_tp = sell_tp,
    dmst_stex_tp = dmst_stex_tp,
    ord_dt = as.character(ord_dt),
    stk_cd = as.character(stk_cd),
    fr_ord_no = as.character(fr_ord_no)
  )
  rc_post(rc, "/kiwoom/acnt", body = body, env = env,
          query = list(tr_api_id = "kt00007"),
          cont_yn = cont_yn, next_key = next_key)
}

# Account orderable cash (kt00010 via /kiwoom/acnt, SSOT acnt_added.md)
rc_acnt_orderable_cash <- function(rc, stk_cd, trde_tp, uv,
                                   io_amt = "", trde_qty = "", exp_buy_unp = "",
                                   env = "live", cont_yn = NULL, next_key = NULL) {
  body <- list(
    io_amt = as.character(io_amt),
    stk_cd = stk_cd,
    trde_tp = trde_tp,
    trde_qty = as.character(trde_qty),
    uv = as.character(uv),
    exp_buy_unp = as.character(exp_buy_unp)
  )
  rc_post(rc, "/kiwoom/acnt", body = body, env = env,
          query = list(tr_api_id = "kt00010"),
          cont_yn = cont_yn, next_key = next_key)
}

# Account profit rate (ka10085 via /kiwoom/acnt, SSOT acnt_added.md)
rc_acnt_profit_rate <- function(rc, stex_tp = "0",
                                env = "live", cont_yn = NULL, next_key = NULL) {
  body <- list(
    stex_tp = stex_tp
  )
  rc_post(rc, "/kiwoom/acnt", body = body, env = env,
          query = list(tr_api_id = "ka10085"),
          cont_yn = cont_yn, next_key = next_key)
}

# Account open orders (ka10075 via /kiwoom/acnt, SSOT acnt_added.md)
rc_acnt_open_orders <- function(rc,
                                all_stk_tp = "1",
                                trde_tp = "0",
                                stk_cd,
                                stex_tp = "0",
                                env = "live",
                                cont_yn = NULL,
                                next_key = NULL) {
  body <- list(
    all_stk_tp = all_stk_tp,
    trde_tp = trde_tp,
    stk_cd = stk_cd,
    stex_tp = stex_tp
  )
  rc_post(rc, "/kiwoom/acnt", body = body, env = env,
          query = list(tr_api_id = "ka10075"),
          cont_yn = cont_yn, next_key = next_key)
}

# Account-routed balance/account (POST /auto/acnt; env from account, query env ignored)
rc_auto_acnt <- function(rc, account_id, tr_api_id = "kt00004",
                        body = list(qry_tp = "0", dmst_stex_tp = "KRX")) {
  rc_post_no_env(rc, "/auto/acnt", body = body,
                 query = list(account_id = account_id, tr_api_id = tr_api_id))
}

# Dashboard by account: kt00004 + ka10085 for a single account_id
rc_print_acnt_dashboard3_by_account <- function(rc, account_id, env = "live", top_n = 20) {
  cat("\n================ ACNT DASHBOARD (", account_id, ") ================\n", sep = "")
  cat("time:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
  cat("---------------------------------------------------\n")

  # 1) kt00004
  ac <- rc_auto_acnt(rc, account_id, tr_api_id = "kt00004",
                     body = list(qry_tp = "0", dmst_stex_tp = "KRX"))

  if (is.null(ac$body$return_code) || as.integer(ac$body$return_code) != 0L) {
    cat("[ACNT] FAIL:", ac$body$return_code %||% NA, "\n")
    return(invisible(NULL))
  }

  b <- ac$body
  cat("[ACNT] OK | acct:", b$acnt_nm %||% "", "\n")
  cat("  예수금:", b$entr %||% "", "\n")

  # 2) ka10085
  pr <- rc_auto_acnt(rc, account_id, tr_api_id = "ka10085",
                     body = list(stex_tp = "0"))

  if (!is.null(pr$body$return_code) && as.integer(pr$body$return_code) == 0L) {
    rows <- pr$body$acnt_prft_rt %||% list()
    cat("[HOLD] positions:", length(rows), "\n")
  } else {
    cat("[HOLD] FAIL\n")
  }

  cat("===================================================\n\n")
  invisible(list(acnt = ac, hold = pr))
}

# Order (tr_api_id required, e.g. kt10000 via /kiwoom/order)
# LEGACY: 멀티계좌 SSOT 라우팅 아님(쿼리 env/기본 cred_profile). 운영에서는 rc_auto_* 사용 권장.
rc_order <- function(rc, tr_api_id, body, env = "live", cont_yn = NULL, next_key = NULL) {
  rc_post(rc, "/kiwoom/order", body = body, env = env,
          query = list(tr_api_id = tr_api_id), cont_yn = cont_yn, next_key = next_key)
}

# Order sell (kt10001 via /kiwoom/order, SSOT order_add.md)
# LEGACY: 멀티계좌 SSOT 라우팅 아님(쿼리 env/기본 cred_profile). 운영에서는 rc_auto_* 사용 권장.
rc_order_sell <- function(rc, dmst_stex_tp, stk_cd, ord_qty, ord_uv = "", trde_tp = "3",
                          cond_uv = "", env = "paper", cont_yn = NULL, next_key = NULL) {
  body <- list(
    dmst_stex_tp = dmst_stex_tp,
    stk_cd = stk_cd,
    ord_qty = as.character(ord_qty),
    ord_uv = as.character(ord_uv),
    trde_tp = trde_tp,
    cond_uv = as.character(cond_uv)
  )
  rc_order(rc, tr_api_id = "kt10001", body = body, env = env, cont_yn = cont_yn, next_key = next_key)
}

# Order modify (kt10002 via /kiwoom/order, SSOT order_add.md)
rc_order_modify <- function(rc, dmst_stex_tp, orig_ord_no, stk_cd, mdfy_qty, mdfy_uv,
                            mdfy_cond_uv = "", env = "paper", cont_yn = NULL, next_key = NULL) {
  body <- list(
    dmst_stex_tp = dmst_stex_tp,
    orig_ord_no = orig_ord_no,
    stk_cd = stk_cd,
    mdfy_qty = as.character(mdfy_qty),
    mdfy_uv = as.character(mdfy_uv),
    mdfy_cond_uv = as.character(mdfy_cond_uv)
  )
  rc_order(rc, tr_api_id = "kt10002", body = body, env = env, cont_yn = cont_yn, next_key = next_key)
}

# Order cancel (kt10003 via /kiwoom/order, SSOT order_add.md)
# LEGACY: 멀티계좌 SSOT 라우팅 아님(쿼리 env/기본 cred_profile). 운영에서는 rc_auto_* 사용 권장.
rc_order_cancel <- function(rc, dmst_stex_tp, orig_ord_no, stk_cd, cncl_qty,
                            env = "paper", cont_yn = NULL, next_key = NULL) {
  body <- list(
    dmst_stex_tp = dmst_stex_tp,
    orig_ord_no = orig_ord_no,
    stk_cd = stk_cd,
    cncl_qty = as.character(cncl_qty)
  )
  rc_order(rc, tr_api_id = "kt10003", body = body, env = env, cont_yn = cont_yn, next_key = next_key)
}

# -----------------------------
# /auto/order (strategy_id SSOT; no env query)
# -----------------------------
rc_auto_order <- function(rc, strategy_id, tr_api_id = "kt10000", body,
                          cont_yn = NULL, next_key = NULL) {
  rc_post_no_env(rc, "/auto/order", body = body,
                 query = list(strategy_id = strategy_id, tr_api_id = tr_api_id),
                 cont_yn = cont_yn, next_key = next_key)
}

# Schema-error retry: direct body then list(params=body) once if "Unknown field(s)" or "Field required"
rc_auto_order_safe <- function(rc, strategy_id, tr_api_id, body, cont_yn = NULL, next_key = NULL) {
  res1 <- rc_auto_order(rc, strategy_id = strategy_id, tr_api_id = tr_api_id, body = body,
                        cont_yn = cont_yn, next_key = next_key)
  n1 <- rc_norm_order(res1)
  if (isTRUE(n1$ok)) return(res1)
  msg1 <- n1$return_msg
  msg1_txt <- if (is.list(msg1)) paste(capture.output(str(msg1, max.level = 2)), collapse = " ") else as.character(msg1)
  need_retry <- grepl("Unknown field\\(s\\)", msg1_txt) || grepl("Field required", msg1_txt)
  if (!need_retry) return(res1)
  rc_auto_order(rc, strategy_id = strategy_id, tr_api_id = tr_api_id, body = list(params = body),
                cont_yn = cont_yn, next_key = next_key)
}

rc_auto_buy_limit <- function(rc, strategy_id, stk_cd, qty = 1L, ord_uv, trde_tp = "0", exchange = "KRX") {
  ex <- toupper(exchange)
  dmst <- if (ex == "NXT") "NXT" else "KRX"
  body <- list(
    dmst_stex_tp = dmst,
    stk_cd = as.character(stk_cd),
    ord_qty = as.character(as.integer(qty)),
    ord_uv = as.character(as.integer(ord_uv)),
    trde_tp = as.character(trde_tp),
    cond_uv = ""
  )
  rc_auto_order_safe(rc, strategy_id, tr_api_id = "kt10000", body = body)
}

rc_auto_buy_limit_ioc <- function(rc, strategy_id, stk_cd, qty = 1L, ord_uv, exchange = "KRX") {
  rc_auto_buy_limit(rc, strategy_id, stk_cd, qty = qty, ord_uv = ord_uv, trde_tp = "10", exchange = exchange)
}

rc_auto_order_sell <- function(rc, strategy_id, dmst_stex_tp, stk_cd, ord_qty, ord_uv = "", trde_tp = "3", cond_uv = "") {
  body <- list(
    dmst_stex_tp = dmst_stex_tp,
    stk_cd = stk_cd,
    ord_qty = as.character(ord_qty),
    ord_uv = as.character(ord_uv),
    trde_tp = trde_tp,
    cond_uv = as.character(cond_uv)
  )
  rc_auto_order_safe(rc, strategy_id, tr_api_id = "kt10001", body = body)
}

rc_auto_order_modify <- function(rc, strategy_id, dmst_stex_tp, orig_ord_no, stk_cd, mdfy_qty, mdfy_uv, mdfy_cond_uv = "") {
  body <- list(
    dmst_stex_tp = dmst_stex_tp,
    orig_ord_no = orig_ord_no,
    stk_cd = stk_cd,
    mdfy_qty = as.character(mdfy_qty),
    mdfy_uv = as.character(mdfy_uv),
    mdfy_cond_uv = as.character(mdfy_cond_uv)
  )
  rc_auto_order_safe(rc, strategy_id, tr_api_id = "kt10002", body = body)
}

rc_auto_order_cancel <- function(rc, strategy_id, dmst_stex_tp, orig_ord_no, stk_cd, cncl_qty) {
  body <- list(
    dmst_stex_tp = dmst_stex_tp,
    orig_ord_no = orig_ord_no,
    stk_cd = stk_cd,
    cncl_qty = as.character(cncl_qty)
  )
  rc_auto_order_safe(rc, strategy_id, tr_api_id = "kt10003", body = body)
}

# Chart (tr_api_id required, e.g. ka10080/ka10081 via /kiwoom/chart)
rc_chart <- function(rc, tr_api_id, body, env = "live", cont_yn = NULL, next_key = NULL) {
  rc_post(rc, "/kiwoom/chart", body = body, env = env,
          query = list(tr_api_id = tr_api_id), cont_yn = cont_yn, next_key = next_key)
}

# Watchlist/stkinfo (ka10095 via /kiwoom/watchlist)
# stk_codes: character vector or already pipe-joined string. If vector length > 100, chunks by 100.
rc_watchlist_set <- function(rc, stk_codes, env = "paper", tr_api_id = "ka10095") {
  if (is.character(stk_codes) && length(stk_codes) == 1L && grepl("|", stk_codes, fixed = TRUE)) {
    stk_str <- stk_codes
  } else {
    stk_str <- paste(as.character(stk_codes), collapse = "|")
  }
  codes_vec <- strsplit(stk_str, "|", fixed = TRUE)[[1L]]
  codes_vec <- codes_vec[nzchar(codes_vec)]
  n <- length(codes_vec)
  if (n == 0L) stop("stk_codes must have at least one code")
  chunk_size <- 100L
  if (n <= chunk_size) {
    body <- list(stk_cd = paste(codes_vec, collapse = "|"))
    return(rc_post(rc, "/kiwoom/watchlist", body = body, env = env,
                   query = list(tr_api_id = tr_api_id)))
  }
  results <- list()
  for (i in seq(1L, n, by = chunk_size)) {
    idx <- i:min(i + chunk_size - 1L, n)
    chunk <- codes_vec[idx]
    body <- list(stk_cd = paste(chunk, collapse = "|"))
    res <- rc_post(rc, "/kiwoom/watchlist", body = body, env = env,
                   query = list(tr_api_id = tr_api_id))
    results[[length(results) + 1L]] <- res
  }
  results
}

# -----------------------------
# Order helpers (kt10000)
# -----------------------------

# normalize order response for printing
rc_norm_order <- function(res) {
  body <- res$body %||% list()
  list(
    ok = isTRUE(suppressWarnings(as.integer(body$return_code %||% NA) == 0)),
    ord_no = body$ord_no %||% (body$odno %||% NA),
    return_code = body$return_code %||% NA,
    return_msg  = body$return_msg  %||% (body$detail %||% ""),
    raw = res
  )
}

# robust order call: try direct body first, then wrapper(list(params=body)) if schema rejects
rc_order_safe <- function(rc, tr_api_id, body, env="live", cont_yn=NULL, next_key=NULL) {
  # 1) direct
  res1 <- rc_order(rc, tr_api_id=tr_api_id, body=body, env=env, cont_yn=cont_yn, next_key=next_key)
  n1 <- rc_norm_order(res1)

  # if success OR server returned a normal code/message, stop here
  if (isTRUE(n1$ok)) return(res1)

  # some gateways validate schema and return "Unknown field(s)" / "Field required" in return_msg (often as list)
  msg1 <- n1$return_msg
  msg1_txt <- if (is.list(msg1)) paste(capture.output(str(msg1, max.level=2)), collapse=" ") else as.character(msg1)

  need_retry <- grepl("Unknown field\\(s\\)", msg1_txt) || grepl("Field required", msg1_txt)

  if (!need_retry) return(res1)

  # 2) wrapper retry: params=body
  res2 <- rc_order(rc, tr_api_id=tr_api_id, body=list(params=body), env=env, cont_yn=cont_yn, next_key=next_key)
  return(res2)
}

# 지정가 매수 (kt10000)
# LEGACY: 멀티계좌 SSOT 라우팅 아님(쿼리 env/기본 cred_profile). 운영에서는 rc_auto_* 사용 권장.
rc_buy_limit <- function(rc, stk_cd, qty=1L, ord_uv, env="live", trde_tp="0") {
  body <- list(
    dmst_stex_tp = "KRX",
    stk_cd       = as.character(stk_cd),
    ord_qty      = as.character(as.integer(qty)),
    ord_uv       = as.character(as.integer(ord_uv)),
    trde_tp      = as.character(trde_tp),  # 0: 지정가, 10: 지정가(IOC), 20: 지정가(FOK)
    cond_uv      = ""
  )
  rc_order_safe(rc, tr_api_id="kt10000", body=body, env=env)
}

# 지정가 IOC 매수
rc_buy_limit_ioc <- function(rc, stk_cd, qty=1L, ord_uv, env="live") {
  rc_buy_limit(rc, stk_cd=stk_cd, qty=qty, ord_uv=ord_uv, env=env, trde_tp="10")
}

# -----------------------------
# 1-min volume helpers (OHLC minute + VSD cache)
# -----------------------------

if (!exists("VSD_CACHE", inherits = FALSE)) VSD_CACHE <- new.env(parent = emptyenv())

get_vol1m <- function(rc, stk_cd, env = "live") {
  res <- tryCatch(
    rc_ohlc_minute(rc, stk_cd = stk_cd, tic_scope = "1", qry_cnt = 2, env = env),
    error = function(e) NULL
  )
  if (is.null(res) || is.null(res$body)) {
    return(list(ok = FALSE, reason = "ohlc_minute_failed"))
  }
  body <- res$body
  rows <- body$stk_min_pole_chart_qry %||% list()
  if (length(rows) < 2L) {
    return(list(ok = FALSE, reason = "not_enough_rows"))
  }
  last <- rows[[2L]]  # index0=latest, index1=prev -> [[2]] in R
  qty_raw <- last$trde_qty %||% ""
  qty_str <- gsub(",", "", as.character(qty_raw))
  last_vol <- suppressWarnings(as.integer(qty_str))
  if (is.na(last_vol)) {
    return(list(ok = FALSE, reason = "invalid_trde_qty"))
  }
  cntr_tm <- as.character(last$cntr_tm %||% "")
  list(ok = TRUE, last_vol = last_vol, cntr_tm = cntr_tm)
}

load_vsd1m <- function(stk_cd, tm_idx) {
  path <- sprintf("c:/lab/vsd/A%s_vsd1m.csv", stk_cd)
  key <- path
  df <- VSD_CACHE[[key]]
  if (is.null(df)) {
    if (!file.exists(path)) {
      return(list(ok = FALSE, reason = sprintf("file_not_found:%s", path)))
    }
    df <- tryCatch(
      read.csv(path, stringsAsFactors = FALSE),
      error = function(e) NULL
    )
    if (is.null(df)) {
      return(list(ok = FALSE, reason = "read_error"))
    }
    VSD_CACHE[[key]] <- df
  }

  time_col <- NULL
  for (cand in c("tm_idx", "tm", "time")) {
    if (cand %in% names(df)) {
      time_col <- cand
      break
    }
  }
  if (is.null(time_col)) {
    return(list(ok = FALSE, reason = "time_column_not_found"))
  }
  row <- df[df[[time_col]] == tm_idx, , drop = FALSE]
  if (nrow(row) < 1L) {
    return(list(ok = FALSE, reason = "tm_idx_not_found"))
  }

  if (!("mean" %in% names(row)) || !("sd3" %in% names(row))) {
    return(list(ok = FALSE, reason = "mean_or_sd3_column_not_found"))
  }
  mean_val <- suppressWarnings(as.numeric(row$mean[1]))
  sd3_val  <- suppressWarnings(as.numeric(row$sd3[1]))
  if (is.na(mean_val) || is.na(sd3_val)) {
    return(list(ok = FALSE, reason = "invalid_mean_or_sd3"))
  }
  list(ok = TRUE, mean = mean_val, sd3 = sd3_val)
}

vol1m_filter <- function(rc, stk_cd, tm_idx, env = "live") {
  tryCatch({
    vol_res <- get_vol1m(rc, stk_cd, env = env)
    vsd_res <- load_vsd1m(stk_cd, tm_idx)

    # 실패 시에는 필터로 막지 않음
    if (!isTRUE(vol_res$ok) || !isTRUE(vsd_res$ok)) {
      return(TRUE)
    }

    last_vol <- as.numeric(vol_res$last_vol)
    mu       <- as.numeric(vsd_res$mean)
    sd3      <- as.numeric(vsd_res$sd3)
    thr      <- mu + sd3
    pass     <- !is.na(last_vol) && !is.na(mu) && !is.na(sd3) && (last_vol > thr)

    cat("[VOL_FILTER]", stk_cd,
        "| last_vol:", last_vol,
        "| mean:", mu,
        "| sd3:", sd3,
        "| thr:", thr,
        "| pass:", pass, "\n")

    pass
  }, error = function(e) {
    TRUE
  })
}

# -----------------------------
# Test snippet (uncomment to run; strategy_id depends on strategies.yaml)
# -----------------------------
# setwd("c:/lab/kiwoom_hybrid")
# source("R_client/rc_gateway.R")
# rc <- rc_gateway()
#
# # paper 전략 예시
# rc_auto_buy_limit(rc, strategy_id = "s101_v2_paper", stk_cd = "005930", qty = 1, ord_uv = 50000)
#
# # live 전략 예시 (strategies.yaml에 없으면 실패 가능)
# rc_auto_buy_limit(rc, strategy_id = "s201_v2_live", stk_cd = "005930", qty = 1, ord_uv = 50000)
#
# # cancel 예시 (주문번호는 예시)
# rc_auto_order_cancel(rc, strategy_id = "s101_v2_paper", dmst_stex_tp = "KRX", orig_ord_no = "0000000", stk_cd = "005930", cncl_qty = 1)
