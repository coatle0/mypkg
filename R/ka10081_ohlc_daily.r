# =============================================================================
# ka10081_ohlc_daily.R
# tqk_get()의 키움 REST 대체 함수
#
# get_ohlc_daily(rc, stk_cd, from, env)
#   - tqk_get(tgt_code, from) 과 완전히 동일한 인터페이스
#   - 성공: tibble 직접 반환
#   - 실패: NULL 반환
#
# 반환 컬럼 (tqk_get 동일):
#   date    Date
#   open    num
#   high    num
#   low     num
#   close   num   (upd_stkpc_tp="1" 수정주가 적용)
#   volume  num
#   chgr    num   당일 등락률 %        = (close / 전일close - 1) * 100
#   pswing  num   전일 위꼬리 크기 %   = (전일high - 전일close) / 전일close * 100
#   nswing  num   전일 아래꼬리 크기 % = (전일low  - 전일close) / 전일close * 100
#
# 의존: rc_gateway.R (rc_paged_post)
# =============================================================================

# -----------------------------------------------------------------------------
# 내부: paged 응답 리스트 → data.frame
# -----------------------------------------------------------------------------
.parse_daily_pages <- function(pages) {
  all_rows <- list()

  for (pg in pages) {
    if (is.null(pg) || is.null(pg$body)) next
    bars <- pg$body$stk_dt_pole_chart_qry
    if (is.null(bars) || length(bars) == 0) next

    for (b in bars) {
      all_rows[[length(all_rows) + 1L]] <- list(
        date   = as.character(b$dt        %||% NA),
        open   = as.numeric(gsub("[^0-9.-]", "", as.character(b$open_pric %||% NA))),
        high   = as.numeric(gsub("[^0-9.-]", "", as.character(b$high_pric %||% NA))),
        low    = as.numeric(gsub("[^0-9.-]", "", as.character(b$low_pric  %||% NA))),
        close  = as.numeric(gsub("[^0-9.-]", "", as.character(b$cur_prc   %||% NA))),
        volume = as.numeric(gsub("[^0-9.-]", "", as.character(b$trde_qty  %||% NA)))
      )
    }
  }

  if (length(all_rows) == 0) return(NULL)
  df <- do.call(rbind, lapply(all_rows, as.data.frame, stringsAsFactors = FALSE))
  df[order(df$date), ]
}

# -----------------------------------------------------------------------------
# get_ohlc_daily(rc, stk_cd, from, env)
#
#   tqk_get() 완전 대체 — tibble 직접 반환, 실패 시 NULL
# -----------------------------------------------------------------------------
get_ohlc_daily <- function(rc,
                           stk_cd,
                           from = "2025-01-01",
                           env  = "live") {
  tryCatch({

    from_dt <- gsub("-", "", as.character(from))   # "YYYYMMDD"
    base_dt <- format(Sys.Date(), "%Y%m%d")

    # ── 연속조회 ────────────────────────────────────────────────────
    pages <- rc_paged_post(
      rc,
      path      = "/kiwoom/ohlc_daily",
      body      = list(
        stk_cd       = stk_cd,
        base_dt      = base_dt,
        upd_stkpc_tp = "1"
      ),
      env       = env,
      max_pages = 100,
      sleep_sec = 0.5
    )

    # ── 파싱 ────────────────────────────────────────────────────────
    df <- .parse_daily_pages(pages)
    if (is.null(df) || nrow(df) == 0) return(NULL)

    # ── from 필터 ────────────────────────────────────────────────────
    df <- df[df$date >= from_dt, ]
    if (nrow(df) == 0) return(NULL)

    # ── NA 처리: LOCF → NOCB (tqk_get 동일) ─────────────────────────
    for (col in c("open", "high", "low", "close", "volume")) {
      df[[col]] <- zoo::na.locf(df[[col]], na.rm = FALSE)
      df[[col]] <- zoo::na.locf(df[[col]], fromLast = TRUE, na.rm = FALSE)
    }

    # ── 파생 지표 (tqk_get 동일) ─────────────────────────────────────
    pcl    <- dplyr::lag(df$close)
    phigh  <- dplyr::lag(df$high)
    plow   <- dplyr::lag(df$low)

    chgr   <- round((df$close / pcl   - 1) * 100, 1)
    pswing <- round((phigh    - pcl) / pcl  * 100, 1)
    nswing <- round((plow     - pcl) / pcl  * 100, 1)

    chgr[is.na(chgr)]     <- 0
    pswing[is.na(pswing)] <- 0
    nswing[is.na(nswing)] <- 0

    df$chgr   <- chgr
    df$pswing <- pswing
    df$nswing <- nswing

    # ── date: "YYYYMMDD" → Date 타입 (tqk_get 동일) ──────────────────
    df$date <- as.Date(df$date, format = "%Y%m%d")

    # ── 반환: tibble 직접 (tqk_get 동일) ─────────────────────────────
    tibble::as_tibble(
      df[, c("date", "open", "high", "low", "close", "volume",
             "chgr", "pswing", "nswing")]
    )

  }, error = function(e) {
    message(sprintf("[get_ohlc_daily] ERROR %s: %s", stk_cd, conditionMessage(e)))
    NULL
  })
}

# =============================================================================
# 사용 예시
# =============================================================================
#
# # tqk_get 방식 그대로 대체
# df <- get_ohlc_daily(rc, "005930", from = "2025-01-01")
# if (!is.null(df)) tail(df)
#
# # vvd.R 내부 패턴
# df <- tryCatch(get_ohlc_daily(rc, tgt_code, from = date_start), error = function(e) NULL)
# if (is.null(df) || nrow(df) < 80) return(...)
#
# ── 응답 필드명 확인 (최초 1회) ─────────────────────────────────────────────
# test <- rc_ohlc_daily(rc, "005930", base_dt = format(Sys.Date(), "%Y%m%d"))
# str(test$body$stk_dt_pole_chart_qry[[1]])
# → 필드명이 다르면 .parse_daily_pages() 내부 수정
