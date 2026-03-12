# =============================================================================
# ka10081_ohlc_daily.R
# tqk_get() 완전 대체 — 키움 REST ka10081 기반
#
# get_ohlc_daily(rc, stk_cd, from, env)
#   - 성공: tibble 직접 반환   ← tqk_get 동일
#   - 실패: NULL               ← tqk_get 동일
#
# 반환 컬럼 (tqk_get 완전 동일):
#   date    Date
#   open    num
#   high    num
#   low     num
#   close   num   (upd_stkpc_tp="1" 수정주가)
#   volume  num
#   chgr    num   당일 등락률 %
#   pswing  num   전일 위꼬리 크기 %
#   nswing  num   전일 아래꼬리 크기 %
#
# 의존: rc_gateway.R (rc_ohlc_daily), rc_normalize.R (rc_norm_ohlc)
# =============================================================================

get_ohlc_daily <- function(rc,
                           stk_cd,
                           from = "2025-01-01",
                           env  = "live") {
  tryCatch({

    from_dt <- gsub("-", "", as.character(from))   # "YYYYMMDD"

    # ── 단건 호출 ────────────────────────────────────────────────────
    res <- rc_ohlc_daily(
      rc,
      stk_cd       = stk_cd,
      base_dt      = format(Sys.Date(), "%Y%m%d"),
      upd_stkpc_tp = "1",
      env          = env
    )

    if (is.null(res$body)) return(NULL)

    # ── 파싱 ─────────────────────────────────────────────────────────
    df <- rc_norm_ohlc(res$body)
    if (is.null(df) || nrow(df) == 0) return(NULL)

    # ── 컬럼 이름 통일 ────────────────────────────────────────────────
    df <- df[, c("dt", "open_pric", "high_pric", "low_pric", "cur_prc", "trde_qty")]
    names(df) <- c("date", "open", "high", "low", "close", "volume")

    # ── 날짜순 정렬 + from 필터 ───────────────────────────────────────
    df <- df[order(df$date), ]
    df <- df[df$date >= as.integer(from_dt), ]
    rownames(df) <- NULL

    if (nrow(df) == 0) return(NULL)

    # ── NA 처리: LOCF → NOCB ─────────────────────────────────────────
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

    # ── date: numeric YYYYMMDD → Date 타입 (tqk_get 동일) ───────────
    df$date <- as.Date(sprintf("%08d", as.integer(df$date)), format = "%Y%m%d")

    # ── 반환: tibble 직접 (tqk_get 완전 동일) ────────────────────────
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
# df <- get_ohlc_daily(rc, "005930", from = "2025-01-01")
# if (!is.null(df)) tail(df)
#
# # tqk_get 쓰던 패턴 그대로
# df <- tryCatch(get_ohlc_daily(rc, tgt_code, from = date_start), error = function(e) NULL)
# if (is.null(df) || nrow(df) < 80) return(...)
