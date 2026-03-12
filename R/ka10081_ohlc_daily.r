# =============================================================================
# ka10081_ohlc_daily.R
# 일봉 OHLCV 수집 유틸리티  (tqk_get의 키움 REST 버전)
#
# 의존:
#   rc_gateway.R   → rc_ohlc_daily
#   rc_normalize.R → rc_norm_ohlc
# =============================================================================

get_ohlc_daily <- function(rc,
                           stk_cd,
                           from = "2025-01-01",
                           env  = "live") {
  tryCatch({
    cat(sprintf("[OHLC_DAILY] 시작 | %s | from=%s\n", stk_cd, from))

    from_dt <- gsub("-", "", as.character(from))

    # ── 단건 호출 ────────────────────────────────────────────────────
    res <- rc_ohlc_daily(
      rc,
      stk_cd       = stk_cd,
      base_dt      = format(Sys.Date(), "%Y%m%d"),
      upd_stkpc_tp = "1",
      env          = env
    )

    if (is.null(res$body)) return(list(ok=FALSE, reason="empty_response"))

    # ── 파싱 ─────────────────────────────────────────────────────────
    df <- rc_norm_ohlc(res$body)
    if (is.null(df) || nrow(df) == 0) return(list(ok=FALSE, reason="no_bars_parsed"))

    # ── 컬럼 이름 통일 ────────────────────────────────────────────────
    df <- df[, c("dt", "open_pric", "high_pric", "low_pric", "cur_prc", "trde_qty")]
    names(df) <- c("date", "open", "high", "low", "close", "volume")

    # ── 날짜순 정렬 + from 필터 ───────────────────────────────────────
    # dt가 numeric이므로 숫자로 비교
    df <- df[order(df$date), ]
    df <- df[df$date >= as.integer(from_dt), ]
    rownames(df) <- NULL

    if (nrow(df) == 0) return(list(ok=FALSE, reason=paste0("no_bars_after_from=", from_dt)))

    # ── NA 처리: LOCF → NOCB ─────────────────────────────────────────
    for (col in c("open", "high", "low", "close", "volume")) {
      df[[col]] <- zoo::na.locf(df[[col]], na.rm=FALSE)
      df[[col]] <- zoo::na.locf(df[[col]], fromLast=TRUE, na.rm=FALSE)
    }

    # ── 파생 지표 (tqk_get 동일) ─────────────────────────────────────
    pcl    <- dplyr::lag(df$close)
    phigh  <- dplyr::lag(df$high)
    plow   <- dplyr::lag(df$low)

    chgr   <- round((df$close / pcl   - 1) * 100, 1)
    pswing <- round((phigh   - pcl) / pcl  * 100, 1)
    nswing <- round((plow    - pcl) / pcl  * 100, 1)

    chgr[is.na(chgr)]     <- 0
    pswing[is.na(pswing)] <- 0
    nswing[is.na(nswing)] <- 0

    df$chgr   <- chgr
    df$pswing <- pswing
    df$nswing <- nswing

    # ── date 형식 YYYYMMDD → YYYY-MM-DD ──────────────────────────────
    # rc_norm_ohlc가 dt를 numeric으로 변환하므로 sprintf로 문자열 복원
    df$date <- as.character(as.Date(sprintf("%08d", as.integer(df$date)), format="%Y%m%d"))

    out <- tibble::as_tibble(
      df[, c("date","open","high","low","close","volume","chgr","pswing","nswing")]
    )

    cat(sprintf("[OHLC_DAILY] 완료 | %s | bars=%d | %s ~ %s\n",
                stk_cd, nrow(out), out$date[1], out$date[nrow(out)]))

    list(ok=TRUE, data=out, n_bars=nrow(out))

  }, error = function(e) {
    cat(sprintf("[OHLC_DAILY] ERROR | %s | %s\n", stk_cd, conditionMessage(e)))
    list(ok=FALSE, reason=conditionMessage(e))
  })
}

# =============================================================================
# 사용 예시
# =============================================================================
#
# source("R_client/rc_gateway.R")
# source("R_client/rc_normalize.R")
# source("reference/ka10081_ohlc_daily.R")
# rc <- rc_gateway()
#
# r <- get_ohlc_daily(rc, "005930", from="2025-01-01")
# r$ok      # TRUE
# r$n_bars  # 봉 수
# r$data    # tibble
