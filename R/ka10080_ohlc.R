# =============================================================================
# ka10080_ohlc.R
# 1분봉 OHLCV 수집 유틸리티
#
# 제공 함수:
#   get_ohlc1m_3m(rc, stk_cd, env)        → 연속조회로 3개월치 전체 수집
#   get_ohlc1m_date(rc, stk_cd, date_str, env) → 특정일 1분봉만 반환
#
# 의존: rc_gateway.R (rc_paged_post, rc_ohlc_minute 포함)
#       rc_normalize.R (rc_norm_ohlc 포함)
#
# 반환 공통 스펙 (data.frame):
#   datetime  chr  "YYYYMMDDHHmmss"
#   date      chr  "YYYYMMDD"
#   tm_idx    chr  "HH:MM"
#   open      num
#   high      num
#   low       num
#   close     num
#   volume    num
# =============================================================================

# -----------------------------------------------------------------------------
# 내부: paged 응답 리스트 → 단일 data.frame 변환
# -----------------------------------------------------------------------------
.parse_ohlc_pages <- function(pages) {
  all_rows <- list()

  for (pg in pages) {
    if (is.null(pg) || is.null(pg$body)) next
    bars <- pg$body$stk_min_pole_chart_qry
    if (is.null(bars) || length(bars) == 0) next

    for (b in bars) {
      # 실제 API 필드명 (ka10080 응답 확인 기준)
      # open_pric / high_pric / low_pric / cur_prc / trde_qty / cntr_tm
      all_rows[[length(all_rows) + 1L]] <- list(
        datetime = as.character(b$cntr_tm   %||% NA),
        open     = as.numeric(gsub("[^0-9]", "", as.character(b$open_pric %||% NA))),
        high     = as.numeric(gsub("[^0-9]", "", as.character(b$high_pric %||% NA))),
        low      = as.numeric(gsub("[^0-9]", "", as.character(b$low_pric  %||% NA))),
        close    = as.numeric(gsub("[^0-9]", "", as.character(b$cur_prc   %||% NA))),
        volume   = as.numeric(gsub("[^0-9]", "", as.character(b$trde_qty  %||% NA)))
      )
    }
  }

  if (length(all_rows) == 0) return(NULL)

  df <- do.call(rbind, lapply(all_rows, as.data.frame, stringsAsFactors = FALSE))

  # 파생 컬럼
  df$date   <- substr(df$datetime, 1, 8)
  df$tm_idx <- paste0(substr(df$datetime, 9, 10), ":", substr(df$datetime, 11, 12))

  # 시간순 정렬 (API는 역순 반환)
  df <- df[order(df$datetime), ]
  rownames(df) <- NULL

  df[, c("datetime", "date", "tm_idx", "open", "high", "low", "close", "volume")]
}

# -----------------------------------------------------------------------------
# get_ohlc1m_3m(rc, stk_cd, env="live")
#
#   연속조회(cont-yn/next-key)로 3개월치 1분봉 OHLCV 전체 수집
#
#   Args:
#     rc      : rc_gateway 객체
#     stk_cd  : 종목코드 ("005930")
#     env     : "live" | "paper"
#
#   Returns:
#     list(
#       ok    = TRUE/FALSE,
#       data  = data.frame (성공시),
#       n_bars= 수집 봉 수,
#       pages = 페이지 수,
#       reason= 실패 사유 (실패시)
#     )
#
#   Note:
#     - 3개월 ≈ 65 거래일 × 390봉 ≈ 25,000봉
#     - max_pages=200, sleep_sec=0.5 로 API 가이드 권장 간격 준수
# -----------------------------------------------------------------------------
get_ohlc1m_3m <- function(rc, stk_cd, env = "live") {
  tryCatch({
    cat(sprintf("[OHLC1M] 3개월치 연속조회 시작 | %s\n", stk_cd))

    pages <- rc_paged_post(
      rc,
      path      = "/kiwoom/ohlc",
      body      = list(
        stk_cd       = stk_cd,
        tic_scope    = "1",
        upd_stkpc_tp = "1"
        # base_dt 없음 → 오늘 기준 최신부터
      ),
      env       = env,
      max_pages = 200,       # 200페이지 × 약 400봉 = 최대 80,000봉
      sleep_sec = 0.5        # API 가이드 권장: 서버 부하 방지
    )

    if (length(pages) == 0) {
      return(list(ok=FALSE, reason="empty pages"))
    }

    df <- .parse_ohlc_pages(pages)
    if (is.null(df) || nrow(df) == 0) {
      return(list(ok=FALSE, reason="parse failed or empty bars"))
    }

    # 3개월 필터 (오늘 기준 약 65 거래일)
    cutoff_dt <- format(Sys.Date() - 95, "%Y%m%d")  # 넉넉히 95일
    df <- df[df$date >= cutoff_dt, ]

    if (nrow(df) == 0) {
      return(list(ok=FALSE, reason=paste0("no bars after cutoff=", cutoff_dt)))
    }

    cat(sprintf("[OHLC1M] 수집 완료 | %s | pages=%d | bars=%d | 기간:[%s ~ %s]\n",
                stk_cd, length(pages), nrow(df),
                min(df$date), max(df$date)))

    list(ok=TRUE, data=df, n_bars=nrow(df), pages=length(pages))

  }, error = function(e) {
    list(ok=FALSE, reason=paste0("get_ohlc1m_3m error: ", conditionMessage(e)))
  })
}

# -----------------------------------------------------------------------------
# get_ohlc1m_date(rc, stk_cd, date_str, env="live")
#
#   특정일 1분봉 OHLCV 수집
#   qry_cnt=400 단건 요청 (1일 = 최대 390봉)
#
#   Args:
#     rc       : rc_gateway 객체
#     stk_cd   : 종목코드 ("005930")
#     date_str : "YYYYMMDD"
#     env      : "live" | "paper"
#
#   Returns:
#     list(
#       ok    = TRUE/FALSE,
#       data  = data.frame (성공시),
#       n_bars= 수집 봉 수,
#       reason= 실패 사유 (실패시)
#     )
# -----------------------------------------------------------------------------
get_ohlc1m_date <- function(rc, stk_cd, date_str, env = "live") {
  tryCatch({
    cat(sprintf("[OHLC1M] 특정일 조회 | %s | %s\n", stk_cd, date_str))

    res <- rc_ohlc_minute(
      rc,
      stk_cd       = stk_cd,
      tic_scope    = "1",
      upd_stkpc_tp = "1",
      qry_cnt      = 400,
      base_dt      = date_str,
      env          = env
    )

    if (is.null(res) || is.null(res$body)) {
      return(list(ok=FALSE, reason="API 응답 없음"))
    }
    if (!isTRUE(res$ok) && !is.null(res$reason)) {
      return(list(ok=FALSE, reason=paste0("API failed: ", res$reason)))
    }

    bars <- res$body$stk_min_pole_chart_qry
    if (is.null(bars) || length(bars) == 0) {
      return(list(ok=FALSE, reason="empty bars"))
    }

    df <- .parse_ohlc_pages(list(res))
    if (is.null(df) || nrow(df) == 0) {
      return(list(ok=FALSE, reason="parse failed"))
    }

    # 해당 날짜만 필터
    df <- df[df$date == date_str, ]

    if (nrow(df) == 0) {
      return(list(ok=FALSE, reason=paste0("no bars for date=", date_str)))
    }

    cat(sprintf("[OHLC1M] 완료 | %s | %s | bars=%d\n",
                stk_cd, date_str, nrow(df)))

    list(ok=TRUE, data=df, n_bars=nrow(df))

  }, error = function(e) {
    list(ok=FALSE, reason=paste0("get_ohlc1m_date error: ", conditionMessage(e)))
  })
}

# =============================================================================
# 사용 예시
# =============================================================================
# source("R_client/rc_gateway.R")
# source("R_client/rc_normalize.R")
# source("reference/ka10080_ohlc.R")
# rc <- rc_gateway()
#
# # 3개월치 전체
# r3m <- get_ohlc1m_3m(rc, "005930")
# if (r3m$ok) head(r3m$data)
#
# # 특정일
# rd <- get_ohlc1m_date(rc, "005930", "20260305")
# if (rd$ok) head(rd$data)
#
# # VSD 생성에 활용
# if (r3m$ok) {
#   vsd <- r3m$data %>%
#     group_by(tm_idx) %>%
#     summarise(mean = mean(volume, na.rm=TRUE),
#               sd3  = 3 * sd(volume, na.rm=TRUE))
#   write.csv(vsd, "c:/lab/vsd/A005930_vsd1m.csv", row.names=FALSE)
# }