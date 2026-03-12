# =============================================================================
# vsd_tools.R
# VSD(Volume Signature Distribution) 생성 및 프로파일 시각화 도구
#
# 제공 함수:
#   gen_vsd_all(rc, ksmb_lst, env, vsd_dir, tic_scope)
#     → ksmb_lst 전체 종목의 vsd1m.csv 일괄 생성
#
#   draw_vsd_profile(rc, stk_cd, date_str, vsd_path, sheet_name, env)
#     → 특정일 특정종목 1분봉 프로파일 → Google Sheets 기록
#
# 의존:
#   reference/ka10080_ohlc.R   (get_ohlc1m_3m, get_ohlc1m_date)
#   R_client/rc_gateway.R      (rc_gateway, write_asgs_sheet)
#   googlesheets4
# =============================================================================

# -----------------------------------------------------------------------------
# write_asgs_sheet — Google Sheets 기록 헬퍼
# (rc_gateway에 없을 경우 여기서 직접 정의)
# -----------------------------------------------------------------------------
if (!exists("write_asgs_sheet", mode = "function")) {
  write_asgs_sheet <- function(tgt_df, tgt_sht, cell_org = "A1") {
    googlesheets4::gs4_auth(email = "coatle0@gmail.com")
    ssid <- "1M0LjBg2tPZprA-BIvsOZXjNPyK_gKm4pY4Ns93gvgJo"
    googlesheets4::range_clear(ssid, sheet = tgt_sht)
    googlesheets4::range_write(ssid, tgt_df,
                               range    = cell_org,
                               col_names = TRUE,
                               sheet    = tgt_sht)
    invisible(TRUE)
  }
}

# =============================================================================
# 1. gen_vsd_all()
#    ksmb_lst 전체 종목 → 3개월치 1분봉 수집 → vsd1m.csv 일괄 생성
#
#  Args:
#    rc        : rc_gateway 객체
#    ksmb_lst  : list of character vectors. 각 그룹의 종목코드 벡터.
#                예) list(c("005930","000660"), c("035420","035720"))
#                또는 단순 벡터 c("005930","000660")도 허용
#    env       : "live" | "paper"
#    vsd_dir   : VSD 저장 폴더. 기본 "c:/lab/vsd"
#    tic_scope : 분봉 단위. 기본 "1" (1분봉)
#    overwrite : 기존 파일 덮어쓰기 여부. 기본 TRUE
#
#  저장 파일명: {vsd_dir}/A{stk_cd}_vsd1m.csv
#  컬럼: tm_idx, mean, sd3
#
#  Returns:
#    data.frame: stk_cd, status("ok"/"skip"/"fail"), n_bars, reason
# =============================================================================
gen_vsd_all <- function(rc,
                        ksmb_lst,
                        env       = "live",
                        vsd_dir   = "c:/lab/vsd",
                        tic_scope = "1",
                        overwrite = TRUE) {

  # ksmb_lst가 list이면 flatten, 벡터면 그대로
  if (is.list(ksmb_lst)) {
    stk_cds <- unique(unlist(ksmb_lst))
  } else {
    stk_cds <- unique(as.character(ksmb_lst))
  }

  # 폴더 생성
  if (!dir.exists(vsd_dir)) {
    dir.create(vsd_dir, recursive = TRUE)
    cat(sprintf("[VSD] 폴더 생성: %s\n", vsd_dir))
  }

  results <- vector("list", length(stk_cds))
  total   <- length(stk_cds)

  for (i in seq_along(stk_cds)) {
    stk_cd   <- stk_cds[i]
    vsd_path <- file.path(vsd_dir, sprintf("A%s_vsd1m.csv", stk_cd))

    cat(sprintf("[VSD][%d/%d] %s ", i, total, stk_cd))

    # overwrite=FALSE 이고 이미 있으면 스킵
    if (!overwrite && file.exists(vsd_path)) {
      cat("→ SKIP (already exists)\n")
      results[[i]] <- data.frame(stk_cd=stk_cd, status="skip",
                                 n_bars=NA_integer_, reason="already exists",
                                 stringsAsFactors=FALSE)
      next
    }

    # 3개월치 수집
    r <- tryCatch(
      get_ohlc1m_3m(rc, stk_cd, env = env),
      error = function(e) list(ok=FALSE, reason=conditionMessage(e))
    )

    if (!isTRUE(r$ok)) {
      cat(sprintf("→ FAIL (%s)\n", r$reason))
      results[[i]] <- data.frame(stk_cd=stk_cd, status="fail",
                                 n_bars=NA_integer_, reason=r$reason,
                                 stringsAsFactors=FALSE)
      next
    }

    df <- r$data

    # tm_idx 별 mean / sd3 집계
    vsd <- do.call(rbind, lapply(split(df, df$tm_idx), function(g) {
      data.frame(
        tm_idx = g$tm_idx[1],
        mean   = mean(g$volume, na.rm = TRUE),
        sd3    = 3 * sd(g$volume,   na.rm = TRUE),
        n_days = nrow(g),
        stringsAsFactors = FALSE
      )
    }))
    vsd <- vsd[order(vsd$tm_idx), ]
    vsd$sd3[is.na(vsd$sd3)] <- 0  # 거래일 1일인 경우 sd=NA → 0

    write.csv(vsd, vsd_path, row.names = FALSE)
    cat(sprintf("→ OK | bars=%d | tm_slots=%d | saved: %s\n",
                nrow(df), nrow(vsd), vsd_path))

    results[[i]] <- data.frame(stk_cd=stk_cd, status="ok",
                               n_bars=nrow(df), reason="",
                               stringsAsFactors=FALSE)

    Sys.sleep(0.3)  # API 부하 방지
  }

  result_df <- do.call(rbind, results)
  ok_n   <- sum(result_df$status == "ok")
  fail_n <- sum(result_df$status == "fail")
  skip_n <- sum(result_df$status == "skip")
  cat(sprintf("\n[VSD] 완료: OK=%d | FAIL=%d | SKIP=%d / 전체=%d\n",
              ok_n, fail_n, skip_n, total))

  if (fail_n > 0) {
    cat("[VSD] 실패 종목:\n")
    fails <- result_df[result_df$status == "fail", ]
    for (i in seq_len(nrow(fails))) {
      cat(sprintf("  - %s: %s\n", fails$stk_cd[i], fails$reason[i]))
    }
  }

  invisible(result_df)
}

# =============================================================================
# 2. draw_vsd_profile()
#    특정일 특정종목 1분봉 → z-score 정규화 → Google Sheets 기록
#
#  Args:
#    rc         : rc_gateway 객체
#    stk_cd     : 종목코드 ("005930")
#    date_str   : "YYYYMMDD"
#    vsd_path   : vsd1m.csv 경로. NULL이면 자동 탐색
#    sheet_name : Sheets 탭 이름. NULL이면 "{stk_cd}_{date_str}"
#    env        : "live" | "paper"
#    vsd_dir    : vsd 파일 기본 폴더. 기본 "c:/lab/vsd"
#
#  출력 Sheets 컬럼:
#    date, stk_cd, tm_idx, open, high, low, close, volume,
#    z_score, day_mean, day_sd,
#    vsd_mean, vsd_sd3, vsd_z_mean, vsd_z_upper, filter_pass
#
#  Returns: invisible(data.frame) 또는 invisible(NULL)
# =============================================================================
draw_vsd_profile <- function(rc,
                             stk_cd,
                             date_str,
                             vsd_path   = NULL,
                             sheet_name = NULL,
                             env        = "live",
                             vsd_dir    = "c:/lab/vsd") {
  tryCatch({
    cat(sprintf("[VSD_PROFILE] 시작 | %s | %s\n", stk_cd, date_str))

    # ── 1. 특정일 1분봉 수집 ────────────────────────────────────────────────
    r <- get_ohlc1m_date(rc, stk_cd, date_str, env = env)
    if (!isTRUE(r$ok)) {
      cat(sprintf("[VSD_PROFILE] FAIL 수집 | %s\n", r$reason))
      return(invisible(NULL))
    }
    df <- r$data
    cat(sprintf("[VSD_PROFILE] 수집 OK | %d봉\n", nrow(df)))

    # ── 2. z-score 정규화 ────────────────────────────────────────────────────
    day_mean <- mean(df$volume, na.rm = TRUE)
    day_sd   <- sd(df$volume,   na.rm = TRUE)

    if (is.na(day_sd) || day_sd == 0) {
      df$z_score <- 0
      cat("[VSD_PROFILE] WARN: day_sd=0 → z_score 전부 0\n")
    } else {
      df$z_score <- round((df$volume - day_mean) / day_sd, 3)
    }

    df$day_mean <- round(day_mean, 1)
    df$day_sd   <- round(day_sd,   1)

    # ── 3. vsd1m 기준선 병합 ─────────────────────────────────────────────────
    if (is.null(vsd_path)) {
      vsd_path <- file.path(vsd_dir, sprintf("A%s_vsd1m.csv", stk_cd))
    }

    df$vsd_mean    <- NA_real_
    df$vsd_sd3     <- NA_real_
    df$vsd_z_mean  <- NA_real_
    df$vsd_z_upper <- NA_real_
    df$filter_pass <- NA

    if (file.exists(vsd_path)) {
      vsd <- tryCatch(read.csv(vsd_path, stringsAsFactors=FALSE), error=function(e) NULL)

      if (!is.null(vsd) && all(c("tm_idx","mean","sd3") %in% names(vsd))) {
        df <- merge(df, vsd[, c("tm_idx","mean","sd3")], by="tm_idx", all.x=TRUE)
        df <- df[order(df$tm_idx), ]

        df$vsd_mean    <- df$mean
        df$vsd_sd3     <- df$sd3
        df$vsd_z_mean  <- round((df$mean - day_mean) / day_sd, 3)
        df$vsd_z_upper <- round((df$mean + df$sd3 - day_mean) / day_sd, 3)
        df$filter_pass <- df$z_score > df$vsd_z_upper

        df$mean <- NULL
        df$sd3  <- NULL
        cat(sprintf("[VSD_PROFILE] vsd1m 병합 OK: %s\n", vsd_path))
      } else {
        cat("[VSD_PROFILE] WARN: vsd1m 컬럼 불일치 → 기준선 미포함\n")
      }
    } else {
      cat(sprintf("[VSD_PROFILE] INFO: vsd1m 없음 (%s) → 기준선 미포함\n", vsd_path))
    }

    # ── 4. 컬럼 정리 ────────────────────────────────────────────────────────
    df$stk_cd <- stk_cd
    df$date   <- date_str

    col_order <- c("date","stk_cd","tm_idx",
                   "open","high","low","close","volume",
                   "z_score","day_mean","day_sd",
                   "vsd_mean","vsd_sd3","vsd_z_mean","vsd_z_upper","filter_pass")
    # 없는 컬럼 제외
    col_order <- intersect(col_order, names(df))
    out_df    <- df[, col_order]
    rownames(out_df) <- NULL

    # ── 5. Google Sheets 기록 ────────────────────────────────────────────────
    sht <- if (!is.null(sheet_name)) sheet_name else
             sprintf("%s_%s", stk_cd, date_str)

    cat(sprintf("[VSD_PROFILE] Sheets 기록 → 탭: %s | rows: %d\n", sht, nrow(out_df)))
    write_asgs_sheet(out_df, tgt_sht = sht, cell_org = "A1")

    cat(sprintf("[VSD_PROFILE] 완료 | z_range:[%.2f, %.2f] | filter_pass: %s/%d\n",
                min(out_df$z_score, na.rm=TRUE),
                max(out_df$z_score, na.rm=TRUE),
                sum(out_df$filter_pass, na.rm=TRUE),
                nrow(out_df)))

    invisible(out_df)

  }, error = function(e) {
    cat(sprintf("[VSD_PROFILE] ERROR | %s\n", conditionMessage(e)))
    invisible(NULL)
  })
}

# =============================================================================
# 사용 예시
# =============================================================================
#
# source("R_client/rc_gateway.R")
# source("reference/ka10080_ohlc.R")
# source("reference/vsd_tools.R")
# rc <- rc_gateway()
#
# ── VSD 파일 일괄 생성 ──────────────────────────────────────────────────────
#
# # ksmb_lst 가 list of vectors 인 경우
# result <- gen_vsd_all(rc, ksmb_lst)
# result[result$status == "fail", ]   # 실패 종목 확인
#
# # 특정 종목만
# gen_vsd_all(rc, list(c("005930","000660")))
#
# # 덮어쓰기 없이 신규만
# gen_vsd_all(rc, ksmb_lst, overwrite = FALSE)
#
# ── 특정일 프로파일 → Sheets ────────────────────────────────────────────────
#
# # 기본 (vsd1m 자동 탐색)
# draw_vsd_profile(rc, "005930", "20260305")
#
# # 탭 이름 지정
# draw_vsd_profile(rc, "005930", "20260305", sheet_name = "삼성전자_프로파일")
#
# # 반환값 활용
# df <- draw_vsd_profile(rc, "005930", "20260305")
# df[df$filter_pass == TRUE, ]   # 필터 통과 구간만
