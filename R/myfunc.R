library(rvest)
library(httr)
library(readr)
library(tibble)


#function for jm_code get
code_get<-function(){
  gen_otp_url =
    'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
  
  down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
  url_jmcode = 'dbms/MDC/STAT/standard/MDCSTAT01901'
  
  otp_jm = list(
    mktId = 'ALL',
    share ='1',
    csvxls_isNo = 'false',
    name = 'fileDown',
    url = url_jmcode
  )
  otp = POST(gen_otp_url, query = otp_jm) %>%
    read_html() %>%
    html_text()
  
  jm_code = POST(down_url, query = list(code = otp),
                 add_headers(referer = gen_otp_url)) %>%
    read_html(encoding = 'EUC-KR') %>%
    html_text() %>%
    read_csv(show_col_types = FALSE)
    colnames(jm_code)<-c('code','scode','fname','name','ename','ipodate','market','stype','class','stype','unitp','issue')
    jm_code <- cbind(jm_code$market, jm_code$name, jm_code$code,jm_code$scode)
    colnames(jm_code)<-c('market','name','code','scode')
  return(as_tibble(jm_code))
}

tqk_get <- function(x,
                    from) {
  gen_otp_url =
    'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
  
  down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
  url_jmcode = 'dbms/MDC/STAT/standard/MDCSTAT01901'

  
  url_ohlc = 'dbms/MDC/STAT/standard/MDCSTAT01701' 
  
  otp_ohlc = list(
    #tboxisuCd_finder_stkisu0_3 = '005930/�Ｚ����',
    isuCd=x,
    #isuCd2 = 'KR7005930003',
    #codeNmisuCd_finder_stkisu0_3: �Ｚ����
    #param1isuCd_finder_stkisu0_3: ALL
    strtDd=gsub('-','',from),
    endDd=format(Sys.Date(),'%Y%m%d'),
    adjStkPrc_check='Y',
    adjStkPrc='2',
    share= '1',
    money= '1',
    csvxls_isNo='false',
    name='fileDown',
    url= url_ohlc
  )
  otp = POST(gen_otp_url, query = otp_ohlc) %>%
    read_html() %>%
    html_text()
  
  
  x_ohlc = POST(down_url, query = list(code = otp),
                 add_headers(referer = gen_otp_url)) %>%
    read_html(encoding = 'EUC-KR') %>%
    html_text() %>%
    read_csv(show_col_types = FALSE)
  
  colnames(x_ohlc)<-c('date','close','chg','chgr','open','high','low','volume','volm','mcap','issue')
  #x_ohlc_tmp <- cbind(x_ohlc$open,x_ohlc$high,x_ohlc$low,x_ohlc$close,x_ohlc$volume,x_ohlc$chgr)
  #colnames(x_ohlc_tmp)<-c('open','high','low','close','volume','chgr')
  pcl<-x_ohlc$close[-1]
  df_len<-length(x_ohlc$close)
  chigh<-x_ohlc$high[-df_len]
  clow <- x_ohlc$low[-df_len]

  pswing <- (chigh-pcl)/pcl
  pswing <- as.numeric(format(pswing * 100,digits=1))
  x_ohlc$pswing <- c(pswing,0)

  nswing <- (clow-pcl)/pcl
  nswing <- as.numeric(format(nswing * 100,digits=1))
  x_ohlc$nswing <- c(nswing,0)
  
  return(as_tibble(x_ohlc[,c('date','open','high','low','close','volume','chgr','pswing','nswing')]))
}

code<-code_get()
#vline<-0
vvol_wk <- function(jm,year,sigmulti){
  date_start <- Sys.Date()
  date_start <- date_start-year*365
  ohlc_df<- tqk_get(code %>% filter(name== jm) %>% pull('code')->tgt_code,date_start)
  
  ohlc_xts <- xts(ohlc_df[,2:7],ohlc_df$date)
  ohlc_w_xts <- to.weekly(ohlc_xts,name = NULL)
  avg<-mean(ohlc_w_xts$Volume)
  sigma <- sd(ohlc_w_xts$Volume)
  vvol <- avg + sigma*sigmulti
  
  vvol_evt<-index(ohlc_w_xts$Volume[ohlc_w_xts$Volume > vvol])
  vline <-which(index(ohlc_w_xts) %in% vvol_evt)
  print(vvol_evt)
  png(filename='test.png')
  chartSeries(ohlc_w_xts,name=jm,TA="addVo();addEMA(5,col='red');addEMA(20,col='green');addEMA(40,col='cyan');addLines(v=vline,on=1,col='yellow')")
  dev.off()
  chartSeries(ohlc_w_xts,name=jm,TA="addVo();addEMA(5,col='red');addEMA(20,col='green');addEMA(40,col='cyan');addLines(v=vline,on=1,col='yellow')")
  bot$sendPhoto(chat_id,'test.png')
  url_tv <- paste0("https://www.tradingview.com/chart/9ZHcOzZN/?symbol=KRX%3A",code %>% filter(name== jm) %>% pull('scode'))
  bot$sendMessage(chat_id = chat_id, text =url_tv)
  
}



hello<-function(){
  print("checking update!!")
}

read_gs_idx<-function(gs_sheet_id){

  gs4_auth(email = "coatle0@gmail.com")
  ssid <- "1Edz1EPV6hqBM2tMKSkA3zNmysmugMrAg1u2H3fheXaM"
  test_idx_wt<-read_sheet(ssid,sheet=gs_sheet_id)
  test_idx_wt_lst <- split.default(test_idx_wt,sub(".*_","",names(test_idx_wt)))
  test_wt <- test_idx_wt_lst[[2]]
  test_idx <- test_idx_wt_lst[[1]]
  #test_ref <- test_idx_wt_lst[[2]]

  kweight_lst<-lapply(test_wt,function(x) x[!is.na(x)])
  ksmb_lst<-lapply(test_idx, function(x) x[!is.na(x)])
  #kref_lst<-lapply(test_ref, function(x) x[!is.na(x)])
  #return(list(ksmb_lst,kweight_lst,kref_lst))
  return(list(ksmb_lst,kweight_lst))
}

# index jm read from googlesheet for mybiz
read_gs_idx2<-function(gs_sheet_id){

  gs4_auth(email = "coatle0@gmail.com")
  ssid <- "1Edz1EPV6hqBM2tMKSkA3zNmysmugMrAg1u2H3fheXaM"
  test_idx_wt<-read_sheet(ssid,sheet=gs_sheet_id)
  test_idx_wt_lst <- split.default(test_idx_wt,sub(".*_","",names(test_idx_wt)))
  test_wt <- test_idx_wt_lst[[3]]
  test_idx <- test_idx_wt_lst[[1]]
  test_ref <- test_idx_wt_lst[[2]]

  kweight_lst<-lapply(test_wt,function(x) x[!is.na(x)])
  ksmb_lst<-lapply(test_idx, function(x) x[!is.na(x)])
  kref_lst<-lapply(test_ref, function(x) x[!is.na(x)])

  return(list(ksmb_lst,kweight_lst,kref_lst))
  #return(list(ksmb_lst,kweight_lst))
}

# realtime jm read from googlesheet for realtime
read_rtgs_idx<-function(gs_sheet_id){

  gs4_auth(email = "coatle0@gmail.com")
  ssid <- "1GWW0Q1RgMNAvSG7S4OyrbXSpcSHDnMSmd2uTsmZyJJE"
  test_idx_wt<-read_sheet(ssid,sheet=gs_sheet_id)
  test_idx_wt_lst <- split.default(test_idx_wt,sub(".*_","",names(test_idx_wt)))
  test_wt <- test_idx_wt_lst[[2]]
  test_idx <- test_idx_wt_lst[[1]]
  #test_ref <- test_idx_wt_lst[[2]]

  kweight_lst<-lapply(test_wt,function(x) x[!is.na(x)])
  ksmb_lst<-lapply(test_idx, function(x) x[!is.na(x)])
  #kref_lst<-lapply(test_ref, function(x) x[!is.na(x)])
  #return(list(ksmb_lst,kweight_lst,kref_lst))
  return(list(ksmb_lst,kweight_lst))
}

read_asgs_sheet<-function(gs_sheet_id){

  gs4_auth(email = "coatle0@gmail.com")
  ssid <- "1M0LjBg2tPZprA-BIvsOZXjNPyK_gKm4pY4Ns93gvgJo"
  test_idx_wt<-read_sheet(ssid,sheet=gs_sheet_id)

  return(test_idx_wt)
}

read_rtgs_sheet<-function(gs_sheet_id){

  gs4_auth(email = "coatle0@gmail.com")

  ssid <- "1GWW0Q1RgMNAvSG7S4OyrbXSpcSHDnMSmd2uTsmZyJJE"
  test_idx_wt<-read_sheet(ssid,sheet=gs_sheet_id)

  return(test_idx_wt)
}

read_sggs_sheet<-function(gs_sheet_id){

  gs4_auth(email = "coatle0@gmail.com")

  ssid <- "1Edz1EPV6hqBM2tMKSkA3zNmysmugMrAg1u2H3fheXaM"
  test_idx_wt<-read_sheet(ssid,sheet=gs_sheet_id)

  return(test_idx_wt)
}

write_asgs_sheet<-function(tgt_df,tgt_sht,cell_org){

  gs4_auth(email = "coatle0@gmail.com")
  ssid <- "1M0LjBg2tPZprA-BIvsOZXjNPyK_gKm4pY4Ns93gvgJo"
  range_clear(ssid,sheet=tgt_sht)
  sheet_nm <- tgt_sht
  range_write(ssid,tgt_df,range=cell_org,col_names = TRUE,sheet = tgt_sht)
}

write_rtgs_sheet<-function(tgt_df,tgt_sht,cell_org){

  gs4_auth(email = "coatle0@gmail.com")
  ssid <- "1GWW0Q1RgMNAvSG7S4OyrbXSpcSHDnMSmd2uTsmZyJJE"
  range_clear(ssid,sheet=tgt_sht)
  sheet_nm <- tgt_sht
  range_write(ssid,tgt_df,range=cell_org,col_names = TRUE,sheet = tgt_sht)
}

write_sggs_sheet<-function(tgt_df,tgt_sht,cell_org){

  gs4_auth(email = "coatle0@gmail.com")
  ssid <- "1Edz1EPV6hqBM2tMKSkA3zNmysmugMrAg1u2H3fheXaM"
  range_clear(ssid,sheet=tgt_sht)
  sheet_nm <- tgt_sht
  range_write(ssid,tgt_df,range=cell_org,col_names = TRUE,sheet = tgt_sht)
}



#function for udpate sheets
#function for udpate sheets
update_kidx <- function(ref_date,sheet_num,idx_fn,start_date){

  idx_gs_lst <- read_gs_idx(idx_fn)
  kweight_lst<-idx_gs_lst[[2]]

  ksmb_lst<-idx_gs_lst[[1]]

  kidx_xts<-lapply(ksmb_lst,function(y){print(y);lapply(y,function(x){ ifelse(!exists(x,envir=ktickerData),
                                                                              {tqk_code<-code[match(x,code$name),3]$code;yahoo_code<-paste0(tqk_code,".KQ");
                                                                              temp<-tryCatch(expr= tqk_get(tqk_code,from=start_date),
                                                                                             error = function(e) { print(paste0(x," new jm, using yahoo"));get_yahoo<-tq_get(yahoo_code, get = "stock.prices", from = start_date);return(get_yahoo[,2:7])},
                                                                                             warning = function(e) print("Warning") );
                                                                              assign(x,xts(temp[,2:6],temp$date),envir=ktickerData)},print(paste0(x,"exitsts")))})})


  ref_prices=lapply(ksmb_lst,function(x) do.call(cbind,lapply(x,function(x) coredata(Cl(get(x,envir=ktickerData)[ref_date])))))
  ref_pf = mapply(function(X,Y){X/Y}, X=kweight_lst,Y=ref_prices,SIMPLIFY=F)

  prices_run=lapply(ksmb_lst, function(x) do.call(cbind,lapply(x,function(x){print(x); coredata(Cl(get(x,envir = ktickerData)))})))
  prices_run_idx = mapply(function(X,Y){X %*% as.numeric(Y)},X=prices_run,Y=ref_pf)

  prices_run_idx[,grepl('spd',colnames(prices_run_idx),fixed=T)]<-prices_run_idx[,grepl('spd',colnames(prices_run_idx),fixed=T)]+100

  prices_run_idx_sort<-prices_run_idx[,order(colSums(tail(prices_run_idx)),decreasing = T)]

  top4_sub60_prices_run<-tail(prices_run_idx_sort[,1:4],n=60)
  top4_sub60_idx <-tail(index(get(ksmb_lst[[1]][1],envir=ktickerData)),n=60)

  top4_sub60_prices_run_nml=sweep(top4_sub60_prices_run*100,2,unlist(top4_sub60_prices_run[1,]),"/")

  top_sub60_prices_run.df<-data.frame(date=top4_sub60_idx,top4_sub60_prices_run_nml)


  top4_sub20_prices_run<-tail(prices_run_idx_sort[,1:4],n=20)
  top4_sub20_idx <-tail(index(get(ksmb_lst[[1]][1],envir=ktickerData)),n=20)
  top4_sub20_prices_run_nml=sweep(top4_sub20_prices_run*100,2,unlist(top4_sub20_prices_run[1,]),"/")

  top_sub20_prices_run.df<-data.frame(date=top4_sub20_idx,top4_sub20_prices_run_nml)


  sector_rank <- order(colSums(tail(prices_run_idx)),decreasing = T)



  prices_run.xts <-xts(prices_run_idx_sort,index(get(ksmb_lst[[1]][1],envir=ktickerData)))[paste0(ref_date,'::')]


  #colnames(prices_run.xts)<- names(ksmb_lst)
  prices_run.df<-data.frame(date=index(prices_run.xts),coredata(prices_run.xts))

  gs4_auth(email = "coatle0@gmail.com")
  ssid <- "1Edz1EPV6hqBM2tMKSkA3zNmysmugMrAg1u2H3fheXaM"
  range_clear(ssid,sheet=sheet_num)
  range_write(ssid,prices_run.df,range="A1",col_names = TRUE,sheet = sheet_num)

  if(sheet_num== 'kidx-Q'){
    range_write(ssid,top_sub60_prices_run.df,range="Q1",col_names = TRUE,sheet = sheet_num)

  }else {
    range_write(ssid,top_sub20_prices_run.df,range="Q1",col_names = TRUE,sheet = sheet_num)
  }

  return(sector_rank)

}


update_ksep <- function(ref_date,sheet_num,idx_fn,start_date){
  idx_gs_lst <- read_gs_idx(idx_fn)
  kweight_lst<-idx_gs_lst[[2]]

  ksmb_lst<-idx_gs_lst[[1]]


  kidx_xts<-lapply(ksmb_lst,function(y){print(y);lapply(y,function(x){ ifelse(!exists(x,envir=ktickerData),
                                                                              {tqk_code<-code[match(x,code$name),3]$code;yahoo_code<-paste0(tqk_code,".KQ");
                                                                              temp<-tryCatch(expr= tqk_get(tqk_code,from=start_date),
                                                                                             error = function(e) { print(paste0(x," new jm, using yahoo"));get_yahoo<-tq_get(yahoo_code, get = "stock.prices", from = start_date);return(get_yahoo[,2:7])},
                                                                                             warning = function(e) print("Warning") );
                                                                              assign(x,xts(temp[,2:6],temp$date),envir=ktickerData)},print(paste0(x,"exitsts")))})})



  #kidx_xts<-lapply(ksmb_lst,function(y){print(y);lapply(y,function(x){ ifelse(!exists(x,envir=ktickerData),{temp = tqk_get(code[match(x,code$name),3]$code,from=start_date); assign(x,xts(temp[,2:6],temp$date),envir=ktickerData)},print(paste0(x,"exists")))})})
  ref_prices=lapply(ksmb_lst,function(x) do.call(cbind,lapply(x,function(x) coredata(Cl(get(x,envir=ktickerData)[ref_date])))))
  #ref_pf = mapply(function(X,Y){X/Y}, X=kweight_lst,Y=ref_prices)

  prices_run=lapply(ksmb_lst, function(x) do.call(cbind,lapply(x,function(x) coredata(Cl(get(x,envir = ktickerData))))))
  prices_run_normal = mapply(function(X,Y,Z){as.data.frame(sweep(X,2,Y,FUN="/")*100) %>% set_names(Z)},X=prices_run,Y=ref_prices,Z=ksmb_lst)
  #

  prices_run.xts <-lapply(prices_run_normal,function(x){xts(x,index(get(ksmb_lst[[1]][1],envir=ktickerData)))[paste0(ref_date,'::')]})
  names(prices_run.xts) <- c()

  prices_run.mrg<-do.call(merge,prices_run.xts)
  prices_run.top8 <-prices_run.mrg[,order(colSums(tail(prices_run.mrg)),decreasing = T)[1:8]]


  #prices_run.df<-do.call(cbind,lapply(prices_run.xts,function(x){data.frame(date=index(x),coredata(x))}))
  prices_run.df<-data.frame(date=index(prices_run.mrg),coredata(prices_run.top8),coredata(prices_run.mrg))



  gs4_auth(email = "coatle0@gmail.com")
  ssid <- "1Edz1EPV6hqBM2tMKSkA3zNmysmugMrAg1u2H3fheXaM"
  range_clear(ssid,sheet=sheet_num)
  range_write(ssid,prices_run.df,range="A1",col_names = TRUE,sheet = sheet_num)

}


update_myidx <- function(kidx_start,data_start,qtr_start,week_start){
  #read index ticker and weight list
  # update yearly idx
  sheet_num = 'kidx-Q'
  ref_date = qtr_start
  start_date = data_start
  idx_fn = "kr_idx"
  update_kidx(ref_date,sheet_num,idx_fn,data_start)

  #update quarterly idx
  sheet_num = 'kidx-W'
  ref_date = week_start
  start_date = data_start
  idx_fn = "kr_idx"
  update_kidx(ref_date,sheet_num,idx_fn,data_start)

  #update lib quarterly idx
  sheet_num = 'lib_idx'
  start_date <- data_start
  ref_date = week_start
  idx_fn = "lib_sub_idx"
  update_kidx(ref_date,sheet_num,idx_fn,data_start)

  #update lib sep chart quarter
  sheet_num = 'lib_sep_w'
  start_date <- data_start
  ref_date = week_start
  idx_fn = "lib_sub_idx"
  update_ksep(ref_date,sheet_num,idx_fn,data_start)

  #update semi quarterly idx
  sheet_num = 'semi_idx'
  start_date <- data_start
  ref_date = week_start
  idx_fn = "semi_sub_idx"
  update_kidx(ref_date,sheet_num,idx_fn,data_start)


  #update semi sep weekly
  sheet_num = 'semi_sep_w'
  start_date <- data_start
  ref_date = week_start
  idx_fn = "semi_sub_idx"
  update_ksep(ref_date,sheet_num,idx_fn,data_start)

  #update auto parts weekly
  sheet_num = 'sub_sector'
  start_date <- data_start
  ref_date = week_start
  idx_fn = "sub_sec_idx"
  update_ksep(ref_date,sheet_num,idx_fn,data_start)
}

#function for udpate sheets
#modifying now 230518
update_uidx <- function(ref_date,sheet_num,idx_fn){
  #envrionment for data


  idx_gs_lst <- read_gs_idx(idx_fn)
  weight_lst<-idx_gs_lst[[2]]

  smb_lst<-idx_gs_lst[[1]]

  db_xts<-lapply(smb_lst,function(x) getSymbols(x[!(x %in% ls(envir=tickerData))],src='yahoo',env=tickerData,from=ref_date))

  ref_prices=lapply(smb_lst,function(x) do.call(cbind,lapply(x,function(x) coredata(Ad(get(x,envir=tickerData)[ref_date])))))
  ref_pf = mapply(function(X,Y){X/Y}, X=weight_lst,Y=ref_prices)

  print('calculate portfolio factor')
  prices_run=lapply(smb_lst, function(x) do.call(cbind,lapply(x,function(x){ print(x);coredata(Ad(get(x,envir = tickerData)))})))
  print("competed xts")
  #prices_run_idx = mapply(function(X,Y){ print(dim(X)); X %*% as.numeric(Y)},X=prices_run,Y=ref_pf,SIMPLIFY = FALSE)
  prices_run_idx = mapply(function(X,Y){ print(dim(X)); X %*% as.numeric(Y)},X=prices_run,Y=ref_pf)

  #modifying here
  prices_run_idx_sort<-prices_run_idx[,order(colSums(tail(prices_run_idx)),decreasing = T)]

  sector_rank <- order(colSums(tail(prices_run_idx)),decreasing = T)


  print('matrix X vector ')
  prices_run.xts <-xts(prices_run_idx_sort,index(get(smb_lst[[1]][1],envir=tickerData)))[paste0(ref_date,'::')]
  #colnames(prices_run.xts)<-names(smb_lst)
  prices_run.df<-data.frame(date=index(prices_run.xts),coredata(prices_run.xts))

  gs4_auth(email = "coatle0@gmail.com")
  ssid <- "1Edz1EPV6hqBM2tMKSkA3zNmysmugMrAg1u2H3fheXaM"
  range_clear(ssid,sheet=sheet_num)
  range_write(ssid,prices_run.df,range="A1",col_names = TRUE,sheet=sheet_num)

  return(sector_rank)
}



update_usep <- function(ref_date,sheet_num,idx_fn,sector_rank){
  #envrionment for data
  tickerData <- new.env()


  idx_gs_lst <- read_gs_idx(idx_fn)
  weight_lst<-idx_gs_lst[[2]]

  smb_lst<-idx_gs_lst[[1]]

  #adopt sector ranking
  weight_lst <- weight_lst[sector_rank]
  smb_lst <- smb_lst[sector_rank]
  nsmb_top5 <- sum(sapply(smb_lst[1:5],length))

  db_xts<-lapply(smb_lst,function(x) getSymbols(x[!(x %in% ls(envir=tickerData))],src='yahoo',env=tickerData,from=ref_date))

  ref_prices=lapply(smb_lst,function(x) do.call(cbind,lapply(x,function(x) coredata(Ad(get(x,envir=tickerData)[ref_date])))))

  ref_pf = mapply(function(X,Y){X/Y}, X=weight_lst,Y=ref_prices)

  print('calculate portfolio factor')
  prices_run=lapply(smb_lst, function(x) do.call(cbind,lapply(x,function(x){ print(x);coredata(Ad(get(x,envir = tickerData)))})))
  print("competed xts")
  prices_run_idx = mapply(function(X,Y,Z){ as.data.frame(sweep(X,2,Y,FUN="/")*100) %>% set_names(Z)},X=prices_run,Y=ref_prices,Z=smb_lst)
  print('matrix X vector ')
  prices_run.xts <-lapply(prices_run_idx,function(x){xts(x,index(get(smb_lst[[1]][1],envir=tickerData)))[paste0(ref_date,'::')]})
  names(prices_run.xts)<- c()

  prices_run.df<-do.call(merge,prices_run.xts)

  prices_run.top8 <-prices_run.df[,order(colSums(tail(prices_run.df)),decreasing = T)[1:8]]

  prices_run.df1<-data.frame(date=index(prices_run.df),coredata(prices_run.top8),coredata(prices_run.df[,1:nsmb_top5]))
  prices_run.df2<-data.frame(date=index(prices_run.df),coredata(prices_run.df[,(nsmb_top5+1):dim(prices_run.df)[2]]))




  gs4_auth(email = "coatle0@gmail.com")
  ssid <- "1Edz1EPV6hqBM2tMKSkA3zNmysmugMrAg1u2H3fheXaM"
  range_clear(ssid,sheet=sheet_num)
  range_write(ssid,prices_run.df1,range="A1",col_names = TRUE,sheet=sheet_num)


  gs4_auth(email = "coatle0@gmail.com")
  ssid <- "1Edz1EPV6hqBM2tMKSkA3zNmysmugMrAg1u2H3fheXaM"
  range_clear(ssid,sheet='Uindex_sep2')
  range_write(ssid,prices_run.df2,range="A1",col_names = TRUE,sheet='Uindex_sep2')
}


update_myuidx<-function(qtr_ref_date,week_ref_date,idx_fn){

  #quarterly update
  sector_rank<-update_uidx(qtr_ref_date,"Uindex-Q",idx_fn)
  sector_rank<-update_uidx(week_ref_date,"Uindex-W",idx_fn)
  update_usep(week_ref_date,"Uindex_sep1",idx_fn,sector_rank)
}



init_volmon<-function(ksmb_lst){
  setwd("~")

  vmonenv <- new.env()
  diff_env <- new.env()
  nor_vol_env <- new.env()

  wd_str<-"C:/Users/coatle/Documents"

  assign("wd_str",wd_str,envir=.GlobalEnv)

  today_str <- Sys.Date()
  assign('today_str',today_str,envir=.GlobalEnv)

  code<-code_get(fresh = TRUE)
  assign('code',code,envir=.GlobalEnv)

  empty_df<-c(open=0,high=0,low=0,close=0,volume=0)
  dt<-as.POSIXct(paste0(today_str,"0900"),format="%Y-%m-%d%H%M");
  empty_xts<-xts(t(empty_df),dt)

  volmon <- read.csv("~/volmon.csv")

  jm_lst<-t(volmon[,2])
  assign('jm_lst',jm_lst,envir=.GlobalEnv)

  init_envir<-lapply(jm_lst,function(x) assign(x,empty_xts,envir=vmonenv))

  #load vsd file
  vsdfm_lst<-lapply(ksmb_lst,function(x){
    dut<-read.csv(paste0(wd_str,'/tm_study/',x,'_vsdfm.csv'));
    dt<-paste0(today_str,' ',dut$idx)
    dt<-as.POSIXct(dt,format="%Y-%m-%d %H:%M");
    #print(y)
    dut_x <- xts(dut[,c('mean','sd3')],dt);
    assign(paste0(code[match(substr(x,2,7),code$code),2]$name,"_vsdfm"),dut_x,envir=vmonenv)
  }
  )

  assign('vmonenv',vmonenv,envir=.GlobalEnv)
  assign('diff_env',diff_env,envir=.GlobalEnv)
  assign('nor_vol_env',nor_vol_env,envir=.GlobalEnv)

}



resume_env<-function(){

  vmonenv<-readRDS("vmonenv.RData")
  diff_env<-readRDS("diff_env.RData")
  nor_vol_env<-readRDS("nor_vol_env.RData")

  assign('vmonenv',vmonenv,envir=.GlobalEnv)
  assign('diff_env',diff_env,envir=.GlobalEnv)
  assign('nor_vol_env',nor_vol_env,envir=.GlobalEnv)
}


update_vmon<-function(today_str){

  volmon <- read.csv("~/volmon.csv")
  jm_lst<-t(volmon[,2])
  time_lst <- volmon$time
  time_unq <- unique(time_lst)
  print(time_unq)
  #data frame to list
  volmon_lst<-split(volmon,row(volmon)[,2])

  #input volmon
  volmon_xts_lst<-lapply(as.list(time_unq),function(x) { mapply(function(X,Y){dt<-ifelse(Y<1000,paste0(today_str,' 0',Y),paste0(today_str,' ',Y));
  dt<-as.POSIXct(dt,format="%Y-%m-%d %H%M");
  x_xts<-xts(X[,c('open','high','low','close','volume')],dt);
  tgt_dut<-get(X[,2],envir=vmonenv);
  if(tgt_dut$open[1]==0){tgt_dut$open[1] = x_xts$open;tgt_dut$close[1] = x_xts$open}
  x_xts_up<-rbind(tgt_dut[!(index(tgt_dut) %in% index(x_xts))],x_xts);
  assign(X[,2],x_xts_up,envir=vmonenv)
  temp<-Y
  },X=volmon_lst,Y=x);
  }
  )
}

volmon_vol_diff <- function(){lapply(volmon$code_name,function(x){
  dut<-get(x,envir=vmonenv)

  vol_diff <- diff(dut$volume)
  vol_diff$volume[1]<-dut$volume[1]
  dut$volume <- vol_diff
  dut_vsd <- get(paste0(x,"_vsdfm"),envir=vmonenv)
  #5min ohlc
  dut10<-to.minutes5(dut)
  #align to 5min
  dut10a<-align.time(dut10,60*5)
  assign(x,dut10a,envir=diff_env)
  dut10a_nor <- (dut10a$dut.Volume-dut_vsd$mean[index(dut10a)])/dut_vsd$sd3[index(dut10a)]
  colnames(dut10a_nor)[1]<- paste0(x,'nvol')
  assign(x,dut10a_nor,envir=nor_vol_env)
  return(dut10a_nor)
}
) %>% `names<-`(volmon$code_name)
}

prices_idx_cal<- function(){
  ref_prices=lapply(ksmb_lst,function(x) do.call(cbind,lapply(
    x,function(x) coredata(Op(get(x,envir=vmonenv)[1]))))%>%`colnames<-`(x))

  prices_run_ft<-lapply(ksmb_lst,function(x){do.call(merge,lapply(x,
                                                                  function(y){Cl(get(y,envir=diff_env))}))}%>%`colnames<-`(x))

  ref_pf = mapply(function(X,Y){X/Y}, X=kweight_lst,Y=ref_prices,SIMPLIFY = FALSE)


  prices_run.idx = mapply(function(X,Y){X %*% as.numeric(Y)},X=prices_run_ft,
                          Y=ref_pf,SIMPLIFY = FALSE)

  prices_run.xts = mapply(function(X,Y){ xts(X,index(Y))},X=prices_run.idx,
                          Y=prices_run_ft,SIMPLIFY = FALSE)
}

vol_nor_idx_cal<-function(){
  vol_run_ft<-lapply(ksmb_lst,function(x){temp<-rowSums(do.call(merge,lapply(x,
                                                                             function(y){(get(y,envir=nor_vol_env))})))/length(x);
  xts(temp,index(get(ksmb_lst[[1]][1],envir=nor_vol_env)))

  }
  )
}


volmon_idx_sep <- function(){
  ref_prices=lapply(jm_lst,function(x) coredata(Op(get(x,envir=vmonenv)[2]))%>%`colnames<-`(x))
  prices_run_ft<-lapply(jm_lst,function(x) Cl(get(x,envir=diff_env)) %>%`colnames<-`(paste0(x,'_idx')))

  prices_run.nor = mapply(function(X,Y,Z){as.data.frame(sweep(X,2,Y,FUN='/')*100)%>% set_names(Z)},X=prices_run_ft,Y=ref_prices,Z=jm_lst)

  prices_run.nor = mapply(function(X,Y){print(colnames(X)); temp<-(coredata(X)/as.numeric(Y))*100;xts(temp,index(prices_run_ft[[1]]))},
                          X=prices_run_ft,Y=ref_prices,SIMPLIFY = FALSE)



  vol_nor_lst <-lapply(jm_lst,function(x) get(x,envir=nor_vol_env) %>%`colnames<-`(paste0(x,'_nvol')))

  sep_idx_nvol_lst <- mapply(function(X,Y,Z){ merge(X,Y) %>% `colnames<-`(c(paste0(Z,'_idx'),paste0(Z,'_nvol'))) },X=prices_run.nor,Y=vol_nor_lst,Z=jm_lst,SIMPLIFY = FALSE)

  names(sep_idx_nvol_lst) <- jm_lst


  #sep_idx_nvol_lst<-sep_idx_nvol_lst[order(sapply(sep_idx_nvol_lst,function(x){chk_strong<-sum(x[,1]);print(chk_strong);return(chk_strong)}),decreasing=TRUE)]
  sep_ind_nvol_lst_mer <- do.call(merge,sep_idx_nvol_lst)

  sep_idx_nvol_df <- coredata(sep_ind_nvol_lst_mer)

  #special jm name to compensate
  #special_jm <- c('JYP','PLUS','KG')
  pfsmb_ary<-unlist(pfsmb_lst)
  #pfsmb_ary<-c(pfsmb_ary,special_jm)
  pf_pattern<-paste(pfsmb_ary,collapse = '|')
  pf_df<-sep_idx_nvol_df[,grep(pf_pattern,colnames(sep_idx_nvol_df))]
  pf_df1 <- data.frame(time=as.character(index(sep_ind_nvol_lst_mer)),pf_df)

  sep_idx_nvol_df1<-sep_idx_nvol_df[,-grep(pf_pattern,colnames(sep_idx_nvol_df))]

  gs4_auth(email = "coatle0@gmail.com")
  ssid<- "1GWW0Q1RgMNAvSG7S4OyrbXSpcSHDnMSmd2uTsmZyJJE"
  range_clear(ssid,sheet='tm_rt_pf')
  range_write(ssid,pf_df1,range="A1",col_names = TRUE,sheet = "tm_rt_pf")

  fssmb_ary<-unlist(fssmb_lst)
  fs_pattern<-paste(fssmb_ary,collapse = '|')
  fs_df<-sep_idx_nvol_df[,grep(fs_pattern,colnames(sep_idx_nvol_df))]
  fs_df1 <- data.frame(time=as.character(index(sep_ind_nvol_lst_mer)),fs_df)

  ifelse(!length(grep(fs_pattern,colnames(sep_idx_nvol_df1))),  sep_idx_nvol_df2<-sep_idx_nvol_df1,
         sep_idx_nvol_df2<-sep_idx_nvol_df1[,-grep(fs_pattern,colnames(sep_idx_nvol_df1))]);



  gs4_auth(email = "coatle0@gmail.com")
  ssid<- "1GWW0Q1RgMNAvSG7S4OyrbXSpcSHDnMSmd2uTsmZyJJE"
  range_clear(ssid,sheet='tm_rt_focus')
  range_write(ssid,fs_df1,range="A1",col_names = TRUE,sheet = "tm_rt_focus")

  ksmb_lst_ary<-unlist(ksmb_lst_sort[1:4])

  sep_top4_idx_nvol_lst <- sep_idx_nvol_lst[ksmb_lst_ary]
  sep_top4_idx_nvol_mer <- do.call(merge,sep_top4_idx_nvol_lst)

  sep_top4_idx_nvol_df <- coredata(sep_top4_idx_nvol_mer)

  idx_all_df1 <- data.frame(time=as.character(index(sep_top4_idx_nvol_mer)),sep_top4_idx_nvol_df)
  #idx_all_df2 <- data.frame(time=as.character(index(sep_ind_nvol_lst_mer)),sep_idx_nvol_df2[,31:dim(sep_idx_nvol_df2)[2]])

  gs4_auth(email = "coatle0@gmail.com")
  ssid<- "1GWW0Q1RgMNAvSG7S4OyrbXSpcSHDnMSmd2uTsmZyJJE"
  range_clear(ssid,sheet='tm_rt_sep1')
  range_write(ssid,idx_all_df1,range="A1",col_names = TRUE,sheet = "tm_rt_sep1")

  #gs4_auth(email = "coatle0@gmail.com")
  #ssid<- "1GWW0Q1RgMNAvSG7S4OyrbXSpcSHDnMSmd2uTsmZyJJE"
  #range_clear(ssid,sheet='tm_rt_sep2')
  #range_write(ssid,idx_all_df2,range="A1",col_names = TRUE,sheet = "tm_rt_sep2")
}


prices_vol_nor_idx_cal<-function(){
  idx_all <-mapply(function(X,Y,Z){ merge(X,Y) %>%
      `colnames<-`(paste0(Z,c('idx','vol')))} ,
      X=volmon_idx,Y=vol_nor_idx,Z=names(ksmb_lst),SIMPLIFY = FALSE)
  idx_sort <- order(sapply(idx_all,function(x) sum(tail(x[,1]))),decreasing=TRUE)
  idx_all<-idx_all[idx_sort]
  ksmb_lst_sort<-ksmb_lst[idx_sort]

  idx_all.xts <- do.call(merge,idx_all)
  idx_all.df <- data.frame(time=as.character(index(idx_all.xts)),coredata(idx_all.xts))


  gs4_auth(email = "coatle0@gmail.com")
  #ssid <- "1Edz1EPV6hqBM2tMKSkA3zNmysmugMrAg1u2H3fheXaM"
  ssid<- "1GWW0Q1RgMNAvSG7S4OyrbXSpcSHDnMSmd2uTsmZyJJE"
  range_clear(ssid,sheet='tm_rt')
  range_write(ssid,idx_all.df,range="A1",col_names = TRUE,sheet = "tm_rt")

  return(ksmb_lst_sort)
}


gen_vsdfm<- function(jm_code){
  tm_stamp <- read.csv("tm_stamp_fm.csv")
  tm_idx <- read.csv("tm_idx_fm.csv")

  lapply(jm_code,function(x){
    dut<-read.csv(paste0(x,'.csv'));
    dt<-ifelse(dut$time < 1000, paste(dut$date,'0',dut$time,sep=""),paste(dut$date,dut$time,sep=""));
    dt<-as.POSIXct(dt,format="%Y%m%d%H%M");
    print(x);
    dut_xts<-xts(dut[,3:8],dt);
    #cal mean and sd of vol (separated volume)
    dut_sd_lst<-lapply(t(tm_stamp),function(x){ dut_t<-dut_xts[x]$vol;
    dut_t_sd<-sd(dut_t);dut_t_mean <- mean(dut_t);
    c(mean(dut_t[!(dut_t > (dut_t_mean+3*dut_t_sd))]),sd(dut_t[!(dut_t$vol>(dut_t_mean+3*dut_t_sd))])*3)});
    #cal mean and sd of volume ( cummulative volume)
    dut_csd_lst<-lapply(t(tm_stamp),function(x){ dut_t<-dut_xts[x]$volume;
    dut_t_sd<-sd(dut_t);dut_t_mean <- mean(dut_t);
    c(mean(dut_t[!(dut_t > (dut_t_mean+3*dut_t_sd))]),sd(dut_t[!(dut_t$volume>(dut_t_mean+3*dut_t_sd))])*3)});

    #separated volume
    dut_sd_df <- as.data.frame(dut_sd_lst);
    dut_sd_dt <- t(dut_sd_df) %>% `colnames<-`(c('mean','sd3'))
    dut_sd_df_wt <- cbind(tm_idx,dut_sd_dt);

    #dut_sd_df <- t(dut_sd_df)*3

    write.csv(dut_sd_df_wt, file = paste0(x,"_vsdfm.csv"),row.names = FALSE,col.names = FALSE);
    assign(x,dut_xts,envir=tmenv);
    assign(paste0(x,"_vsdfm"),dut_sd_df_wt,envir=tmenv)
  })
}
