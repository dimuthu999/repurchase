rm(list=ls())
setwd("E:/repurchase")
require(gdata)
require(dplyr)


# INITIALIZATION ----------------------------------------------------------

.jinit(classpath="C:/Users/dnratnadiwakara/Documents/sas.core.jar", parameters="-Xmx4g")
.jaddClassPath("C:/Users/dnratnadiwakara/Documents/sas.intrnet.javatools.jar")


run_and_fetch <- function(sql){
  fetch(dbSendQuery(wrds,sql),n=-1)
}

run_query <- function(sql){
  res <<-dbSendQuery(wrds,sql)
}


fetch_last_query <- function(name="data",rows=-1)  {
  if(is.null(res)) cat("No res object","\n")
  eval(parse(text=paste(name," <<- fetch(res, n = ",rows,")",sep="")))
}

trim <- function (x) gsub("^\\s+|\\s+$", "", x)


# DATA EXPLORATION --------------------------------------------------------

wrds <- wrdsconnect(user=user1, pass=pass1)

run_and_fetch("select distinct libname from dictionary.tables")
run_and_fetch("select distinct memname from dictionary.columns where libname='IBES'")
run_and_fetch("select name from dictionary.columns where libname='IBES' and memname='DET_GUIDANCE'")

# restrict to guidance_code %in% c(4,5,6,7,8)
  # 4 = The company has announced either the inclusion or exclusion of a charge for the period indicated.
  # 5 = The company has announced either the inclusion or exclusion of a gain for the period indicated.
  # 6 = The company has provided guidance but not specified whether they will meet, bear or miss the street.
  # 7 = The company has announced that they will not meet sales projections for the period indicated.
  # 8 = The company has announced that they will beat sales projections for the period indicated.
guidance_codes <- paste("'",paste(c('04','05','06','07','08'),collapse = "','"),"'",sep="")
run_and_fetch("select distinct(guidance_code),count(ticker) from IBES.DET_GUIDANCE group by guidance_code")
# only 06 is in the database

run_and_fetch("select distinct(measure),count(ticker) from IBES.DET_GUIDANCE where guidance_code='06' group by measure")
# CPX : Capital Expenditure
# CPXPAR : Capital Expenditure –Parent
# DPS : Dividends per Share
# EBS : EBITDA per Share
# EBSPAR : EBITDA per Share –Parent
# EBT : EBITDA
# EBTPAR: EBITDA –Parent
# EPS : Earnings Per Share
# EPSPAR : Earnings Per Share –Parent
# FFO : Funds From Operations Per Share
# FFOPAR : Funds From Operations Per Share –Parent
# GPS : Fully Reported Earnings Per Share
# GPSPAR : Fully Reported Earnings Per Share –Parent
# GRM : Gross Margin
# GRMPAR : Gross Margin –Parent 
# NET : Net Income
# NETPAR : Net Income –Parent
# OPR : Operating Profit
# OPRPAR : Operating Profit –Parent
# PRE : Pretax Income
# PREPAR: Pretax Income –Parent
# ROA : Return on Assets (%)
# ROAPAR : Return on Assets (%)-Parent
# ROE : Return on Equity (%)
# ROEPAR : Return on Equity (%)-Parent
# SAL : Sales
# SALPAR : Sales –Parent APPENDIX

selected_measures <- c('CPX','EPS','GPS','SAL') # 82%

run_query(paste("select * from IBES.DET_GUIDANCE where trim(guidance_code)='06' and usfirm=1",sep=""))
fetch_last_query("DET_GUIDANCE")
DET_GUIDANCE$measure <- trim(DET_GUIDANCE$measure)
DET_GUIDANCE <- DET_GUIDANCE[DET_GUIDANCE$measure %in% selected_measures,]
saveRDS(DET_GUIDANCE,file = "DET_GUIDANCE.rds")

run_query("select * from IBES.ID_GUIDANCE")
fetch_last_query("ID_GUIDANCE")
saveRDS(ID_GUIDANCE,file = "ID_GUIDANCE.rds")