library(DBI)
library(tidyverse)

pg <- dbConnect(RPostgres::Postgres(), bigint="integer")

rs <- dbExecute(pg, "SET work_mem TO '10GB'")
rs <- dbExecute(pg, "SET search_path TO far")

surpsumu <- tbl(pg, sql("SELECT * FROM ibes.surpsumu"))
actpsumu_epsus <- tbl(pg, sql("SELECT * FROM ibes.actpsumu_epsus"))
id <- tbl(pg, sql("SELECT * FROM ibes.id"))
stocknames <- tbl(pg, sql("SELECT * FROM crsp.stocknames"))

# IBES-CRSP link ----
permnos <-
  stocknames %>%
  select(permno, ncusip, namedt, nameenddt) %>%
  distinct() %>%
  rename(cusip = ncusip)

ibes_reused_cusips <-
  id %>%
  select(ticker, cusip) %>%
  distinct() %>%
  group_by(cusip) %>%
  filter(n() > 1) %>%
  ungroup()

ilink <-
  id %>%
  select(ticker, cusip) %>%
  anti_join(ibes_reused_cusips, by = "cusip") %>%
  inner_join(permnos, by = "cusip") %>%
  select(ticker, permno, namedt, nameenddt) %>%
  distinct() %>%
  compute()

# Core IBES data ----
ibes <-
  surpsumu %>%
  filter(measure == "EPS", fiscalp == "ANN", usfirm == 1) %>%
  rename(consensus = surpmean) %>%
  mutate(datadate = date_trunc('month', as.Date(paste0(pyear, "-", pmon, "-01")))) %>%
  mutate(datadate = as.Date(datadate + sql("INTERVAL '1 month - 1 day'"))) %>%
  select(-measure, -usfirm, -fiscalp, -suescore, -surpstdev, -pmon, -pyear) %>%
  compute()

ibesshrs <-
  actpsumu_epsus %>%
  select(ticker, prdays, shout) %>%
  rename(ibesshrout = shout) %>%
  distinct() %>%
  compute()

ibesshrs_link <-
  ibes %>%
  inner_join(ibesshrs, by = "ticker") %>%
  filter(prdays < anndats,
         age(anndats, prdays) < sql("interval '2 months'")) %>%
  group_by(ticker, anndats) %>%
  summarize(prdays = max(prdays, na.rm = TRUE)) %>%
  ungroup() %>%
  compute()

ibes %>%
  count(ticker, datadate) %>%
  filter(n > 1)

ibes_all_years <-
  ibesshrs %>%
  inner_join(ibesshrs_link, by = c("ticker", "prdays")) %>%
  select(ticker, anndats, ibesshrout) %>%
  inner_join(ibes, by = c("ticker", "anndats")) %>%
  compute()

# Restrict to 1993-2018; Link to PERMNO; Swap out ticker for permno
dbExecute(pg, "DROP TABLE IF EXISTS ibes1993to2018")

ibes1993to2018 <-
  ibes_all_years %>%
  filter(between(year(anndats), 1993, 2018)) %>%
  inner_join(ilink, by = "ticker") %>%
  filter(!is.na(permno),
         anndats >= namedt, anndats < nameenddt) %>%
  select(-ticker) %>%
  compute(name = "ibes1993to2018", temporary = FALSE,
          indexes = list(c("datadate", "permno")))

dbExecute(pg, "ALTER TABLE ibes1993to2018 OWNER TO far")
dbExecute(pg, "GRANT SELECT ON TABLE ibes1993to2018 TO far_access")

ibes1993to2018 %>%
  count(permno, datadate) %>%
  filter(n > 1)

# DJT: 106475;
ibes1993to2018 %>% count()

ibes1993to2018 %>%
    group_by(permno) %>%
    arrange(datadate) %>%
    mutate(fe = (actual - consensus) * ibesshrout,
           datadate_p1 = lead(datadate),
           check_p1 = year(lead(datadate)) == year(datadate) + 1,
           fe_p1 = if_else(check_p1, lead(fe), NA_real_)) %>%
    select(permno, datadate, datadate_p1,
           anndats, fe, fe_p1, check_p1)

#ibes1993to2018 %>%
#  collect() %>%
#  haven::write_dta("acct270/ibes1993to2018.dta")
