library(DBI)
library(dplyr, warn.conflicts = FALSE)
library(ggplot2)

pg <- dbConnect(RPostgres::Postgres(),
                dbname = "crsp",
                user = "dtayl",
                host = "iangow.me",
                port = 5434L,
                password = "Xtian_luv_4eva",
                bigint="integer")

rs <- dbExecute(pg, "SET work_mem TO '1GB'")
rs <- dbExecute(pg, "SET search_path TO far")

compustat1980to2017 <- tbl(pg, "compustat1980to2017")
ibes1993to2018 <- tbl(pg, "ibes1993to2018")

sloan <-
  compustat1980to2017 %>%
  select(gvkey, permno, datadate, fyear, ib, at, xrd, oancf) %>%
  group_by(gvkey) %>%
  dbplyr::window_order(fyear) %>%
  mutate(fyear_m1 = lag(fyear),
         fyear_p1 = lead(fyear),
         check_m1 = fyear == fyear_m1 + 1,
         check_p1 = fyear == fyear_p1 - 1,
         ib_m1 = lag(ib),
         at_m1 = lag(at),
         ib_p1 = lead(ib),
         at_p1 = lead(at)) %>%
  mutate(roa_p1 = if_else(at_p1 > 0, ib_p1/at_p1, NA_real_),
         roa = if_else(at > 0, ib/at, NA_real_),
         roa_m1 = if_else(at_m1 > 0, ib_m1/at_m1, NA_real_),
         cfo = if_else(at > 0, oancf/at, NA_real_),
         accruals= if_else(at > 0, (ib-oancf)/at, NA_real_)) %>%
  filter(check_m1, check_p1) %>%
  ungroup()

ibes <-
  ibes1993to2018 %>%
  group_by(permno) %>%
  dbplyr::window_order(datadate) %>%
  mutate(check_p1 = year(lead(datadate)) == year(datadate) + 1,
         actual_p1 = if_else(check_p1, lead(actual), NA_real_),
         consensus_p1 = if_else(check_p1, lead(consensus), NA_real_),
         ibesshrout_p1  = if_else(check_p1, lead(ibesshrout), NA_real_)) %>%
  ungroup() %>%
  select(permno, datadate, actual, consensus, ibesshrout, anndats,
         actual_p1, consensus_p1, ibesshrout_p1)

train_test <-
  ibes %>%
  inner_join(sloan, by = c("datadate", "permno")) %>%
  mutate(roa_ibes = if_else(at > 0, actual * ibesshrout/at, NA_real_),
         roa_ibes_p1 = if_else(at > 0, actual_p1 * ibesshrout_p1/at, NA_real_),
         analyst = if_else(at > 0, consensus * ibesshrout/at, NA_real_),
         analyst_p1 = if_else(at > 0, consensus_p1 * ibesshrout_p1/at, NA_real_)) %>%
  mutate(rd = coalesce(if_else(at > 0, xrd/at, NA_real_), 0)) %>%
  filter(!is.na(roa_p1), !is.na(analyst), !is.na(analyst_p1), !is.na(cfo),
         !is.na(accruals), !is.na(rd), !is.na(roa_ibes_p1), !is.na(roa_ibes),
         1993 <= fyear) %>%
  select(permno, fyear, roa, roa_p1, roa_ibes, roa_ibes_p1, cfo, accruals,
         rd, analyst_p1, analyst) %>%
  collect()

train_test_target <-
  train_test %>%
  mutate(target_p1 = roa_ibes_p1,
         target = roa_ibes,
         afe = analyst - target)

train <-
  train_test_target %>%
  filter(2015 >= fyear)

train %>%
  count(fyear) %>%
  arrange(desc(fyear))

train %>%
  select(matches("^(roa|afe)"), afe, cfo, accruals, rd, analyst) %>%
  psych::describe(quant=c(.25,.75)) %>%
  as_tibble(rownames="rowname")  %>%
  select(rowname, n, mean, sd, `Q0.25`, median, `Q0.75`)

fm1 <- lm(target_p1 ~ afe + cfo + accruals + rd,
          data = train, subset = fyear <= 2015)
summary(fm1)

fm2 <- lm(target_p1 ~ afe + cfo + accruals + rd + analyst_p1,
          data = train, subset = fyear <= 2015)
summary(fm2)

fm3 <- lm(target_p1 ~ analyst_p1,
          data = train, subset = fyear <= 2015)
summary(fm3)

fitted <-
  train_test_target %>%
  mutate(predicted1 = predict(fm1, .),
         predicted2 = predict(fm2, .),
         predicted3 = predict(fm3, .),
         abfe1 = abs(target_p1 - predicted1),
         msfe1 = (target_p1 - predicted1)^2,
         abfe2 = abs(target_p1 - predicted2),
         msfe2 = (target_p1 - predicted2)^2,
         abfe3 = abs(target_p1 - predicted3),
         msfe3 = (target_p1 - predicted3)^2,
         abfe_anal = abs(target_p1 - analyst_p1),
         msfe_anal = (target_p1 - analyst_p1)^2)

# In-sample analysis
fitted %>%
  filter(fyear <= 2015) %>%
  # filter(abs(predicted) < 0.1) %>%
  select(matches("^(ab|ms)fe")) %>%
  psych::describe() %>%
  as_tibble(rownames="rowname")  %>%
  select(rowname, n, mean)

# Out-of-sample analysis
outofsample <-
  fitted %>%
  filter(fyear > 2015)

outofsample %>%
  # filter(abs(predicted) < 0.1) %>%
  select(matches("^(ab|ms)fe")) %>%
  psych::describe() %>%
  as_tibble(rownames="rowname")  %>%
  select(rowname, n, mean)

outofsample %>%
  ggplot(aes(y = target)) +
  geom_point(aes(x = predicted1, color="model1")) +
#  geom_point(aes(x = predicted3, color="model2")) +
  geom_point(aes(x = predicted3, color="model3")) +
  geom_point(aes(x = analyst_p1, color="analyst"))

