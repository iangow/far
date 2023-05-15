library(DBI)
library(dplyr, warn.conflicts = FALSE)

pg <- dbConnect(RPostgres::Postgres(), bigint="integer")

rs <- dbExecute(pg, "SET work_mem TO '10GB'")
rs <- dbExecute(pg, "SET search_path TO far")

ind_12 <- tbl(pg, sql("SELECT * FROM ff.ind_12"))
funda <- tbl(pg, sql("SELECT * FROM comp.funda"))
wrds_csa_unrestated <- tbl(pg, sql("SELECT * FROM compsnap.wrds_csa_unrestated"))
ccmxpf_linktable <- tbl(pg, sql("SELECT * FROM crsp.ccmxpf_linktable"))
compustat <-
    wrds_csa_unrestated %>%
    filter(datafmt=='STD', popsrc=='D', consol=='C',
           curcd=='USD', indfmt=='INDL') %>%
    filter(between(fyear, 1980, 2017),
           at>0, sale>0, !is.na(ib)) %>%
    select(-datafmt, -popsrc, -consol, -curcd, -indfmt,
           -pdate, -fdate, -upd, -src, -last_date, -cyear, -rd, -coafnd_id) %>%
    compute()

# No dupes by (gvkey, datadate) OR by (gvkey, fyear),
# so I skip the "PROC SORT NODUPKEY" bit.
obs_to_keep <-
    compustat %>%
    group_by(gvkey, fyear) %>%
    summarize(datadate = max(datadate, na.rm = TRUE)) %>%
    compute()

compustat_filtered <-
    compustat %>%
    inner_join(obs_to_keep, by = c("gvkey", "fyear", "datadate"))

compustat_filtered %>%
    count(gvkey, fyear) %>%
    filter(n > 1)

# DJT: *289787;
compustat_filtered %>%
    count()

permno_link <-
    ccmxpf_linktable %>%
    filter(usedflag==1, linkprim %in% c("P", "C")) %>%
    select(gvkey, permno = lpermno, linkdt, linkenddt) %>%
    compute()

addpermno <-
    compustat_filtered %>%
    inner_join(permno_link, by = "gvkey") %>%
    filter(linkdt <= datadate | is.na(linkdt),
           datadate <= linkenddt | is.na(linkenddt))

# DJT:  *213691;
addpermno %>%
    count()

fyears_to_delete <-
    addpermno %>%
    count(permno, fyear) %>%
    filter(n > 1) %>%
    compute()

# DJT: *213689;
# DJT deletes 2; IDG deletes 4
addpermno %>%
    anti_join(fyears_to_delete, by = c("fyear", "permno")) %>%
    count()

sics <-
    funda %>%
    filter(datafmt=='STD', popsrc=='D', consol=='C',
            curcd=='USD', indfmt=='INDL') %>%
    filter(!is.na(sich)) %>%
    group_by(gvkey) %>%
    summarize(sic = max(sich, na.rm = TRUE)) %>%
    compute()

ind_12

# Tables produced by code here:
# https://github.com/iangow/ff/blob/master/get_ff_ind.R
ff_inds <-
    ind_12 %>%
    mutate(sic = generate_series(sic_min, sic_max)) %>%
    inner_join(sics, by = "sic") %>%
    select(gvkey, ff_ind_short_desc) %>%
    compute()

dbExecute(pg, "DROP TABLE IF EXISTS compustat1980to2017")
rs <- dbExecute(pg, "SET maintenance_work_mem TO '10GB'")

compustat1980to2017 <-
    addpermno %>%
    anti_join(fyears_to_delete, by = c("fyear", "permno")) %>%
    left_join(ff_inds, by = "gvkey") %>%
    mutate(ffic12 = coalesce(ff_ind_short_desc, "Other")) %>%
    select(-ff_ind_short_desc) %>%
    compute(name = "compustat1980to2017", temporary = FALSE)
            # indexes = list(c("datadate", "permno")))

dbExecute(pg, "ALTER TABLE compustat1980to2017 OWNER TO far")
dbExecute(pg, "GRANT SELECT ON TABLE compustat1980to2017 TO far_access")
# DJT: *213617;
compustat1980to2017 %>%
    count()

compustat1980to2017 %>%
    count(permno, datadate) %>%
    filter(n > 1)

#compustat1980to2017 %>%
#    collect() %>%
#    haven::write_dta("acct270/compustat1980to2017.dta")
