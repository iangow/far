data msft;
  set crsp.dsf;
  where permno=10107 AND date >= "03jan2018"d
    AND date <= "05jan2018"d;
  keep permno date ret;
run;

proc means data=msft;
run;

data dsf2;
  set crsp.dsf;
  ret = ret * 1;
run;

proc contents data=dsf2;
run;

data msft2;
  set dsf2;
  where permno=10107 AND date >= "03jan2018"d
    AND date <= "05jan2018"d;
  keep permno date ret;
run;


proc means data=msft2;
run;

data dsf3;
  set crsp.dsf;
  ret = ret * 1;
run;


proc sql;
   create index permno
      on dsf3 (permno);
quit;

proc contents data=dsf3;
run;

data msft3;
  set dsf3;
  where permno=10107 AND date >= "03jan2018"d
    AND date <= "05jan2018"d;
  keep permno date ret;
run;

proc means data=msft3;
run;

