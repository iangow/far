
*******************************************************;
* EXECUTE LIBNAME STATEMENTS EVERY TIME YOU START SAS *;
*******************************************************;

*These tell SAS the location of the data;

libname mydata "s:\sasdata"; 	*shared data for the course;
libname home "d:\users\dtayl\desktop\programs";	*your personal folder;
options ps=max ls=150 nocenter;

proc sql;
create table addlagyear as 
select a.*, b.ib as ib_m1, b.at as at_m1
from mydata.compustat1980to2017 a 
inner join mydata.compustat1980to2017 b
on a.gvkey=b.gvkey and b.fyear=a.fyear-1;
quit;

proc sql;
create table addfutyear as
select a.*, b.ib as ib_p1, b.at as at_p1
from addlagyear a
inner join mydata.compustat1980to2017 b
on a.gvkey=b.gvkey and b.fyear=a.fyear+1;
quit;

data sloan;
set addfutyear;
roa_p1=ib_p1/at_p1;
roa=ib/at;
roa_m1=ib_m1/at_m1;
cfo=oancf/at;
accruals=(ib-oancf)/at;
run;

proc sql;
create table addibes as
select a.*, b.actual, 
b.consensus, 
b.ibesshrout, 
b.anndats
from sloan a
inner join mydata.ibes1993to2018 b
on a.permno=b.permno 
and a.datadate=b.datadate;
quit;

proc sql;
create table addibesfut as
select a.*, 
b.actual as actual_p1, 
b.consensus as consensus_p1, 
b.ibesshrout as ibesshrout_p1
from addibes a
inner join mydata.ibes1993to2018 b
on a.permno=b.permno 
and year(a.datadate)+1=year(b.datadate);
quit;

data addibesfut1; 
set addibesfut; 
AFE_p1=((actual_p1-consensus_p1)*ibesshrout_p1)/at;
AFE=((actual-consensus)*ibesshrout)/at;
run;


*********************************************;
* PLAY *;
*********************************************;
data insample;
set addibesfut1;
if xrd^=. then rd=xrd/at; 
if xrd=. then  rd=0;
if nmiss(roa_p1,afe_p1,afe, cfo ,accruals, rd)=0 and 1993<=fyear;
run;

proc freq data=insample; where fyear<=2015;
tables fyear; 
quit;

proc means data=insample n mean std p25 p50 p75 maxdec=3; where fyear<=2015;
var roa_p1 afe_p1 afe cfo accruals rd; 
quit;

proc reg data=insample; where fyear<=2015;
model roa_p1 = afe cfo accruals rd ;
quit;
proc reg data=insample; where fyear<=2015;
model afe_p1 = afe cfo accruals rd ;
quit;





****************************;
* OUT OF SAMPLE PREDICTION *;
****************************;
data outofsample;
set insample; 
if fyear=2016; * fyear 2016 is holdout;
predicted=-0.01570+(-0.00547*afe)+(0.67164*cfo)+(0.31465*accruals)+(-0.27558*rd);
ABFE=abs(roa_p1-predicted);
MSFE=(roa_p1-predicted)**2;
abfe_anal=abs(afe_p1);
msfe_anal=afe_p1**2;
run;

proc means data=outofsample mean n;
var abfe abfe_anal msfe msfe_anal;
quit; 

