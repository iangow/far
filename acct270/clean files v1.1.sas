libname class 'c:\users\dtayl\desktop\class\acct270\sasdata'; 


***************;
* CLEAN AAERS *;
***************;

data aaer; set class.aaerann82to16;
	cik10=put(cik,z10.); fyear=yeara; 
	if .<fyear<=2012 and not missing(cik10);
	keep coname cik10 fyear p_aaer understatement;
run;

proc sql;
	create table class.AAER1982to2012 (drop=cik10)
	as select unique a.gvkey, a.datadate, b.*
	from class.funda a inner join aaer b
	on a.CIK = b.CIK10 
	and a.fyear=b.fyear;   
quit; *1484;

proc contents data=class.AAER1982to2012; run;



**********************;
* CLEAN RESTATEMENTS *;
**********************;

* Add restatements as in ALOT;
data restatements; set class.auditnonreli; 

      CIK=trim(company_fkey);  
      start=RES_BEGIN_DATE;   end=res_end_date; 

      if nmiss(start,end,cik)=0;

      ACCTG=RES_ACCOUNTING ;            
      FRAUD=RES_FRAUD  ;
      OTHER=RES_OTHER ;
      ERROR=RES_CLER_ERR ;
      ACCTG_CODE=RES_ACC_RES_FKEY_LIST ;
      SEC=res_SEC_invest;
		
	  restate=1;
      if SEC=1 or Fraud=1 then restate_intentional=1; else restate_intentional=0; *apply Hennes-Leone-Miller (TAR, 2008) classification rule (either "fraud" or "irregularity" or "SEC investigation");
	  if restate_intentional=0 and error=1 then restate_error=1; else restate_error=0;

      keep cik start end restate_intentional restate_error restate; 
run; *17,765;

proc sql;
	create table class.Restatements1999to2012
	as select unique gvkey, conm, datadate, fyear, 
	max(b.restate_intentional) as restate_intentional,
	max(restate_error) as restate_error,
	max(restate) as restate
	from class.funda a inner join restatements b
	on a.CIK = b.CIK 
	and (	(end>=intnx("month",datadate,-11)>=start) or 
			(end>=intnx("month",datadate,0)>=start) or 
			(intnx("month",datadate,0)>=end and start>=intnx("month",datadate,-11))	)
			and 1999<=fyear<=2012
	group by gvkey, datadate;   
quit; *15916;




******************************;
* CLEAN COMPUSTAT UNRESTATED *;
******************************;

* Require Sales, Assets, MV>0, non-missing ib;
data compustat1980to2017; set class.wrds_csa_unrestated; 
where 1980<=fyear<=2017 and at>0 and sale>0 and ib>.
		and datafmt='STD' and popsrc='D' and consol='C' 
		and curcd='USD' and indfmt='INDL';		 
drop datafmt popsrc consol curcd indfmt coafnd_id rd pdate fdate cyear UPD SRC Last_date;
run; 

proc sort data=compustat1980to2017 nodupkey; by gvkey datadate; run;
proc sort data=compustat1980to2017 nodupkey; by gvkey fyear descending datadate; run;
proc sort data=compustat1980to2017 nodupkey; by gvkey fyear; run; *289787;

proc sql;
	create table addpermno as select unique a.*, b.lpermno as permno
	from compustat1980to2017 as a inner join class.ccmxpf_linktable as b
	on a.gvkey = b.gvkey and b.LINKTYPE in ("LU","LC","LD","LN","LS","LX") 
	and usedflag=1 and linkprim in ("P","C") 
	and (b.LINKDT <= a.datadate or b.LINKDT = .B) 
	and (a.datadate <= b.LINKENDDT or b.LINKENDDT = .E);  
quit; *213691;
proc sort data=addpermno nodupkey; by gvkey datadate fyear; run; *213691;
proc sort data=addpermno nodupkey; by gvkey fyear; run; *213691;
proc sort data=addpermno nodupkey; by permno fyear; run; *213689;

proc sql;
create table addtldata as 
select a.*, b.prcc_f, b.adjex_f, b.csho, b.sich, b.conm
from addpermno (drop=csho) a inner join class.funda b 
on a.gvkey=b.gvkey and a.datadate=b.datadate
and b.datafmt='STD' and b.popsrc='D' and b.consol='C' 
and b.curcd='USD' and b.indfmt='INDL' and b.at>0 and b.ib>.; 

create table addtldata (drop=sich) as select unique a.*, max(sich) as sic
from addtldata a
group by gvkey;
quit; *213617;

proc sort data=addtldata nodupkey; by permno datadate; run;

%macro ffic12(data,sic);
data &data;set &data;
if &sic in (/*Consumer NonDurables -- Food, Tobacco, Textiles, Apparel, Leather, Toys*/
          100:999,
          2000:2399,
          2700:2749,
          2770:2799,
          3100:3199,
          3940:3989) then ffic12="NODUR";
else if &sic in(/*Consumer Durables -- Cars, TV's, Furniture, Household Appliances*/
          2500:2519,
          2590:2599,
          3630:3659,
          3710:3711,
          3714:3714,
          3716:3716,
          3750:3751,
          3792:3792,
          3900:3939,
          3990:3999) then ffic12="DURBL";
else if &sic in(/*Manufacturing -- Machinery, Trucks, Planes, Off Furn, Paper, Com Printing*/
          2520:2589,
          2600:2699,
          2750:2769,
          3000:3099,
          3200:3569,
          3580:3629,
          3700:3709,
          3712:3713,
          3715:3715,
          3717:3749,
          3752:3791,
          3793:3799,
          3830:3839,
          3860:3899) then ffic12="MANUF";
else if &sic in(/*Oil, Gas, and Coal Extraction and Products*/
          1200:1399,
          2900:2999) then ffic12="ENRGY";
else if &sic in(/*Chemicals and Allied Products*/
          2800:2829,
          2840:2899) then ffic12="CHEMS";
else if &sic in(/*Business Equipment -- Computers, Software, and Electronic Equipment*/
          3570:3579,
          3660:3692,
          3694:3699,
          3810:3829,
          7370:7379) then ffic12="BUSEQ";
else if &sic in(4800:4899) then ffic12="TELCM"; /*Telephone and Television Transmission*/         
else if &sic in(4900:4949) then ffic12="UTILS"; /*Utilities*/       
else if &sic in( /*Wholesale, Retail, and Some Services (Laundries, Repair Shops)*/
          5000:5999,
          7200:7299,
          7600:7699) then ffic12="SHOPS";
else if &sic in( /*Healthcare, Medical Equipment, and Drugs*/
          2830:2839,
          3693:3693,
          3840:3859,
          8000:8099) then ffic12="HLTH";
else if &sic in(6000:6999) then ffic12="MONEY";     
else if &sic>. and missing(ffic12) then ffic12= "OTHER"; /*Mines, Constr, BldMt, Trans, Hotels, Bus Serv, Entertainment*/
label ffic12='Fama-French 12 Industry Group Class';
run;
%mend ffic12;

%ffic12(addtldata,sic);

data class.compustat1980to2017; set addtldata; drop sic; run;




******************************;
** 			IBES			**; 
******************************;

* IBES-CRSP LINK FILE;
proc sort data=class.iclink out=t1 nodupkey; where score<=1; by permno score ticker; run;
*18621...<=1 is CUSIPs and CUSIP dates match, best possible links;
proc sql; create table t2 as select a.* from t1 a group by permno having min(score)=score; quit;
*18571...keep only those permnos with lowest score;
data t2; set t2; by permno score; if first.permno and last.permno then output; run;
*18352..if among the lowest scores & 1 PERMNO is matched to more than 1 TICK then remove;
proc sql; create table t3 as select a.* from t2 a group by ticker having min(score)=score; quit;
*18239...keep only those tickers with lowest score;
data t3; set t3; by ticker score; if first.ticker and last.ticker then output; run;
*17496...if among the lowest scores & 1 TICK is matched to more than 1 PERMNO then remove;
proc sort data=t3 nodupkey; by permno; run; *kill 0;
proc sort data=t3 out=ilink nodupkey; by ticker; run; *kill 0...since unique permnos and unique tickers we have a unique 1-1 match;

data IBES; set class.surpsumu; 
where measure in ("EPS") and fiscalp="ANN" and USFIRM=1;
CONSENSUS=SurpMean;
DATADATE=intnx('month',mdy(pmon,15,pyear),0,'end');
drop measure USFIRM FISCALP SUEscore Surpstdev surpmean pmon pyear; 
format datadate mmddyyn8.;
run;

proc sql;
    create table IBES1 as 
	select a.*, b.permno
    from IBES a inner join ilink b on a.ticker=b.ticker;

	create table IBES2 as 
	select unique a.*, b.shout as IBESSHROUT
	from ibes1 a inner join class.actpsum_epsus b
	on a.ticker=b.ticker
	and prdays<anndats and 0<=intck('month',prdays,anndats)<=1
	group by permno, datadate
	having max(prdays)=prdays;
quit; 
proc sort data=ibes2 nodupkey; by permno datadate; run;

data class.IBES1993to2018; set ibes2; 
if 2018>=year(anndats)>=1993; drop ticker;
run; *106475;

