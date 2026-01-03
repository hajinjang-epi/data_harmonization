
*************************************************************************
	*  TITLE: NFFI Data harmonization: WHI LLS	 					*
	*																*	
	*  PROGRAMMER:   Hajin Jang   	 								*
	*																*
	*  DESCRIPTION: Data harmonization variables 		      		*
	*  			  		  (WHI LLS)      							*
	*																*
	*  DATE:    created 3/6/2025									*
	*---------------------------------------------------------------*
	*  LANGUAGE:     SAS VERSION 9.4								*
	*---------------------------------------------------------------*
	*  NOTES: 		Data pooling								    *
*************************************************************************;


libname data "Z:\Fall Injuries Combined Studies\HABC, MrOS, CHS, WHI Combined\Final datasets for NFFI";
libname created "Z:\Fall Injuries Combined Studies\Harmonization Coding\Hajin code\Hajin datasets\WHI";

	dm "odsresults; clear";
	dm "log; clear";

	
	options nofmterr;

data whi; set data.whi_master_hj_032425; run;
/*proc freq data=whi; table f33_membership lls_membership; run;*/

/*proc contents data=whi; run;*/

/* Calculate dates of falls */


/******************/
/* Fall variables */
/******************/


data f33date; set whi;
falldate_1=enrldate+f33days_1;
falldate_2=enrldate+f33days_2;
falldate_3=enrldate+f33days_3;
falldate_4=enrldate+f33days_4;
falldate_5=enrldate+f33days_5;
falldate_6=enrldate+f33days_6;
falldate_7=enrldate+f33days_7;
falldate_8=enrldate+f33days_8;
falldate_9=enrldate+f33days_9;
falldate_10=enrldate+f33days_10;
falldate_11=enrldate+f33days_11;
falldate_12=enrldate+f33days_12;
falldate_13=enrldate+f33days_13;
falldate_14=enrldate+f33days_14;
falldate_15=enrldate+f33days_15;
falldate_16=enrldate+f33days_16;
falldate_17=enrldate+f33days_17;
falldate_18=enrldate+f33days_18;
falldate_19=enrldate+f33days_19;

format falldate_1 falldate_2 falldate_3 falldate_4 falldate_5 falldate_6 falldate_7 falldate_8 falldate_9 falldate_10 falldate_11
 falldate_12 falldate_13 falldate_14 falldate_15 falldate_16 falldate_17 falldate_18 falldate_19 mmddyy10.;
run;


* Transpose from wide to long;
%macro falldata(num);
data fall&num.; set f33date;
keep id f33days_&num. f33fallsx2_&num. falldate_&num. F33VTYP_&num. F33Vclo_&num. f33_basedate enrldate f33_membership lls_membership;
rename f33days_&num.=f33days f33fallsx2_&num.=f33fallsx2 falldate_&num.=falldate F33VTYP_&num.=F33VTYP F33Vclo_&num.=f33vclo;
run;
%mend falldata;

%macro run_falldata;
    %do num = 1 %to 19;
        %falldata(&num);
    %end;
%mend run_falldata;

%run_falldata;

/* Long dataset with multiple fall follow-up variables */
data fall_long; 
set fall1 fall2 fall3 fall4 fall5 fall6 fall7 fall8 fall9 fall10 fall11 fall12 fall13 fall14 fall15 fall16 fall17 fall18 fall19; run;

/* Remove falldate=. (remove missing during the follow-up) */
data fall_long2; set fall_long;
if falldate ne .; run;

/* Remove if the F33 preceded the baseline */
data fall_long3; set fall_long2;
if falldate<f33_basedate then delete;
run;

* Remove if the F33 collected after Dec 31, 2016;
data fall_long4; set fall_long3;
if falldate >= 20820 then delete; run;

/* Remove if there are duplicates at baseline */
/* use F33VCLO=1 (Closest to visit within visit type and year) */
data fall_long5; set fall_long4;
if id=154079 and f33days=5540 and f33fallsx2=2 then delete; /* id=154079 has f33fallsx2=1 as her F33VCLO=1 */ 
else if id=198853 and f33days=5232 and f33fallsx2=2 then delete; /* id=198853 has f33fallsx2=0 as her F33VCLO=1 */
run;

/* Include annual visits only */
proc sort data=fall_long5; by id falldate; run;
data created.annual; set fall_long5;
if f33vtyp=4 then delete;
run;







/* annual: long datset */
data annual; set created.annual; run;

/* Sort the dataset by ID, falldate, and f33fallsx2 in descending order */
proc sort data=annual; by ID falldate descending f33fallsx2; run;

/* Delete duplicates submitted on the same date. Keep the largest and non-missing value among */
data annual2; set annual;
by ID falldate;
if first.falldate;
run;

/* f33_order */
data annual3; set annual2;
by id falldate;
if first.id then f33_order=1;
else f33_order + 1;
run;


/* Permanent dataset */
data created.annual3; set annual3; run;


data annual3; set created.annual3; run;


/***********/
/* whi_yrs */
/***********/


/* Year 1 */
data whi_yrs1_updated; set annual3;
if falldate=f33_basedate then whi_yr=1;
run;
data created.year1; set whi_yrs1_updated; if whi_yr=1; run;


/* Year 2~ */
%macro whi_yr(prev,num,base);
data whi_yrs&num.; set whi_yrs&prev._updated;
diff_yr&num. = falldate - &base.; 

if 0<diff_yr&num.<396 then ind_yr&num.=1; 
else ind_yr&num.=0;

by id falldate;
retain prev_falldate;

if first.id then do;
	prev_falldate = .;
	fall_diff_days = .;
end;
else do;
	fall_diff_days = falldate - prev_falldate;
end;
prev_falldate = falldate;

run;

proc sort data=whi_yrs&num.; by ID ind_yr&num. descending fall_diff_days; run;

data whi_yrs&num.; set whi_yrs&num.; by ID ind_yr&num.;
if ind_yr&num. = 0 then output;
if ind_yr&num. = 1 then do;
if first.ind_yr&num. then output;
end;
run;

proc sql;
    create table base_yr&num._updates as
    select 
        ID,
        case
            when ind_yr&num. = 0 and f33_order = 2 then &base. + 396
            when ind_yr&num. = 1 then falldate
        end as new_base_yr&num.
    from whi_yrs&num.
    where (ind_yr&num. = 0 and f33_order = 2) or ind_yr&num. = 1
    group by ID
    having new_base_yr&num. = max(new_base_yr&num.);
quit;

/* Step 2: Merge the updates back into the original dataset */
data whi_yrs&num._updated;
    merge whi_yrs&num. base_yr&num._updates;
    by ID;
    if not missing(new_base_yr&num.) then base_yr&num. = new_base_yr&num.;
    drop new_base_yr&num. fall_diff_days prev_falldate diff_yr&num. f33_order;
run;

data whi_yrs&num._updated; set whi_yrs&num._updated;
if ind_yr&num.=1 then whi_yr=&num.;
run;

data created.year&prev.; set whi_yrs&num._updated;
if whi_yr=&prev.;
run;

data whi_yrs&num._updated; set whi_yrs&num._updated;
if whi_yr=&prev. then delete;
run;

proc sort data=whi_yrs&num._updated; by id falldate; run;

data whi_yrs&num._updated; set whi_yrs&num._updated;
by id falldate;
if first.id then f33_order=1;
else f33_order + 1;
run;

%mend whi_yr;


%whi_yr(1,2,f33_basedate);
%whi_yr(2,3,base_yr2);
%whi_yr(3,4,base_yr3);
%whi_yr(4,5,base_yr4);
%whi_yr(5,6,base_yr5);
%whi_yr(6,7,base_yr6);
%whi_yr(7,8,base_yr7);
%whi_yr(8,9,base_yr8);
%whi_yr(9,10,base_yr9);


/* keep and drop variables from Year1~8 datasets */
data year1; set created.year1;
keep id f33_membership lls_membership enrldate f33_basedate f33fallsx2 falldate;
rename f33fallsx2=f33fallsx2_1 falldate=falldate_1;
run;
proc sort data=year1; by id; run;

%macro keep(num);
data year&num.; set created.year&num.;
keep id f33fallsx2 falldate;
rename f33fallsx2=f33fallsx2_&num. falldate=falldate_&num.;
run;
proc sort data=year&num.; by id; run;
%mend keep;

%keep(2);
%keep(3);
%keep(4);
%keep(5);
%keep(6);
%keep(7);
%keep(8);

data whi_final; merge year1 year2 year3 year4 year5 year6 year7 year8; by id; run;

data created.whi_fall; set whi_final; run;


/************/
/************/
/************/
/************/


/* Fall indicator */
data fallvar_whi; set created.whi_fall;

if f33fallsx2_1 >= 1 then FallIND21_WHI = 1;
else if f33fallsx2_1 = 0 then FallIND21_WHI = 0;
else FallIND21_WHI = .;

if f33fallsx2_2 >= 1 then FallIND22_WHI = 1;
else if f33fallsx2_2 = 0 then FallIND22_WHI = 0;
else FallIND22_WHI = .;

if f33fallsx2_3 >= 1 then FallIND23_WHI = 1;
else if f33fallsx2_3 = 0 then FallIND23_WHI = 0;
else FallIND23_WHI = .;

if f33fallsx2_4 >= 1 then FallIND24_WHI = 1;
else if f33fallsx2_4 = 0 then FallIND24_WHI = 0;
else FallIND24_WHI = .;

if f33fallsx2_5 >= 1 then FallIND25_WHI = 1;
else if f33fallsx2_5 = 0 then FallIND25_WHI = 0;
else FallIND25_WHI = .;
run;

proc freq data=fallvar_whi;
tables FallIND21_WHI FallIND22_WHI FallIND23_WHI FallIND24_WHI FallIND25_WHI; run;


/* Number of times fallen */
data fallvar_whi; set fallvar_whi;
FallNum21_WHI = f33fallsx2_1;
FallNum22_WHI = f33fallsx2_2;
FallNum23_WHI = f33fallsx2_3;
FallNum24_WHI = f33fallsx2_4;
FallNum25_WHI = f33fallsx2_5;
run;

proc print data=fallvar_whi;
var
FallNum21_WHI  f33fallsx2_1
FallNum22_WHI  f33fallsx2_2
FallNum23_WHI  f33fallsx2_3
FallNum24_WHI  f33fallsx2_4
FallNum25_WHI  f33fallsx2_5;
run;

proc freq data=fallvar_whi;
tables FallNum21_WHI FallNum22_WHI FallNum23_WHI FallNum24_WHI FallNum25_WHI ; run;


/* Number of falls categorized as 1=one time / 2=2 or more times */
data fallvar_whi; set fallvar_whi;

if FallIND21_WHI=0 then FallNumbers21_WHI=0;
else if FallIND21_WHI=1 then do;
if FallNum21_WHI=1 then FallNumbers21_WHI=1;
else if FallNum21_WHI>=2 then FallNumbers21_WHI=2;
else if FallNum21_WHI=. then FallNumbers21_WHI=.;
end;

if FallIND22_WHI=0 then FallNumbers22_WHI=0;
else if FallIND22_WHI=1 then do;
if FallNum22_WHI=1 then FallNumbers22_WHI=1;
else if FallNum22_WHI>=2 then FallNumbers22_WHI=2;
else if FallNum22_WHI=. then FallNumbers22_WHI=.;
end;

if FallIND23_WHI=0 then FallNumbers23_WHI=0;
else if FallIND23_WHI=1 then do;
if FallNum23_WHI=1 then FallNumbers23_WHI=1;
else if FallNum23_WHI>=2 then FallNumbers23_WHI=2;
else if FallNum23_WHI=. then FallNumbers23_WHI=.;
end;

if FallIND24_WHI=0 then FallNumbers24_WHI=0;
else if FallIND24_WHI=1 then do;
if FallNum24_WHI=1 then FallNumbers24_WHI=1;
else if FallNum24_WHI>=2 then FallNumbers24_WHI=2;
else if FallNum24_WHI=. then FallNumbers24_WHI=.;
end;

if FallIND25_WHI=0 then FallNumbers25_WHI=0;
else if FallIND25_WHI=1 then do;
if FallNum25_WHI=1 then FallNumbers25_WHI=1;
else if FallNum25_WHI>=2 then FallNumbers25_WHI=2;
else if FallNum25_WHI=. then FallNumbers25_WHI=.;
end;

run;

proc freq data=fallvar_whi;
tables FallNumbers21_WHI FallNumbers22_WHI FallNumbers23_WHI FallNumbers24_WHI FallNumbers25_WHI; run;



data fallvar_whi; set fallvar_whi;

if FallNumbers21_WHI ne . then RecurrentFalls21_WHI=(FallNumbers21_WHI=2);
if FallNumbers22_WHI ne . then RecurrentFalls22_WHI=(FallNumbers22_WHI=2);
if FallNumbers23_WHI ne . then RecurrentFalls23_WHI=(FallNumbers23_WHI=2);
if FallNumbers24_WHI ne . then RecurrentFalls24_WHI=(FallNumbers24_WHI=2);
if FallNumbers25_WHI ne . then RecurrentFalls25_WHI=(FallNumbers25_WHI=2);

run;


proc print data=fallvar_whi (obs=30); run;



data created.fallvar_whi; set fallvar_whi;

keep
id FallIND21_WHI FallIND22_WHI FallIND23_WHI FallIND24_WHI FallIND25_WHI 
FallNumbers21_WHI FallNumbers22_WHI FallNumbers23_WHI FallNumbers24_WHI FallNumbers25_WHI
RecurrentFalls21_WHI RecurrentFalls22_WHI RecurrentFalls23_WHI RecurrentFalls24_WHI RecurrentFalls25_WHI
;

label
FallIND21_WHI = 'Fall Indicator Harmonized Variable for WHI Y21: Yes(1), No(0)'
FallIND22_WHI = 'Fall Indicator Harmonized Variable for WHI Y22: Yes(1), No(0)'
FallIND23_WHI = 'Fall Indicator Harmonized Variable for WHI Y23: Yes(1), No(0)'
FallIND24_WHI = 'Fall Indicator Harmonized Variable for WHI Y24: Yes(1), No(0)'
FallIND25_WHI = 'Fall Indicator Harmonized Variable for WHI Y25: Yes(1), No(0)'

FallNumbers21_WHI = 'Number of Falls Harmonized Variable for WHI Y21: None(0), One time(1), Two or more times(2)'
FallNumbers22_WHI = 'Number of Falls Harmonized Variable for WHI Y22: None(0), One time(1), Two or more times(2)'
FallNumbers23_WHI = 'Number of Falls Harmonized Variable for WHI Y23: None(0), One time(1), Two or more times(2)'
FallNumbers24_WHI = 'Number of Falls Harmonized Variable for WHI Y24: None(0), One time(1), Two or more times(2)'
FallNumbers25_WHI = 'Number of Falls Harmonized Variable for WHI Y25: None(0), One time(1), Two or more times(2)'

RecurrentFalls21_WHI = 'Recurrent Falls Harmonized Variable for WHI Y21: None or once(0), Twice or more(1)'
RecurrentFalls22_WHI = 'Recurrent Falls Harmonized Variable for WHI Y22: None or once(0), Twice or more(1)'
RecurrentFalls23_WHI = 'Recurrent Falls Harmonized Variable for WHI Y23: None or once(0), Twice or more(1)'
RecurrentFalls24_WHI = 'Recurrent Falls Harmonized Variable for WHI Y24: None or once(0), Twice or more(1)'
RecurrentFalls25_WHI = 'Recurrent Falls Harmonized Variable for WHI Y25: None or once(0), Twice or more(1)'
;
run;


data created.fallvar_whi_final; set created.fallvar_whi; run;


/*************************/
/* Maximum grip strength */
/*************************/


proc contents data=whi; run;

proc print data=whi (obs=30);
var LGRIPSTR1 LGRIPSTR2 RGRIPSTR1 RGRIPSTR2;
run;

proc freq data=whi;
tables LGRIPSTR1 LGRIPSTR2 RGRIPSTR1 RGRIPSTR2;
run;

data gsvar_whi; set whi;
MaxGS_WHI = max(LGRIPSTR1, LGRIPSTR2, RGRIPSTR1, RGRIPSTR2);
run;


data created.gsvar_whi; set gsvar_whi;

keep 
id MaxGS_WHI;

label 
MaxGS_WHI = 'Maximum Grip Strength (kg) Harmonized Variable for WHI';

run;


/**********************/
/* Metabolic syndrome */
/**********************/


*waist > 88 cm for women;
*year 1;
proc means data=whi; var WAIST; run;

data whi1; set whi;
if WAIST ne . then mets_wc21=(WAIST>88);
run;

proc freq data=whi1; table mets_wc21; run;


*triglycerides >= 150 mg/dL;
*year 1;
proc means data=whi; var testval_tri; run;

data whi2; set whi1;
if testval_tri ne . then mets_tri21=(testval_tri>=150);
run;

proc freq data=whi2; table testval_tri; run;


*hdl < 50 mg/dL for women;
*year 1;
proc means data=whi; var testval_hdlc; run;

data whi3; set whi2;
if testval_hdlc ne . then mets_hdl21=(testval_hdlc<50);
run;

proc freq data=whi3; table mets_hdl21; run;


*bp sys >= 130 or dia >= 85;
*year 5, 6, 7, 9, 10, 11, 18;
proc means data=whi; var SYSTBP1 SYSTBP2  DIASBP1 DIASBP2; run;

data whi4; set whi3;
avesys = mean(SYSTBP1, SYSTBP2);
avedia = mean(DIASBP1, DIASBP2);
run;

data whi4; set whi4;
if (avesys ne . and avesys>=130) or (avedia ne . and avedia>=85) then mets_bp21=1; else mets_bp21=0;
run;

proc freq data=whi4; table mets_bp21; run;


*gluc >= 110 md/dL;
*year 1;
proc means data=whi; var testval_gluc; run;

data whi5; set whi4;
if testval_gluc ne . then mets_glu21=(testval_gluc>=110);
run;

proc freq data=whi5; table mets_glu21; run;


*final mets dataset;
data created.mets_whi; set whi5;
keep id mets_wc21 testval_tri mets_hdl21 mets_bp21 mets_glu21;
run;


/***************/
/* Demographic */
/***************/

/********************************** sex, race, edu vars**********************************/
Data racesexedvar;
set WHI;

*Race_H: Race/Ethnicity - WHI (1=White, 2=Black, 3=AI/AN, 4=Asian/Pacific Islander, 5=Other);
if racenih= . then Race_WHI=.;
else if racenih=5 then Race_WHI=1;
else if racenih=4 then Race_WHI=2;
else if racenih in(1,2) then Race_WHI=3;
else if racenih=3 then Race_WHI=4;
else if racenih=6 then Race_WHI=5;
else if racenih=9 then Race_WHI=.;

label Race_WHI= 'Race Harmonized Variable for WHI: White (1), Black(2), Asian, American Indian or Alaskan Native(3), Native Hawaiian or other Pacific Islander(4),
Other (5)';
Race_H= Race_WHI; 
label Race_H= 'Race Harmonized Variable White (1), Black(2), Asian, American Indian or Alaskan Native(3), Native Hawaiian or other Pacific Islander(4),
Other (5)';

*Gender_H (1=Male, 0=Female);
Gender_WHI=0;
label Gender_WHI= 'Gender Harmonized Variable for WHI Male(1), Female(0)';
Gender_H=0;
label Gender_H= 'Gender Harmonized Variable: Male(1), Female(0)';

*EDUCA_H (Definition: 1=less than HS, 2=HS grad, 3=postsecondary);
if educ= . then EDUCA_WHI=.;
else if educ in (1,2,3,4) then EDUCA_WHI=1;
else if educ=5 then EDUCA_WHI=2;
else if educ in (6,7,8,9,10,11) then EDUCA_WHI=3;

label EDUCA_WHI= 'Education Harmonized Variable for WHI: < than HS(1), HS grad(2), postsecondary(3)';

EDUCA_H=EDUCA_WHI;
label EDUCA_H='Education Harmonized Variable: < than HS(1), HS grad(2), postsecondary(3)';
run;

proc contents data=racesexedvar;run;


/*save final age,sex,race dataset*/
data outWHI.racesexedvar_WHI;
set racesexedvar;
keep ID Race_WHI Race_H Gender_WHI Gender_H EDUCA_WHI EDUCA_H;
run;

proc freq data=outWHI.racesexedvar_WHI; tables Race_H; run;
proc freq data=WHI; tables racenih; run;
proc print data=WHI;var racenih; run;


/********************************** age   **********************************/

data whi_age; set whi;
**AgeX_WHI: Age at baseline;
f2day_diff=examdy-f2days;
age_re= age + (f2day_diff/365.25);
age21_WHI=round(age_re);/*no age for other years, so calculate it in next step.*/
run;
/*proc print data=whi_age;run;*/

data whi_age; set whi_age;
age22_WHI= age21_WHI +1;
age23_WHI= age21_WHI +2;
age24_WHI= age21_WHI +3;
age25_WHI= age21_WHI +4;
ageINT21_WHI= age21_WHI;
ageINT22_WHI= ageINT21_WHI +1;
ageINT23_WHI= ageINT21_WHI +2;
ageINT24_WHI= ageINT21_WHI +3;
ageINT25_WHI= ageINT21_WHI +4;
run;

proc print data=whi_age; var  age21_WHI age22_WHI age23_WHI age24_WHI age25_WHI;
run;

data final_data;
set whi_age;
label age21_whi= 'Age(years) Harmonized Variable for WHI Y21'
		age22_whi= 'Age(years) Harmonized Variable for WHI Y22'
		age23_whi= 'Age(years) Harmonized Variable for WHI Y23'
		age24_whi= 'Age(years) Harmonized Variable for WHI Y24'
		age25_whi= 'Age(years) Harmonized Variable for WHI Y25'
		ageINT21_whi= 'Age(years) Harmonized Variable for WHI Y21- copy of age21_H-- not interpolated'
		ageINT22_whi= 'Age(years) Harmonized Variable for WHI Y22- copy of age21_H-- not interpolated'
		ageINT23_whi= 'Age(years) Harmonized Variable for WHI Y23- copy of age21_H-- not interpolated'
		ageINT24_whi= 'Age(years) Harmonized Variable for WHI Y24- copy of age21_H-- not interpolated'
		ageINT25_whi= 'Age(years) Harmonized Variable for WHI Y25- copy of age21_H-- not interpolated';
run;

data outWHI.agevar_WHI;
set final_data;
keep id ageINT21_WHI ageINT22_WHI ageINT23_WHI ageINT24_WHI ageINT25_WHI age21_WHI age22_WHI age23_WHI age24_WHI age25_WHI ;
run;

proc print data=outWHI.agevar_WHI;  run;

/********************************** height   **********************************/

/*Height: CONVERT: WHI = HEIGHT*25.4     */
data height_WHI;
set whi;
height21_WHI= HEIGHT*25.4;
/*But we need to carry this height over for ALL the years of the study*/
height22_WHI= HEIGHT*25.4;
height23_WHI= HEIGHT*25.4;
height24_WHI= HEIGHT*25.4;
height25_WHI= HEIGHT*25.4;
heightINT21_WHI= HEIGHT*25.4;/*need to align with the interpolated height*/
heightINT22_WHI= HEIGHT*25.4;
heightINT23_WHI= HEIGHT*25.4;
heightINT24_WHI= HEIGHT*25.4;
heightINT25_WHI= HEIGHT*25.4;
run;

data final_data;
set height_WHI;
label height21_whi= 'Average Standing Height (mm) Harmonized Variable for whi Y21'
		height22_whi= 'Average Standing Height (mm) Harmonized Variable for whi Y22- Carried forward from Baseline (Y21)'
		height23_whi= 'Average Standing Height (mm) Harmonized Variable for whi Y23- Carried forward from Baseline (Y21)'
		height24_whi= 'Average Standing Height (mm) Harmonized Variable for whi Y24- Carried forward from Baseline (Y21)'
		height25_whi= 'Average Standing Height (mm) Harmonized Variable for whi Y25- Carried forward from Baseline (Y21)'
	heightINT21_whi= 'Average Standing Height (mm) Harmonized Variable for whi Y21- NOT INTERPOLATED'
		heightINT22_whi= 'Average Standing Height (mm) Harmonized Variable for whi Y22- Carried forward from Baseline (Y21)- NOT INTERPOLATED'
		heightINT23_whi= 'Average Standing Height (mm) Harmonized Variable for whi Y23- Carried forward from Baseline (Y21)- NOT INTERPOLATED'
		heightINT24_whi= 'Average Standing Height (mm) Harmonized Variable for whi Y24- Carried forward from Baseline (Y21)- NOT INTERPOLATED'
		heightINT25_whi= 'Average Standing Height (mm) Harmonized Variable for whi Y25- Carried forward from Baseline (Y21)- NOT INTERPOLATED';
run;

proc print data=final_data; var height height21_WHI height20_WHI height22_WHI height23_WHI height24_WHI height25_WHI; run;

proc univariate data=height_whi; var height21_WHI;run;
/*Possible outliers: lowest two heights are 1168.4 (3'10") and the highest height 6149 (7'1")*/

/*save to dataset*/
data outWHI.heightvar_WHI;
set final_data;
keep id heightINT21_WHI heightINT22_WHI heightINT23_WHI heightINT24_WHI heightINT25_WHI height21_WHI height22_WHI height23_WHI height24_WHI height25_WHI ;
run;


/*********************/
/* Physical activity */
/*********************/


**************************************************************************************************************************
												PHYSICAL ACTIVITY VARIABLES
**************************************************************************************************************************;
/*We need everything in the same units, but MrOS's PA is based on a scale with propietary weights so the best way to harmonize is to 
Standardize each study individually and then harmonize:
The reason I am standardizing each separately rather than combining the 3 first and standardizing it is to prevent the larger dataset 
(if we'd already combined the 3 into 1) or those with higher variance (possibly the MrOS dataset) to disproportionately influence the overall 
scaling factors- biasing the standardization. 
 

Should ask Elsa if we need to Harmonize the other three studies into a harmonized variable too.
.*/

/*set up dataset and check mean/sd*/
data PhysActivityVar;
set WHI;
PhysActivity21_WHI = TEXPWK ;
Keep id PhysActivity21_WHI;
run;

proc univariate data=PhysActivityVar; var PhysActivity21_WHI; histogram PhysActivity21_WHI;run;
/*strongly skewed right*/

/*The distribution is strongly skewed right. Take the logarithm to keep consistent.*/
data PhysActivityVar;
    set PhysActivityVar;
    PhysActivity21_WHI_L = log(PhysActivity21_WHI);
	keep id PhysActivity21_WHI_L;
run;


proc univariate data=PhysActivityVar; var PhysActivity21_WHI_L ; histogram PhysActivity21_WHI_L;run;

/*It seems like keeping the method consistent across studies is more important than making sure that each distribution is approx. Normal
before standardizing/combining so I will do LOGARITHM for each study*/

/*standardize the variable and then rename it */
PROC STANDARD DATA=PhysActivityVar MEAN=0 STD=1 OUT=PhysActivityVar_std;
  VAR PhysActivity21_WHI_L;
  run;

  proc univariate data=PhysActivityVar_std; var PhysActivity21_WHI_L ; histogram PhysActivity21_WHI_L;run;

data PhysActivityVar_std2;
    set PhysActivityVar_std;
	rename PhysActivity21_WHI_L=PhysActivity21_WHI;
run;

/*try sqrt*/
*data PhysActivityVar;
*   set PhysActivityVar;
*    PhysActivity21_WHI_L = sqrt(PhysActivity21_WHI);
*	keep id PhysActivity21_WHI_L;
*run;
*proc univariate data=PhysActivityVar; *var PhysActivity21_WHI_L ; *histogram PhysActivity21_WHI_L; *run;

data PhysActivityVar_std3;
    set PhysActivityVar_std2;
PhysActivity22_WHI= PhysActivity21_WHI;
PhysActivity23_WHI= PhysActivity21_WHI;
PhysActivity24_WHI= PhysActivity21_WHI;
PhysActivity25_WHI= PhysActivity21_WHI;
    label PhysActivity21_WHI= 'Physical Activity for WHI(MET-hours/week), NFFI year21- log transformed and standardized'
PhysActivity22_WHI= 'Physical Activity for WHI(MET-hours/week), NFFI year22- Carried forward from Baseline (Y21)log transformed and standardized'
PhysActivity23_WHI= 'Physical Activity for WHI(MET-hours/week), NFFI year23- Carried forward from Baseline (Y21)log transformed and standardized'
PhysActivity24_WHI= 'Physical Activity for WHI(MET-hours/week), NFFI year24- Carried forward from Baseline (Y21)log transformed and standardized'
PhysActivity25_WHI= 'Physical Activity for WHI(MET-hours/week), NFFI year25- Carried forward from Baseline (Y21)log transformed and standardized';
	run;


/*look at distribution of var now that its standardized.*/
proc univariate data=PhysActivityVar_std3; var PhysActivity21_WHI; histogram PhysActivity21_WHI;run;

  proc univariate data= PhysActivityVar_std3;
  where PhysActivity21_WHI > 3 OR PhysActivity21_WHI <=- 3;
  run;
/*n=90 obs outside of 2.5 SDs,  (there were 82 with log)
  0 obs outside of 3 SD,
  highest/lowest is 2.53 and -2.66*/



/*save final PA dataset*/
data outWHI.PhysActivityvar_WHI;
set PhysActivityVar_std3;
run;


/****************/
/* Vital status */
/****************/

/******************************************************************   Die Before 90 (Y/N)   *********************************************************************/

/*get variables i need to calculate age at death.*/
data neededvars;
set whi;
keep id age F2Days deathdy_outc;
run;

/*calculate age at death*/
data ageatdeath_Whi;
set neededvars;
f2day_diff=deathdy_outc-f2days;
age_re= age + (f2day_diff/365.25);
ageatdeath_WHI= round(age_re);
run;

/*code the variable*/
data die_after_90;
set ageatdeath_Whi;
if 1 < ageatdeath_WHI <90 then dieafter90_WHI=0;
else if ageatdeath_WHI >=90 then dieafter90_WHI=1;
else if ageatdeath_WHI= . then dieafter90_WHI= .;
run;

proc print data=die_after_90; var age_death dieafter90_WHI;run;
proc freq data=die_after_90; table dieafter90_WHI;run;
/*n=356 died after 90*/

/*save variable*/
data outWHI.Dieafter90var_WHI;
set die_after_90;
keep ID dieafter90_WHI ageatdeath_WHI;
run;

proc print data=outWHI.Dieafter90var_WHI;run;



**************************************************************************************************************************
												Creating vitality variables
**************************************************************************************************************************;
/*THe variable will be defined as: 
VITALX_HABC=
1 if alive at year k 
0 if died before year k 
. if missing/unknown

The Lost to followup variable will be:
STATUS= 0:died, 1:alive through study end, 2:lost to follow up/censored

We have the variables: 
1. ‘DEATH: did the participant die during a consented study phase?’ 
2. ‘days since enrollment to study visit (e.g., F33DAYS)’ or do we use F2 days? hajin says F2 days! go back to look at age atdeath var.
3. ‘days since enrollment to death (e.g., DEATHDY)

go based on age at death+ f2days var.*/

proc contents data=whi;run;



/*THE BELOW CODE NEEDS EDITING--- the status var is a mess, totally redo. The vital vars are good, EXCEPT when it should be ltfu, the vital status should be . instead of 0.*/


data harmonized_simple ;
     set WHI;

  array visits[5] F33DAYS_1-F33DAYS_5;
  array vital[5] Vital1-Vital5;

  /* --- LTFU --- */
  last_visit_index = 0;
  last_visit_day = .;
  do i = 5 to 1 by -1;
    if not missing(visits[i]) then do;
      last_visit_index = i;
      last_visit_day = visits[i];
      leave;
    end;
  end;

  ltfu = 0;
  if DEATH_outc = 0 then ltfu = 0; /* alive through study */
  else if DEATH_outc = 1 then do;
    if last_visit_index < 5 and not missing(DEATHDY_outc) and not missing(last_visit_day)
       and (DEATHDY_outc - last_visit_day) >= 365 then ltfu = 1;
  end;

  /* --- Vital statuses --- */
  died_flag = 0;
  do k = 1 to 5;
    if died_flag = 1 then vital[k] = 0; /* once dead, stay dead */
    else do;
      /* If visit observed before death day ? alive */
      if not missing(visits[k]) and (DEATH_outc=0 or visits[k] <= DEATHDY_outc) then vital[k] = 1;

      /* If death occurred before this year’s visit window ? dead */
      else if DEATH_outc=1 and not missing(DEATHDY_outc) and
              (visits[k]=. or DEATHDY_outc <= visits[k]) then do;
        vital[k] = 0;
        died_flag = 1;
      end;

      /* If LTFU ? missing */
      else if ltfu=1 then vital[k] = .;

      /* Otherwise unknown/missing */
      else vital[k] = .;
    end;
  end;

  /* --- Overall status --- */
  cnt_nonmiss = n(of Vital1-Vital5);
  sum_v = sum(of Vital1-Vital5);

  if ltfu = 1 then status_WHI = 2;   /* use 2 for LTFU */
  else if cnt_nonmiss = 5 and sum_v = 5 then status_WHI = 1; /* alive all years */
  else if min(of Vital1-Vital5) = 0 then status_WHI = 0;     /* died */
  else status_WHI = .; /* ambiguous */

  drop i k died_flag cnt_nonmiss sum_v last_visit_index last_visit_day;
run;


proc print data=harmonized_simple; var  death_outc DEATHDY_outc f33days_1-f33days_5 vital1-vital5 status_WHI ltfu ;where death_outc=1; run;

