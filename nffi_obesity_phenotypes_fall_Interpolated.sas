
*************************************************************************
	*  TITLE: NFFI Data harmonization GEE analysis 					*
	*																*	
	*  PROGRAMMER:   Hajin Jang   	 								*
	*																*
	*  DESCRIPTION: Obesity phenotypes -> Fall outcomes      		*
	*  									  (Interpolated)      		*
	*																*
	*  DATE:    created 7/25/2025									*
	*---------------------------------------------------------------*
	*  LANGUAGE:     SAS VERSION 9.4								*
	*---------------------------------------------------------------*
	*  NOTES: 		Analysis									    *
*************************************************************************;


libname data "Z:\Fall Injuries Combined Studies\Harmonization Coding\Harmonized dataset";
libname created "Z:\Fall Injuries Combined Studies\Harmonization Coding\Analysis\Hajin's analysis\Interpolated";

	dm "odsresults; clear";
	dm "log; clear";

	
	options nofmterr;



/***************/
/* Eligibility */
/***************/

data full; set data.final_harmonizedonly_041125; run;
proc contents data=full; run;

data eligible; set full;
if study="HABC" then do;
if weight6_H=. or heightint6_H=. or waistcirc6_H=. or maxgs6_H=. then delete;
end;

if study="MrOS" then do;
if weight9_H=. or heightint9_H=. or waistcirc9_H=. or maxgs9_H=. then delete;
end;

if study="CHS" then do;
if weight1_H=. or heightint1_H=. or waistcirc1_H=. or maxgs1_H=. then delete;
end;

if study="WHI" then do;
if weight21_H=. or heightint21_H=. or waistcirc21_H=. or maxgs21_H=. then delete;
end;
run;

data created.eligible; set eligible; run;



/*****************************/
/* Drop _H and code exposure */
/*****************************/

data data; set created.eligible; run;

/* drop "_H" from the variable names */
proc sql;
    select cats(name, "=", substr(name, 1, length(name)-2)) into :renames separated by ' '
    from dictionary.columns
    where libname="WORK" and memname="DATA";
quit;
data data;
    set data(rename=(&renames));
run;

/* fix errors */
data data; set data;
rename 
ID_NF=ID_NFFI 
STU=STUDY 
Study=StudyNo 
id_C=id_CHS 
id_Mr=id_MrOS 
id_W=id_WHI 
id_ha=id_HABC
;
run;

data data; set data;
drop ethnicn racen;
run;

proc contents data=data; run;


data dataex; set data;

/* HABC: baseline=6 */
if study="HABC" then do;
if weight6 ne . and heightint6 ne . then bmi=weight6/((heightint6/1000)**2);

if .<bmi<18.5 then wbobecat=1;
else if 18.5<=bmi<25 then wbobecat=2;
else if 25<=bmi<30 then wbobecat=3;
else if bmi>=30 then wbobecat=4;

if bmi>=30 then wbobe=1;
else if .<bmi<30 then wbobe=0;

if waistcirc6 ne . and gender=1 then abobe=(waistcirc6>=102);
else if waistcirc6 ne . and gender=0 then abobe=(waistcirc6>=88);

if maxgs6 ne . and gender=1 then sarco=(maxgs6<35.5);
else if maxgs6 ne . and gender=0 then sarco=(maxgs6<20);

if (wbobe ne . or abobe ne .) and sarco ne . then do;
if (wbobe=1 or abobe=1) and sarco=1 then saobe=1; else saobe=0;
end;

height_baseline = heightint6;
bmi_baseline = bmi;
waistcirc_baseline = waistcirc6;
maxgs_baseline = maxgs6;

end;

/* MrOS: baseline=9 */
if study="MrOS" then do;
if weight9 ne . and heightint9 ne . then bmi=weight9/((heightint9/1000)**2);

if .<bmi<18.5 then wbobecat=1;
else if 18.5<=bmi<25 then wbobecat=2;
else if 25<=bmi<30 then wbobecat=3;
else if bmi>=30 then wbobecat=4;

if bmi>=30 then wbobe=1;
else if .<bmi<30 then wbobe=0;

if waistcirc9 ne . and gender=1 then abobe=(waistcirc9>=102);
else if waistcirc9 ne . and gender=0 then abobe=(waistcirc9>=88);

if maxgs9 ne . and gender=1 then sarco=(maxgs9<35.5);
else if maxgs9 ne . and gender=0 then sarco=(maxgs9<20);


if (wbobe ne . or abobe ne .) and sarco ne . then do;
if (wbobe=1 or abobe=1) and sarco=1 then saobe=1; else saobe=0;
end;

height_baseline = heightint9;
bmi_baseline = bmi;
waistcirc_baseline = waistcirc9;
maxgs_baseline = maxgs9;

end;

/* CHS: baseline=1 */
if study="CHS" then do;
if weight1 ne . and heightint1 ne . then bmi=weight1/((heightint1/1000)**2);

if .<bmi<18.5 then wbobecat=1;
else if 18.5<=bmi<25 then wbobecat=2;
else if 25<=bmi<30 then wbobecat=3;
else if bmi>=30 then wbobecat=4;

if bmi>=30 then wbobe=1;
else if .<bmi<30 then wbobe=0;

if waistcirc1 ne . and gender=1 then abobe=(waistcirc1>=102);
else if waistcirc1 ne . and gender=0 then abobe=(waistcirc1>=88);

if maxgs1 ne . and gender=1 then sarco=(maxgs1<35.5);
else if maxgs1 ne . and gender=0 then sarco=(maxgs1<20);


if (wbobe ne . or abobe ne .) and sarco ne . then do;
if (wbobe=1 or abobe=1) and sarco=1 then saobe=1; else saobe=0;
end;

height_baseline = heightint1;
bmi_baseline = bmi;
waistcirc_baseline = waistcirc1;
maxgs_baseline = maxgs1;

end;

/* WHI: baseline=21 */
if study="WHI" then do;
if weight21 ne . and heightint21 ne . then bmi=weight21/((heightint21/1000)**2);

if .<bmi<18.5 then wbobecat=1;
else if 18.5<=bmi<25 then wbobecat=2;
else if 25<=bmi<30 then wbobecat=3;
else if bmi>=30 then wbobecat=4;

if bmi>=30 then wbobe=1;
else if .<bmi<30 then wbobe=0;

if waistcirc21 ne . and gender=1 then abobe=(waistcirc21>=102);
else if waistcirc21 ne . and gender=0 then abobe=(waistcirc21>=88);

if maxgs21 ne . and gender=1 then sarco=(maxgs21<35.5);
else if maxgs21 ne . and gender=0 then sarco=(maxgs21<20);

if (wbobe ne . or abobe ne .) and sarco ne . then do;
if (wbobe=1 or abobe=1) and sarco=1 then saobe=1; else saobe=0;
end;

height_baseline = heightint21;
bmi_baseline = bmi;
waistcirc_baseline = waistcirc21;
maxgs_baseline = maxgs21;

end;

run;

data dataex2; set dataex;
if wbobe ne . and abobe ne . and sarco ne . then do;
if wbobe=0 and abobe=0 and sarco=0 then saobecat=1;
else if (wbobe=1 or abobe=1) and sarco=0 then saobecat=2;
else if wbobe=0 and abobe=0 and sarco=1 then saobecat=3;
else if (wbobe=1 or abobe=1) and sarco=1 then saobecat=4;
end;
run;

proc freq data=dataex2; table wbobe abobe sarco saobe wbobecat saobecat; run;

data created.widedata; set dataex2; run;



/*******************************/
/* Transpose from wide to long */
/*******************************/

data data; set created.widedata; run;

%macro long(var);
/* Create a format to handle the special case for 145 */
proc format;
    value yearfmt 
        145 = 14
        other = [best.];
run;

data &var._wide; 
    set data;
    keep id_nffi 
    &var.1 &var.2 &var.3 &var.4 &var.5 
    &var.6 &var.7 &var.8 &var.9 &var.10 
    &var.11 &var.12 &var.13 &var.145 
    &var.15 &var.16 &var.17 &var.18 
    &var.19 &var.20 &var.21 &var.22 
    &var.23 &var.24 &var.25;
run;

proc transpose data=&var._wide out=&var.;
   by id_nffi;
run;

data &var.; 
    set &var. (rename=(col1=&var.));
    /* Improved year extraction */
    length year_str $5;
    
    /* Find the position where the numbers start */
    num_pos = anydigit(_name_);
    
    if num_pos > 0 then do;
        year_str = substr(_name_, num_pos);
        
        /* Handle the special 145 case */
        if year_str = '145' then do;
            nffiyear = 145;
            nffiyear_formatted = 14;
        end;
        else do;
            nffiyear = input(year_str, 5.);
            nffiyear_formatted = put(nffiyear, yearfmt.);
        end;
    end;
    
    drop _name_ num_pos year_str;
run; 

proc datasets library=work nolist;
    delete &var._wide;
run;
quit;

proc sort data=&var.; by id_nffi nffiyear_formatted; run;
%mend long;


%long(fallind);
%long(fallnumbers);
%long(maxgs);
%long(physactivity);
%long(age);
%long(ageint);
%long(curdrnk);
%long(height);
%long(heightint);
%long(smk);
%long(waistcirc);
%long(weight);
%long(weightint);


/* Time-fixed variables set */
data fixed; set data; 
keep ID_NFFI educa gender race study studyno ageflag heightflag id_chs id_mros id_whi id_habc 
wbobe abobe sarco saobe wbobecat saobecat 
height_baseline bmi_baseline waistcirc_baseline maxgs_baseline 
;
run;

proc sort data=fixed; by id_nffi; run;

/* Combine time-varying and time-fixed variables */
data varying; 
merge fallind fallnumbers maxgs physactivity age ageint curdrnk height heightint smk waistcirc weight weightint; 
by id_nffi nffiyear_formatted; 
run;

data longdata; merge fixed varying; by id_nffi; run;

data longdata2; set longdata;
if fallnumbers ne . then recurrentfall=(fallnumbers=2);

if weight ne . and heightint ne . then bmi_long=weight/((heightint/1000)**2);
run;

data longdata3; set longdata2; 
if race ne . then do;
if race=1 then race_re=1; 
else if race=2 then race_re=2;
else if race in (3,4) then race_re=3;
else if race=5 then race_re=4;
end;
drop race;
run;
data longdata3 ; set longdata3; rename race_re=race; run;

data created.longdata; set longdata3; run;




/*******/
/* GEE */
/*******/

data data_gee; set created.longdata; run;

proc sort data=data_gee; by id_nffi nffiyear_formatted; run;

/* multicolinearity check */
proc reg data=data_gee;
model fallind = studyno ageint gender race educa physactivity smk curdrnk /vif; *vif<2 for all;
run;

/* QIC */
*wbobe;
%gee_wb(fallind, studyno gender race educa smk curdrnk, ageint physactivity, exch, binomial, logit);
%gee_wb(fallind, studyno gender race educa smk curdrnk, ageint physactivity, ar(1), binomial, logit);
%gee_wb(fallind, studyno gender race educa smk curdrnk, ageint physactivity, toep, binomial, logit);

%gee_wb(recurrentfall, studyno gender race educa smk curdrnk, ageint physactivity, exch, binomial, logit);
%gee_wb(recurrentfall, studyno gender race educa smk curdrnk, ageint physactivity, ar(1), binomial, logit);
%gee_wb(recurrentfall, studyno gender race educa smk curdrnk, ageint physactivity, toep, binomial, logit);

%gee_wb(fallnumbers, studyno gender race educa smk curdrnk, ageint physactivity, ind, multinomial, cumlogit);

*abobe;
%gee_ab(fallind, studyno gender race educa smk curdrnk, height_baseline ageint physactivity, exch, binomial, logit);
%gee_ab(fallind, studyno gender race educa smk curdrnk, height_baseline ageint physactivity, ar(1), binomial, logit);
%gee_ab(fallind, studyno gender race educa smk curdrnk, height_baseline ageint physactivity, toep, binomial, logit);

%gee_ab(recurrentfall, studyno gender race educa smk curdrnk, height_baseline ageint physactivity, exch, binomial, logit);
%gee_ab(recurrentfall, studyno gender race educa smk curdrnk, height_baseline ageint physactivity, ar(1), binomial, logit);
%gee_ab(recurrentfall, studyno gender race educa smk curdrnk, height_baseline ageint physactivity, toep, binomial, logit);

%gee_ab(fallnumbers, studyno gender race educa smk curdrnk, height_baseline ageint physactivity, ind, multinomial, cumlogit);

*saobe;
%gee_so(fallind, studyno gender race educa smk curdrnk, ageint physactivity, exch, binomial, logit);
%gee_so(fallind, studyno gender race educa smk curdrnk, ageint physactivity, ar(1), binomial, logit);
%gee_so(fallind, studyno gender race educa smk curdrnk, ageint physactivity, toep, binomial, logit);

%gee_so(recurrentfall, studyno gender race educa smk curdrnk, ageint physactivity, exch, binomial, logit);
%gee_so(recurrentfall, studyno gender race educa smk curdrnk, ageint physactivity, ar(1), binomial, logit);
%gee_so(recurrentfall, studyno gender race educa smk curdrnk, ageint physactivity, toep, binomial, logit);

%gee_so(fallnumbers, studyno gender race educa smk curdrnk, ageint physactivity, ind, multinomial, cumlogit);



/**********************/
/* Whole-body obesity */
/**********************/
%macro gee_wb(out, class, cont, type, dist, link);
proc genmod data=data_gee descending;
    class id_nffi wbobecat(ref='2') nffiyear_formatted &class./ param=ref;
    model &out. = wbobecat &class. &cont. / dist=&dist. link=&link.;
    repeated subject=id_nffi / within=nffiyear_formatted type=&type. corrw;
	estimate "Underweight vs Normal" wbobecat 1 0 0 / exp;
	estimate "Overweight  vs Normal" wbobecat 0 1 0 / exp;
	estimate "Obesity     vs Normal" wbobecat 0 0 1 / exp;
run;
%mend gee_wb;

%gee_wb(fallind, , , ar(1), binomial, logit);
%gee_wb(fallind, studyno, , ar(1), binomial, logit);
%gee_wb(fallind, studyno gender race educa, ageint, ar(1), binomial, logit);
%gee_wb(fallind, studyno gender race educa smk curdrnk, ageint physactivity, ar(1), binomial, logit);

%gee_wb(recurrentfall, , , ar(1), binomial, logit);
%gee_wb(recurrentfall, studyno, , ar(1), binomial, logit);
%gee_wb(recurrentfall, studyno gender race educa, ageint, ar(1), binomial, logit);
%gee_wb(recurrentfall, studyno gender race educa smk curdrnk, ageint physactivity, ar(1), binomial, logit);

%gee_wb(fallnumbers, , , ind, multinomial, cumlogit);
%gee_wb(fallnumbers, studyno, , ind, multinomial, cumlogit);
%gee_wb(fallnumbers, studyno gender race educa, ageint, ind, multinomial, cumlogit);
%gee_wb(fallnumbers, studyno gender race educa smk curdrnk, ageint physactivity, ind, multinomial, cumlogit);


/*********************/
/* Abdominal obesity */
/*********************/
%macro gee_ab(out, class, cont, type, dist, link);
proc genmod data=data_gee descending;
    class id_nffi abobe(ref='0') nffiyear_formatted &class./ param=ref;
    model &out. = abobe &class. &cont. / dist=&dist. link=&link.;
    repeated subject=id_nffi / within=nffiyear_formatted type=&type. corrw;
	estimate "Abdominal obesity 1 vs. 0" abobe 1 / exp;
run;
%mend gee_ab;

*%gee_ab(fallind, , , ar(1), binomial, logit);
%gee_ab(fallind, , height_baseline, ar(1), binomial, logit);
%gee_ab(fallind, studyno, height_baseline, ar(1), binomial, logit);
%gee_ab(fallind, studyno gender race educa, height_baseline ageint, ar(1), binomial, logit);
%gee_ab(fallind, studyno gender race educa smk curdrnk, height_baseline ageint physactivity, ar(1), binomial, logit);

*%gee_ab(recurrentfall, , , ar(1), binomial, logit);
%gee_ab(recurrentfall, , height_baseline, ar(1), binomial, logit);
%gee_ab(recurrentfall, studyno, height_baseline, ar(1), binomial, logit);
%gee_ab(recurrentfall, studyno gender race educa, height_baseline ageint, ar(1), binomial, logit);
%gee_ab(recurrentfall, studyno gender race educa smk curdrnk, height_baseline ageint physactivity, ar(1), binomial, logit);

*%gee_ab(fallnumbers, , , ar(1), multinomial, cumlogit);
%gee_ab(fallnumbers, , height_baseline, ind, multinomial, cumlogit);
%gee_ab(fallnumbers, studyno, height_baseline, ind, multinomial, cumlogit);
%gee_ab(fallnumbers, studyno gender race educa, height_baseline ageint, ind, multinomial, cumlogit);
%gee_ab(fallnumbers, studyno gender race educa smk curdrnk, height_baseline ageint physactivity, ind, multinomial, cumlogit);


/**********************/
/* Sarcopenic obesity */
/**********************/
%macro gee_so(out, class, cont, type, dist, link);
proc genmod data=data_gee descending;
    class id_nffi saobecat(ref='1') nffiyear_formatted &class./ param=ref;
    model &out. = saobecat &class. &cont. / dist=&dist. link=&link.;
    repeated subject=id_nffi / within=nffiyear_formatted type=&type. corrw;
	estimate "Obesity only       vs None" saobecat 1 0 0 / exp;
	estimate "Sarcopenia only    vs None" saobecat 0 1 0 / exp;
	estimate "Sarcopenic obesity vs None" saobecat 0 0 1 / exp;
run;
%mend gee_so;

%gee_so(fallind, , , ar(1), binomial, logit);
%gee_so(fallind, studyno, , ar(1), binomial, logit);
%gee_so(fallind, studyno gender race educa, ageint, ar(1), binomial, logit);
%gee_so(fallind, studyno gender race educa smk curdrnk, ageint physactivity, ar(1), binomial, logit);

%gee_so(recurrentfall, , , ar(1), binomial, logit);
%gee_so(recurrentfall, studyno, , ar(1), binomial, logit);
%gee_so(recurrentfall, studyno gender race educa, ageint, ar(1), binomial, logit);
%gee_so(recurrentfall, studyno gender race educa smk curdrnk, ageint physactivity, ar(1), binomial, logit);

%gee_so(fallnumbers, , , ind, multinomial, cumlogit);
%gee_so(fallnumbers, studyno, , ind, multinomial, cumlogit);
%gee_so(fallnumbers, studyno gender race educa, ageint, ind, multinomial, cumlogit);
%gee_so(fallnumbers, studyno gender race educa smk curdrnk, ageint physactivity, ind, multinomial, cumlogit);



/******************************/
/* Check Ns of complete cases */
/******************************/
data data_gee; set created.longdata; run;

data m1; set data_gee; 
if wbobe=. or abobe=. or sarco=. or fallind=. or fallnumbers=. then delete;
run;
proc sort data=m1 nodupkey; by id_nffi; run; * 144086 observations, 18744 participants for unadjusted and model 1 ;
data m1; set m1; m1flag=1; run;

data m2; set data_gee; 
if wbobe=. or abobe=. or sarco=. or fallind=. or fallnumbers=. 
or ageint=. or race=. or gender=. or educa=. then delete;
run;
proc sort data=m2 nodupkey; by id_nffi; run; * 105887 observations, 18334 participants ;
data m2; set m2; m2flag=1; run;

data m3; set data_gee;
if wbobe=. or abobe=. or sarco=. or fallind=. or fallnumbers=. 
or ageint=. or race=. or gender=. or educa=. 
or smk=. or curdrnk=. or physactivity=. then delete;
run;
proc sort data=m3 nodupkey; by id_nffi; run; * 84875 observations, 15054 participants ;
data m3; set m3; m3flag=1; run;

proc freq data=m3; table wbobecat abobe saobecat*gender race gender fallind fallnumbers; run;
proc means data=m3 mean std; var age; run;

proc freq data=m3; table gender; run;

proc contents data=m3; run;
proc freq data=m3;  table study*gender; run;


data fullmodel; set m3;
keep id_nffi m3flag wbobecat abobe sarco saobecat bmi_baseline waistcirc_baseline maxgs_baseline;
run;


/* data for descriptive */
data baseline; set data.final_baseline; run;

proc sort data=baseline; by id_nffi; run;
proc sort data=fullmodel; by id_nffi; run;

data base_complete; merge baseline fullmodel; by id_nffi; run;
data base_complete; set base_complete; if m3flag=1; run;

data created.base_complete; set base_complete; run;

proc freq data=base_complete; table gender_b race_b; run;
proc means data=base_complete mean median std range q1 q3; var ageint_b; run;

data m3_1; set data_gee;
if wbobe=. or abobe=. or sarco=. or fallind=. 
or ageint=. or race=. or gender=. or educa=. 
or smk=. or curdrnk=. or physactivity=. then delete;
run;
data m3_2; set data_gee;
if wbobe=. or abobe=. or sarco=. or fallind=. or fallnumbers=. 
or ageint=. or race=. or gender=. or educa=. 
or smk=. or curdrnk=. or physactivity=. then delete;
run;

proc sort data=m3_1 nodupkey; by id_nffi; run; * 84988 observations, 15056 participants ;
proc sort data=m3_2 nodupkey; by id_nffi; run; * 84875 observations, 15054 participants ;


proc freq data=m3_2; table (race gender educa curdrnk smk)*saobecat; run;

proc sort data=base_complete; by saobecat; run;
data base_complete; set base_complete;
if fallnumbers_b ne . then do;
if fallnumbers_b<=1 then recurrentfall_b=0;
else if fallnumbers_b>=2 then recurrentfall_b=1;
end;
run;
proc means data=base_complete mean std median q1 q3; by saobecat; var age_b bmi_baseline waistcirc_baseline maxgs_baseline physactivity_b fallind_b recurrentfall_b fallnumbers_b ;run;


data completeonly; set m3;
if fallnumbers ne . then do;
if fallnumbers<=1 then recurrentfall=0;
else if fallnumbers>=2 then recurrentfall=1;
end;
run;

proc freq data=completeonly; table recurrentfall*(wbobecat abobe saobecat); run;



/**********************************************/
/* Fallind analyses using only complete cases */
/**********************************************/

data data_gee_complete; set data_gee; if fallnumbers ne .; run;

proc sort data=data_gee_complete; by id_nffi nffiyear_formatted; run;

/* Wbole-body obesity */
%macro gee_wb_c(out, class, cont, type, dist, link);
proc genmod data=data_gee_complete descending;
    class id_nffi wbobecat(ref='2') nffiyear_formatted &class./ param=ref;
    model &out. = wbobecat &class. &cont. / dist=&dist. link=&link.;
    repeated subject=id_nffi / within=nffiyear_formatted type=&type. corrw;
	estimate "Underweight vs Normal" wbobecat 1 0 0 / exp;
	estimate "Overweight  vs Normal" wbobecat 0 1 0 / exp;
	estimate "Obesity     vs Normal" wbobecat 0 0 1 / exp;
run;
%mend gee_wb_c;

%gee_wb_c(recurrentfall, studyno gender race educa smk curdrnk, ageint physactivity, ar(1), binomial, logit);


/* abdominal obesity */
%macro gee_ab_c(out, class, cont, type, dist, link);
proc genmod data=data_gee_complete descending;
    class id_nffi abobe(ref='0') nffiyear_formatted &class./ param=ref;
    model &out. = abobe &class. &cont. / dist=&dist. link=&link.;
    repeated subject=id_nffi / within=nffiyear_formatted type=&type. corrw;
	estimate "Abdominal obesity 1 vs. 0" abobe 1 / exp;
run;
%mend gee_ab_c;

%gee_ab_c(recurrentfall, studyno gender race educa smk curdrnk, height_baseline ageint physactivity, ar(1), binomial, logit);


/* Sarcopenic obesity */
%macro gee_so_c(out, class, cont, type, dist, link);
proc genmod data=data_gee_complete descending;
    class id_nffi saobecat(ref='1') nffiyear_formatted &class./ param=ref;
    model &out. = saobecat &class. &cont. / dist=&dist. link=&link.;
    repeated subject=id_nffi / within=nffiyear_formatted type=&type. corrw;
	estimate "Obesity only       vs None" saobecat 1 0 0 / exp;
	estimate "Sarcopenia only    vs None" saobecat 0 1 0 / exp;
	estimate "Sarcopenic obesity vs None" saobecat 0 0 1 / exp;
run;
%mend gee_so_c;

%gee_so_c(recurrentfall, studyno gender race educa smk curdrnk, ageint physactivity, ar(1), binomial, logit);


data created.data_gee_complete; set data_gee_complete; run;

proc freq data=m3; table wbobecat abobe saobecat; run;
proc freq data=data_gee_complete; table fallind recurrentfall; run;


proc means data=m3  mean std; var ageint; run;
proc freq data=m3; table race gender; run;
proc freq data=m3; table fallind fallnumbers ; run;

proc sort data=m3; by saobecat; run;
proc means data=m3 mean std; by saobecat; var ageint; run;

proc freq data=m3; where study="WHI"; table nffiyear_formatted*(smk curdrnk); run;



/********************/
/* Interaction term */
/********************/

data test; set data_gee_complete;
if wbobecat ne . and abobe ne . then do;
if wbobecat=4 or abobe=1 then anyobe=1;
else anyobe=0;
end;
run;

proc genmod data=test descending;
    class id_nffi nffiyear_formatted studyno gender race educa smk curdrnk anyobe sarco / param=ref;
    model fallind = anyobe sarco anyobe*sarco gender race educa smk curdrnk ageint physactivity 
        / dist=binomial link=logit;
    repeated subject=id_nffi / within=nffiyear_formatted type=ar(1) corrw;
    estimate "Interaction: Obesity*Sarcopenia" anyobe*sarco 1 / exp;
run;


/************/
/* Check Ns */
/************/

data test; set data_gee; run;

data test1; set test;
if wbobe=. or abobe=. or sarco=. or fallind=. or fallnumbers=. then delete; run;
proc sort data=test1 nodupkey; by id_nffi; run; *18744;
 
data test2; set test;
if wbobe=. or abobe=. or sarco=. or fallind=. or fallnumbers=. or ageint=. then delete; run;
proc sort data=test2 nodupkey; by id_nffi; run; *107813, 18744;

data test3; set test;
if wbobe=. or abobe=. or sarco=. or fallind=. or fallnumbers=. or ageint=. or race=. then delete; run;
proc sort data=test3 nodupkey; by id_nffi; run; *106277, 18395;

data test4; set test;
if wbobe=. or abobe=. or sarco=. or fallind=. or fallnumbers=. or ageint=. or race=. or educa=. then delete; run;
proc sort data=test4 nodupkey; by id_nffi; run; *105982, 18334;


data test5; set test;
if wbobe=. or abobe=. or sarco=. or fallind=. or fallnumbers=. or ageint=. or race=. or educa=. 
or smk=. then delete; run;
proc sort data=test5 nodupkey; by id_nffi; run; *103215, 17953;


data test6; set test;
if wbobe=. or abobe=. or sarco=. or fallind=. or fallnumbers=. or ageint=. or race=. or educa=. 
or smk=. or curdrnk=. then delete; run;
proc sort data=test6 nodupkey; by id_nffi; run; *102029, 17908;

data test7; set data_gee;
if wbobe=. or abobe=. or sarco=. or fallind=. or fallnumbers=. or ageint=. or race=. or educa=. 
or smk=. or curdrnk=. or physactivity=. then delete; run;
proc sort data=test7 nodupkey; by id_nffi; run; *86848, 15054;

proc freq data=test7; table gender*saobecat saobecat; run;


data test8; set test;
if educa=. then educa_missing=1; else educa_missing=0;
if smk=. then smk_missing=1; else smk_missing=0;
if curdrnk=. then curdrnk_missing=1; else curdrnk_missing=0;
if physactivity=. then pa_missing=1; else pa_missing=0;
run;

proc sort data=test8 nodupkey; by id_nffi; run; *86848, 15054;

proc freq data=test8; table educa_missing*smk_missing educa_missing*curdrnk_missing educa_missing*pa_missing;run;

proc freq data=test7;
where study="MrOS";
table (smk curdrnk )*nffiyear_formatted; run;

proc sort data=test7; by nffiyear_formatted; run;
proc means data=test7 min max mean std median;
where study="MrOS";
by nffiyear_formatted;
var ageint heightint physactivity;
run;
