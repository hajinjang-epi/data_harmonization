/* Obesity phenotypes and falls ASBMR 2024 */
/* Writer: Hajin Jang */
/* Date: 3/11/2024 */
/* Dataset: Women's Health Initiative Long Life Study - OPACH */



libname a "Z:\Jang\ASBMR";

/*************/
/* data step */
/*************/

/* data1: ppts with BMI, WC, and >=1 fall calendar (N=5906)*/
data data1; set a.data1; run;


/* data2: grip strength variables */
data data2; set data1;
if RGRIPSTR1="NA" then RGRIPSTR1=.;
if RGRIPSTR2="NA" then RGRIPSTR2=.;
if LGRIPSTR1="NA" then LGRIPSTR1=.;
if LGRIPSTR2="NA" then LGRIPSTR2=.;
if calcount="NA" then calcount=.;

griprg1=input(RGRIPSTR1,best.);
griprg2=input(RGRIPSTR2,best.);
griplg1=input(LGRIPSTR1,best.);
griplg2=input(LGRIPSTR2,best.);

mean_right=mean(griprg1, griprg2);
mean_left=mean(griplg1, griplg2);

max_right=max(griprg1, griprg2);
max_left=max(griplg1, griplg2);
run;


/* data3: maximal grip strength */
* max_dom: maximal grip strength, prioritized right-hand ;
* max_dom2: maximal grip strength, whichever hand with maximal grip strength ;
data data3; set data2;
if gripdom=1 and max_right ne . then max_dom=max_right;
else if gripdom=2 and max_left ne . then max_dom=max_left;
else if gripdom in (3,4) and max_right ne . then max_dom=max_right;
else if gripdom in (3,4) and max_left ne . then max_dom=max_left;
else if gripdom=. and max_right ne . then max_dom=max_right;
else if gripdom=. and max_left ne . then max_dom=max_left;

if gripdom=1 and max_right ne . then max_dom2=max_right;
else if gripdom=2 and max_left ne . then max_dom2=max_left;
else if gripdom in (3,4,.) and (max_right ne . or max_left ne .) then max_dom2=max(max_right,max_left);
run;


/* data4: average grip strength */
* mean_dom: average grip strength, prioritized right-hand ;
* mean_dom2: average grip strength, whichever hand with higher mean grip strength ;
data data4; set data3;
if gripdom=1 and mean_right ne . then mean_dom=mean_right;
else if gripdom=2 and mean_left ne . then mean_dom=mean_left;
else if gripdom in (3,4) and mean_right ne . then mean_dom=mean_right;
else if gripdom in (3,4) and mean_left ne . then mean_dom=mean_left;
else if gripdom=. and mean_right ne . then mean_dom=mean_right;
else if gripdom=. and mean_left ne . then mean_dom=mean_left;

if gripdom=1 and mean_right ne . then mean_dom2=mean_right;
else if gripdom=2 and mean_left ne . then mean_dom2=mean_left;
else if gripdom in (3,4,.) and (mean_right ne . or mean_left ne .) then mean_dom2=max(mean_right,mean_left);
run;


/* data5: convert BMI_cat (character) to BMI_level (numerical) for convenience,
log-transform calcount and transfer totalpa_mins to numerical variable*/
data data5; set data4; 
if BMI_cat="underweight" then BMI_level=1;
else if BMI_cat="normal" then BMI_level=2;
else if BMI_cat="overweight" then BMI_level=3;
else if BMI_cat="obese" then BMI_level=4;

if calcount ne . then logcal=log(calcount); 

if totalpa_mins="NA" then totalpa_mins=.;
totalpa = input(totalpa_mins, best.);
run;


/* data6: generate 4 categories of sarcopenic obesity */
data data6; set data5;
if max_dom ne . then do;
if max_dom<20 then sar_max=1;
else sar_max=0;
end;

* this one is used;
if max_dom2 ne . then do;
if max_dom2<20 then sar_max2=1;
else sar_max2=0;
end;

if mean_dom ne . then do;
if mean_dom<20 then sar_mean=1;
else sar_mean=0;
end;

if mean_dom2 ne . then do;
if mean_dom2<20 then sar_mean2=1;
else sar_mean2=0;
end;

if wbobe=1 or abobe=1 then obecon=1; else obecon=0;

if sar_max in (0,1) and obecon in (0,1) then do;
	if sar_max=0 and obecon=0 then saobe_max=1;
	else if sar_max=0 and obecon=1 then saobe_max=2;
	else if sar_max=1 and obecon=0 then saobe_max=3;
	else if sar_max=1 and obecon=1 then saobe_max=4;
end;

* this one is used;
if sar_max2 in (0,1) and obecon in (0,1) then do;
	if sar_max2=0 and obecon=0 then saobe_max2=1;
	else if sar_max2=0 and obecon=1 then saobe_max2=2;
	else if sar_max2=1 and obecon=0 then saobe_max2=3;
	else if sar_max2=1 and obecon=1 then saobe_max2=4;
end;

if sar_mean in (0,1) and obecon in (0,1) then do;
	if sar_mean=0 and obecon=0 then saobe_mean=1;
	else if sar_mean=0 and obecon=1 then saobe_mean=2;
	else if sar_mean=1 and obecon=0 then saobe_mean=3;
	else if sar_mean=1 and obecon=1 then saobe_mean=4;
end;

if sar_mean2 in (0,1) and obecon in (0,1) then do;
	if sar_mean2=0 and obecon=0 then saobe_mean2=1;
	else if sar_mean2=0 and obecon=1 then saobe_mean2=2;
	else if sar_mean2=1 and obecon=0 then saobe_mean2=3;
	else if sar_mean2=1 and obecon=1 then saobe_mean2=4;
end;

run;


/* data7: exclude if sarcopenic obesity is missing */
data data7; set data6;
if saobe_max2=. then delete;
run;


/* data8: exclude if physical activity is missing */
data data8; set data7;
if totalpa=. then delete;
run;


/* data8: 5370 ppts with non-missing BMI, WC, grip strength, fall calendar >=1, and physical activity measures */
data a.data8; set data8; run;
/* data1: 5906 ppts with BMI, WC, and >=1 fall calendar */
data a.data1; set data1; run;



/*********************/
/* further exclusion */
/*********************/

data data8; set a.data8; run;

/* model1: univariate */
data model1; set data8; run;

/* model2: further excluding missing race and education */
data model2; set model1;
	if race="Unknown/Not reported" then delete;
	if educ2="Missing" then delete;
run;

/* model3: exclude missing physical activity (same as model2 since missing physical activity was already excluded in the data step) */
data model3; set model2; 
	if totalpa=. then delete;
run;

proc freq data=model3; table alcohol smoknow; run;

/* model4: further excluding missing alcohol and smoking */
data model4; set model3;
	*if FALL12MOS="NA" then FALL12MOS=.;
	if alcohol="NA" then delete;
run;

data model5; set model4;
	if smoknow=. then delete;
run;

/* model5: further excluding missing self-rated health */
data model6; set model5; 
	if HLTHC1Y="NA" then delete;
run;


data a.model6; set model6; run;




data model6; set a.model6; run;

/***********/
/* table 1 */
/***********/

proc contents data=model6; run;

/* macro for table 1 */
%macro tab1(data,ex);
proc sort data=&data.; by &ex.; run;
proc means data=&data. mean std min max; by &ex.; var age totalpa bmi_calculated; run;

proc freq data=&data.;
table &ex.*(race ethnicnih educ2 smoknow alcohol HLTHC1Y bmi_cat abobe saobe_max2 saobe_mean2)/chisq;
run;

%mend tab1;

%tab1(model1,bmi_level);
%tab1(model2,fall);
%tab1(model3,injfallflg);
%tab1(model4,abobe);
%tab1(model5,saobe_max2);

proc means data=model6 mean std min max; var age totalpa bmi_calculated; run;
proc freq data=model6;
table race ethnicnih educ2 smoknow alcohol HLTHC1Y bmi_cat abobe saobe_max2 /chisq;
run;


%tab1(model6,saobe_max2);


%macro pairwise(data,var);
proc glm data=&data.;
  class saobe_max2;
  model &var. = saobe_max2;
  lsmeans saobe_max2 / pdiff=all adjust=tukey;  /* or adjust=bon for Bonferroni */
run;
quit;
%mend pairwise;

%pairwise(model6,age);
%pairwise(model6,totalpa);
%pairwise(model6,bmi_calculated);
%pairwise(model6,waist);
%pairwise(model6,max_dom2);

data model6; set model6;
	fallrate_calculated=fallcount/calcount; 
run;
proc univariate data=model6; var fallrate_calculated; run;


%pairwise(model6,fallrate_calculated);




%macro pairwise_chisq_ref(data=, ex=, ref=, var=);
  /* Get unique levels of exposure variable except reference */
  proc sql noprint;
    select distinct &ex. into :levels separated by ' '
    from &data.
    where &ex. ne &ref.;
  quit;

  %let n = %sysfunc(countw(&levels));

  %do i = 1 %to &n;
    %let comp = %scan(&levels, &i);

    data _subset;
      set &data.;
      if &ex. in (&ref., &comp);
    run;

    title "Chi-Square Test: &var. by &ex. (&comp vs &ref.)";
    proc freq data=_subset;
      tables &ex.*&var. / chisq expected norow nocol nopercent;
    run;
    title;
  %end;
%mend;


%pairwise_chisq_ref(data=model6, ex=saobe_max2, ref=2, var=race);
%pairwise_chisq_ref(data=model6, ex=saobe_max2, ref=2, var=ethnicnih);
%pairwise_chisq_ref(data=model6, ex=saobe_max2, ref=2, var=educ2);
%pairwise_chisq_ref(data=model6, ex=saobe_max2, ref=2, var=smoknow);
%pairwise_chisq_ref(data=model6, ex=saobe_max2, ref=2, var=alcohol);
%pairwise_chisq_ref(data=model6, ex=saobe_max2, ref=2, var=HLTHC1Y);
%pairwise_chisq_ref(data=model6, ex=saobe_max2, ref=2, var=bmi_cat);
%pairwise_chisq_ref(data=model6, ex=saobe_max2, ref=2, var=abobe);
%pairwise_chisq_ref(data=model6, ex=saobe_max2, ref=2, var=fall);






proc freq data=model6; table BMI_level abobe saobe_max2; run;

data model6; set model6; fallrate_calculated=fallcount/calcount; run;
proc means data=model6 mean std; by saobe_max2; var waist fallrate_calculated ;run;
proc freq data=model6; table saobe_max2*fall fall bmi_level*abobe; run;


/* average fall rate */
data model6; set model6;
	fallrate_calculated=fallcount/calcount; 
run;
proc univariate data=model6; var fallrate_calculated; run;


/*******************************************/
/* fall rate: negative binomial regression */
/*******************************************/

/* whole-body obesity */
%macro wbobe(data,var);
proc genmod data=&data.;
    class BMI_level race ethnicnih educ2 alcohol smoknow HLTHC1Y;    
	model fallcount = BMI_level &var./ dist=nb link=log offset=logcal; *using log-transformed calcount;
    estimate 'IRR BMI_level=1 vs. BMI_level=2' BMI_level 1 -1 0 0 / exp;
    estimate 'IRR BMI_level=3 vs. BMI_level=2' BMI_level 0 -1 1 0 / exp;
    estimate 'IRR BMI_level=4 vs. BMI_level=2' BMI_level 0 -1 0 1 / exp;
run;
%mend wbobe;

%wbobe(model1,);
%wbobe(model2,age race ethnicnih educ2);
%wbobe(model3,age race ethnicnih educ2 totalpa);
%wbobe(model4,age race ethnicnih educ2 totalpa alcohol smoknow);
%wbobe(model5,age race ethnicnih educ2 totalpa alcohol smoknow hlthc1y);


/* abdominal obesity */
%macro abobe(data,var);
proc genmod data=&data.;
    class abobe race ethnicnih educ2 smoknow alcohol HLTHC1Y;    
	model fallcount = abobe &var./ dist=nb link=log offset=logcal; *using log-transformed calcount;
    estimate 'IRR abobe=1 vs. abobe=0' abobe -1 1 / exp;
run;
%mend abobe;

%abobe(model1,);
%abobe(model2,height age race ethnicnih educ2);
%abobe(model3,height age race ethnicnih educ2 totalpa);
%abobe(model4,height age race ethnicnih educ2 totalpa alcohol smoknow);
%abobe(model5,height age race ethnicnih educ2 totalpa alcohol smoknow hlthc1y);


/* sarcopenic obesity */
%macro saobe(data,saobe,var);
proc genmod data=&data.;
    class &saobe. race ethnicnih educ2 smoknow alcohol HLTHC1Y;    
	model fallcount = &saobe. &var./ dist=nb link=log offset=logcal;
    estimate 'saobe2=2 vs. saobe2=1' &saobe. -1 1 0 0 / exp;
    estimate 'saobe2=3 vs. saobe2=1' &saobe. -1 0 1 0 / exp;
    estimate 'saobe2=4 vs. saobe2=1' &saobe. -1 0 0 1 / exp;
run;
%mend saobe;

* saobe_max ;
%saobe(model1,saobe_max,);
%saobe(model2,saobe_max,age race ethnicnih educ2);
%saobe(model3,saobe_max,age race ethnicnih educ2 totalpa);
%saobe(model4,saobe_max,age race ethnicnih educ2 totalpa alcohol smoknow);
%saobe(model5,saobe_max,age race ethnicnih educ2 totalpa alcohol smoknow hlthc1y);

* saobe_max2 ;
%saobe(model1,saobe_max2,);
%saobe(model2,saobe_max2,age race ethnicnih educ2);
%saobe(model3,saobe_max2,age race ethnicnih educ2 totalpa);
%saobe(model4,saobe_max2,age race ethnicnih educ2 totalpa alcohol smoknow);
%saobe(model5,saobe_max2,age race ethnicnih educ2 totalpa alcohol smoknow hlthc1y);

* saobe_mean ;
%saobe(model1,saobe_mean,);
%saobe(model2,saobe_mean,age race ethnicnih educ2);
%saobe(model3,saobe_mean,age race ethnicnih educ2 totalpa);
%saobe(model4,saobe_mean,age race ethnicnih educ2 totalpa alcohol smoknow);
%saobe(model5,saobe_mean,age race ethnicnih educ2 totalpa alcohol smoknow hlthc1y);

* saobe_mean2 ;
%saobe(model1,saobe_mean2,);
%saobe(model2,saobe_mean2,age race ethnicnih educ2);
%saobe(model3,saobe_mean2,age race ethnicnih educ2 totalpa);
%saobe(model4,saobe_mean2,age race ethnicnih educ2 totalpa alcohol smoknow);
%saobe(model5,saobe_mean2,age race ethnicnih educ2 totalpa alcohol smoknow hlthc1y);


/*********/
/* added (8/24/24) */
proc freq data=model1; table saobe_max2; run;
proc freq data=model5; table saobe_max2; run;

proc freq data=model5; table wbobe abobe sar_max2 saobe_max2 ; run;
proc freq data=model5; table wbobe*abobe*sar_max2 saobe_max2 ; run;


/*average fall rate */
data test; set model5;
	fallrate_calculated=fallcount/calcount; 
	fall1=(fallcount>=1);
run;
proc sort data=test; by saobe_max2;run;
proc means data=test mean std; by saobe_max2; var fallrate_calculated; run;
proc univariate data=test; by saobe_max2; var fallrate_calculated; run;
proc freq data=test; by saobe_max2; table fall1; run;




















/************************************/
/* fall injury: logistic regression */
/************************************/

/********/
/* data */
/********/
data injury1; set model3;
met2=input(TEXPWK,best.);
run;

data injury2; set injury1;
	if met2=. then delete;

	if met2>=21 then active=1;
	else active=0;

	if active=1 then injwt=1;
	else if active=0 then injwt=5;
run;
proc freq data=injury2; table active injwt; run;

/* only injury */
data injurious; set injury2;
	if injfallflg=1;
run;

/* only non-injurious */
data noninjurious; set injury2;
	if injfallflg=0;
run;

/* no falls */
data nofall; set injury2;
	if fall=0;
run;
