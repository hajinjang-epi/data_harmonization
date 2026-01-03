
*************************************************************************
	*  TITLE: NFFI Data harmonization: CHS		 					*
	*																*	
	*  PROGRAMMER:   Hajin Jang   	 								*
	*																*
	*  DESCRIPTION: Data harmonization variables 		      		*
	*  			  		  (CHS)      								*
	*																*
	*  DATE:    created 3/6/2025									*
	*---------------------------------------------------------------*
	*  LANGUAGE:     SAS VERSION 9.4								*
	*---------------------------------------------------------------*
	*  NOTES: 		Data pooling								    *
*************************************************************************;


libname data "Z:\Fall Injuries Combined Studies\HABC, MrOS, CHS, WHI Combined\Final datasets for NFFI";
libname created "Z:\Fall Injuries Combined Studies\Harmonization Coding\Hajin code\Hajin datasets\CHS";

	dm "odsresults; clear";
	dm "log; clear";

	
	options nofmterr;

data chs; set data.chs_master_032124; run;

proc contents data=chs; run;


/******************/
/* Fall variables */
/******************/


/* Fall indicator */
data fallvar_chs; set chs;
FallIND1_CHS = fall_y5;
FallIND2_CHS = fall_y6;
FallIND3_CHS = fall_y7;
FallIND4_CHS = fall_y8;
FallIND5_CHS = fall_y9;
FallIND6_CHS = fall_y10;
FallIND7_CHS = fall_y11;

FallIND145_CHS = fall_y18;
run;


/* check the distribution of falls */
proc freq data=fallvar_chs; 
tables FallIND1_CHS FallIND2_CHS FallIND3_CHS FallIND4_CHS FallIND5_CHS FallIND6_CHS FallIND7_CHS FallIND145_CHS;
run;


/* Number of times fallen */
data fallvar_chs; set fallvar_chs;
FallNum1_CHS = timfal_y5;
FallNum2_CHS = timfal_y6;
FallNum3_CHS = timfal_y7;
FallNum4_CHS = timfal_y8;
FallNum5_CHS = timfal_y9;
FallNum6_CHS = timfal_y10;
FallNum7_CHS = timfal_y11;

FallNum145_CHS = timfal_y18;
run;

/* check that it worked */
proc print data=fallvar_chs; 
var 
FallNum1_CHS  timfal_y5
FallNum2_CHS  timfal_y6
FallNum3_CHS  timfal_y7
FallNum4_CHS  timfal_y8
FallNum5_CHS  timfal_y9
FallNum6_CHS  timfal_y10
FallNum7_CHS  timfal_y11

FallNum145_CHS  timfal_y18;
run;

proc freq data=fallvar_chs; 
tables FallNum1_CHS FallNum2_CHS FallNum3_CHS FallNum4_CHS FallNum5_CHS FallNum6_CHS FallNum7_CHS FallNum145_CHS;
run;


/* Number of falls categorized as 1=one time / 2=2 or more times */
data fallvar_chs; set fallvar_chs;

if FallIND1_CHS=0 then FallNumbers1_CHS=0;
else if FallIND1_CHS=1 then do;
if FallNum1_CHS=1 then FallNumbers1_CHS=1;
else if FallNum1_CHS>=2 then FallNumbers1_CHS=2;
else if FallNum1_CHS=. then FallNumbers1_CHS=.;
end;

if FallIND2_CHS=0 then FallNumbers2_CHS=0;
else if FallIND2_CHS=1 then do;
if FallNum2_CHS=1 then FallNumbers2_CHS=1;
else if FallNum2_CHS>=2 then FallNumbers2_CHS=2;
else if FallNum2_CHS=. then FallNumbers2_CHS=.;
end;

if FallIND3_CHS=0 then FallNumbers3_CHS=0;
else if FallIND3_CHS=1 then do;
if FallNum3_CHS=1 then FallNumbers3_CHS=1;
else if FallNum3_CHS>=2 then FallNumbers3_CHS=2;
else if FallNum3_CHS=. then FallNumbers3_CHS=.;
end;

if FallIND4_CHS=0 then FallNumbers4_CHS=0;
else if FallIND4_CHS=1 then do;
if FallNum4_CHS=1 then FallNumbers4_CHS=1;
else if FallNum4_CHS>=2 then FallNumbers4_CHS=2;
else if FallNum4_CHS=. then FallNumbers4_CHS=.;
end;

if FallIND5_CHS=0 then FallNumbers5_CHS=0;
else if FallIND5_CHS=1 then do;
if FallNum5_CHS=1 then FallNumbers5_CHS=1;
else if FallNum5_CHS>=2 then FallNumbers5_CHS=2;
else if FallNum5_CHS=. then FallNumbers5_CHS=.;
end;

if FallIND6_CHS=0 then FallNumbers6_CHS=0;
else if FallIND6_CHS=1 then do;
if FallNum6_CHS=1 then FallNumbers6_CHS=1;
else if FallNum6_CHS>=2 then FallNumbers6_CHS=2;
else if FallNum6_CHS=. then FallNumbers6_CHS=.;
end;

if FallIND7_CHS=0 then FallNumbers7_CHS=0;
else if FallIND7_CHS=1 then do;
if FallNum7_CHS=1 then FallNumbers7_CHS=1;
else if FallNum7_CHS>=2 then FallNumbers7_CHS=2;
else if FallNum7_CHS=. then FallNumbers7_CHS=.;
end;

if FallIND145_CHS=0 then FallNumbers145_CHS=0;
else if FallIND145_CHS=1 then do;
if FallNum145_CHS=1 then FallNumbers145_CHS=1;
else if FallNum145_CHS>=2 then FallNumbers145_CHS=2;
else if FallNum145_CHS=. then FallNumbers145_CHS=.;
end;

run;

proc freq data=fallvar_chs;
table FallNumbers1_CHS FallNumbers2_CHS FallNumbers3_CHS FallNumbers4_CHS FallNumbers5_CHS FallNumbers6_CHS FallNumbers7_CHS FallNumbers145_CHS;
run;


data fallvar_chs; set fallvar_chs;

if FallNumbers1_CHS ne . then RecurrentFalls1_CHS=(FallNumbers1_CHS=2);
if FallNumbers2_CHS ne . then RecurrentFalls2_CHS=(FallNumbers2_CHS=2);
if FallNumbers3_CHS ne . then RecurrentFalls3_CHS=(FallNumbers3_CHS=2);
if FallNumbers4_CHS ne . then RecurrentFalls4_CHS=(FallNumbers4_CHS=2);
if FallNumbers5_CHS ne . then RecurrentFalls5_CHS=(FallNumbers5_CHS=2);
if FallNumbers6_CHS ne . then RecurrentFalls6_CHS=(FallNumbers6_CHS=2);
if FallNumbers7_CHS ne . then RecurrentFalls7_CHS=(FallNumbers7_CHS=2);
if FallNumbers145_CHS ne . then RecurrentFalls145_CHS=(FallNumbers145_CHS=2);

run;


proc print data=fallvar_chs (obs=30); run;

data created.fallvar_chs; set fallvar_chs;

keep
idno FallIND1_CHS FallIND2_CHS FallIND3_CHS FallIND4_CHS FallIND5_CHS FallIND6_CHS FallIND7_CHS FallIND145_CHS
FallNumbers1_CHS FallNumbers2_CHS FallNumbers3_CHS FallNumbers4_CHS FallNumbers5_CHS FallNumbers6_CHS FallNumbers7_CHS FallNumbers145_CHS
RecurrentFalls1_CHS RecurrentFalls2_CHS RecurrentFalls3_CHS RecurrentFalls4_CHS RecurrentFalls5_CHS RecurrentFalls6_CHS RecurrentFalls7_CHS RecurrentFalls145_CHS
;

label
FallIND1_CHS = 'Fall Indicator Harmonized Variable for CHS Y1: Yes(1), No(0)'
FallIND2_CHS = 'Fall Indicator Harmonized Variable for CHS Y2: Yes(1), No(0)'
FallIND3_CHS = 'Fall Indicator Harmonized Variable for CHS Y3: Yes(1), No(0)'
FallIND4_CHS = 'Fall Indicator Harmonized Variable for CHS Y4: Yes(1), No(0)'
FallIND5_CHS = 'Fall Indicator Harmonized Variable for CHS Y5: Yes(1), No(0)'
FallIND6_CHS = 'Fall Indicator Harmonized Variable for CHS Y6: Yes(1), No(0)'
FallIND7_CHS = 'Fall Indicator Harmonized Variable for CHS Y7: Yes(1), No(0)'
FallIND145_CHS = 'Fall Indicator Harmonized Variable for CHS Y14.5: Yes(1), No(0)'

FallNumbers1_CHS = 'Number of Falls Harmonized Variable for CHS Y1: None(0), One time(1), Two or more times(2)'
FallNumbers2_CHS = 'Number of Falls Harmonized Variable for CHS Y2: None(0), One time(1), Two or more times(2)'
FallNumbers3_CHS = 'Number of Falls Harmonized Variable for CHS Y3: None(0), One time(1), Two or more times(2)'
FallNumbers4_CHS = 'Number of Falls Harmonized Variable for CHS Y4: None(0), One time(1), Two or more times(2)'
FallNumbers5_CHS = 'Number of Falls Harmonized Variable for CHS Y5: None(0), One time(1), Two or more times(2)'
FallNumbers6_CHS = 'Number of Falls Harmonized Variable for CHS Y6: None(0), One time(1), Two or more times(2)'
FallNumbers7_CHS = 'Number of Falls Harmonized Variable for CHS Y7: None(0), One time(1), Two or more times(2)'
FallNumbers145_CHS = 'Number of Falls Harmonized Variable for CHS Y14.5: None(0), One time(1), Two or more times(2)'

RecurrentFalls1_CHS = 'Recurrent Falls Harmonized Variable for CHS Y1: None or once(0), Twice or more(1)'
RecurrentFalls2_CHS = 'Recurrent Falls Harmonized Variable for CHS Y2: None or once(0), Twice or more(1)'
RecurrentFalls3_CHS = 'Recurrent Falls Harmonized Variable for CHS Y3: None or once(0), Twice or more(1)'
RecurrentFalls4_CHS = 'Recurrent Falls Harmonized Variable for CHS Y4: None or once(0), Twice or more(1)'
RecurrentFalls5_CHS = 'Recurrent Falls Harmonized Variable for CHS Y5: None or once(0), Twice or more(1)'
RecurrentFalls6_CHS = 'Recurrent Falls Harmonized Variable for CHS Y6: None or once(0), Twice or more(1)'
RecurrentFalls7_CHS = 'Recurrent Falls Harmonized Variable for CHS Y7: None or once(0), Twice or more(1)'
RecurrentFalls145_CHS = 'Recurrent Falls Harmonized Variable for CHS Y145: None or once(0), Twice or more(1)'
;
run;


data created.fallvar_chs_final; set created.fallvar_chs; run;


/*************************/
/* Maximum grip strength */
/*************************/


proc print data=chs (obs=30);
var try1_y5 try1_y6 try1_y7 try1_y8 try1_y9 try1_y10 try1_y11
try2_y5 try2_y6 try2_y7 try2_y8 try2_y9 try2_y10 try2_y11
try21i_y5 try21i_y6 try21i_y7 try21i_y8 try21i_y9
try22i_y5 try22i_y6 try22i_y7 try22i_y8 try22i_y9;
run;

proc freq data=chs;
tables try1_y5 try1_y6 try1_y7 try1_y8 try1_y9 try1_y10 try1_y11
try2_y5 try2_y6 try2_y7 try2_y8 try2_y9 try2_y10 try2_y11
try21i_y5 try21i_y6 try21i_y7 try21i_y8 try21i_y9
try22i_y5 try22i_y6 try22i_y7 try22i_y8 try22i_y9; 
run;

data gsvar_chs; set chs;
MaxGS1_CHS = max(try1_y5, try2_y5, try21i_y5, try22i_y5);
MaxGS2_CHS = max(try1_y6, try2_y6, try21i_y6, try22i_y6);
MaxGS3_CHS = max(try1_y7, try2_y7, try21i_y7, try22i_y7);
MaxGS4_CHS = max(try1_y8, try2_y8, try21i_y8, try22i_y8);
MaxGS5_CHS = max(try1_y9, try2_y9, try21i_y9, try22i_y9);
MaxGS6_CHS = max(try1_y10, try2_y10);
MaxGS7_CHS = max(try1_y11, try2_y11);
run;


data created.gsvar_chs; set gsvar_chs;

keep 
idno MaxGS1_CHS MaxGS2_CHS MaxGS3_CHS MaxGS4_CHS MaxGS5_CHS MaxGS6_CHS MaxGS7_CHS;

label 
MaxGS1_CHS = 'Maximum Grip Strength (kg) Harmonized Variable for CHS Y1'
MaxGS2_CHS = 'Maximum Grip Strength (kg) Harmonized Variable for CHS Y2'
MaxGS3_CHS = 'Maximum Grip Strength (kg) Harmonized Variable for CHS Y3'
MaxGS4_CHS = 'Maximum Grip Strength (kg) Harmonized Variable for CHS Y4'
MaxGS5_CHS = 'Maximum Grip Strength (kg) Harmonized Variable for CHS Y5'
MaxGS6_CHS = 'Maximum Grip Strength (kg) Harmonized Variable for CHS Y6'
MaxGS7_CHS = 'Maximum Grip Strength (kg) Harmonized Variable for CHS Y7';

run;


/**********************/
/* Metabolic syndrome */
/**********************/


*waist > 88 cm for women and > 102 cm for men;
*year 5, 9, 11, 18;
proc means data=chs; var waist_y5; run;

data chs1; set chs;
if gend01=0 then do;
if waist_y5 ne . then mets_wc1=(waist_y5>88);
if waist_y9 ne . then mets_wc5=(waist_y9>88);
if waist_y11 ne . then mets_wc7=(waist_y11>88);
if waist_y18 ne . then mets_wc14=(waist_y18>88);
end;

else if gend01=1 then do;
if waist_y5 ne . then mets_wc1=(waist_y5>102);
if waist_y9 ne . then mets_wc5=(waist_y9>102);
if waist_y11 ne . then mets_wc7=(waist_y11>102);
if waist_y18 ne . then mets_wc14=(waist_y18>102);
end;
run;

proc freq data=chs1; table gend01*(mets_wc1 mets_wc5 mets_wc7 mets_wc14); run;


*triglycerides >= 150 mg/dL;
*year 5, 18;
proc means data=chs; var trig_y5 trig_y18; run;

data chs2; set chs1;
if trig_y5 ne . then mets_tri1=(trig_y5>=150);
if trig_y18 ne . then mets_tri14=(trig_y18>=150);
run;

proc freq data=chs2; table mets_tri1 mets_tri14; run;


*hdl <40 mg/dL for men and < 50 mg/dL for women;
*year 5, 18;
proc means data=chs; var hdl_y5 hdl_y18; run;

data chs3; set chs2;
if gend01=0 then do;
if hdl_y5 ne . then mets_hdl1=(hdl_y5<50);
if hdl_y18 ne . then mets_hdl14=(hdl_y18<50);
end;

else if gend01=1 then do;
if hdl_y5 ne . then mets_hdl1=(hdl_y5<40);
if hdl_y18 ne . then mets_hdl14=(hdl_y18<40);
end;
run;

proc freq data=chs3; table mets_hdl1 mets_hdl14; run;


*bp sys >= 130 or dia >= 85;
*year 5, 6, 7, 9, 10, 11, 18;
proc means data=chs; var avesys_y5 avesys_y6 avesys_y7 avesys_y9 avesys_y10 avesys_y11 avesys_y18 
avedia_y5 avedia_y6 avedia_y7 avedia_y9 avedia_y10 avedia_y11 avedia_y18; run;

data chs4; set chs3;
if (avesys_y5 ne . and avesys_y5>=130) or (avedia_y5 ne . and avedia_y5>=85) then mets_bp1=1; else mets_bp1=0;
if (avesys_y6 ne . and avesys_y6>=130) or (avedia_y6 ne . and avedia_y6>=85) then mets_bp2=1; else mets_bp2=0;
if (avesys_y7 ne . and avesys_y7>=130) or (avedia_y7 ne . and avedia_y7>=85) then mets_bp3=1; else mets_bp3=0;
if (avesys_y9 ne . and avesys_y9>=130) or (avedia_y9 ne . and avedia_y9>=85) then mets_bp5=1; else mets_bp5=0;
if (avesys_y10 ne . and avesys_y10>=130) or (avedia_y10 ne . and avedia_y10>=85) then mets_bp6=1; else mets_bp6=0;
if (avesys_y11 ne . and avesys_y11>=130) or (avedia_y11 ne . and avedia_y11>=85) then mets_bp7=1; else mets_bp7=0;
if (avesys_y18 ne . and avesys_y18>=130) or (avedia_y18 ne . and avedia_y18>=85) then mets_bp14=1; else mets_bp18=0;
run;

proc freq data=chs4; table mets_bp1 mets_bp2 mets_bp3 mets_bp5 mets_bp6 mets_bp7 mets_bp14; run;


*gluc >= 110 md/dL;
*year 5, 7, 9, 11, 18;
proc means data=chs; var gluadj_y5 gluadj_y7 gluadj_y9 gluadj_y11 gluadj_y18; run;

data chs5; set chs4;
if gluadj_y5 ne . then mets_glu1=(gluadj_y5>=110);
if gluadj_y7 ne . then mets_glu3=(gluadj_y7>=110);
if gluadj_y9 ne . then mets_glu5=(gluadj_y9>=110);
if gluadj_y11 ne . then mets_glu7=(gluadj_y11>=110);
if gluadj_y18 ne . then mets_glu14=(gluadj_y18>=110);
run;

proc freq data=chs5; table mets_glu1 mets_glu3 mets_glu5 mets_glu7 mets_glu14; run;


*final mets dataset;
data created.mets_chs; set chs5; 
keep idno mets_wc1 mets_wc5 mets_wc7 mets_wc14 mets_tri1 mets_tri14 mets_hdl1 mets_hdl14 mets_bp1 mets_bp2 mets_bp3 mets_bp5 mets_bp6 mets_bp7 mets_bp14 mets_glu1 mets_glu3 mets_glu5 mets_glu7 mets_glu14;
run;


/***************/
/* Demographic */
/***************/

************************* RACE SEX ED ****************************************************************;
Data racesexedvar;
set CHS;

*Race_H: Race/Ethnicity - CHS (1=White, 2=Black, 3=AI/AN, 4=Asian/Pacific Islander, 5=Other);
Race_CHS=race01;
label Race_CHS= 'Race Harmonized Variable for CHS: White (1), Black(2), Asian, American Indian or Alaskan Native(3), Native Hawaiian or other Pacific Islander(4),
Other (5)';
Race_H= Race_CHS; 
label Race_H= 'Race Harmonized Variable White (1), Black(2), Asian, American Indian or Alaskan Native(3), Native Hawaiian or other Pacific Islander(4),
Other (5)';

*Gender_H (1=Male, 0=Female);
If GEND01 =. then Gender_CHS=.;
else if GEND01=1 then Gender_CHS=1;
else if GEND01=0 then Gender_CHS=0;
label Gender_CHS= 'Gender Harmonized Variable for CHS Male(1), Female(0)';

Gender_H=Gender_CHS;
label Gender_H= 'Gender Harmonized Variable: Male(1), Female(0)';

*EDUCA_H (Definition: 1=less than HS, 2=HS grad, 3=postsecondary);
if GRADE01 =. then EDUCA_CHS=.;
else if GRADE01 in (0,1,2,3,4,5,6,7,8,9,10,11) then EDUCA_CHS=1;
else if GRADE01 in (12,13,14,15,16) then EDUCA_CHS=2;
else if GRADE01 in (17,18,19,20,21) then EDUCA_CHS=3;
label EDUCA_CHS= 'Education Harmonized Variable for CHS: < than HS(1), HS grad(2), postsecondary(3)';

EDUCA_H=EDUCA_CHS;
label EDUCA_H='Education Harmonized Variable: < than HS(1), HS grad(2), postsecondary(3)';
run;

proc contents data=racesexedvar;run;

/*save final age,sex,race dataset*/
data outCHS.racesexedvar_CHS;
set racesexedvar;
keep IDNO Race_CHS Race_H Gender_CHS Gender_H EDUCA_CHS EDUCA_H;
run;

******************************   AGE   ******************************************************************;

	/*Need age at years 5-11, 18*/
data agevarCHS;
set CHS;
age1_CHS= agea_y5;
age2_CHS= agea_y6;
age3_CHS= agea_y7;
age4_CHS= agea_y8;
age5_CHS= agea_y9;
age6_CHS= agea_y10;
age7_CHS= agea_y11;
age14_CHS= agea_y18;
run;

/*check it worked*/
proc print data=agevarCHS;
var age1_CHS  agea_y5
age2_CHS  agea_y6
age3_CHS  agea_y7
age4_CHS  agea_y8
age5_CHS  agea_y9
age6_CHS  agea_y10
age7_CHS  agea_y11
age14_CHS  agea_y18;
run;

proc print data=agevarCHS; var age1_CHS age2_CHS age3_CHS age4_CHS age5_CHS age6_CHS age7_CHS age14_CHS;
run;

/*now, I need to make copies of all the variables so that I can create versions that are imputated*/
data agevarCHS_interpolated;
set agevarCHS;
ageINT1_CHS= age1_CHS;
ageINT2_CHS= age2_CHS;
ageINT3_CHS= age3_CHS;
ageINT4_CHS= age4_CHS;
ageINT5_CHS= age5_CHS;
ageINT6_CHS= age6_CHS;
ageINT7_CHS= age7_CHS;
ageINT8_CHS= age7_CHS+1;
ageINT9_CHS= age7_CHS+2;
ageINT10_CHS= age7_CHS+3;
ageINT11_CHS= age7_CHS+4;
ageINT12_CHS= age7_CHS+5;
ageINT13_CHS= age7_CHS+6;
ageINT14_CHS= age14_CHS;
run;

proc print data=agevarCHS_interpolated; var ageINT1_CHS ageINT2_CHS ageINT3_CHS ageINT4_CHS ageINT5_CHS ageINT6_CHS ageINT7_CHS ageINT14_CHS;
run;

/*I need to interpolate any missing data- ie. if there are any non-missing values in the ageX_CHS variables (all with the same suffix), 
then it will take the data point right before the missing and add one (unless its missing) to the continuous variable*/

data agevar_interpCHS;
    set agevarCHS_interpolated; 

    /* Define the variables to check */
    array vars ageINT1_CHS ageINT2_CHS ageINT3_CHS ageINT4_CHS ageINT5_CHS ageINT6_CHS ageINT7_CHS ageINT8_CHS ageINT9_CHS ageINT10_CHS ageINT11_CHS ageINT12_CHS ageINT13_CHS ageINT14_CHS;

/* If the first variable is missing, find the next non-missing value and subtract 1 */
 if missing(vars[1]) then do;
        do j = 2 to dim(vars);
            if not missing(vars[j]) then do;
                vars[1] = vars[j] - (j - 1); /* Subtract the correct amount */
                leave;
            end;
        end;
    end;

do i = 1 to dim(vars); /* Loop through all variables to impute missing values*/
        if missing(vars[i]) then do;
            /* Check if there's a non-missing value later */
            found_nonmissing = 0; /* Flag to indicate a non-missing value is found */
            do j = i + 1 to dim(vars);
                if not missing(vars[j]) then do;
                    found_nonmissing = 1; /* Set flag */
                    leave; /* Exit the loop once a non-missing value is found */
                end;
            end;

            /* Impute only if a non-missing value is found later */
            if found_nonmissing then do;
                do k = i - 1 to 1 by -1; /* Look backward for the last non-missing value */
                    if not missing(vars[k]) then do;
                        vars[i] = vars[k] + 1; /* Assign the previous value plus 1 */
                        leave; /* Exit the loop once a value is assigned */
                    end;
                end;
            end;
        end;
    end;

    drop i j k found_nonmissing; /* Drop loop and flag variables */
run;


proc print data= agevar_interpCHS;
var ageINT1_CHS ageINT2_CHS ageINT3_CHS ageINT4_CHS ageINT5_CHS ageINT6_CHS ageINT7_CHS ageINT8_CHS ageINT9_CHS ageINT10_CHS ageINT11_CHS ageINT12_CHS ageINT13_CHS ageINT14_CHS; run;

proc print data=agevar_interpCHS; where ageINT1_CHS <50;run;
/*creating a flag variable to see if there are any situations where there are more than 2 years between ages in consecutive years. This would indicate a problem*/
data agevar_flagCHS;
set agevar_interpCHS;
array vars ageINT1_CHS ageINT2_CHS ageINT3_CHS ageINT4_CHS ageINT5_CHS ageINT6_CHS ageINT7_CHS;


    ageflag_CHS = 0;/* Initialize the flag variable */
    		 /* Loop through the variables in the array */
    do i = 1 to dim(vars)-1; /* Loop from first to second-to-last variable */
        /* Check for non-missing values and calculate the difference */
        if not missing(vars[i]) and not missing(vars[i+1]) then do;
            if abs(vars[i+1] - vars[i]) > 2 then ageflag_CHS = 1; /* Set flag if difference > 2 */
        end;
    end;

    drop i; /* Drop the loop counter variable for cleaner output */
run;


proc freq data=agevar_flagCHS;
tables ageflag_CHS;
run;
/*no flagged vars*/

/*creating a flag variable to see if there are any situations where there are not between 5 and 9 years between ages between years 7 and 14. This would indicate a problem*/
data agevar_flag2CHS;
set agevar_flagCHS;
array vars ageINT7_CHS ageINT14_CHS;


    ageflag_CHS = 0;/* Initialize the flag variable */
    		 /* Loop through the variables in the array */
    do i = 1 to dim(vars)-1; /* Loop from first to second-to-last variable */
        /* Check for non-missing values and calculate the difference */
        if not missing(vars[i]) and not missing(vars[i+1]) then do;
            if 5 > abs(vars[i+1] - vars[i]) > 9 then ageflag_CHS = 1; 
        end;
    end;

    drop i; /* Drop the loop counter variable for cleaner output */
run;


proc freq data=agevar_flag2CHS;
tables ageflag_CHS;
run;
/*no flagged variables*/

data agevar_CHS;
set agevar_flagCHS;
label age1_CHS= 'Age(years) Harmonized Variable for CHS Y1'
age2_CHS= 'Age(years) Harmonized Variable for CHS Y2'
age3_CHS= 'Age(years) Harmonized Variable for CHS Y3'
age4_CHS= 'Age(years) Harmonized Variable for CHS Y4'
age5_CHS= 'Age(years) Harmonized Variable for CHS Y5'
age6_CHS= 'Age(years) Harmonized Variable for CHS Y6'
age7_CHS= 'Age(years) Harmonized Variable for CHS Y7'
age14_CHS= 'Age(years) Harmonized Variable for CHS Y14'
ageINT1_CHS= 'Age(years) Harmonized Variable for CHS Y1- Interpolated'
ageINT2_CHS= 'Age(years) Harmonized Variable for CHS Y2- Interpolated'
ageINT3_CHS= 'Age(years) Harmonized Variable for CHS Y3- Interpolated'
ageINT4_CHS= 'Age(years) Harmonized Variable for CHS Y4- Interpolated'
ageINT5_CHS= 'Age(years) Harmonized Variable for CHS Y5- Interpolated'
ageINT6_CHS= 'Age(years) Harmonized Variable for CHS Y6- Interpolated'
ageINT7_CHS= 'Age(years) Harmonized Variable for CHS Y7- Interpolated'
ageINT8_CHS= 'Age(years) Harmonized Variable for CHS Y8- Interpolated'
ageINT9_CHS= 'Age(years) Harmonized Variable for CHS Y9- Interpolated'
ageINT10_CHS= 'Age(years) Harmonized Variable for CHS Y10- Interpolated'
ageINT11_CHS= 'Age(years) Harmonized Variable for CHS Y11- Interpolated'
ageINT12_CHS= 'Age(years) Harmonized Variable for CHS Y12- Interpolated'
ageINT13_CHS= 'Age(years) Harmonized Variable for CHS Y13- Interpolated'
ageINT14_CHS= 'Age(years) Harmonized Variable for CHS Y14- Interpolated'
ageflag_CHS= 'Flag (1) where >2 years between ages in consecutive years and 5>X>9 between years 7 and 14';
run;
/*no need to bring any flags into the dataset bc nothing was flagged*/
proc contents data=agevar_CHS;run;
/*it worked*/

/*now,put the IDNOand only the variables I need in this dataset*/
data outCHS.agevar_CHS;
set agevar_CHS;
keep idno ageINT1_CHS ageINT2_CHS ageINT3_CHS ageINT4_CHS ageINT5_CHS ageINT6_CHS ageINT7_CHS ageINT8_CHS ageINT9_CHS
ageINT10_CHS ageINT11_CHS ageINT12_CHS ageINT13_CHS ageINT14_CHS
age1_CHS age2_CHS age3_CHS age4_CHS age5_CHS age6_CHS age7_CHS age14_CHS ageflag_CHS;
run;

proc print data=outCHS.agevar_CHS;run;

*********************************************************************************************;
/*Height: Convert CHS cm to mm...CHS = stht_yX*10*/
/*has height at CHS years 5, 9, 18 (and NFFI years 1,5,14)*/
*********************************************************************************************;
data heightCHS;
set CHS;
height1_CHS= (stht_y5*10);
height5_CHS= (stht_y9*10);
height14_CHS= (stht_y18*10);
run;

proc means data=heightCHS n nmiss; var height1_CHS height5_CHS height14_CHS;run;
/*Variable:	N (N Miss)
height1_CHS: 4846(688)
height5_CHS: 3488(2046)
height14_CHS: 1096(4438)*/

/*create an Interpolated variable for next step by making copies, making the variables without values missing so we can interpolate them as well.*/
data heightCHS;
set heightCHS;
heightINT1_CHS= height1_CHS;
heightINT2_CHS= .;
heightINT3_CHS= .;
heightINT4_CHS= .;
heightINT5_CHS= height5_CHS;
heightINT6_CHS= .;
heightINT7_CHS= .;
heightINT8_CHS= .;
heightINT9_CHS= .;
heightINT10_CHS= .;
heightINT11_CHS= .;
heightINT12_CHS= .;
heightINT13_CHS= .;
heightINT14_CHS= height14_CHS;
run;


/* Perform Linear Interpolation for missing heights*/


/* Reshape from Wide to Long Format */
proc transpose data=heightCHS out=long_data(rename=(col1=Height)) name=YEARVar;
    by idno;
    var heightINT1_CHS heightINT2_CHS heightINT3_CHS heightINT4_CHS heightINT5_CHS heightINT6_CHS heightINT7_CHS heightINT8_CHS heightINT9_CHS 
heightINT10_CHS heightINT11_CHS heightINT12_CHS heightINT13_CHS heightINT14_CHS;
run;

/*  Extract YEAR from Variable Names */
data long_data;
    set long_data;
	if YEARVar = 'heightINT1_CHS' then YEAR = 1;
	else if YEARVar = 'heightINT2_CHS' then YEAR = 2;
	else if YEARVar = 'heightINT3_CHS' then YEAR = 3;
	else if YEARVar = 'heightINT4_CHS' then YEAR = 4;
	else if YEARVar = 'heightINT5_CHS' then YEAR = 5;
	else if YEARVar = 'heightINT6_CHS' then YEAR = 6;
	else if YEARVar = 'heightINT7_CHS' then YEAR = 7;
	else if YEARVar = 'heightINT8_CHS' then YEAR = 8;
	else if YEARVar = 'heightINT9_CHS' then YEAR = 9;
	else if YEARVar = 'heightINT10_CHS' then YEAR = 10;
	else if YEARVar = 'heightINT11_CHS' then YEAR = 11;
	else if YEARVar = 'heightINT12_CHS' then YEAR = 12;
	else if YEARVar = 'heightINT13_CHS' then YEAR = 13;
    else if YEARVar = 'heightINT14_CHS' then YEAR = 14;
     *drop YEARVar;
run;

/* Sort Data by IDNOand YEAR */
proc sort data=long_data noduprecs;
    by idno YEAR;
run;

/*Create Prev_YEAR and Prev_Height */
/*Find the most recent past value */
data heightdata;
    set long_data;
    by idno YEAR;
    
    retain Prev_YEAR Prev_Height; /* Retain values across iterations */

       /* First observation per ID: Set Prev_YEAR and Prev_Height to missing */
    if first.idno then do;
        Prev_YEAR = .;
        Prev_Height = .;
    end; 

    if not missing(Height) then do;
        Prev_YEAR = YEAR;
        Prev_Height = Height;
    end;
        
run;
proc print data=heightdata; var Prev_year year prev_height height;run;


/* Find the closest future value */
proc sort data=heightdata;
    by idno descending YEAR;
run;

data heightdata2;
    set heightdata;
    by idno descending YEAR;
    
    retain Next_YEAR Next_Height; /* Retain values across iterations */
    
  /* Last observation per ID: Set Next_YEAR and Next_Height to missing */
    if first.idno then do;
        Next_YEAR = .;
        Next_Height = .;
    end;

    if not missing(Height) then do;
        Next_YEAR = YEAR;
        Next_Height = Height;
    end;
   
run;

proc print data=heightdata2; var next_year year next_height height;run;


/*Restore chronological order */
proc sort data=heightdata2;
    by idno YEAR;
run;

/*Apply interpolation based on the closest past and future values */
data heightdata2;
    set heightdata2;

    /* Only interpolate if both previous and next values exist */
    if missing(Height) and not missing(Prev_Height) and not missing(Next_Height) then do;
        Height = Prev_Height + ((Next_Height - Prev_Height) / (Next_YEAR - Prev_YEAR)) * (YEAR - Prev_YEAR);
    end;

    /* Debugging Output */
    put "id=" idno " YEAR=" YEAR " Height=" Height
        " Prev_YEAR=" Prev_YEAR " Prev_Height=" Prev_Height
        " Next_YEAR=" Next_YEAR " Next_Height=" Next_Height;

run;

proc means data=heightdata2 n nmiss; var height;run;
/*n=21854, MISSING N=22418*/

/*Reshape Data Back to Wide Format */
proc transpose data=heightdata2 out=final_data(drop=_NAME_) prefix=height;
    by idno; 
    ID YEAR;
    var Height;
run;

/*check for duplicate by variable issues*/
proc sql;
select idno, YEAR, count(*)
from heightdata2
group by idno, YEAR
having count(*) > 1;
quit;
/*none*/

**********************************************************************************************************************************;

/* Create a flag variable for each consecutive year comparison */
/* Flag if height increases by >5mm/year or decreases by >10mm/year */
data heightdata3; 
    set final_data; 

    /* Initialize flag to 0 */
    Flag = 0;

    /* Define an array for all height variables */
    array heights height1 height2 height3 height4 height5 height6 height7 heightINT14 ;
    array years (8) (1,2,3,4,5,6,7,14); /* Corresponding years */
    array flags flag1-flag7; /* One less flag variable than height variables */
    array difference difference1-difference7; /* Store yearly differences */

    /* Loop through available years and calculate yearly differences */
    do i = 1 to dim(heights)-1;
        if not missing(heights[i]) and not missing(heights[i+1]) then do;
            /* Calculate the actual year difference */
            year_diff = years[i+1] - years[i]; /* Dynamically determine year gap */

            /* Calculate height difference per year */
            rate_of_change = (heights[i+1] - heights[i]) / year_diff;
            difference[i] = heights[i+1] - heights[i];

            /* Flag if loss is greater than 10mm/year or gain is greater than 5mm/year */
            if rate_of_change < -10 OR rate_of_change > 5 then flags[i] = 1;
            else flags[i] = 0;
        end;
        else do;
            flags[i] = .; /* Missing if height values are missing */
            difference[i] = .;
        end;
    end;

    drop i year_diff rate_of_change;
run;

/*combine to one flag*/
data heightdata3_flag;
set heightdata3;
if flag1=1 OR flag2=1 or flag3=1 OR flag4=1 or flag5=1 OR flag6=1 or flag7=1 then heightflag_CHS=1;
else if flag1=. AND flag2=. AND flag3=. AND flag4=. AND flag5=. AND flag6=. AND flag7=. then heightflag_CHS=.;
else heightflag_CHS=0;
drop flag1 flag2 flag3 flag4 flag5 flag6 flag7;
run;

proc freq data=heightdata3_flag; tables heightflag_CHS;run;
/*this flagged n=172 (out of 3407-- 5%)*/

proc print data= heightdata3_flag;
where heightflag_CHS=1;
run;

/*at 7mm increase, there are 294 (5%), at 10 mm, there are 187 (3%)and 15 mm there are 140 (2.5%)*/
proc freq data=heightdata3;table flag1 flag2  /*difference_var2*/;run;
proc univariate data=heightdata3;var difference_var1 /*difference_var2*/;run;
/*figure out what differences are a problem. then recode above.*/



/* View flagged observations */
proc print data=heightdata3_flag;
    where heightflag_CHS = 1;
	var height1 height5 height14;
    title "Observations with Consecutive Yearly Increase more than 5mm or Decrease more than 10mm";
run;




/*Rename Variables to Match Original Format and then label harmonized variable */
data final_data;
    set heightdata3_flag;
    rename height1= heightINT1_CHS height2= heightINT2_CHS height3= heightINT3_CHS height4= heightINT4_CHS height5= heightINT5_CHS height6= heightINT6_CHS height7= heightINT7_CHS 
height8= heightINT8_CHS height9= heightINT9_CHS  height10= heightINT10_CHS  height11= heightINT11_CHS  height12= heightINT12_CHS  height13= heightINT13_CHS 
height14=heightINT14_CHS ;
	run;

/*Merge to same dataset: interpolated and non-interpolated versions of height*/
proc sql;
create table combined as 
select a.*, b.height1_CHS, b.height5_CHS, b.height14_CHS
from final_data as a LEFT JOIN heightCHS as b
on a.idno = b.idno;
quit;

/*Merge heightflag into dataset*/
proc sql;
create table combined2 as 
select a.*, b.heightflag_CHS
from combined as a LEFT JOIN heightdata3_flag as b
on a.idno = b.idno;
quit;


data final_data;
set combined2;
label height1_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y1'
		height5_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y5'
		height14_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y14'
		heightINT1_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y1- Interpolated'
		heightINT2_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y2- Interpolated'
		heightINT3_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y3- Interpolated'
		heightINT4_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y4- Interpolated'
		heightINT5_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y5- Interpolated'
		heightINT6_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y6- Interpolated'
		heightINT7_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y7- Interpolated'
		heightINT8_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y8- Interpolated'
		heightINT9_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y9- Interpolated'
		heightINT10_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y10- Interpolated'
		heightINT11_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y11- Interpolated'
		heightINT12_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y12- Interpolated'
		heightINT13_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y13- Interpolated'
		heightINT14_CHS= 'Average Standing Height (mm) Harmonized Variable for CHS Y14- Interpolated'
		heightflag_CHS= 'CHS Flag (1) if height increases by >5mm/year or decreases by >10mm/year ';
run;

/*Print Final Dataset */
proc print data=final_data noobs;
    title "Final Dataset with Interpolated Heights";
run;

/*save to dataset*/
data outCHS.heightvar_CHS;
set final_data;
keep idno height1_CHS height5_CHS	height14_CHS heightINT1_CHS heightINT2_CHS	heightINT3_CHS	heightINT4_CHS
		heightINT5_CHS	heightINT6_CHS	heightINT7_CHS	heightINT8_CHS heightINT9_CHS 
heightINT10_CHS heightINT11_CHS heightINT12_CHS heightINT13_CHS heightINT14_CHS heightflag_CHS;
run;


proc contents data= outCHS.heightvar_CHS;run;


/*********************/
/* physical activity */
/*********************/


/*We need everything in the same units, but MrOS's PA is based on a scale with propietary weights so the best way to harmonize is to 
Standardize each study individually and then harmonize:
The reason I am standardizing each separately rather than combining the 3 first and standardizing it is to prevent the larger dataset 
(if we'd already combined the 3 into 1) or those with higher variance (possibly the MrOS dataset) to disproportionately influence the overall 
scaling factors- biasing the standardization. 
 

Should ask Elsa if we need to Harmonize the other three studies into a harmonized variable too.
.*/

/*set up dataset and check mean/sd*/
data PhysActivityVar;
set CHS;
PhysActivity1_CHS = kcal_y5;
PhysActivity5_CHS = kcal_y9;
Keep idno PhysActivity1_CHS  PhysActivity5_CHS  ;
run;


/*look at distributions*/
proc univariate data=PhysActivityVar; var PhysActivity1_CHS  PhysActivity5_CHS; 
histogram PhysActivity1_CHS  PhysActivity5_CHS;run;
/*strongly skewed right*/

/*The distribution is strongly skewed right. Take the logarithm.*/
data PhysActivityVar;
    set PhysActivityVar;
    PhysActivity1_CHS_L = log(PhysActivity1_CHS);
	PhysActivity5_CHS_L = log(PhysActivity5_CHS);
	keep idno PhysActivity1_CHS_L  PhysActivity5_CHS_L ;
run;


proc univariate data=PhysActivityVar; var PhysActivity1_CHS_L  PhysActivity5_CHS_L ; histogram PhysActivity1_CHS_L  PhysActivity5_CHS_L ;run;

/*It seems like keeping the method consistent across studies is more important than making sure that each distribution is approx. Normal
before standardizing/combining so I will do LOGARITHM for each study*/

/*try the sqrt method*/
*data PhysActivityVar;
 *   set PhysActivityVar;
  *  PhysActivity1_CHS_L = sqrt(PhysActivity1_CHS);
*	PhysActivity5_CHS_L = sqrt(PhysActivity5_CHS);
*	keep idno PhysActivity1_CHS_L  PhysActivity5_CHS_L ;
*run;

proc univariate data=PhysActivityVar; var PhysActivity1_CHS_L  PhysActivity5_CHS_L ; histogram PhysActivity1_CHS_L  PhysActivity5_CHS_L ;run;

/*rename the variable and then standardize it */
data PhysActivityVar;
set PhysActivityVar;
	rename PhysActivity1_CHS_L=PhysActivity1_CHS PhysActivity5_CHS_L=PhysActivity5_CHS;
run;

PROC STANDARD DATA=PhysActivityVar MEAN=0 STD=1 OUT=PhysActivityVar_std;
  VAR PhysActivity1_CHS PhysActivity5_CHS;
  run;

  proc univariate data= PhysActivityVar_std;
  where PhysActivity6_CHS > 2.5 OR PhysActivity6_CHS <=- 2.5;
  run;
/*n=38 obs outside of 2.5 SDs,  (there were 82 with log)
  10 obs outside of 3 SD,
  highest/lowest is 3.4 and -3.06*/

/*CARRY FORWARD PHYS ACTIVITY VARS. SO NFFI years 1-4 will be the same and years 5-7 will be the same.*/
data PhysActivityVar_std2;
    set PhysActivityVar_std;
PhysActivity1_CHS= PhysActivity1_CHS;
PhysActivity2_CHS= PhysActivity1_CHS;
PhysActivity3_CHS= PhysActivity1_CHS;
PhysActivity4_CHS= PhysActivity1_CHS;
PhysActivity6_CHS= PhysActivity5_CHS;
PhysActivity7_CHS= PhysActivity5_CHS;
PhysActivity8_CHS= PhysActivity5_CHS;
PhysActivity9_CHS= PhysActivity5_CHS;
PhysActivity10_CHS= PhysActivity5_CHS;
PhysActivity11_CHS= PhysActivity5_CHS;
PhysActivity12_CHS= PhysActivity5_CHS;
PhysActivity13_CHS= PhysActivity5_CHS;
PhysActivity14_CHS= PhysActivity5_CHS;
    label PhysActivity1_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year1- log transformed and standardized'
	PhysActivity5_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year5- log transformed and standardized'
	PhysActivity2_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year2- log transformed and standardized- CARRIED forward FROM NFFI Y1'
PhysActivity3_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year3- log transformed and standardized- CARRIED forward FROM NFFI Y1'
PhysActivity4_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year4- log transformed and standardized- CARRIED forward FROM NFFI Y1'
PhysActivity6_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year6- log transformed and standardized- CARRIED forward FROM NFFI Y5'
PhysActivity7_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year7- log transformed and standardized- CARRIED forward FROM NFFI Y5'
PhysActivity8_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year8- log transformed and standardized- CARRIED forward FROM NFFI Y5'
PhysActivity9_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year9- log transformed and standardized- CARRIED forward FROM NFFI Y5'
PhysActivity10_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year10- log transformed and standardized- CARRIED forward FROM NFFI Y5'
PhysActivity11_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year11- log transformed and standardized- CARRIED forward FROM NFFI Y5'
PhysActivity12_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year12- log transformed and standardized- CARRIED forward FROM NFFI Y5'
PhysActivity13_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year13- log transformed and standardized- CARRIED forward FROM NFFI Y5'
PhysActivity14_CHS= 'Physical Activity for CHS(kcal/kg/week), NFFI year14- log transformed and standardized- CARRIED forward FROM NFFI Y5';
	run;
proc univariate data=PhysActivityVar_std2; var PhysActivity1_CHS PhysActivity5_CHS; 
histogram PhysActivity1_CHS PhysActivity5_CHS;
run;

/*save final PA dataset*/
data outCHS.PhysActivityvar_CHS;
set PhysActivityVar_std2;
run;



/****************/
/* Vital status */
/****************/

**************************************************************************************************************************
												Die Before 90 (Y/N)
**************************************************************************************************************************;

/*get variables i need to calculate age at death.*/
data diebefore90;
set chs;
keep idno ttodth_y5;
run;


/*merge ageINT1_CHS from outCHS.agevar_CHS*/
proc sql;
create table merged_agettd as
select a.*,b.ageINT1_CHS
from diebefore90 as a LEFT JOIN outCHS.agevar_CHS as b
on a.idno=b.idno;
quit;


/*Now, find age at death, but need to convert time to death in years before doing this */
data age_at_death;
    set merged_agettd;
   if not missing(ttodth_y5) then do;
        ageatdeath_CHS = ageINT1_CHS + (ttodth_y5 / 365.25);
    end;
run;

proc print data= age_at_death; var ageINT1_CHS ttodth_y5 ageatdeath_CHS;run;

/*code the variable*/
data die_after_90;
set age_at_death;
if 1 <ageatdeath_CHS<90 then dieafter90_CHS=0;
else if ageatdeath_CHS >=90 then dieafter90_CHS=1;
else if ageatdeath_CHS= . then dieafter90_CHS= .;
run;

proc freq data=die_after_90; table dieafter90_CHS;run;
/*1974 died after 90*/

/*save variable*/
data outCHS.Dieafter90var_CHS;
set die_after_90;
keep IDno dieafter90_CHS ageatdeath_CHS;
run;

proc print data=outCHS.Dieafter90var_CHS;run;


**************************************************************************************************************************
												Creating vitality variables
**************************************************************************************************************************;
/*THe variable will be defined as: 
VITALX_HABC=
1 if alive at year k 
0 if died before year k 
. if missing/unknown

The Lost to followup variable will be:
STATUS= 0:died, 1:alive through study end, 2:lost to follow up/censored*/

/*What we have: a death indicator variable from Baseline (Y5) (died before study end (1), alive (0))--will need 
to flip those values  AND a time to death from Y5 Baseline 

variables i need for each year (k) are:
If they died before or during year k, then Vitalk = 0.
If they are alive through year k, then Vitalk = 1.
If missing, then Vitalk (lost to followup?) = .*/

proc freq data=chs;table death;run;
proc contents data=chs;run;
proc print data=chs; var death ttodth_y5 stdytimea_y5imp stdytimea_y6-stdytimea_y18;run;

data harmonized_simple ;
   set CHS (rename= (stdytimea_y5imp=stdytimea_y5));

  /* --- Step 1: Redefine death within study --- */
  array st[5:18] stdytimea_y5-stdytimea_y18;

  if not missing(st[18]) then death14 = 0;      /* Seen alive at Y18 */
  else if not missing(ttodth_y5) and ttodth_y5 < 5117 then death14 = 1;  /* Died within 14 yrs */
  else death14 = 0;                             /* Otherwise treat as alive */

  /* --- Step 2: Last observed contact --- */
  last_study_time = .;
  do k = 5 to 18;
    if not missing(st[k]) then last_study_time = st[k];
  end;

  /* --- Step 3: Compute death day (if applicable) --- */
  if death14=1 then death_day = stdytimea_y5 + ttodth_y5;
  else death_day = .;

  /* --- Step 4: Flag LTFU --- */
  ltfu = 0;
  if death14=0 and missing(st[18]) then ltfu = 1;         /* No death + never made Y18 ? LTFU */
  if death14=1 and not missing(death_day) then do;
     if death_day > last_study_time + 365 then ltfu = 1;  /* death too late after last visit */
  end;

  drop k;

  /*make overall status var*/
  if death14=1 and ltfu=0 then status_CHS = 0;  
  else if death14=1 and ltfu=1 then status_CHS = .;  /* died */
  else if death14=0 and ltfu=0 then status_CHS = 1; /* alive through study end */
  else if death14=0 and ltfu=1 then status_CHS = .;        /* censored / LTFU */

run;

/*creating the vital variables*/
data harmonized_vital;
  set harmonized_simple;

  array st[5:18] stdytimea_y5 - stdytimea_y18;
  array vital[1:14] Vital1 - Vital14;

  do kvis = 5 to 18;
    idx = kvis - 4;  /* map Y5..Y18 ? Vital1..Vital14 */

    if not missing(st[kvis]) then vital[idx] = 1;  /* visit observed ? alive */
    else do;
      /* Check if any later visits are non-missing */
      later_nonmissing = 0;
      do kk = kvis+1 to 18;
        if not missing(st[kk]) then later_nonmissing = 1;
      end;

      if later_nonmissing = 1 then vital[idx] = 1;  /* missing but later visits exist */
      else do;
        if ltfu = 1 then vital[idx] = .;           /* missing at end and LTFU ? . */
        else vital[idx] = 0;                        /* missing at end and not LTFU ? dead */
      end;
    end;
  end;

  drop kvis kk later_nonmissing;
run;






proc print data=harmonized_simple; var death  stdytimea_y5-stdytimea_y18 ttodth_y5 death_day ltfu /*vital1-vital14*/;run;
proc print data=harmonized_vital; var  stdytimea_y5-stdytimea_y18  vital1-vital14 status_CHS ltfu ttodth_y5; run;


/*rename variables and save only those variables*/
data outCHS.vitalstatus_CHS;
set harmonized_vital (rename=(VITAL1=VITAL1_CHS
VITAL2=VITAL2_CHS
VITAL3=VITAL3_CHS
VITAL4=VITAL4_CHS
VITAL5=VITAL5_CHS
VITAL6=VITAL6_CHS
VITAL7=VITAL7_CHS
VITAL8=VITAL8_CHS
VITAL9=VITAL9_CHS
VITAL10=VITAL10_CHS
VITAL11=VITAL11_CHS
VITAL12=VITAL12_CHS
VITAL13=VITAL13_CHS
VITAL14=VITAL14_CHS));
keep idno VITAL1_CHS
VITAL2_CHS
VITAL3_CHS
VITAL4_CHS
VITAL5_CHS
VITAL6_CHS
VITAL7_CHS
VITAL8_CHS
VITAL9_CHS
VITAL10_CHS
VITAL11_CHS
VITAL12_CHS
VITAL13_CHS
VITAL14_CHS
status_CHS;
run;

proc print data=outCHS.vitalstatus_CHS; run;
