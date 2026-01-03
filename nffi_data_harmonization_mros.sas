
*************************************************************************
	*  TITLE: NFFI Data harmonization: MrOS		 					*
	*																*	
	*  PROGRAMMER:   Hajin Jang   	 								*
	*																*
	*  DESCRIPTION: Data harmonization variables 		      		*
	*  			  		  (MrOS)     		 						*
	*																*
	*  DATE:    created 3/24/2025									*
	*---------------------------------------------------------------*
	*  LANGUAGE:     SAS VERSION 9.4								*
	*---------------------------------------------------------------*
	*  NOTES: 		Data pooling								    *
*************************************************************************;


libname data "Z:\Fall Injuries Combined Studies\HABC, MrOS, CHS, WHI Combined\Final datasets for NFFI";
libname created "Z:\Fall Injuries Combined Studies\Harmonization Coding\Hajin code\Hajin datasets\MrOS";

	dm "odsresults; clear";
	dm "log; clear";

	
	options nofmterr;

data mros; set data.mros_master_nh_012425; run;


/******************/
/* Fall variables */
/******************/


data pdaug24; set created.pdaug24; 
keep
id fua1fall fua2fall fua3fall fua4fall fua5fall fua6fall fua7fall fua8fall fua9fall fua10fall
fua11fall fua12fall fua13fall fua14fall fua15fall fua16fall
fufaln1 fufaln2 fufaln3 fufaln4 fufaln5 fufaln6 fufaln7 fufaln8 fufaln9 fufaln10
fufaln11 fufaln12 fufaln13 fufaln14 fufaln15 fufaln16 fufaln17 fufaln18 fufaln19 fufaln20
fufaln21 fufaln22 fufaln23 fufaln24 fufaln25 fufaln26 fufaln27 fufaln28 fufaln29 fufaln30
fufaln31 fufaln32 fufaln33 fufaln34 fufaln35 fufaln36 fufaln37 fufaln38 fufaln39 fufaln40
fufaln41 fufaln42 fufaln43 fufaln44 fufaln45 fufaln46 fufaln47 fufaln48
;
run;


/* Code character values as missing */
data pdaug24; set pdaug24;

array 
num_vars fua1fall fua2fall fua3fall fua4fall fua5fall fua6fall fua7fall fua8fall fua9fall fua10fall
fua11fall fua12fall fua13fall fua14fall fua15fall fua16fall
fufaln1 fufaln2 fufaln3 fufaln4 fufaln5 fufaln6 fufaln7 fufaln8 fufaln9 fufaln10
fufaln11 fufaln12 fufaln13 fufaln14 fufaln15 fufaln16 fufaln17 fufaln18 fufaln19 fufaln20
fufaln21 fufaln22 fufaln23 fufaln24 fufaln25 fufaln26 fufaln27 fufaln28 fufaln29 fufaln30
fufaln31 fufaln32 fufaln33 fufaln34 fufaln35 fufaln36 fufaln37 fufaln38 fufaln39 fufaln40
fufaln41 fufaln42 fufaln43 fufaln44 fufaln45 fufaln46 fufaln47 fufaln48;

do i = 1 to dim(num_vars);
num_vars[i] = input(num_vars[i], ?? best32.);
if missing(num_vars[i]) then num_vars[i] = .;
end;

drop i; 
run;


/* Fall indicator */
data pdaug24; set pdaug24;
FallIND10_MrOS = fua1fall;
FallIND11_MrOS = fua2fall;
FallIND12_MrOS = fua3fall;
FallIND13_MrOS = fua4fall;
FallIND145_MrOS = fua5fall;
FallIND15_MrOS = fua6fall;
FallIND16_MrOS = fua7fall;
FallIND17_MrOS = fua8fall;
FallIND18_MrOS = fua9fall;
FallIND19_MrOS = fua10fall;
FallIND20_MrOS = fua11fall;
FallIND21_MrOS = fua12fall;
FallIND22_MrOS = fua13fall;
FallIND23_MrOS = fua14fall;
FallIND24_MrOS = fua15fall;
FallIND25_MrOS = fua16fall;
run;

proc freq data=pdaug24;
tables FallIND10_MrOS FallIND11_MrOS FallIND12_MrOS FallIND13_MrOS FallIND145_MrOS FallIND15_MrOS FallIND16_MrOS
FallIND17_MrOS FallIND18_MrOS FallIND19_MrOS FallIND20_MrOS FallIND21_MrOS FallIND22_MrOS FallIND23_MrOS FallIND24_MrOS FallIND25_MrOS;
run;

proc print data=pdaug24;
var 
FallIND10_MrOS  fua1fall
FallIND11_MrOS  fua2fall
FallIND12_MrOS  fua3fall
FallIND13_MrOS  fua4fall
FallIND145_MrOS  fua5fall
FallIND15_MrOS  fua6fall
FallIND16_MrOS  fua7fall
FallIND17_MrOS  fua8fall
FallIND18_MrOS  fua9fall
FallIND19_MrOS  fua10fall
FallIND20_MrOS  fua11fall
FallIND21_MrOS  fua12fall
FallIND22_MrOS  fua13fall
FallIND23_MrOS  fua14fall
FallIND24_MrOS  fua15fall
FallIND25_MrOS  fua16fall;
run;


/* Number of times fallen */
data pdaug24; set pdaug24;
FallNum10_MrOS = sum(fufaln1, fufaln2, fufaln3);
FallNum11_MrOS = sum(fufaln4, fufaln5, fufaln6);
FallNum12_MrOS = sum(fufaln7, fufaln8, fufaln9);
FallNum13_MrOS = sum(fufaln10, fufaln11, fufaln12);
FallNum145_MrOS = sum(fufaln13, fufaln14, fufaln15);
FallNum15_MrOS = sum(fufaln16, fufaln17, fufaln18);
FallNum16_MrOS = sum(fufaln19, fufaln20, fufaln21);
FallNum17_MrOS = sum(fufaln22, fufaln23, fufaln24);
FallNum18_MrOS = sum(fufaln25, fufaln26, fufaln27);
FallNum19_MrOS = sum(fufaln28, fufaln29, fufaln30);
FallNum20_MrOS = sum(fufaln31, fufaln32, fufaln33);
FallNum21_MrOS = sum(fufaln34, fufaln35, fufaln36);
FallNum22_MrOS = sum(fufaln37, fufaln38, fufaln39);
FallNum23_MrOS = sum(fufaln40, fufaln41, fufaln42);
FallNum24_MrOS = sum(fufaln43, fufaln44, fufaln45);
FallNum25_MrOS = sum(fufaln46, fufaln47, fufaln48);

run;

proc freq data=pdaug24; 
tables 
	  FallNum10_MrOS FallNum11_MrOS FallNum12_MrOS FallNum13_MrOS
      FallNum145_MrOS FallNum15_MrOS FallNum16_MrOS FallNum17_MrOS
      FallNum18_MrOS FallNum19_MrOS FallNum20_MrOS FallNum21_MrOS
      FallNum22_MrOS FallNum23_MrOS FallNum24_MrOS FallNum25_MrOS;
run;


/* Number of falls categorized as 1=one time / 2=2 or more times */
/* Include non-fallers as FallNumbersX_HABC=0 */
%macro pd(num);
data pdaug24; set pdaug24;

if FallIND&num._MrOS=0 then FallNumbers&num._MrOS=0;
else if FallIND&num._MrOS=1 then do;
if FallNum&num._MrOS=1 then FallNumbers&num._MrOS=1;
else if FallNum&num._MrOS>=2 then FallNumbers&num._MrOS=2;
else if FallNum&num._MrOS=. then FallNumbers&num._MrOS=.;
end;

run;
%mend pd;

%pd(10);
%pd(11);
%pd(12);
%pd(13);
%pd(145);
%pd(15);
%pd(16);
%pd(17);
%pd(18);
%pd(19);
%pd(20);
%pd(21);
%pd(22);
%pd(23);
%pd(24);
%pd(25);

data pdaug24_2; set pdaug24;

if FallNumbers10_MrOS ne . then RecurrentFalls10_MrOS=(FallNumbers10_MrOS=2);
if FallNumbers11_MrOS ne . then RecurrentFalls11_MrOS=(FallNumbers11_MrOS=2);
if FallNumbers12_MrOS ne . then RecurrentFalls12_MrOS=(FallNumbers12_MrOS=2);
if FallNumbers13_MrOS ne . then RecurrentFalls13_MrOS=(FallNumbers13_MrOS=2);
if FallNumbers145_MrOS ne . then RecurrentFalls145_MrOS=(FallNumbers145_MrOS=2);
if FallNumbers15_MrOS ne . then RecurrentFalls15_MrOS=(FallNumbers15_MrOS=2);
if FallNumbers16_MrOS ne . then RecurrentFalls16_MrOS=(FallNumbers16_MrOS=2);
if FallNumbers17_MrOS ne . then RecurrentFalls17_MrOS=(FallNumbers17_MrOS=2);
if FallNumbers18_MrOS ne . then RecurrentFalls18_MrOS=(FallNumbers18_MrOS=2);
if FallNumbers19_MrOS ne . then RecurrentFalls19_MrOS=(FallNumbers19_MrOS=2);
if FallNumbers20_MrOS ne . then RecurrentFalls20_MrOS=(FallNumbers20_MrOS=2);
if FallNumbers21_MrOS ne . then RecurrentFalls21_MrOS=(FallNumbers21_MrOS=2);
if FallNumbers22_MrOS ne . then RecurrentFalls22_MrOS=(FallNumbers22_MrOS=2);
if FallNumbers23_MrOS ne . then RecurrentFalls23_MrOS=(FallNumbers23_MrOS=2);
if FallNumbers24_MrOS ne . then RecurrentFalls24_MrOS=(FallNumbers24_MrOS=2);
if FallNumbers25_MrOS ne . then RecurrentFalls25_MrOS=(FallNumbers25_MrOS=2);

run;


data pdaug24_fall; set pdaug24_2;
keep
id FallIND10_MrOS FallIND11_MrOS FallIND12_MrOS FallIND13_MrOS FallIND145_MrOS FallIND15_MrOS FallIND16_MrOS FallIND17_MrOS FallIND18_MrOS
FallIND19_MrOS FallIND20_MrOS FallIND21_MrOS FallIND22_MrOS FallIND23_MrOS FallIND24_MrOS FallIND25_MrOS

FallNumbers10_MrOS FallNumbers11_MrOS FallNumbers12_MrOS FallNumbers13_MrOS FallNumbers145_MrOS FallNumbers15_MrOS FallNumbers16_MrOS FallNumbers17_MrOS 
FallNumbers18_MrOS FallNumbers19_MrOS FallNumbers20_MrOS FallNumbers21_MrOS FallNumbers22_MrOS FallNumbers23_MrOS FallNumbers24_MrOS FallNumbers25_MrOS

RecurrentFalls10_MrOS RecurrentFalls11_MrOS RecurrentFalls12_MrOS RecurrentFalls13_MrOS RecurrentFalls145_MrOS RecurrentFalls15_MrOS RecurrentFalls16_MrOS 
RecurrentFalls17_MrOS RecurrentFalls18_MrOS RecurrentFalls19_MrOS RecurrentFalls20_MrOS RecurrentFalls21_MrOS RecurrentFalls22_MrOS RecurrentFalls23_MrOS
RecurrentFalls24_MrOS RecurrentFalls25_MrOS
;
run;


data created.mros_merged; merge mros (in=a) pdaug24_fall (in=b);
by id;
if a;
run;



data created.fallvar_mros; set created.mros_merged;

    keep
    id FallIND10_MrOS FallIND11_MrOS FallIND12_MrOS FallIND13_MrOS FallIND145_MrOS FallIND15_MrOS FallIND16_MrOS FallIND17_MrOS FallIND18_MrOS
    FallIND19_MrOS FallIND20_MrOS FallIND21_MrOS FallIND22_MrOS FallIND23_MrOS FallIND24_MrOS FallIND25_MrOS

    FallNumbers10_MrOS FallNumbers11_MrOS FallNumbers12_MrOS FallNumbers13_MrOS FallNumbers145_MrOS FallNumbers15_MrOS FallNumbers16_MrOS FallNumbers17_MrOS 
    FallNumbers18_MrOS FallNumbers19_MrOS FallNumbers20_MrOS FallNumbers21_MrOS FallNumbers22_MrOS FallNumbers23_MrOS FallNumbers24_MrOS FallNumbers25_MrOS

	RecurrentFalls10_MrOS RecurrentFalls11_MrOS RecurrentFalls12_MrOS RecurrentFalls13_MrOS RecurrentFalls145_MrOS RecurrentFalls15_MrOS RecurrentFalls16_MrOS 
	RecurrentFalls17_MrOS RecurrentFalls18_MrOS RecurrentFalls19_MrOS RecurrentFalls20_MrOS RecurrentFalls21_MrOS RecurrentFalls22_MrOS RecurrentFalls23_MrOS
	RecurrentFalls24_MrOS RecurrentFalls25_MrOS
	;

    label
    FallIND10_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y10: Yes(1), No(0)'
    FallIND11_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y11: Yes(1), No(0)'
    FallIND12_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y12: Yes(1), No(0)'
    FallIND13_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y13: Yes(1), No(0)'
    FallIND145_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y14.5: Yes(1), No(0)'
    FallIND15_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y15: Yes(1), No(0)'
    FallIND16_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y16: Yes(1), No(0)'
    FallIND17_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y17: Yes(1), No(0)'
    FallIND18_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y18: Yes(1), No(0)'
    FallIND19_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y19: Yes(1), No(0)'
    FallIND20_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y20: Yes(1), No(0)'
    FallIND21_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y21: Yes(1), No(0)'
    FallIND22_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y22: Yes(1), No(0)'
    FallIND23_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y23: Yes(1), No(0)'
    FallIND24_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y24: Yes(1), No(0)'
    FallIND25_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y25: Yes(1), No(0)'

    FallNumbers10_MrOS = 'Number of Falls Harmonized Variable for MrOS Y10: None(0), One time(1), Two or more times(2)'
    FallNumbers11_MrOS = 'Number of Falls Harmonized Variable for MrOS Y11: None(0), One time(1), Two or more times(2)'
    FallNumbers12_MrOS = 'Number of Falls Harmonized Variable for MrOS Y12: None(0), One time(1), Two or more times(2)'
    FallNumbers13_MrOS = 'Number of Falls Harmonized Variable for MrOS Y13: None(0), One time(1), Two or more times(2)'
    FallNumbers145_MrOS = 'Number of Falls Harmonized Variable for MrOS Y14.5: None(0), One time(1), Two or more times(2)'
    FallNumbers15_MrOS = 'Number of Falls Harmonized Variable for MrOS Y15: None(0), One time(1), Two or more times(2)'
    FallNumbers16_MrOS = 'Number of Falls Harmonized Variable for MrOS Y16: None(0), One time(1), Two or more times(2)'
    FallNumbers17_MrOS = 'Number of Falls Harmonized Variable for MrOS Y17: None(0), One time(1), Two or more times(2)'
    FallNumbers18_MrOS = 'Number of Falls Harmonized Variable for MrOS Y18: None(0), One time(1), Two or more times(2)'
    FallNumbers19_MrOS = 'Number of Falls Harmonized Variable for MrOS Y19: None(0), One time(1), Two or more times(2)'
    FallNumbers20_MrOS = 'Number of Falls Harmonized Variable for MrOS Y20: None(0), One time(1), Two or more times(2)'
    FallNumbers21_MrOS = 'Number of Falls Harmonized Variable for MrOS Y21: None(0), One time(1), Two or more times(2)'
    FallNumbers22_MrOS = 'Number of Falls Harmonized Variable for MrOS Y22: None(0), One time(1), Two or more times(2)'
    FallNumbers23_MrOS = 'Number of Falls Harmonized Variable for MrOS Y23: None(0), One time(1), Two or more times(2)'
    FallNumbers24_MrOS = 'Number of Falls Harmonized Variable for MrOS Y24: None(0), One time(1), Two or more times(2)'
    FallNumbers25_MrOS = 'Number of Falls Harmonized Variable for MrOS Y25: None(0), One time(1), Two or more times(2)'

	RecurrentFalls10_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y10: None or once(0), Twice or more(1)'
	RecurrentFalls11_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y11: None or once(0), Twice or more(1)'
	RecurrentFalls12_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y12: None or once(0), Twice or more(1)'
	RecurrentFalls13_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y13: None or once(0), Twice or more(1)'
	RecurrentFalls145_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y145: None or once(0), Twice or more(1)'
	RecurrentFalls15_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y15: None or once(0), Twice or more(1)'
	RecurrentFalls16_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y16: None or once(0), Twice or more(1)'
	RecurrentFalls17_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y17: None or once(0), Twice or more(1)'
	RecurrentFalls18_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y18: None or once(0), Twice or more(1)'
	RecurrentFalls19_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y19: None or once(0), Twice or more(1)'
	RecurrentFalls20_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y20: None or once(0), Twice or more(1)'
	RecurrentFalls21_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y21: None or once(0), Twice or more(1)'
	RecurrentFalls22_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y22: None or once(0), Twice or more(1)'
	RecurrentFalls23_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y23: None or once(0), Twice or more(1)'
	RecurrentFalls24_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y24: None or once(0), Twice or more(1)'
	RecurrentFalls25_MrOS = 'Recurrent Falls Harmonized Variable for MrOS Y25: None or once(0), Twice or more(1)'
    ;
run;


/* Add MrOS baseline fall variables */
data mros_v1; set mros;
keep id MHFALL_V1 MHFALLTM_V1; run;

proc freq data=mros_v1; table MHFALL_V1 MHFALLTM_V1; run;

data mros_v1; set mros_v1;

array 
num_vars MHFALL_V1 MHFALLTM_V1;

do i = 1 to dim(num_vars);
num_vars[i] = input(num_vars[i], ?? best32.);
if missing(num_vars[i]) then num_vars[i] = .;
end;

drop i; 
run;

data mros_v1; set mros_v1;
FallIND9_MrOS = MHFALL_V1;
FallNum9_MrOS = MHFALLTM_V1;
run;

data mros_v1; set mros_v1;

if FallIND9_MrOS=0 then FallNumbers9_MrOS=0;
else if FallIND9_MrOS=1 then do;
if FallNum9_MrOS=1 then FallNumbers9_MrOS=1;
else if FallNum9_MrOS>=2 then FallNumbers9_MrOS=2;
else if FallNum9_MrOS=. then FallNumbers9_MrOS=.;
end;

run;

proc contents data=mros_v1; run;

proc freq data=mros_v1; table FallIND9_MrOS FallNum9_MrOS FallNumbers9_MrOS; run; 


data mros_v1; set mros_v1;
keep id FallIND9_MrOS FallNumbers9_MrOS;
label
    FallIND9_MrOS = 'Fall Indicator Harmonized Variable for MrOS Y9: Yes(1), No(0)'
    FallNumbers9_MrOS = 'Number of Falls Harmonized Variable for MrOS Y9: None(0), One time(1), Two or more times(2)';
run;

proc sort data=mros_v1; by id; run;
proc sort data=created.fallvar_mros; by id; run;

data created.fallvar_mros_032425; merge created.fallvar_mros mros_v1; by id; run;


data created.fallvar_mros_final; set created.fallvar_mros_032425; run;


/*************************/
/* Maximum grip strength */
/*************************/


proc print data=mros (obs=30);
var GSGRPMAX_V1 GSGRPMAX_V2 GSGRPMAX_V3 GSGRPMAX_V4; run;

proc freq data=mros;
tables GSGRPMAX_V1 GSGRPMAX_V2 GSGRPMAX_V3 GSGRPMAX_V4; run;

data gsvar_mros; set mros;
array num_vars GSGRPMAX_V1 GSGRPMAX_V2 GSGRPMAX_V3 GSGRPMAX_V4;

do i = 1 to dim(num_vars);
num_vars[i] = input(num_vars[i], ?? best32.);
if missing(num_vars[i]) then num_vars[i] = .;
end;

drop i; 
run;

proc print data=gsvar_mros (obs=30);
var GSGRPMAX_V1 GSGRPMAX_V2 GSGRPMAX_V3 GSGRPMAX_V4; run;


data gsvar_mros; set gsvar_mros;
MaxGS9_MrOS = GSGRPMAX_V1;
MaxGS145_MrOS = GSGRPMAX_V2;
MaxGS16_MrOS = GSGRPMAX_V3;
MaxGS23_MrOS = GSGRPMAX_V4;
run;


data created.gsvar_mros; set gsvar_mros;

keep 
id MaxGS9_MrOS MaxGS145_MrOS MaxGS16_MrOS MaxGS23_MrOS;

label 
MaxGS9_MrOS = 'Maximum Grip Strength (kg) Harmonized Variable for MrOS Y9'
MaxGS145_MrOS = 'Maximum Grip Strength (kg) Harmonized Variable for MrOS Y14.5'
MaxGS16_MrOS = 'Maximum Grip Strength (kg) Harmonized Variable for MrOS Y16'
MaxGS23_MrOS = 'Maximum Grip Strength (kg) Harmonized Variable for MrOS Y23';

run;


/**********************/
/* Metabolic syndrome */
/**********************/


*waist > 102 cm for men;
*v3;
proc means data=mros; var HWWAIS_V3; run;

data mros1; set mros;
if HWWAIS_V3 ne . then mets_wc16=(HWWAIS_V3>102);
run;

proc freq data=mros1; table mets_wc16; run;


*triglycerides >= 150 mg/dL;
*v1;
proc means data=mros; var ASTRIG; run;

data mros2; set mros1;
if ASTRIG ne . then mets_tri9=(ASTRIG>=150);
run;

proc freq data=mros2; table mets_tri9; run;


*hdl <40 mg/dL for men;
*v1;
proc means data=mros; var ASHDL_C; run;

data mros3; set mros2;
if ASHDL_C ne . then mets_hdl9=(ASHDL_C<40);
run;

proc freq data=mros3; table mets_hdl9; run;


*bp sys >= 130 or dia >= 85;
*v3, v4;
proc means data=mros; var BPBPSYSM_v3 BPBPSYSM_v4  BPBPDIAM_v3 BPBPDIAM_v4; run;

data mros4; set mros3;
if (BPBPSYSM_v3 ne . and BPBPSYSM_v3>=130) or (BPBPDIAM_v3 ne . and BPBPDIAM_v3>=85) then mets_bp16=1; else mets_bp16=0;
if (BPBPSYSM_v4 ne . and BPBPSYSM_v4>=130) or (BPBPDIAM_v4 ne . and BPBPDIAM_v4>=85) then mets_bp23=1; else mets_bp23=0;
run;

proc freq data=mros4; table mets_bp16 mets_bp23; run;


*gluc >= 110 md/dL;
*v1, v3;
proc means data=mros; var D1FGLUC D1FGLUCJH_V3; run;

data mros5; set mros4;
if D1FGLUC ne . then mets_glu9=(D1FGLUC>=110);
if D1FGLUCJH_V3 ne . then mets_glu16=(D1FGLUCJH_V3>=110);
run;

proc freq data=mros5; table mets_glu9 mets_glu16; run;


*final mets dataset;
data created.mets_mros; set mros5;
keep id mets_wc16 mets_tri9 mets_hdl9 mets_bp16 mets_bp23 mets_glu9 mets_glu16;
run;


/***************/
/* Demographic */
/***************/

proc contents data=MrOS;run;
/*v2age 1- age at visit 2 in years, V3y1, etc.*/


************************* RACE SEX ED ****************************************************************;
Data racesexedvar;
set MrOS;
/*Race_H: Race/Ethnicity - MrOS (1=white, 2=African American, 3=Asian, 4=Native Hawaiian or other Pacific Islander,
5=American Indian or Alaskan Native, 6=Multiracial, 7=Unknown;*/
if girace= . then Race_MrOS=.;
if girace=1 then Race_MrOS=1;
else if girace=2 then Race_MrOS=2;
else if girace=5 then Race_MrOS=3;
else if girace in (3,4) then Race_MrOS=4;
else if girace=6 then Race_MrOS=5;
else if girace=7 then Race_MrOS=.;
label Race_MrOS= 'Race Harmonized Variable for MrOS: White (1), Black(2), Asian, Native Hawaiian or other Pacific Islander(3), 
Native Hawaiian or other Pacific Islander(4),
Other (5)';
Race_H= Race_MrOS;
label Race_H= 'Race Harmonized Variable: White (1), Black(2), Asian, American Indian or Alaskan Native(3), 
American Indian or Alaskan Native(4), Other (5)';

*Gender_H (1=Male, 0=Female);
Gender_MrOS=1;
label Gender_MrOS= 'Gender Harmonized Variable for MrOS Male(1), Female(0)';
Gender_H=1;
label Gender_H= 'Gender Harmonized Variable: Male(1), Female(0)';

*EDUC_H1 (Definition: 1=less than HS, 2=HS grad, 3=postsecondary);
if GIEDUC=. then EDUCA_MrOS=.;
if GIEDUC in (1,2,3) then EDUCA_MrOS=1;
else if GIEDUC=4 then EDUCA_MrOS=2;
else if GIEDUC in (5,6,7,8) then EDUCA_MrOS=3;
label EDUCA_MrOS= 'Education Harmonized Variable for MrOS: < than HS(1), HS grad(2), postsecondary(3)';
EDUCA_H= EDUCA_MrOS;
label EDUCA_MrOS= 'Education Harmonized Variable: < than HS(1), HS grad(2), postsecondary(3)';
run;

/*save final age,sex,race dataset*/
data outMrOS.racesexedvar_MrOS;
set racesexedvar;
keep id Race_MrOS Race_H Gender_MrOS Gender_H EDUCA_MrOS EDUCA_H;
run;



/**********************************************************************age*********************************************************************/
proc print data=MrOS;var v1age1;run;

data MrOS_age;
set MrOS;
age9_MrOS=GIAGE1;
age11_MrOS=viage1;
age14_MrOS=v2age1;
age16_MrOS=v3age1;
age18_MrOS=vi2age1;
age23_MrOS=v4age1;
run;

proc print data=MrOS_age;var age9_MrOS age11_MrOS age14_MrOS age16_MrOS age18_MrOS age23_MrOS;
run;

/*now, I need to make copies of all the variables so that I can create versions that are imputated*/
data agevarMrOS;
set MrOS_age;
ageINT9_MrOS=GIAGE1;
ageINT10_MrOS= .;
ageINT11_MrOS=viage1;
ageINT12_MrOS=.;
ageINT13_MrOS= .;
ageINT14_MrOS=v2age1;
ageINT15_MrOS= .;
ageINT16_MrOS=v3age1;
ageINT17_MrOS=.;
ageINT18_MrOS=vi2age1;
ageINT19_MrOS=.;
ageINT20_MrOS=.;
ageINT21_MrOS=.;
ageINT22_MrOS=.;
ageINT23_MrOS=v4age1;
run;


proc print data=agevar_imputedMrOS ; var ageINT9_MrOS ageINT10_mrOS ageINT11_MrOS ageINT12_MrOS ageINT13_MrOS 
ageINT14_MrOS ageINT15_MrOS ageINT16_MrOS ageINT17_MrOS ageINT18_MrOS ageINT19_MrOS ageINT20_MrOS ageINT21_MrOS 
ageINT22_MrOS ageINT23_MrOS;
run;

/*I need to calculate any missing data- ie. if there are any non-missing values in the ageX_MrOS variables 
(all with the same suffix), then it will take the data point right before the missing and add two (unless its missing)
to the continuous variable*/

data agevar_imputedMrOS;
    set agevarMrOS;

    /* Define the variables to interpolate */
    array vars[15] 
        ageINT9_MrOS ageINT10_mrOS ageINT11_MrOS ageINT12_MrOS ageINT13_MrOS
        ageINT14_MrOS ageINT15_MrOS ageINT16_MrOS ageINT17_MrOS ageINT18_MrOS
        ageINT19_MrOS ageINT20_MrOS ageINT21_MrOS ageINT22_MrOS ageINT23_MrOS;

		/* Define the actual corresponding years */
    array years[15] (9 10 11 12 13 14 15 16 17 18 19 20 21 22 23);

    do i = 1 to dim(vars);
        if missing(vars[i]) then do;

            /* Find previous non-missing value */
            prev_idx = .;
            do j = i - 1 to 1 by -1;
                if not missing(vars[j]) then do;
                    prev_idx = j;
                    leave;
                end;
            end;

            /* Find next non-missing value */
            next_idx = .;
            do k = i + 1 to dim(vars);
                if not missing(vars[k]) then do;
                    next_idx = k;
                    leave;
                end;
            end;

            /* Interpolate and round if both neighbors exist */
            if not missing(prev_idx) and not missing(next_idx) then do;
                vars[i] = round(
                              vars[prev_idx] + 
                              ((years[i] - years[prev_idx]) / (years[next_idx] - years[prev_idx])) *
                              (vars[next_idx] - vars[prev_idx])
                          );
            end;
        end;
    end;

    drop i j k prev_idx next_idx;
run;



proc print data=agevar_imputedMrOS;var ageINT9_MrOS ageINT10_mrOS ageINT11_MrOS ageINT12_MrOS ageINT13_MrOS ageINT14_MrOS ageINT15_MrOS ageINT16_MrOS ageINT17_MrOS ageINT18_MrOS ageINT19_MrOS ageINT20_MrOS ageINT21_MrOS ageINT22_MrOS ageINT23_MrOS;
run;

/*create a flag variable to see if there are any situations where there are more than 4 years between ages in "consecutive" MrOS years for visits 
VI1,V2,V3, and IV2. This would indicate a problem*/
data agevar_flaghMrOS;
set agevar_imputedMrOS;
array vars ageINT9_MrOS ageINT10_mrOS ageINT11_MrOS ageINT12_MrOS ageINT13_MrOS ageINT14_MrOS ageINT15_MrOS ageINT16_MrOS ageINT17_MrOS ageINT18_MrOS
ageINT19_MrOS ageINT20_MrOS ageINT21_MrOS ageINT22_MrOS ageINT23_MrOS ;

    ageflag_mros = 0;/* Initialize the flag variable */
    		 /* Loop through the variables in the array */
    do i = 1 to dim(vars)-1; /* Loop from first to second-to-last variable */
        /* Check for non-missing values and calculate the difference */
        if not missing(vars[i]) and not missing(vars[i+1]) then do;
            if abs(vars[i+1] - vars[i]) > 2 then ageflag_mros = 1; /* Set flag if difference > 2 */
        end;
    end;

    drop i; /* Drop the loop counter variable for cleaner output */
run;
/*there are none.*/

/*Now, look for more than 7 years between IV2 and V4*/
*data agevar_flag2hMrOS;
*set agevar_flaghMrOS;
*array vars /*age11_MrOS age14_MrOS age16_MrOS*/ age18_MrOS age23_MrOS;

 *   ageflag_mros = 0;/* Initialize the flag variable */
    		 /* Loop through the variables in the array */
  *  do i = 1 to dim(vars)-1; /* Loop from first to second-to-last variable */
        /* Check for non-missing values and calculate the difference */
   *     if not missing(vars[i]) and not missing(vars[i+1]) then do;
    *        if abs(vars[i+1] - vars[i]) > 7.1 then ageflag_mros = 1; /* Set flag if difference > 2 */
     *   end;
   * end;

    *drop i; /* Drop the loop counter variable for cleaner output */
*run;
/*1 obs has 8years.*/

proc freq data= agevar_flaghMrOS; tables ageflag_mros;run;
proc print data=agevar_flaghMrOS; where ageflag_mros=1; var ageINT9_MrOS  ageINT10_mrOS ageINT11_MrOS  ageINT12_MrOS ageINT13_MrOS ageINT14_MrOS  ageINT15_MrOS ageINT16_MrOS  ageINT17_MrOS ageINT18_MrOS ageINT19_MrOS ageINT20_MrOS ageINT21_MrOS ageINT22_MrOS ageINT23_MrOS  ;
run;

proc freq data= agevar_flag2hMrOS; tables ageflag_mros;run;

data agevar_flag2hMrOS;
set agevar_flaghMrOS;
label 	age9_MrOS='Age(years) Harmonized Variable for MrOS Y9'
		age11_MrOS= 'Age(years) Harmonized Variable for MrOS Y11'
		age14_MrOS= 'Age(years) Harmonized Variable for MrOS Y14'
		age16_MrOS= 'Age(years) Harmonized Variable for MrOS Y16'
		age18_MrOS= 'Age(years) Harmonized Variable for MrOS Y18'
		age23_MrOS= 'Age(years) Harmonized Variable for MrOS Y23'
		ageINT9_MrOS='Age(years) Harmonized Variable for MrOS Y9- Interpolated'
		ageINT10_MrOS='Age(years) Harmonized Variable for MrOS Y10- Interpolated'
		ageINT11_MrOS= 'Age(years) Harmonized Variable for MrOS Y11- Interpolated'
		ageINT12_MrOS='Age(years) Harmonized Variable for MrOS Y12- Interpolated'
		ageINT13_MrOS='Age(years) Harmonized Variable for MrOS Y13- Interpolated'
		ageINT14_MrOS= 'Age(years) Harmonized Variable for MrOS Y14- Interpolated'
		ageINT15_MrOS='Age(years) Harmonized Variable for MrOS Y15- Interpolated'
		ageINT16_MrOS= 'Age(years) Harmonized Variable for MrOS Y16- Interpolated'
		ageINT17_MrOS='Age(years) Harmonized Variable for MrOS Y17- Interpolated'
		ageINT18_MrOS= 'Age(years) Harmonized Variable for MrOS Y18- Interpolated'
		ageINT19_MrOS='Age(years) Harmonized Variable for MrOS Y19- Interpolated'
		ageINT20_MrOS='Age(years) Harmonized Variable for MrOS Y20- Interpolated'
		ageINT21_MrOS='Age(years) Harmonized Variable for MrOS Y21- Interpolated'
		ageINT22_MrOS='Age(years) Harmonized Variable for MrOS Y22- Interpolated'
		ageINT23_MrOS= 'Age(years) Harmonized Variable for MrOS Y23- Interpolated';
run;

/*now,put the id and only the variables I need in this dataset*/
data outMrOS.agevar_MrOS;
set agevar_flag2hMrOS;
keep id age9_MrOS age11_MrOS age14_MrOS age16_MrOS age18_MrOS age23_MrOS ageINT9_MrOS ageINT10_MrOS ageINT11_MrOS ageINT12_MrOS ageINT13_MrOS ageINT14_MrOS ageINT15_MrOS ageINT16_MrOS ageINT17_MrOS ageINT18_MrOS ageINT19_MrOS ageINT20_MrOS ageINT21_MrOS ageINT22_MrOS ageINT23_MrOS  ageflag_mros;
run;


proc univariate data=outMrOS.agevar_MrOS;var ageINT9_MrOS ageINT11_MrOS ageINT14_MrOS ageINT16_MrOS ageINT18_MrOS ageINT23_MrOS;run;
proc contents data=outMrOS.agevar_MrOS;run;
proc freq data=outMrOS.agevar_MrOS;
    tables ageINT9_MrOS ageINT11_MrOS ageINT14_MrOS ageINT16_MrOS ageINT18_MrOS ageINT23_MrOS / missing;
run;
proc means data=outMrOS.agevar_MrOS n nmiss;
    var ageINT23_MrOS;  /* or whichever variable */
run;

**************************************************************************************************;
/*Height: Convert MrOS cm to mm...MrOS = HWHGT*10 */
**************************************************************************************************;

proc print data=MrOS;var id HWHGT_V1 efdate ;
run;

proc contents data=MrOS;run;

data heightMrOS;
set MrOS;
height9_MrOS= HWHGT_V1*10;
height14_MrOS=HWHGT_V2*10;
height16_MrOS=HWHGT_V3*10;
height23_MrOS=HWHGT_V4*10;
run;
/*no height variables at VI1 and VI2?.*/

/*a lot of 'A' instead of missing-- fix that, set it all to . */
data heightMrOS;
    set heightMrOS;
	if height9_MrOS< 0 then height9_MrOS_new=.;
else  height9_MrOS_new= height9_MrOS;
drop height9_MrOS;
rename height9_MrOS_new=height9_MrOS;

	if height14_MrOS< 0 then height14_MrOS_new=.;
else  height14_MrOS_new= height14_MrOS;
drop height14_MrOS;
rename height14_MrOS_new=height14_MrOS;

	if height16_MrOS< 0 then height16_MrOS_new=.;
else  height16_MrOS_new= height16_MrOS;
drop height16_MrOS;
rename height16_MrOS_new=height16_MrOS;

	if height23_MrOS< 0 then height23_MrOS_new=.;
else  height23_MrOS_new= height23_MrOS;
drop height23_MrOS;
rename height23_MrOS_new=height23_MrOS;

run;

/*create an Interpolated variable for next step by
making copies, making the variables without values
missing so we can interpolate them as well.*/
data heightMrOS;
set heightMrOS;
heightINT9_MrOS=Height9_MrOS;
heightINT10_MrOS= .;
heightINT11_MrOS=.;
heightINT12_MrOS=.;
heightINT13_MrOS= .;
heightINT14_MrOS=Height14_MrOS;
heightINT15_MrOS= .;
heightINT16_MrOS=Height16_MrOS;
heightINT17_MrOS=.;
heightINT18_MrOS=.;
heightINT19_MrOS=.;
heightINT20_MrOS=.;
heightINT21_MrOS=.;
heightINT22_MrOS=.;
heightINT23_MrOS=Height23_MrOS;
run;

/*Now, calculate the missing heights using the linear interpolation method*/

/* Reshape from Wide to Long Format */
proc transpose data=HEIGHTMrOS out=long_data(rename=(col1=Height)) name=YEARVar;
    by ID;
    var heightINT9_MrOS heightINT10_MrOS heightINT11_MrOS heightINT12_MrOS
heightINT13_MrOS heightINT14_MrOS heightINT15_MrOS heightINT16_MrOS heightINT17_MrOS heightINT18_MrOS heightINT19_MrOS heightINT20_MrOS heightINT21_MrOS
heightINT22_MrOS heightINT23_MrOS; run;

/*  Extract YEAR from Variable Names */
data long_data;
    set long_data;
    if YEARVar = 'heightINT9_MrOS' then YEAR = 9;
	else if YEARVar = 'heightINT10_MrOS' then YEAR = 10;
	else if YEARVar = 'heightINT11_MrOS' then YEAR = 11;
	else if YEARVar = 'heightINT12_MrOS' then YEAR = 12;
	else if YEARVar = 'heightINT13_MrOS' then YEAR = 13;
	else if YEARVar = 'heightINT14_MrOS' then YEAR = 14;
	else if YEARVar = 'heightINT15_MrOS' then YEAR = 15;
    else if YEARVar = 'heightINT16_MrOS' then YEAR = 16;
	else if YEARVar = 'heightINT17_MrOS' then YEAR = 17;
	else if YEARVar = 'heightINT18_MrOS' then YEAR = 18;
	else if YEARVar = 'heightINT19_MrOS' then YEAR = 19;
	else if YEARVar = 'heightINT20_MrOS' then YEAR = 20;
	else if YEARVar = 'heightINT21_MrOS' then YEAR = 21;
	else if YEARVar = 'heightINT22_MrOS' then YEAR = 22;
    else if YEARVar = 'heightINT23_MrOS' then YEAR = 23;
    drop YEARVar;
run;

/* Sort Data by ID and YEAR */
proc sort data=long_data noduprecs;
    by ID YEAR;
run;

/* Perform Linear Interpolation */

/*Create Prev_YEAR and Prev_Height */
data heightdata;
    set long_data;
    by ID YEAR;
    
    retain Prev_YEAR Prev_Height; /* Retain values across iterations */

       /* First observation per ID: Set Prev_YEAR and Prev_Height to missing */
    if first.ID then do;
        Prev_YEAR = .;
        Prev_Height = .;
    end; 

    if not missing(Height) then do;
        Prev_YEAR = YEAR;
        Prev_Height = Height;
    end;
        
run;

/* Create Next_YEAR and Next_Height */
proc sort data=heightdata;   by ID descending YEAR; run;

data your_dataset;
    set heightdata;
    by ID descending YEAR;

   retain Next_YEAR Next_Height; /* Retain values across iterations */
    
  /* Last observation per ID: Set Next_YEAR and Next_Height to missing */
    if first.ID then do;
        Next_YEAR = .;
        Next_Height = .;
    end;

    if not missing(Height) then do;
        Next_YEAR = YEAR;
        Next_Height = Height;
    end;
   
run;

/* Re-sort the dataset to restore chronological order */
proc sort data=your_dataset;
    by ID YEAR;
run;

data your_dataset2;
    set your_dataset;

  /* Apply interpolation only when both previous and next heights exist */
    if missing(Height) and not missing(Prev_Height) and not missing(Next_Height) then do;
        Height = Prev_Height + ((Next_Height - Prev_Height) / (Next_YEAR - Prev_YEAR)) * (YEAR - Prev_YEAR);
		
    end;

if Height_I=. then Height_I=Height;
    /* Debugging Print */
    put "ID=" ID " YEAR=" YEAR " Height=" Height
        " Prev_YEAR=" Prev_YEAR " Prev_Height=" Prev_Height
        " Next_YEAR=" next_YEAR " Next_Height=" next_Height;
    
    drop i;
run;
/*this worked!*/


/* Step 6: Reshape Data Back to Wide Format */
proc transpose data=your_dataset2 out=final_data(drop=_NAME_) prefix=height;
    by ID;
    id YEAR;
    var Height;
run;

/* Step 7: Rename Variables to Match Original Format */
data final_data;
    set final_data;
    rename height9=heightINT9_MrOS  height14= heightINT14_MrOS height10=heightINT10_MrOS height11=heightINT11_MrOS height12=heightINT12_MrOS height13=heightINT13_MrOS height15=heightINT15_MrOS
height16=heightINT16_MrOS height17=heightINT17_MrOS height18=heightINT18_MrOS height19=heightINT19_MrOS height20=heightINT20_MrOS height21=heightINT21_MrOS height22=heightINT22_MrOS height23=heightINT23_MrOS;
run;

/* Step 8: Print Final Dataset */
proc print data=final_data noobs;
    title "Final Dataset with Interpolated Heights";
run;


/*Now, look at differences in height to make sure there are no oddities in the data. for the thresholds for "acceptable change in height" 
(due to spinal disc compression, posture changes, etc.) without concern:
			> 10mm loss and >5 mm gain per year   */

data height_flagMrOS;
    set final_data;

  /* Initialize flag to 0 */
    Flag = 0;

    array heights heightINT9_MrOS heightINT10_MrOS heightINT11_MrOS heightINT12_MrOS heightINT13_MrOS heightINT14_MrOS heightINT15_MrOS 
heightINT16_MrOS heightINT17_MrOS heightINT18_MrOS
heightINT19_MrOS heightINT20_MrOS heightINT21_MrOS heightINT22_MrOS heightINT23_MrOS ;  
    array flag_vars(*) flag1-flag14; /* One less flag variable than height variables */
	array year_diff(14)difference1-difference14; /* Stores year differences */
    array rate_of_change(14); /* Stores rate of height change per year */

    /* Define years explicitly */
    array years[15] _temporary_ (9, 10, 11, 12, 13, 14, 15, 16,17, 18, 19, 20, 21, 22, 23); /* Extracted from variable names */

      do i = 1 to dim(heights)-1; 
        if heights[i] ne . and heights[i+1] ne . then do;
            /* Calculate the number of years between measurements */
            year_diff[i] = years[i+1] - years[i];

            /* Ensure no division by zero */
            if year_diff[i] > 0 then do;
                /* Calculate height change per year */
                rate_of_change[i] = (heights[i+1] - heights[i]) / year_diff[i];

                /* Flag if loss is greater than 10mm per year */
                if rate_of_change[i] < -10 or rate_of_change[i] >= 5 then flag_vars[i] = 1; 
                else flag_vars[i] = 0;
            end;
            else do;
                rate_of_change[i] = .;
                flag_vars[i] = .;
            end;
        end;
        else do;
            year_diff[i] = .;
            rate_of_change[i] = .;
            flag_vars[i] = .; /* Missing if one of the height values is missing */
        end;
    end;

    *drop i;
run;
proc print data=height_flagMrOS; var  id height9_MrOS height14_MrOS height16_MrOS height23_MrOS flag1 flag2 flag3 year_diff1 year_diff2 year_diff3 rate_of_change1 rate_of_change2 rate_of_change3; run;

proc freq data=height_flagMrOS; tables rate_of_change1 rate_of_change2 rate_of_change3 ; where  Flag1 = 1 or Flag2=1 or Flag3=1; run;
/*flag1: 29, flag2:29 flag3: 28.*/

proc freq data=height_flagMrOS;table flag1-flag14 /*difference_var2*/;run;

proc univariate data=height_flagMrOS;var difference_var1 /*difference_var2*/;run;
/*figure out what differences are a problem. then recode above.*/

/*Combine the flags into 1*/
data combined_flag;
set height_flagMrOS;
IF flag1= 1 OR flag2= 1 OR flag3= 1 OR flag4= 1 OR flag5= 1 THEN heightflag_mros=1;
else if flag1= . AND flag2= . AND flag3= . AND flag4= . AND flag5= . THEN heightflag_mros=.;
else heightflag_mros=0;
run;

proc freq data=combined_flag; tables heightflag_mros; run;
/*32 flagged*/

proc print data=combined_flag;
where heightflag_mros = 1;run;

/*combine the un-interpolated data with the interpolated data +flag*/
proc sql;
create table combined as 
select a.id, a.heightINT9_MrOS, a.heightINT10_MrOS, a.heightINT11_MrOS, a.heightINT12_MrOS, 
a.heightINT13_MrOS, a.heightINT14_MrOS, a.heightINT15_MrOS, a.heightINT16_MrOS, a.heightINT17_MrOS,
a.heightINT18_MrOS, a.heightINT19_MrOS, a.heightINT20_MrOS, a.heightINT21_MrOS, a.heightINT22_MrOS, 
a.heightINT23_MrOS, a.heightflag_mros, b.Height9_MrOS,  b.Height14_MrOS, b.Height16_MrOS, b.Height23_MrOS
from combined_flag as a LEFT JOIN heightMrOS as b
on a.id = b.id;
quit;

/*Label all the variables here*/
data height_data;
set combined;
label 	HeightINT9_MrOS='Height(mm) Harmonized Variable for MrOS Y9- Interpolated'
HeightINT10_MrOS='Height(mm) Harmonized Variable for MrOS Y10- Interpolated'
HeightINT11_MrOS='Height(mm) Harmonized Variable for MrOS Y11- Interpolated'
HeightINT12_MrOS='Height(mm) Harmonized Variable for MrOS Y12- Interpolated'
HeightINT13_MrOS='Height(mm) Harmonized Variable for MrOS Y13- Interpolated'
HeightINT14_MrOS='Height(mm) Harmonized Variable for MrOS Y14- Interpolated'
HeightINT15_MrOS='Height(mm) Harmonized Variable for MrOS Y15- Interpolated'
HeightINT16_MrOS='Height(mm) Harmonized Variable for MrOS Y16- Interpolated'
HeightINT17_MrOS='Height(mm) Harmonized Variable for MrOS Y17- Interpolated'
HeightINT18_MrOS='Height(mm) Harmonized Variable for MrOS Y18- Interpolated'
HeightINT19_MrOS='Height(mm) Harmonized Variable for MrOS Y19- Interpolated'
HeightINT20_MrOS='Height(mm) Harmonized Variable for MrOS Y20- Interpolated'
HeightINT21_MrOS='Height(mm) Harmonized Variable for MrOS Y21- Interpolated'
HeightINT22_MrOS='Height(mm) Harmonized Variable for MrOS Y22- Interpolated'
HeightINT23_MrOS='Height(mm) Harmonized Variable for MrOS Y23- Interpolated'
Height9_MrOS='Height(mm) Harmonized Variable for MrOS Y9'
Height14_MrOS='Height(mm) Harmonized Variable for MrOS Y14'
Height16_MrOS='Height(mm) Harmonized Variable for MrOS Y16'
Height23_MrOS='Height(mm) Harmonized Variable for MrOS Y23'
heightflag_mros= 'MrOS Flag (1) if height increases by >5mm/year or decreases by >10mm/year';
run;

/*save final height dataset*/
data outMrOS.heightvar_MrOS;
set height_data;
run;


/*********************/
/* Physical activity */
/*********************/


**************************************************************************************************************************
												Physical Activity VARIABLES
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
set MrOS;
PhysActivity9_MrOS = PASCORE_V1;
PhysActivity10_MrOS = PASCORE_V1;/*carry forward*/
PhysActivity11_MrOS = PASCORE_V1; *No VI PA score so carry this forward;
PhysActivity12_MrOS = PASCORE_V1;/*carry forward*/
PhysActivity13_MrOS = PASCORE_V1;/*carry forward*/
PhysActivity14_MrOS = PASCORE_V2;
PhysActivity15_MrOS = PASCORE_V2;/*carry forward*/
PhysActivity16_MrOS = PASCORE_V3;
PhysActivity17_MrOS = PASCORE_V3; /*carry forward*/
PhysActivity18_MrOS = PASCORE_VI2;
PhysActivity19_MrOS = PASCORE_VI2; /*carry forward*/
PhysActivity20_MrOS = PASCORE_VI2; /*carry forward*/
PhysActivity21_MrOS = PASCORE_VI2; /*carry forward*/
PhysActivity22_MrOS = PASCORE_VI2; /*carry forward*/
PhysActivity23_MrOS = PASCORE_V4;
Keep id PhysActivity9_MrOS  PhysActivity10_MrOS PhysActivity11_MrOS  PhysActivity12_MrOS PhysActivity13_MrOS
PhysActivity14_MrOS PhysActivity15_MrOS PhysActivity16_MrOS PhysActivity17_MrOS PhysActivity18_MrOS 
PhysActivity19_MrOS   PhysActivity20_MrOS  PhysActivity21_MrOS  PhysActivity22_MrOS  PhysActivity23_MrOS;
run;

/*a lot of 'A' or 'N' instead of missing-- fix that, set it all to . */
data PhysActivityVar;
    set PhysActivityVar;
	if PhysActivity9_MrOS< 0 then PhysActivity9_MrOS_new=.;
else  PhysActivity9_MrOS_new= PhysActivity9_MrOS;
drop PhysActivity9_MrOS;
rename PhysActivity9_MrOS_new=PhysActivity9_MrOS;

	if PhysActivity10_MrOS< 0 then PhysActivity10_MrOS_new=.;
else  PhysActivity10_MrOS_new= PhysActivity10_MrOS;
drop PhysActivity10_MrOS;
rename PhysActivity10_MrOS_new=PhysActivity10_MrOS;

	if PhysActivity11_MrOS< 0 then PhysActivity11_MrOS_new=.;
else  PhysActivity11_MrOS_new= PhysActivity11_MrOS;
drop PhysActivity11_MrOS;
rename PhysActivity11_MrOS_new=PhysActivity11_MrOS;

	if PhysActivity12_MrOS< 0 then PhysActivity12_MrOS_new=.;
else  PhysActivity12_MrOS_new= PhysActivity12_MrOS;
drop PhysActivity12_MrOS;
rename PhysActivity12_MrOS_new=PhysActivity12_MrOS;

	if PhysActivity13_MrOS< 0 then PhysActivity13_MrOS_new=.;
else  PhysActivity13_MrOS_new= PhysActivity13_MrOS;
drop PhysActivity13_MrOS;
rename PhysActivity13_MrOS_new=PhysActivity13_MrOS;

	if PhysActivity14_MrOS< 0 then PhysActivity14_MrOS_new=.;
else  PhysActivity14_MrOS_new= PhysActivity14_MrOS;
drop PhysActivity14_MrOS;
rename PhysActivity14_MrOS_new=PhysActivity14_MrOS;

	if PhysActivity15_MrOS< 0 then PhysActivity15_MrOS_new=.;
else  PhysActivity15_MrOS_new= PhysActivity15_MrOS;
drop PhysActivity15_MrOS;
rename PhysActivity15_MrOS_new=PhysActivity15_MrOS;

	if PhysActivity16_MrOS< 0 then PhysActivity16_MrOS_new=.;
else  PhysActivity16_MrOS_new= PhysActivity16_MrOS;
drop PhysActivity16_MrOS;
rename PhysActivity16_MrOS_new=PhysActivity16_MrOS;

	if PhysActivity17_MrOS< 0 then PhysActivity17_MrOS_new=.;
else  PhysActivity17_MrOS_new= PhysActivity17_MrOS;
drop PhysActivity17_MrOS;
rename PhysActivity17_MrOS_new=PhysActivity17_MrOS;

	if PhysActivity18_MrOS< 0 then PhysActivity18_MrOS_new=.;
else  PhysActivity18_MrOS_new= PhysActivity18_MrOS;
drop PhysActivity18_MrOS;
rename PhysActivity18_MrOS_new=PhysActivity18_MrOS;

	if PhysActivity19_MrOS< 0 then PhysActivity19_MrOS_new=.;
else  PhysActivity19_MrOS_new= PhysActivity19_MrOS;
drop PhysActivity19_MrOS;
rename PhysActivity19_MrOS_new=PhysActivity19_MrOS;

	if PhysActivity20_MrOS< 0 then PhysActivity20_MrOS_new=.;
else  PhysActivity20_MrOS_new= PhysActivity20_MrOS;
drop PhysActivity20_MrOS;
rename PhysActivity20_MrOS_new=PhysActivity20_MrOS;

	if PhysActivity21_MrOS< 0 then PhysActivity21_MrOS_new=.;
else  PhysActivity21_MrOS_new= PhysActivity21_MrOS;
drop PhysActivity21_MrOS;
rename PhysActivity21_MrOS_new=PhysActivity21_MrOS;

	if PhysActivity22_MrOS< 0 then PhysActivity22_MrOS_new=.;
else  PhysActivity22_MrOS_new= PhysActivity22_MrOS;
drop PhysActivity22_MrOS;
rename PhysActivity22_MrOS_new=PhysActivity22_MrOS;

	if PhysActivity23_MrOS< 0 then PhysActivity23_MrOS_new=.;
else  PhysActivity23_MrOS_new= PhysActivity23_MrOS;
drop PhysActivity23_MrOS;
rename PhysActivity23_MrOS_new=PhysActivity23_MrOS;
run;

proc univariate data=PhysActivityVar; var PhysActivity9_MrOS PhysActivity14_MrOS PhysActivity16_MrOS PhysActivity18_MrOS PhysActivity23_MrOS; histogram PhysActivity9_MrOS PhysActivity14_MrOS PhysActivity16_MrOS PhysActivity18_MrOS PhysActivity23_MrOS;run;


/*the distribution is moderately skewed right. But to keep consistent
across studies, I will use LOGARITHM.*/

/*try log transform this variable to try to make it Normal
(Tried log- it overcorrected too)*/
data PhysActivityVar_log;
   set PhysActivityVar;
 PhysActivity9_MrOS_L = log (PhysActivity9_MrOS);
PhysActivity10_MrOS_L = log (PhysActivity10_MrOS);
PhysActivity11_MrOS_L = log (PhysActivity11_MrOS);
PhysActivity12_MrOS_L = log (PhysActivity12_MrOS);
PhysActivity13_MrOS_L = log (PhysActivity13_MrOS);
PhysActivity14_MrOS_L = log (PhysActivity14_MrOS);
PhysActivity15_MrOS_L = log (PhysActivity15_MrOS);
PhysActivity16_MrOS_L = log (PhysActivity16_MrOS);
PhysActivity17_MrOS_L = log (PhysActivity17_MrOS);
PhysActivity18_MrOS_L = log (PhysActivity18_MrOS);
PhysActivity19_MrOS_L = log (PhysActivity19_MrOS);
PhysActivity20_MrOS_L = log (PhysActivity20_MrOS);
PhysActivity21_MrOS_L = log (PhysActivity21_MrOS);
PhysActivity22_MrOS_L = log (PhysActivity22_MrOS);
PhysActivity23_MrOS_L = log (PhysActivity23_MrOS);
	keep ID PhysActivity9_MrOS_L
PhysActivity10_MrOS_L
PhysActivity11_MrOS_L
PhysActivity12_MrOS_L
PhysActivity13_MrOS_L
PhysActivity14_MrOS_L
PhysActivity15_MrOS_L
PhysActivity16_MrOS_L
PhysActivity17_MrOS_L
PhysActivity18_MrOS_L
PhysActivity19_MrOS_L
PhysActivity20_MrOS_L
PhysActivity21_MrOS_L
PhysActivity22_MrOS_L
PhysActivity23_MrOS_L
  ;
run;

/*look at distributions again*/
proc univariate data=PhysActivityVar_log; var PhysActivity9_MrOS_L /*PhysActivity11_MrOS_L*/ PhysActivity14_MrOS_L PhysActivity16_MrOS_L PhysActivity18_MrOS_L PhysActivity23_MrOS_L;
histogram PhysActivity9_MrOS_L PhysActivity11_MrOS_L PhysActivity14_MrOS_L PhysActivity16_MrOS_L PhysActivity18_MrOS_L PhysActivity23_MrOS_L;
run;

/*look at distributions again*/
proc univariate data=PhysActivityVar_sqrt; var PhysActivity9_MrOS_L /*PhysActivity11_MrOS_L*/ PhysActivity14_MrOS_L PhysActivity16_MrOS_L PhysActivity18_MrOS_L PhysActivity23_MrOS_L;
histogram PhysActivity9_MrOS_L PhysActivity11_MrOS_L PhysActivity14_MrOS_L PhysActivity16_MrOS_L PhysActivity18_MrOS_L PhysActivity23_MrOS_L;
run;

data PhysActivityVar_f;
   set PhysActivityVar_log;
    Rename PhysActivity9_MrOS_L = PhysActivity9_MrOS
PhysActivity10_MrOS_L = PhysActivity10_MrOS
PhysActivity11_MrOS_L = PhysActivity11_MrOS
PhysActivity12_MrOS_L = PhysActivity12_MrOS
PhysActivity13_MrOS_L = PhysActivity13_MrOS
PhysActivity14_MrOS_L = PhysActivity14_MrOS
PhysActivity15_MrOS_L = PhysActivity15_MrOS
PhysActivity16_MrOS_L = PhysActivity16_MrOS
PhysActivity17_MrOS_L = PhysActivity17_MrOS
PhysActivity18_MrOS_L = PhysActivity18_MrOS
PhysActivity19_MrOS_L = PhysActivity19_MrOS
PhysActivity20_MrOS_L = PhysActivity20_MrOS
PhysActivity21_MrOS_L = PhysActivity21_MrOS
PhysActivity22_MrOS_L = PhysActivity22_MrOS
PhysActivity23_MrOS_L = PhysActivity23_MrOS;
run;

data PhysActivityVar_f;
set PhysActivityVar_f;
label PhysActivity9_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y9- log transformed and standardized'
PhysActivity10_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y10- log transformed and standardized- CARRIED FORWARD FROM Y9'
PhysActivity11_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y11- log transformed and standardized- CARRIED FORWARD FROM Y9'
PhysActivity12_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y12- log transformed and standardized- CARRIED FORWARD FROM Y9'
PhysActivity13_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y13- log transformed and standardized- CARRIED FORWARD FROM Y9'
PhysActivity14_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y14- log transformed and standardized'
PhysActivity15_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y15- log transformed and standardized- CARRIED FORWARD FROM Y14'
PhysActivity16_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y16- log transformed and standardized'
PhysActivity17_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y17- log transformed and standardized- CARRIED FORWARD FROM Y16'
PhysActivity18_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y18- log transformed and standardized'
PhysActivity19_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y19- log transformed and standardized- CARRIED FORWARD FROM Y18'
PhysActivity20_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y20- log transformed and standardized- CARRIED FORWARD FROM Y18'
PhysActivity21_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y21- log transformed and standardized- CARRIED FORWARD FROM Y18'
PhysActivity22_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y22- log transformed and standardized- CARRIED FORWARD FROM Y18'
PhysActivity23_MrOS= 'Physical Activity Variable for MrOS (PASE Scale) NFFI Y23- log transformed and standardized'
;	
run;

/*now, standardize and look at distribution*/
PROC STANDARD DATA=PhysActivityVar_f MEAN=0 STD=1 OUT=PhysActivityVar_std;
  VAR PhysActivity9_MrOS
PhysActivity10_MrOS
PhysActivity11_MrOS
PhysActivity12_MrOS
PhysActivity13_MrOS
PhysActivity14_MrOS
PhysActivity15_MrOS
PhysActivity16_MrOS
PhysActivity17_MrOS
PhysActivity18_MrOS
PhysActivity19_MrOS
PhysActivity20_MrOS
PhysActivity21_MrOS
PhysActivity22_MrOS
PhysActivity23_MrOS ;
  run;

  proc univariate data= PhysActivityVar_std;
  where PhysActivity6_MrOS > 2.5 OR PhysActivity6_MrOS <=- 2.5;
  run;
/*n=38 obs outside of 2.5 SDs,  (there were 82 with log)
  10 obs outside of 3 SD,
  highest/lowest is 3.4 and -3.06*/


proc univariate data=PhysActivityVar_std; var PhysActivity9_MrOS /*PhysActivity11_MrOS*/ PhysActivity14_MrOS PhysActivity16_MrOS PhysActivity18_MrOS PhysActivity23_MrOS;
   histogram PhysActivity9_MrOS /*PhysActivity11_MrOS*/ PhysActivity14_MrOS PhysActivity16_MrOS PhysActivity18_MrOS PhysActivity23_MrOS;
    run;

	/*save final PA dataset*/
data outMrOS.PhysActivityvar_MrOS;
set PhysActivityVar_std;
run;

/*Test*/
data test;
set PhysActivityVar_std;
run;

proc contents data=outMrOS.PhysActivityvar_MrOS;run;


/****************/
/* vital status */
/****************/


/******************************************************************  Age at Death (#) & Die Before 90 (Y/N)   *********************************************************************/

/*get enrollment status data and save it to bring in to merge at the 
end when i've already transposed age.*/
proc sql;
create table ageatbaseline as
select a.*, b.ageint9_mros
from mros as a LEFT JOIN outMrOS.agevar_MrOS as b
on a.id=b.id;
quit;

data ageatdeath;
set ageatbaseline;
if FUCYTIME ne . then do; /*FUCYTIME= followup years from enrollment*/
ageatdeath_mros = ageint9_mros + FUCYTIME;
end;

run;

/*indicator variable*/
data die_after_90;
set ageatdeath;
if 1 <ageatdeath_MrOS<90 then dieafter90_MrOS=0;
else if ageatdeath_MrOS >=90 then dieafter90_MrOS=1;
else if ageatdeath_MrOS= . then dieafter90_MrOS= .;
run;

proc print data=die_after_90;var id FUCYTIME ageint9_mros ageatdeath_MrOS dieafter90_mros; run;
proc freq data=die_after_90; table dieafter90_MrOS;run;
/*n=356 died after 90*/

/*save variable*/
data outMrOS.Dieafter90var_MrOS (keep=ID ageatdeath_MrOS dieafter90_MrOS);
set die_after_90;
run;




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

/*Notes about formats for status vars:
EFSTATUS: 0: active, 1: deceased, 2: terminated, 3: postcard only

So I need to edit this before running the code to identify last observed visit day*/
data mros_status;
  set mros;
  if EFSTATUS= 0 or EFSTATUS=3 then status=1; /*alive*/
  else if EFSTATUS= 1 then status=0;/* died */
  else status1=.;                                          /* LTFU */
run;

proc freq data=mros_status; table  FUCYTIME; run;
/*no missing time to death- so don't need to worry about contradictions between fucytime and status*/

/*Create vital status variables*/
data harmonized_simple;
set MroS_status;
vital0=1; /*set all alive at enrollment;
  /* --- 1) create annual vital indicators Vital1..Vital14 --- */
  array vital[14] Vital1 - Vital14;

  do k = 1 to 14;
		if status = 0 and FUCYTIME < k then do; /* died */
      vital[k] = 0;
    end;
    else if FUCYTIME >= k then do; /* alive and observed at/after year k */
      vital[k] = 1;
    end;
    else do; /* censored before reaching year k */
      vital[k] = .;
    end;
  end;

/* censored/LTFU */

  /* Force status to alive if Vital14 = 1 */
  if vital[14] = 1 then status = 1;


  drop k;
run;

proc print data=harmonized_simple; var id FUCYTIME vital1-vital14 status;run;



/*Rename and Keep only vital vars--- make all */
data outMrOS.vitalstatus_MrOS; 
set harmonized_simple (rename=(VITAL0= VITAL9_MrOS
VITAL1=VITAL10_MrOS
VITAL2=VITAL11_MrOS
VITAL3=VITAL12_MrOS
VITAL4=VITAL13_MrOS
VITAL5=VITAL14_MrOS
VITAL6=VITAL15_MrOS
VITAL7=VITAL16_MrOS
VITAL8=VITAL17_MrOS
VITAL9=VITAL18_MrOS
VITAL10=VITAL19_MrOS
VITAL11=VITAL20_MrOS
VITAL12=VITAL21_MrOS
VITAL13=VITAL22_MrOS
VITAL14=VITAL23_MrOS
));
keep id VITAL9_MrOS
VITAL10_MrOS
VITAL11_MrOS
VITAL12_MrOS
VITAL13_MrOS
VITAL14_MrOS
VITAL15_MrOS
VITAL16_MrOS
VITAL17_MrOS
VITAL18_MrOS
VITAL19_MrOS
VITAL20_MrOS
VITAL21_MrOS
VITAL22_MrOS
VITAL23_MrOS status;
run;


proc print data= outMrOS.vitalstatus_MrOS; run;

