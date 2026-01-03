
*************************************************************************
	*  TITLE: NFFI Data harmonization: Health ABC 					*
	*																*	
	*  PROGRAMMER:   Hajin Jang   	 								*
	*																*
	*  DESCRIPTION: Data harmonization variables 		      		*
	*  			  		  (Health ABC)      						*
	*																*
	*  DATE:    created 3/6/2025									*
	*---------------------------------------------------------------*
	*  LANGUAGE:     SAS VERSION 9.4								*
	*---------------------------------------------------------------*
	*  NOTES: 		Data pooling								    *
*************************************************************************;




libname data "Z:\Fall Injuries Combined Studies\HABC, MrOS, CHS, WHI Combined\Final datasets for NFFI";
libname created "Z:\Fall Injuries Combined Studies\Harmonization Coding\Hajin code\Hajin datasets\HABC";

	dm "odsresults; clear";
	dm "log; clear";

	
	options nofmterr;

data healthabc; set data.habc_master_nh_021925; run;


/******************/
/* Fall variables */
/******************/


proc contents data=healthabc; run;

/* Fall indicator */
data fallvar_habc; set healthabc;
FallIND6_HABC= PQAJFALL; /*Y1*/
FallIND7_HABC= BZAJFALL; /*Y2*/
FallIND8_HABC= CLAJFALL;/*Y3*/
FallIND9_HABC= DAAJFALL;/*Y4*/
FallIND10_HABC= EBAJFALL;/*Y5*/
FallIND11_HABC= FBAJFALL;/*Y6*/
FallIND12_HABC= GAAJFALL;/*Y7*/
FallIND13_HABC= RHAJFALL_Y8;/*Y8*/
FallIND145_HABC= RHAJFALL_Y9;/*Y9*/
FallIND15_HABC= RHAJFALL_Y10;/*Y10*/
FallIND16_HABC= Y11AJFALL;/*Y11*/
FallIND17_HABC= Y12AJFALL;/*Y12*/
FallIND18_HABC= Y13AJFALL;/*Y13*/
run;

/* check that it worked */
proc print data=fallvar_habc; 
var FallIND6_HABC PQAJFALL  /*Y1*/
FallIND7_HABC BZAJFALL  /*Y2*/
FallIND8_HABC CLAJFALL /*Y3*/
FallIND9_HABC DAAJFALL /*Y4*/
FallIND10_HABC EBAJFALL /*Y5*/
FallIND11_HABC FBAJFALL /*Y6*/
FallIND12_HABC GAAJFALL /*Y7*/
FallIND13_HABC RHAJFALL_Y8 /*Y8*/
FallIND145_HABC RHAJFALL_Y9 /*Y9*/
FallIND15_HABC RHAJFALL_Y10 /*Y10*/
FallIND16_HABC Y11AJFALL /*Y11*/
FallIND17_HABC Y12AJFALL /*Y12*/
FallIND18_HABC Y13AJFALL; /*Y13*/
run;

/* check the distribution of falls */
proc freq data=fallvar_habc; 
tables FallIND6_HABC FallIND7_HABC FallIND8_HABC FallIND9_HABC FallIND10_HABC
FallIND11_HABC FallIND12_HABC FallIND13_HABC FallIND145_HABC FallIND15_HABC FallIND16_HABC FallIND17_HABC FallIND18_HABC; 
run;

/* 7- refused and 8-don't know are coded as missing */
data fallvar_habc; set fallvar_habc;
if FallIND6_HABC= 7 or FallIND6_HABC= 8 then FallIND6_HABC= .;
if FallIND7_HABC= 7 or FallIND7_HABC= 8 then FallIND7_HABC= .;
if FallIND8_HABC= 7 or FallIND8_HABC= 8 then FallIND8_HABC= .;
if FallIND9_HABC= 7 or FallIND9_HABC= 8 then FallIND9_HABC= .;
if FallIND10_HABC= 7 or FallIND10_HABC= 8 then FallIND10_HABC= .;
if FallIND11_HABC= 7 or FallIND11_HABC= 8 then FallIND11_HABC= .;
if FallIND12_HABC= 7 or FallIND12_HABC= 8 then FallIND12_HABC= .;
if FallIND13_HABC= 7 or FallIND13_HABC= 8 then FallIND13_HABC= .;
if FallIND145_HABC= 7 or FallIND145_HABC= 8 then FallIND145_HABC= .;	
if FallIND15_HABC= 7 or FallIND15_HABC= 8 then FallIND15_HABC= .;
if FallIND16_HABC= 7 or FallIND16_HABC= 8 then FallIND16_HABC= .;
if FallIND17_HABC= 7 or FallIND17_HABC= 8 then FallIND17_HABC= .;
if FallIND18_HABC= 7 or FallIND18_HABC= 8 then FallIND18_HABC= .;
run;

/* Number of times fallen */
data fallvar_habc; set fallvar_habc;
FallNum6_HABC= PQAJFNUM; *Y1;
FallNum7_HABC= BZAJFNUM; *Y2;
FallNum8_HABC= CLAJFNUM;/*Y3*/
FallNum9_HABC= DAAJFNUM;/*Y4*/
FallNum10_HABC= EBAJFNUM;/*Y5*/
FallNum11_HABC= FBAJFNUM;/*Y6*/
FallNum12_HABC= GAAJFNUM;/*Y7*/
FallNum13_HABC= RHAJFNUM_Y8;/*Y8*/
FallNum145_HABC= RHAJFNUM_Y9;/*Y9*/
FallNum15_HABC= RHAJFNUM_Y10;/*Y10*/
FallNum16_HABC= Y11AJFNUM ;/*Y11*/
FallNum17_HABC= Y12AJFNUM;/*Y12*/
FallNum18_HABC= Y13AJFNUM;/*Y13*/
run;

/*check that it worked*/
proc print data=fallvar_habc; 
var FallNum6_HABC PQAJFNUM  /*Y1*/
FallNum7_HABC BZAJFNUM  /*Y2*/
FallNum8_HABC CLAJFNUM /*Y3*/
FallNum9_HABC DAAJFNUM /*Y4*/
FallNum10_HABC EBAJFNUM /*Y5*/
FallNum11_HABC FBAJFNUM /*Y6*/
FallNum12_HABC GAAJFNUM /*Y7*/
FallNum13_HABC RHAJFNUM_Y8 /*Y8*/
FallNum145_HABC RHAJFNUM_Y9 /*Y9*/
FallNum15_HABC RHAJFNUM_Y10 /*Y10*/
FallNum16_HABC Y11AJFNUM /*Y11*/
FallNum17_HABC Y12AJFNUM /*Y12*/
FallNum18_HABC Y13AJFNUM /*Y13*/
;
run;

/*check the distribution of falls*/
proc freq data=fallvar_habc; 
tables FallNum6_HABC FallNum7_HABC FallNum8_HABC FallNum9_HABC FallNum10_HABC 
FallNum11_HABC FallNum12_HABC FallNum13_HABC FallNum145_HABC FallNum15_HABC FallNum16_HABC FallNum17_HABC FallNum18_HABC;
run;

/*Get rid of A's*/
data fallvar_habc; set fallvar_habc;
if FallNum6_HABC< 0 then FallNum6_HABC_new=.;
else  FallNum6_HABC_new= FallNum6_HABC;
drop FallNum6_HABC;
rename FallNum6_HABC_new=FallNum6_HABC;

if FallNum7_HABC< 0 then FallNum7_HABC_new=.;
else  FallNum7_HABC_new= FallNum7_HABC;
drop FallNum7_HABC;
rename FallNum7_HABC_new=FallNum7_HABC;

if FallNum8_HABC< 0 then FallNum8_HABC_new=.;
else  FallNum8_HABC_new= FallNum8_HABC;
drop FallNum8_HABC;
rename FallNum8_HABC_new=FallNum8_HABC;

if FallNum9_HABC< 0 then FallNum9_HABC_new=.;
else  FallNum9_HABC_new= FallNum9_HABC;
drop FallNum9_HABC;
rename FallNum9_HABC_new=FallNum9_HABC;

if FallNum10_HABC< 0 then FallNum10_HABC_new=.;
else  FallNum10_HABC_new= FallNum10_HABC;
drop FallNum10_HABC;
rename FallNum10_HABC_new=FallNum10_HABC;

if FallNum11_HABC< 0 then FallNum11_HABC_new=.;
else  FallNum11_HABC_new= FallNum11_HABC;
drop FallNum11_HABC;
rename FallNum11_HABC_new=FallNum11_HABC;

if FallNum12_HABC< 0 then FallNum12_HABC_new=.;
else  FallNum12_HABC_new= FallNum12_HABC;
drop FallNum12_HABC;
rename FallNum12_HABC_new=FallNum12_HABC;

if FallNum13_HABC< 0 then FallNum13_HABC_new=.;
else  FallNum13_HABC_new= FallNum13_HABC;
drop FallNum13_HABC;
rename FallNum13_HABC_new=FallNum13_HABC;

if FallNum145_HABC< 0 then FallNum145_HABC_new=.;
else  FallNum145_HABC_new= FallNum145_HABC;
drop FallNum145_HABC;
rename FallNum145_HABC_new=FallNum145_HABC;

if FallNum15_HABC< 0 then FallNum15_HABC_new=.;
else  FallNum15_HABC_new= FallNum15_HABC;
drop FallNum15_HABC;
rename FallNum15_HABC_new=FallNum15_HABC;

if FallNum16_HABC< 0 then FallNum16_HABC_new=.;
else  FallNum16_HABC_new= FallNum16_HABC;
drop FallNum16_HABC;
rename FallNum16_HABC_new=FallNum16_HABC;

if FallNum17_HABC< 0 then FallNum17_HABC_new=.;
else  FallNum17_HABC_new= FallNum17_HABC;
drop FallNum17_HABC;
rename FallNum17_HABC_new=FallNum17_HABC;

if FallNum18_HABC< 0 then FallNum18_HABC_new=.;
else  FallNum18_HABC_new= FallNum18_HABC;
drop FallNum18_HABC;
rename FallNum18_HABC_new=FallNum18_HABC;

run;

proc print data=fallvar_habc;
var FallNum6_HABC FallNum7_HABC FallNum8_HABC FallNum9_HABC FallNum10_HABC 
FallNum11_HABC FallNum12_HABC FallNum13_HABC FallNum145_HABC FallNum15_HABC FallNum16_HABC FallNum17_HABC FallNum18_HABC;
run;

proc freq data=fallvar_habc;
table FallNum6_HABC FallNum7_HABC FallNum8_HABC FallNum9_HABC FallNum10_HABC 
FallNum11_HABC FallNum12_HABC FallNum13_HABC FallNum145_HABC FallNum15_HABC FallNum16_HABC FallNum17_HABC FallNum18_HABC;
run;


/* Number of falls categorized as 1=one time / 2=2 or more times */
/* Include non-fallers as FallNumbersX_HABC=0 */
data fallvar_habc; set fallvar_habc;

if FallIND6_HABC=0 then FallNumbers6_HABC=0;
else if FallIND6_HABC=1 then do;
if FallNum6_HABC=1 then FallNumbers6_HABC=1;
else if FallNum6_HABC in (2,4,6) then FallNumbers6_HABC=2;
else if FallNum6_HABC in (7,.) then FallNumbers6_HABC=.;
end;

if FallIND7_HABC=0 then FallNumbers7_HABC=0;
else if FallIND7_HABC=1 then do;
if FallNum7_HABC=1 then FallNumbers7_HABC=1;
else if FallNum7_HABC in (2,4,6) then FallNumbers7_HABC=2;
else if FallNum7_HABC in (8,.) then FallNumbers7_HABC=.;
end;

if FallIND8_HABC=0 then FallNumbers8_HABC=0;
else if FallIND8_HABC=1 then do;
if FallNum8_HABC=1 then FallNumbers8_HABC=1;
else if FallNum8_HABC in (2,4,6) then FallNumbers8_HABC=2;
else if FallNum8_HABC in (8,.) then FallNumbers8_HABC=.;
end;

if FallIND9_HABC=0 then FallNumbers9_HABC=0;
else if FallIND9_HABC=1 then do;
if FallNum9_HABC=1 then FallNumbers9_HABC=1;
else if FallNum9_HABC in (2,4,6) then FallNumbers9_HABC=2;
else if FallNum9_HABC in (8,.) then FallNumbers9_HABC=.;
end;

if FallIND10_HABC=0 then FallNumbers10_HABC=0;
else if FallIND10_HABC=1 then do;
if FallNum10_HABC=1 then FallNumbers10_HABC=1;
else if FallNum10_HABC in (2,4,6) then FallNumbers10_HABC=2;
else if FallNum10_HABC in (8,.) then FallNumbers10_HABC=.;
end;

if FallIND11_HABC=0 then FallNumbers11_HABC=0;
else if FallIND11_HABC=1 then do;
if FallNum11_HABC=1 then FallNumbers11_HABC=1;
else if FallNum11_HABC in (2,4,6) then FallNumbers11_HABC=2;
else if FallNum11_HABC in (8,.) then FallNumbers11_HABC=.;
end;

if FallIND12_HABC=0 then FallNumbers12_HABC=0;
else if FallIND12_HABC=1 then do;
if FallNum12_HABC=1 then FallNumbers12_HABC=1;
else if FallNum12_HABC in (2,4,6) then FallNumbers12_HABC=2;
else if FallNum12_HABC in (8,.) then FallNumbers12_HABC=.;
end;

if FallIND13_HABC=0 then FallNumbers13_HABC=0;
else if FallIND13_HABC=1 then do;
if FallNum13_HABC=1 then FallNumbers13_HABC=1;
else if FallNum13_HABC in (2,4,6) then FallNumbers13_HABC=2;
else if FallNum13_HABC in (8,.) then FallNumbers13_HABC=.;
end;

if FallIND145_HABC=0 then FallNumbers145_HABC=0;
else if FallIND145_HABC=1 then do;
if FallNum145_HABC=1 then FallNumbers145_HABC=1;
else if FallNum145_HABC in (2,4,6) then FallNumbers145_HABC=2;
else if FallNum145_HABC in (8,.) then FallNumbers145_HABC=.;
end;

if FallIND15_HABC=0 then FallNumbers15_HABC=0;
else if FallIND15_HABC=1 then do;
if FallNum15_HABC=1 then FallNumbers15_HABC=1;
else if FallNum15_HABC in (2,4,6) then FallNumbers15_HABC=2;
else if FallNum15_HABC in (8,.) then FallNumbers15_HABC=.;
end;

if FallIND16_HABC=0 then FallNumbers16_HABC=0;
else if FallIND16_HABC=1 then do;
if FallNum16_HABC=1 then FallNumbers16_HABC=1;
else if FallNum16_HABC in (2,4,6) then FallNumbers16_HABC=2;
else if FallNum16_HABC in (8,.) then FallNumbers16_HABC=.;
end;

if FallIND17_HABC=0 then FallNumbers17_HABC=0;
else if FallIND17_HABC=1 then do;
if FallNum17_HABC=1 then FallNumbers17_HABC=1;
else if FallNum17_HABC in (2,4,6) then FallNumbers17_HABC=2;
else if FallNum17_HABC in (8,.) then FallNumbers17_HABC=.;
end;

if FallIND18_HABC=0 then FallNumbers18_HABC=0;
else if FallIND18_HABC=1 then do;
if FallNum18_HABC=1 then FallNumbers18_HABC=1;
else if FallNum18_HABC in (2,4,6) then FallNumbers18_HABC=2;
else if FallNum18_HABC in (8,.) then FallNumbers18_HABC=.;
end;

run;


proc freq data=fallvar_habc;
table FallNumbers6_HABC FallNumbers7_HABC FallNumbers8_HABC FallNumbers9_HABC FallNumbers10_HABC 
FallNumbers11_HABC FallNumbers12_HABC FallNumbers13_HABC FallNumbers145_HABC FallNumbers15_HABC FallNumbers16_HABC FallNumbers17_HABC FallNumbers18_HABC;
run;



/* Recurrent falls */
data fallvar_habc; set fallvar_habc;

if FallNumbers6_HABC ne . then RecurrentFalls6_HABC=(FallNumbers6_HABC=2);
if FallNumbers7_HABC ne . then RecurrentFalls7_HABC=(FallNumbers7_HABC=2);
if FallNumbers8_HABC ne . then RecurrentFalls8_HABC=(FallNumbers8_HABC=2);
if FallNumbers9_HABC ne . then RecurrentFalls9_HABC=(FallNumbers9_HABC=2);
if FallNumbers10_HABC ne . then RecurrentFalls10_HABC=(FallNumbers10_HABC=2);
if FallNumbers11_HABC ne . then RecurrentFalls11_HABC=(FallNumbers11_HABC=2);
if FallNumbers12_HABC ne . then RecurrentFalls12_HABC=(FallNumbers12_HABC=2);
if FallNumbers13_HABC ne . then RecurrentFalls13_HABC=(FallNumbers13_HABC=2);
if FallNumbers145_HABC ne . then RecurrentFalls145_HABC=(FallNumbers145_HABC=2);
if FallNumbers15_HABC ne . then RecurrentFalls15_HABC=(FallNumbers15_HABC=2);
if FallNumbers16_HABC ne . then RecurrentFalls16_HABC=(FallNumbers16_HABC=2);
if FallNumbers17_HABC ne . then RecurrentFalls17_HABC=(FallNumbers17_HABC=2);
if FallNumbers18_HABC ne . then RecurrentFalls18_HABC=(FallNumbers18_HABC=2);

run;

proc print data=fallvar_habc (obs=30); run;


data created.fallvar_habc; set fallvar_habc;

keep 
habcid FallIND6_HABC FallIND7_HABC FallIND8_HABC FallIND9_HABC FallIND10_HABC FallIND11_HABC FallIND12_HABC 
FallIND13_HABC FallIND145_HABC FallIND15_HABC FallIND16_HABC FallIND17_HABC FallIND18_HABC 

FallNumbers6_HABC FallNumbers7_HABC FallNumbers8_HABC FallNumbers9_HABC FallNumbers10_HABC FallNumbers11_HABC FallNumbers12_HABC
FallNumbers13_HABC FallNumbers145_HABC FallNumbers15_HABC FallNumbers16_HABC FallNumbers17_HABC FallNumbers18_HABC

RecurrentFalls6_HABC RecurrentFalls7_HABC RecurrentFalls8_HABC RecurrentFalls9_HABC RecurrentFalls10_HABC RecurrentFalls11_HABC RecurrentFalls12_HABC
RecurrentFalls13_HABC RecurrentFalls145_HABC RecurrentFalls15_HABC RecurrentFalls16_HABC RecurrentFalls17_HABC RecurrentFalls18_HABC
;

label 
FallIND6_HABC = 'Fall Indicator Harmonized Variable for HABC Y6: Yes(1), No(0)'
FallIND7_HABC = 'Fall Indicator Harmonized Variable for HABC Y7: Yes(1), No(0)'
FallIND8_HABC = 'Fall Indicator Harmonized Variable for HABC Y8: Yes(1), No(0)'
FallIND9_HABC = 'Fall Indicator Harmonized Variable for HABC Y9: Yes(1), No(0)'
FallIND10_HABC = 'Fall Indicator Harmonized Variable for HABC Y10: Yes(1), No(0)'
FallIND11_HABC = 'Fall Indicator Harmonized Variable for HABC Y11: Yes(1), No(0)'
FallIND12_HABC = 'Fall Indicator Harmonized Variable for HABC Y12: Yes(1), No(0)'
FallIND13_HABC = 'Fall Indicator Harmonized Variable for HABC Y13: Yes(1), No(0)'
FallIND145_HABC = 'Fall Indicator Harmonized Variable for HABC Y14.5: Yes(1), No(0)'
FallIND15_HABC = 'Fall Indicator Harmonized Variable for HABC Y15: Yes(1), No(0)'
FallIND16_HABC = 'Fall Indicator Harmonized Variable for HABC Y16: Yes(1), No(0)'
FallIND17_HABC = 'Fall Indicator Harmonized Variable for HABC Y17: Yes(1), No(0)'
FallIND18_HABC = 'Fall Indicator Harmonized Variable for HABC Y18: Yes(1), No(0)'
    
FallNumbers6_HABC = 'Number of Falls Harmonized Variable for HABC Y6: None(0), One time(1), Two or more times(2)'
FallNumbers7_HABC = 'Number of Falls Harmonized Variable for HABC Y7: None(0), One time(1), Two or more times(2)'
FallNumbers8_HABC = 'Number of Falls Harmonized Variable for HABC Y8: None(0), One time(1), Two or more times(2)'
FallNumbers9_HABC = 'Number of Falls Harmonized Variable for HABC Y9: None(0), One time(1), Two or more times(2)'
FallNumbers10_HABC = 'Number of Falls Harmonized Variable for HABC Y10: None(0), One time(1), Two or more times(2)'
FallNumbers11_HABC = 'Number of Falls Harmonized Variable for HABC Y11: None(0), One time(1), Two or more times(2)'
FallNumbers12_HABC = 'Number of Falls Harmonized Variable for HABC Y12: None(0), One time(1), Two or more times(2)'
FallNumbers13_HABC = 'Number of Falls Harmonized Variable for HABC Y13: None(0), One time(1), Two or more times(2)'
FallNumbers145_HABC = 'Number of Falls Harmonized Variable for HABC Y14.5: None(0), One time(1), Two or more times(2)'
FallNumbers15_HABC = 'Number of Falls Harmonized Variable for HABC Y15: None(0), One time(1), Two or more times(2)'
FallNumbers16_HABC = 'Number of Falls Harmonized Variable for HABC Y16: None(0), One time(1), Two or more times(2)'
FallNumbers17_HABC = 'Number of Falls Harmonized Variable for HABC Y17: None(0), One time(1), Two or more times(2)'
FallNumbers18_HABC = 'Number of Falls Harmonized Variable for HABC Y18: None(0), One time(1), Two or more times(2)'

RecurrentFalls6_HABC = 'Recurrent Falls Harmonized Variable for HABC Y6: None or once(0), Twice or more(1)'
RecurrentFalls7_HABC = 'Recurrent Falls Harmonized Variable for HABC Y7: None or once(0), Twice or more(1)'
RecurrentFalls8_HABC = 'Recurrent Falls Harmonized Variable for HABC Y8: None or once(0), Twice or more(1)'
RecurrentFalls9_HABC = 'Recurrent Falls Harmonized Variable for HABC Y9: None or once(0), Twice or more(1)'
RecurrentFalls10_HABC = 'Recurrent Falls Harmonized Variable for HABC Y10: None or once(0), Twice or more(1)'
RecurrentFalls11_HABC = 'Recurrent Falls Harmonized Variable for HABC Y11: None or once(0), Twice or more(1)'
RecurrentFalls12_HABC = 'Recurrent Falls Harmonized Variable for HABC Y12: None or once(0), Twice or more(1)'
RecurrentFalls13_HABC = 'Recurrent Falls Harmonized Variable for HABC Y13: None or once(0), Twice or more(1)'
RecurrentFalls145_HABC = 'Recurrent Falls Harmonized Variable for HABC Y145: None or once(0), Twice or more(1)'
RecurrentFalls15_HABC = 'Recurrent Falls Harmonized Variable for HABC Y15: None or once(0), Twice or more(1)'
RecurrentFalls16_HABC = 'Recurrent Falls Harmonized Variable for HABC Y16: None or once(0), Twice or more(1)'
RecurrentFalls17_HABC = 'Recurrent Falls Harmonized Variable for HABC Y17: None or once(0), Twice or more(1)'
RecurrentFalls18_HABC = 'Recurrent Falls Harmonized Variable for HABC Y18: None or once(0), Twice or more(1)'
;

run;

data created.fallvar_habc_final; set created.fallvar_habc; run;



/*************************/
/* Maximum Grip strength */
/*************************/


proc print data=healthabc (obs=30); 
var habcid P4LTR1 P4LTR2 P4RTR1 P4RTR2 
B3LTR1 B3LTR2 B3RTR1 B3RTR2 
D3LTR1 D3LTR2 D3RTR1 D3RTR2 
F3LTR1 F3LTR2 F3RTR1 F3RTR2 
S8LTR1_Y8 S8LTR2_Y8 S8RTR1_Y8 S8RTR2_Y8 
S8LTR1_Y10 S8LTR2_Y10 S8RTR1_Y10 S8RTR2_Y10;
run;

data gsvar_habc; set healthabc;
array num_vars P4LTR1 P4LTR2 P4RTR1 P4RTR2 
               B3LTR1 B3LTR2 B3RTR1 B3RTR2 
               D3LTR1 D3LTR2 D3RTR1 D3RTR2 
               F3LTR1 F3LTR2 F3RTR1 F3RTR2 
               S8LTR1_Y8 S8LTR2_Y8 S8RTR1_Y8 S8RTR2_Y8 
               S8LTR1_Y10 S8LTR2_Y10 S8RTR1_Y10 S8RTR2_Y10;

do i = 1 to dim(num_vars);
num_vars[i] = input(num_vars[i], ?? best32.);
if missing(num_vars[i]) then num_vars[i] = .;
end;

drop i; 
run;

proc print data=gsvar_habc (obs=30); 
var habcid P4LTR1 P4LTR2 P4RTR1 P4RTR2 
B3LTR1 B3LTR2 B3RTR1 B3RTR2 
D3LTR1 D3LTR2 D3RTR1 D3RTR2 
F3LTR1 F3LTR2 F3RTR1 F3RTR2 
S8LTR1_Y8 S8LTR2_Y8 S8RTR1_Y8 S8RTR2_Y8 
S8LTR1_Y10 S8LTR2_Y10 S8RTR1_Y10 S8RTR2_Y10;
run;

data gsvar_habc; set gsvar_habc;
MaxGS6_HABC = max(P4LTR1, P4LTR2, P4RTR1, P4RTR2); /* Year 1*/
MaxGS7_HABC = max(B3LTR1, B3LTR2, B3RTR1, B3RTR2); /* Year 2*/
MaxGS9_HABC = max(D3LTR1, D3LTR2, D3RTR1, D3RTR2); /* Year 4*/
MaxGS11_HABC = max(F3LTR1, F3LTR2, F3RTR1, F3RTR2); /* Year 6*/
MaxGS13_HABC = max(S8LTR1_Y8, S8LTR2_Y8, S8RTR1_Y8, S8RTR2_Y8); /* Year 8*/
MaxGS15_HABC = max(S8LTR1_Y10, S8LTR2_Y10, S8RTR1_Y10, S8RTR2_Y10); /* Year 8*/
run;


data created.gsvar_habc; set gsvar_habc;

keep 
habcid MaxGS6_HABC MaxGS7_HABC MaxGS9_HABC MaxGS11_HABC MaxGS13_HABC MaxGS15_HABC;

label 
MaxGS6_HABC = 'Maximum Grip Strength (kg) Harmonized Variable for HABC Y6'
MaxGS7_HABC = 'Maximum Grip Strength (kg) Harmonized Variable for HABC Y7'
MaxGS9_HABC = 'Maximum Grip Strength (kg) Harmonized Variable for HABC Y9'
MaxGS11_HABC = 'Maximum Grip Strength (kg) Harmonized Variable for HABC Y11'
MaxGS13_HABC = 'Maximum Grip Strength (kg) Harmonized Variable for HABC Y13'
MaxGS15_HABC = 'Maximum Grip Strength (kg) Harmonized Variable for HABC Y15';

run;



/**********************/
/* Metabolic Syndrome */
/**********************/

*waist > 88 cm for women and > 102 cm for men;
*year 1;
proc means data=habc; var P3AB; run;

data habc1; set habc; 
if gender=2 then do;
if P3AB ne . then mets_wc6=(P3AB>88);
end;

else if gender=1 then do;
if P3AB ne . then mets_wc6=(P3AB>102);
end;
run;

proc freq data=habc1; table mets_wc6 gender*mets_wc6; run;


*triglycerides >= 150 mg/dL;
*year 1, 6, 8, 10, 11;
proc means data=habc; var FAST8TRIG1 FAST8TRIG6 FAST8TRIG8 FAST8TRIG10 FAST8TRIG11; run;

data habc2; set habc1;
if FAST8TRIG1 ne . then mets_tri6=(FAST8TRIG1>=150);
if FAST8TRIG6 ne . then mets_tri11=(FAST8TRIG6>=150);
if FAST8TRIG8 ne . then mets_tri13=(FAST8TRIG8>=150);
if FAST8TRIG10 ne . then mets_tri15=(FAST8TRIG10>=150);
if FAST8TRIG11 ne . then mets_tri16=(FAST8TRIG11>=150);
run;

proc freq data=habc2; table mets_tri6 mets_tri11 mets_tri13 mets_tri15 mets_tri16; run;


*hdl <40 mg/dL for men and < 50 mg/dL for women;
*year 1, 6, 8, 10, 11;
proc means data=habc; var FAST8HDL1 FAST8HDL6 FAST8HDL8 FAST8HDL10 FAST8HDL11; run;

data habc3; set habc2;
if gender=2 then do;
if FAST8HDL1 ne . then mets_hdl6=(FAST8HDL1<50);
if FAST8HDL6 ne . then mets_hdl11=(FAST8HDL6<50);
if FAST8HDL8 ne . then mets_hdl13=(FAST8HDL8<50);
if FAST8HDL10 ne . then mets_hdl15=(FAST8HDL10<50);
if FAST8HDL11 ne . then mets_hdl16=(FAST8HDL11<50);
end;

if gender=1 then do;
if FAST8HDL1 ne . then mets_hdl6=(FAST8HDL1<40);
if FAST8HDL6 ne . then mets_hdl11=(FAST8HDL6<40);
if FAST8HDL8 ne . then mets_hdl13=(FAST8HDL8<40);
if FAST8HDL10 ne . then mets_hdl15=(FAST8HDL10<40);
if FAST8HDL11 ne . then mets_hdl16=(FAST8HDL11<40);
end;
run;

proc freq data=habc3; table mets_hdl6 mets_hdl11 mets_hdl13 mets_hdl15 mets_hdl16; run;


*bp sys >= 130 or dia >= 85;
*year 3, 4, 5, 6, 8, 10, 11;
proc means data=habc; var SYSBP_Y3 SYSBP_Y4 SYSBP_Y5 SYSBP_Y6 SYSBP_Y8 SYSBP_Y10 SYSBP_Y11 
DIABP_Y3 DIABP_Y4 DIABP_Y5 DIABP_Y6 DIABP_Y8 DIABP_Y10 DIABP_Y11; run;

data habc4; set habc3;
if (SYSBP_Y3 ne . and SYSBP_Y3>=130) or (DIABP_Y3 ne . and DIABP_Y3>=85) then mets_bp8=1; else mets_bp8=0;
if (SYSBP_Y4 ne . and SYSBP_Y4>=130) or (DIABP_Y4 ne . and DIABP_Y4>=85) then mets_bp9=1; else mets_bp9=0;
if (SYSBP_Y5 ne . and SYSBP_Y5>=130) or (DIABP_Y5 ne . and DIABP_Y5>=85) then mets_bp10=1; else mets_bp10=0;
if (SYSBP_Y6 ne . and SYSBP_Y6>=130) or (DIABP_Y6 ne . and DIABP_Y6>=85) then mets_bp11=1; else mets_bp11=0;
if (SYSBP_Y8 ne . and SYSBP_Y8>=130) or (DIABP_Y8 ne . and DIABP_Y8>=85) then mets_bp13=1; else mets_bp13=0;
if (SYSBP_Y10 ne . and SYSBP_Y10>=130) or (DIABP_Y10 ne . and DIABP_Y10>=85) then mets_bp15=1; else mets_bp15=0;
if (SYSBP_Y11 ne . and SYSBP_Y11>=130) or (DIABP_Y11 ne . and DIABP_Y11>=85) then mets_bp16=1; else mets_bp16=0;
run;

proc freq data=habc4; table mets_bp8 mets_bp9 mets_bp10 mets_bp11 mets_bp13 mets_bp15 mets_bp16; run;


*gluc >= 110 md/dL;
*year 1, 2, 4, 6, 10, 11;
proc means data=habc; var FAST8GLU1 FAST8GLU2 FAST8GLU4 FAST8GLU6 FAST8GLU10 FAST8GLU11; run;

data habc5; set habc4;
if FAST8GLU1 ne . then mets_glu6=(FAST8GLU1>=110);
if FAST8GLU2 ne . then mets_glu7=(FAST8GLU2>=110);
if FAST8GLU4 ne . then mets_glu9=(FAST8GLU4>=110);
if FAST8GLU6 ne . then mets_glu11=(FAST8GLU6>=110);
if FAST8GLU10 ne . then mets_glu15=(FAST8GLU10>=110);
if FAST8GLU11 ne . then mets_glu16=(FAST8GLU11>=110);
run;

proc freq data=habc5; table mets_glu6 mets_glu7 mets_glu9 mets_glu11 mets_glu15 mets_glu16; run;


*final mets dataset;
data created.mets_habc; set habc5;
keep habcid mets_wc6 gender mets_wc6 mets_tri6 mets_tri11 mets_tri13 mets_tri15 mets_tri16 mets_hdl6 mets_hdl11 mets_hdl13 mets_hdl15 mets_hdl16 
mets_bp8 mets_bp9 mets_bp10 mets_bp11 mets_bp13 mets_bp15 mets_bp16 mets_glu6 mets_glu7 mets_glu9 mets_glu11 mets_glu15 mets_glu16;
run;



/***************/
/* Demographic */
/***************/


************************* RACE SEX ED ****************************************************************;
Data racesexedvar;
set healtHABC;

Race_HABC=Race;
label Race_HABC= 'Race Harmonized Variable for HABC: White (1), Black(2), Asian, American Indian or Alaskan Native(3), Native Hawaiian or other Pacific Islander(4),
Other (5)';
Race_H= Race; 
label Race_H= 'Race Harmonized Variable White (1), Black(2), Asian, American Indian or Alaskan Native(3), Native Hawaiian or other Pacific Islander(4),
Other (5)';

*Gender_H (1=Male, 0=Female);
if gender=1 then Gender_HABC=1;
else if gender=2 then Gender_HABC=0;
else Gender_HABC=.;
label Gender_HABC= 'Gender Harmonized Variable for HABC Male(1), Female(0)';
Gender_H=Gender_HABC;
label Gender_H= 'Gender Harmonized Variable: Male(1), Female(0)';

*EDUCA_H (Definition: 1=less than HS, 2=HS grad, 3=postsecondary);
EDUCA_HABC=educ;
label EDUCA_HABC= 'Education Harmonized Variable for HABC: < than HS(1), HS grad(2), postsecondary(3)';
EDUCA_H=educ;
label EDUCA_H='Education Harmonized Variable: < than HS(1), HS grad(2), postsecondary(3)';
run;

proc contents data=racesexedvar;run;

/*save final age,sex,race dataset*/
data outHABC.racesexedvar_habc;
set racesexedvar;
keep habcid Race_HABC Race_H Gender_HABC Gender_H EDUCA_HABC EDUCA_H;
run;

******************************   AGE   ******************************************************************;

data agevarHABC;
    set healtHABC;
	age6_HABC=cv1age;
age7_HABC=cv2age;
age8_HABC=cv3age;
age9_HABC=cv4age;
age10_HABC=cv5age;
age11_HABC=cv6age;
age12_HABC=cv7age;
age13_HABC=cv8age;
age14_HABC=cv9age;
age15_HABC=cv10age;
age16_HABC=cv11age;
age17_HABC=cv12age;
age18_HABC=cv13age;
run;

proc print data=agevarHABC; var age16_HABC cv11age;run;

/*now, I need to make copies of all the variables so that I can create versions that are imputated*/
data agevarHABC_interpolated;
set agevarHABC;
ageINT6_HABC= age6_HABC;
ageINT7_HABC= age7_HABC;
ageINT8_HABC= age8_HABC;
ageINT9_HABC= age9_HABC;
ageINT10_HABC= age10_HABC;
ageINT11_HABC= age11_HABC;
ageINT12_HABC= age12_HABC;
ageINT13_HABC= age13_HABC;
ageINT14_HABC= age14_HABC;
ageINT15_HABC= age15_HABC;
ageINT16_HABC= age16_HABC;
ageINT17_HABC= age17_HABC;
ageINT18_HABC= age18_HABC;
run;

/*I need to impute any missing data- ie. if there are any non-missing values in the ageX_HABC variables (all with the same suffix), 
then it will take the data point right before the missing and add one (unless its missing) to the continuous variable*/
data agevar_imputedHABC;
    set agevarHABC_interpolated; 

    /* Define the variables to check */
    array vars /*age1_HABC age2_HABC age3_HABC age4_HABC age5_HABC*/ ageINT6_HABC ageINT7_HABC ageINT8_HABC ageINT9_HABC ageINT10_HABC ageINT11_HABC ageINT12_HABC
				ageINT13_HABC ageINT14_HABC ageINT15_HABC ageINT16_HABC ageINT17_HABC ageINT18_HABC;

    do i = 1 to dim(vars); /* Loop through all variables */
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

proc print data=agevar_imputedHABC; var ageINT6_HABC ageINT7_HABC ageINT8_HABC ageINT9_HABC ageINT10_HABC ageINT11_HABC ageINT12_HABC
				ageINT13_HABC ageINT14_HABC ageINT15_HABC ageINT16_HABC ageINT17_HABC ageINT18_HABC;run;

/*creating a flag variable to see if there are any situations where there are more than 2 years between ages in consecutive years. This would indicate a problem*/
data agevar_flagHABC;
set agevar_imputedHABC;
array vars ageINT6_HABC ageINT7_HABC ageINT8_HABC ageINT9_HABC ageINT10_HABC ageINT11_HABC ageINT12_HABC
				ageINT13_HABC ageINT14_HABC ageINT15_HABC ageINT16_HABC ageINT17_HABC ageINT18_HABC;

    ageflag_HABC = 0;/* Initialize the flag variable */
    		 /* Loop through the variables in the array */
    do i = 1 to dim(vars)-1; /* Loop from first to second-to-last variable */
        /* Check for non-missing values and calculate the difference */
        if not missing(vars[i]) and not missing(vars[i+1]) then do;
            if abs(vars[i+1] - vars[i]) > 2 then ageflag_HABC = 1; /* Set flag if difference > 2 */
        end;
    end;

    drop i; /* Drop the loop counter variable for cleaner output */
	label ageflag_HABC= 'Flag (1) where >2 years between ages in consecutive years';
run;

data agevar_flagHABC;
set agevar_flagHABC;	
label age6_HABC= 'Age(years) Harmonized Variable for HABC Y6'
age7_HABC= 'Age(years) Harmonized Variable for HABC Y7'
age8_HABC= 'Age(years) Harmonized Variable for HABC Y8'
age9_HABC= 'Age(years) Harmonized Variable for HABC Y9'
age10_HABC= 'Age(years) Harmonized Variable for HABC Y10'
age11_HABC= 'Age(years) Harmonized Variable for HABC Y11'
age12_HABC= 'Age(years) Harmonized Variable for HABC Y12'
age13_HABC= 'Age(years) Harmonized Variable for HABC Y13'
age14_HABC= 'Age(years) Harmonized Variable for HABC Y14'
age15_HABC= 'Age(years) Harmonized Variable for HABC Y15'
age16_HABC= 'Age(years) Harmonized Variable for HABC Y16'
age17_HABC= 'Age(years) Harmonized Variable for HABC Y17'
age18_HABC= 'Age(years) Harmonized Variable for HABC Y18'
ageINT6_HABC= 'Age(years) Harmonized Variable for HABC Y6- Interpolated'
ageINT7_HABC= 'Age(years) Harmonized Variable for HABC Y7- Interpolated'
ageINT8_HABC= 'Age(years) Harmonized Variable for HABC Y8- Interpolated'
ageINT9_HABC= 'Age(years) Harmonized Variable for HABC Y9- Interpolated'
ageINT10_HABC= 'Age(years) Harmonized Variable for HABC Y10- Interpolated'
ageINT11_HABC= 'Age(years) Harmonized Variable for HABC Y11- Interpolated'
ageINT12_HABC= 'Age(years) Harmonized Variable for HABC Y12- Interpolated'
ageINT13_HABC= 'Age(years) Harmonized Variable for HABC Y13- Interpolated'
ageINT14_HABC= 'Age(years) Harmonized Variable for HABC Y14- Interpolated'
ageINT15_HABC= 'Age(years) Harmonized Variable for HABC Y15- Interpolated'
ageINT16_HABC= 'Age(years) Harmonized Variable for HABC Y16- Interpolated'
ageINT17_HABC= 'Age(years) Harmonized Variable for HABC Y17- Interpolated'
ageINT18_HABC= 'Age(years) Harmonized Variable for HABC Y18- Interpolated';
run;

proc freq data=agevar_flagHABC;
tables ageflag_HABC;
run;
/*There is one participant- ID 1215 that has an incorrect age for age17_HABC/cv12age.  the ages go up sequentially to 80, then down to 76. Will change to 81.*/

/*manually fix one participant*/
proc sql;
update agevar_flagHABC
set age17_HABC=81
where HABCid=1215;quit;

proc sql;
update agevar_flagHABC
set ageINT17_HABC=81
where HABCid=1215;quit;

proc sql;
update agevar_flagHABC
set ageflag_HABC=0
where HABCid=1215;quit;

/*check that it worked*/
proc print data= agevar_flagHABC; var age6_HABC age7_HABC age8_HABC age9_HABC age10_HABC age11_HABC age12_HABC
				age13_HABC age14_HABC age15_HABC age16_HABC ageINT17_HABC age18_HABC;
where HABCid=1215;
run;

/*now,put the id and only the variables I need in this dataset*/
data outHABC.agevar_HABC;
set agevar_flagHABC;
keep HABCid age6_HABC age7_HABC age8_HABC age9_HABC age10_HABC age11_HABC age12_HABC
				age13_HABC age14_HABC age15_HABC age16_HABC age17_HABC age18_HABC ageINT6_HABC ageINT7_HABC ageINT8_HABC ageINT9_HABC ageINT10_HABC ageINT11_HABC ageINT12_HABC
				ageINT13_HABC ageINT14_HABC ageINT15_HABC ageINT16_HABC ageINT17_HABC ageINT18_HABC ageflag_HABC;
run;

 
************************************************************************************************************
/*Height: no changes in units-->mm  **There is a different variable per year it was taken*/
************************************************************************************************************;

data heightHABC1;
    set healtHABC;
	height6_HABC=P2SH;/*HABC year 1- Y1Calc*/
	height9_HABC=D2SH;/*HABC year 4- Y4calc*/
	height11_HABC=F3SH;/*HABC year 6- Y6Calc*/
	height13_HABC=S4SH_Y8;/*HABC year 8-Y8Calc*/
	height15_HABC=S4SH_Y10;/*HABC year 10-Y10Calc*/
	height16_HABC=Y11SH;/*HABC year 11- jimmie*/
						/* no HABC year 12 var  or Y13 var*/
	run;


proc print data=heightHABC1; var 
height6_HABC P2SH
	height9_HABC D2SH
	height11_HABC F3SH
	height13_HABC S4SH_Y8
	height15_HABC S4SH_Y10
	height16_HABC Y11SH;
	run;

proc univariate data=heightHABC1; var height6_HABC height9_HABC height11_HABC F3SH height13_HABC height15_HABC height16_HABC; run;

/*create an Interpolated variable for next step by making copies, making the variables without values missing so we can interpolate them as well.*/
data heightHABC;
set heightHABC1;
heightINT6_HABC= height6_HABC;
heightINT7_HABC= .;
heightINT8_HABC= .;
heightINT9_HABC= height9_HABC;
heightINT10_HABC= .;
heightINT11_HABC= height11_HABC;
heightINT12_HABC= .;
heightINT13_HABC= height13_HABC;
heightINT14_HABC= .;
heightINT15_HABC= height15_HABC;
heightINT16_HABC= height16_HABC;
heightINT17_HABC= height16_HABC;
heightINT18_HABC= height16_HABC;
run;

/* Perform Linear Interpolation for missing heights*/

/* Reshape from Wide to Long Format */
proc transpose data=heightHABC out=long_data(rename=(col1=Height)) name=YEARVar;
    by HABCid;
    var heightINT6_HABC heightINT7_HABC heightINT8_HABC heightINT9_HABC heightINT10_HABC heightINT11_HABC heightINT12_HABC heightINT13_HABC heightINT14_HABC heightINT15_HABC heightINT16_HABC heightINT17_HABC heightINT18_HABC;
run;

/*  Extract YEAR from Variable Names */
data long_datayr;
    set long_data;
	if YEARVar = 'heightINT6_HABC' then YEAR = 6;
	else if YEARVar = 'heightINT7_HABC' then YEAR = 7;
	else if YEARVar = 'heightINT8_HABC' then YEAR = 8;
	else if YEARVar = 'heightINT9_HABC' then YEAR = 9;
	else if YEARVar = 'heightINT10_HABC' then YEAR = 10;
    else if YEARVar = 'heightINT11_HABC' then YEAR = 11;
	else if YEARVar = 'heightINT12_HABC' then YEAR = 12;
    else if YEARVar = 'heightINT13_HABC' then YEAR = 13;
	else if YEARVar = 'heightINT14_HABC' then YEAR = 14;
    else if YEARVar = 'heightINT15_HABC' then YEAR = 15;
	else if YEARVar = 'heightINT16_HABC' then YEAR = 16;
    else if YEARVar = 'heightINT17_HABC' then YEAR = 17;
	else if YEARVar = 'heightINT18_HABC' then YEAR = 18;
    *drop YEARVar;
run;

/* Sort Data by ID and YEAR */
proc sort data=long_datayr noduprecs;
    by HABCID YEAR;
run;


/*Create Prev_YEAR and Prev_Height */
data your_dataset;
    set long_datayr;
	by HABCid YEAR;
retain Prev_YEAR Prev_Height; /* Retain values across iterations */

       /* First observation per ID: Set Prev_YEAR and Prev_Height to missing */
    if first.HABCid then do;
        Prev_YEAR = .;
        Prev_Height = .;
    end; 

    if not missing(Height) then do;
        Prev_YEAR = YEAR;
        Prev_Height = Height;
    end;
        

run;

/* Create Next_YEAR and Next_Height */
proc sort data=your_dataset;   by HABCid descending YEAR; run;

/* Find the closest future value */
proc sort data=your_dataset;
    by HABCid descending YEAR;
run;

data your_dataset2;
    set your_dataset;
    by HABCid descending YEAR;
    
    retain Next_YEAR Next_Height; /* Retain values across iterations */
    
  /* Last observation per ID: Set Next_YEAR and Next_Height to missing */
    if first.HABCid then do;
        Next_YEAR = .;
        Next_Height = .;
    end;

    if not missing(Height) then do;
        Next_YEAR = YEAR;
        Next_Height = Height;
    end;
   
run;
/* Re-sort the dataset to restore chronological order */
proc sort data=your_dataset2;
    by HABCid YEAR;
run;

/*make sure there aren't any missing for id*/
proc freq data=your_dataset2;tables HABCid/missing;run;

/*make sure no year is repeated per id*/
proc sort data=your_dataset2 nodupkey;
    by HABCid YEAR;
run;

/*make sure habcid is numeric*/
proc contents data=your_dataset2;
run;

data your_dataset3;
    set your_dataset2;

  /* Apply interpolation only when both previous and next heights exist */
    if missing(Height) and not missing(Prev_Height) and not missing(Next_Height) then do;
        Height = Prev_Height + ((Next_Height - Prev_Height) / (Next_YEAR - Prev_YEAR)) * (YEAR - Prev_YEAR);
		
    end;

if Height_I=. then Height_I=Height;
    /* Debugging Print */
    put "HABCid=" HABCid " YEAR=" YEAR " Height=" Height
        " Prev_YEAR=" Prev_YEAR " Prev_Height=" Prev_Height
        " Next_YEAR=" next_YEAR " Next_Height=" next_Height;
    
    drop i;
run;
/*this worked!*/

/*check for duplicate by variable issues*/
proc sql;
select HABCid, YEAR, count(*)
from your_dataset3
group by HABCid, YEAR
having count(*) > 1;
quit;
/*none*/

/* Reshape Data Back to Wide Format */
proc transpose data=your_dataset3 out=final_data(drop=_NAME_) prefix=height;
    by HABCid;
    id YEAR;
    var Height;
run;

/*Rename Variables to Match Original Format and then label harmonized variable */
data final_data;
    set final_data;
    rename Height6 = HeightINT6_HABC
Height7 = HeightINT7_HABC
Height8 = HeightINT8_HABC
Height9 = HeightINT9_HABC
Height10 = HeightINT10_HABC
Height11 = HeightINT11_HABC
Height12 = HeightINT12_HABC
Height13 = HeightINT13_HABC
Height14 = HeightINT14_HABC
Height15 = HeightINT15_HABC
Height16 = HeightINT16_HABC
Height17 = HeightINT17_HABC
Height18 = HeightINT18_HABC; /*should we take height 16 and carry it forward for 17 and 18?*/
	run;


proc sql;
create table combined as 
select a.*, b.Height6_HABC, b.Height9_HABC, b.Height11_HABC, b.Height13_HABC,b.Height15_HABC, b.Height16_HABC
from final_data as a LEFT JOIN heightHABC1 as b
on a.HABCid = b.HABCid;
quit;




/*Print Final Dataset */
proc print data=final_data noobs;
    title "Final Dataset with Interpolated Heights";
run;


/*Now, look at differences in height to make sure there are no oddities in the data. for the thresholds for "acceptable change in height" 
(due to spinal disc compression, posture changes, etc.) without concern:
			> 10mm loss and >5 mm gain per year   */

data height_flagHABC;
    set combined;
    
    array heights(*) heightINT6_HABC heightINT7_HABC heightINT8_HABC heightINT9_HABC heightINT10_HABC heightINT11_HABC heightINT12_HABC heightINT13_HABC heightINT14_HABC heightINT15_HABC heightINT16_HABC heightINT17_HABC heightINT18_HABC;; /* Adjust variable names accordingly */
    array flag_vars(*) flag1-flag5; /* There will be one less flag variable than height variables */

    do i = 1 to 5;
        if heights[i] ne . and heights[i+1] ne . then do;
            diff = heights[i+1] - heights[i];
            if diff < -10 or diff > 5 then flag_vars[i] = 1; 
            else flag_vars[i] = 0;
        end;
        else flag_vars[i] = .; /* Missing if one of the values is missing */
    end;

    drop i diff;
run;

proc print data=height_flagHABC; var height6_HABC height9_HABC flag1 flag2 flag3 flag4 flag5; run;

proc freq data=height_flagHABC; tables flag1 flag2 flag3 flag4 flag5; run;
/*none for flag 3.*/

proc freq data=height_flagHABC;table flag1 flag2 difference_var1 /*difference_var2*/;run;

proc univariate data=height_flagHABC;var difference_var1 /*difference_var2*/;run;
/*figure out what differences are a problem. then recode above.*/



proc print data=height_flagHABC;
    where Flag = 1;
	var height6_HABC height9_HABC height11_HABC height13_HABC height15_HABC height16_HABC;
    title "Observations with Consecutive Yearly Increase > 15 mm";
run;


/*Combine the flags into 1*/
data combined_flag;
set height_flagHABC;
IF flag1= 1 OR flag2= 1 OR flag3= 1 OR flag4= 1 OR flag5= 1 THEN heightflag_HABC=1;
else if flag1= . AND flag2= . AND flag3= . AND flag4= . AND flag5= . THEN heightflag_HABC=.;
else heightflag_HABC=0;
run;

/*drop the other flags*/
data combined_flag;
set combined_flag;
drop flag1 flag2 flag3 flag4 flag5;
run;

/*label*/
data combined2;
set combined_flag;
label Height6_HABC=  'Height (mm) Harmonized Variable for HABC Y6'
Height9_HABC=  'Height (mm) Harmonized Variable for HABC Y9'
Height11_HABC=  'Height (mm) Harmonized Variable for HABC Y11'
Height13_HABC=  'Height (mm) Harmonized Variable for HABC Y13'
Height15_HABC=  'Height (mm) Harmonized Variable for HABC Y15'
Height16_HABC=  'Height (mm) Harmonized Variable for HABC Y16'
HeightINT6_HABC=  'Height (mm) Harmonized Variable for HABC Y6- Interpolated'
HeightINT7_HABC=  'Height (mm) Harmonized Variable for HABC Y7- Interpolated'
HeightINT8_HABC=  'Height (mm) Harmonized Variable for HABC Y8- Interpolated'
HeightINT9_HABC=  'Height (mm) Harmonized Variable for HABC Y9- Interpolated'
HeightINT10_HABC=  'Height (mm) Harmonized Variable for HABC Y10- Interpolated'
HeightINT11_HABC=  'Height (mm) Harmonized Variable for HABC Y11- Interpolated'
HeightINT12_HABC=  'Height (mm) Harmonized Variable for HABC Y12- Interpolated'
HeightINT13_HABC=  'Height (mm) Harmonized Variable for HABC Y13- Interpolated'
HeightINT14_HABC=  'Height (mm) Harmonized Variable for HABC Y14- Interpolated'
HeightINT15_HABC=  'Height (mm) Harmonized Variable for HABC Y15- Interpolated'
HeightINT16_HABC=  'Height (mm) Harmonized Variable for HABC Y16- Interpolated'
HeightINT17_HABC=  'Height (mm) Harmonized Variable for HABC Y17- Interpolated, Carried forward from Y16'
HeightINT18_HABC=  'Height (mm) Harmonized Variable for HABC Y18- Interpolated, Carried forward from Y16'
heightflag_HABC= 'HABC Flag (1) if height increases by >5mm/year or decreases by >10mm/year ';
run;

/*save final height dataset*/
data outHABC.heightvar_HABC;
set combined2;
run;


/*********************/
/* Physical activity */
/*********************/


/*We need everything in the same units, but MrOS's PA is based on a scale with propietary weights so the best way to harmonize is to 
Standardize each study individually and then harmonize:
The reason I am standardizing each separately rather than combining the 3 first and standardizing it is to prevent the larger dataset 
(if we'd already combined the 3 into 1) or those with higher variance (possibly the MrOS dataset) to disproportionately influence the overall 
scaling factors- biasing the standardization. 
 

Should ask Elsa if we need to Harmonize the other three studies into a harmonized variable too.
.*/

/*only have this data for 1 year... should I carry it forward?*/
/*set up dataset and check mean/sd*/
data PhysActivityVar;
set healtHABC;
PhysActivity6_HABC=TOTKKWK;
Keep habcid PhysActivity6_HABC;
run;

proc univariate data=PhysActivityVar; var PhysActivity6_HABC; histogram PhysActivity6_HABC;run;
/*mean 82.9 (std=69.3), med=64.5 (IQR=68.1) This distribution is strongly skewed right, which will bias results
if I standardize while being this skewed.*/

/*try sqrt*/
*data PhysActivityVar;
 *   set PhysActivityVar;
  *  PhysActivity6_HABC_L = sqrt(PhysActivity6_HABC);
	*keep habcid PhysActivity6_HABC_L ;
*run;

/*check again*/
proc univariate data=PhysActivityVar; var PhysActivity6_HABC_L; histogram PhysActivity6_HABC_L;run;




/*Need to log transform this variable to try to make it Normal*/
data PhysActivityVar;
    set PhysActivityVar;
    PhysActivity6_HABC_L = log(PhysActivity6_HABC);
	keep habcid PhysActivity6_HABC_L ;
run;

/*check again*/
proc univariate data=PhysActivityVar; var PhysActivity6_HABC_L; histogram PhysActivity6_HABC_L;run;

/*rename the variable and then standardize it */
data PhysActivityVar;
    set PhysActivityVar;
	rename PhysActivity6_HABC_L=PhysActivity6_HABC;
run;

PROC STANDARD DATA=PhysActivityVar MEAN=0 STD=1 OUT=PhysActivityVar_std;
  VAR PhysActivity6_HABC;
  run;

data PhysActivityVar_std2;
    set PhysActivityVar_std;
	PhysActivity7_HABC= PhysActivity6_HABC;
PhysActivity8_HABC= PhysActivity6_HABC;
PhysActivity9_HABC= PhysActivity6_HABC;
PhysActivity10_HABC= PhysActivity6_HABC;
PhysActivity11_HABC= PhysActivity6_HABC;
PhysActivity12_HABC= PhysActivity6_HABC;
PhysActivity13_HABC= PhysActivity6_HABC;
PhysActivity14_HABC= PhysActivity6_HABC;
PhysActivity15_HABC= PhysActivity6_HABC;
PhysActivity16_HABC= PhysActivity6_HABC;
PhysActivity17_HABC= PhysActivity6_HABC;
PhysActivity18_HABC= PhysActivity6_HABC;
    label PhysActivity6_HABC= 'Physical Activity for HABC(kcal/kg/week), NFFI year6- log transformed and standardized'
PhysActivity7_HABC= 'Physical Activity for HABC(kcal/kg/week), NFFI year7- log transformed and standardized- CARRIED FORWARD FROM NFFI Y6'
PhysActivity8_HABC= 'Physical Activity for HABC(kcal/kg/week), NFFI year8- log transformed and standardized- CARRIED FORWARD FROM NFFI Y6'
PhysActivity9_HABC= 'Physical Activity for HABC(kcal/kg/week), NFFI year9- log transformed and standardized- CARRIED FORWARD FROM NFFI Y6'
PhysActivity10_HABC= 'Physical Activity for HABC(kcal/kg/week), NFFI year10- log transformed and standardized- CARRIED FORWARD FROM NFFI Y6'
PhysActivity11_HABC= 'Physical Activity for HABC(kcal/kg/week), NFFI year11- log transformed and standardized- CARRIED FORWARD FROM NFFI Y6'
PhysActivity12_HABC= 'Physical Activity for HABC(kcal/kg/week), NFFI year12- log transformed and standardized- CARRIED FORWARD FROM NFFI Y6'
PhysActivity13_HABC= 'Physical Activity for HABC(kcal/kg/week), NFFI year13- log transformed and standardized- CARRIED FORWARD FROM NFFI Y6'
PhysActivity14_HABC= 'Physical Activity for HABC(kcal/kg/week), NFFI year14- log transformed and standardized- CARRIED FORWARD FROM NFFI Y6'
PhysActivity15_HABC= 'Physical Activity for HABC(kcal/kg/week), NFFI year15- log transformed and standardized- CARRIED FORWARD FROM NFFI Y6'
PhysActivity16_HABC= 'Physical Activity for HABC(kcal/kg/week), NFFI year16- log transformed and standardized- CARRIED FORWARD FROM NFFI Y6'
PhysActivity17_HABC= 'Physical Activity for HABC(kcal/kg/week), NFFI year17- log transformed and standardized- CARRIED FORWARD FROM NFFI Y6'
PhysActivity18_HABC= 'Physical Activity for HABC(kcal/kg/week), NFFI year18- log transformed and standardized- CARRIED FORWARD FROM NFFI Y6';
run;

  proc univariate data= PhysActivityVar_std2;
  where PhysActivity6_HABC > 2.5 OR PhysActivity6_HABC <=- 2.5;
  run;
/*n=38 obs outside of 2.5 SDs,  (there were 82 with log)
  10 obs outside of 3 SD,
  highest/lowest is 3.4 and -3.06*/


proc univariate data=PhysActivityVar_std2; var PhysActivity6_HABC; histogram PhysActivity6_HABC;run;

/*save final PA dataset*/
data outHABC.PhysActivityvar_HABC;
set PhysActivityVar_std2;
run;


proc contents data=outHABC.PhysActivityvar_HABC;run;


/****************/
/* Vital status */
/****************/


**************************************************************************************************************************
												Creating a "Died before 90 variable"
**************************************************************************************************************************;


/*finding the difference between DOB and DOD*/
data age_at_death;
    set healthabc;
   ageatdeath_HABC = yrdif(DOB, DOD, 'AGE');
run;

proc contents data=age_at_death;run;

proc print data= age_at_death;var ageatdeath_HABC habcid DOD DOB CV1AGE CV2AGE CV3AGE CV4AGE CV5AGE CV6AGE CV7AGE CV8AGE CV9AGE CV10AGE CV11AGE CV12AGE CV13AGE; where ageatdeath_HABC < max(CV1AGE, CV2AGE, CV3AGE, CV4AGE, CV5AGE, CV6AGE, CV7AGE, CV8AGE, CV9AGE, CV10AGE, CV11AGE, CV12AGE, CV13AGE); run;


proc print data=age_at_death;run;

/*indicator variable*/
data die_before_90;
set age_at_death;
if 1 < ageatdeath_HABC <90 then dieafter90_HABC=0;
else if ageatdeath_HABC>=90 then dieafter90_HABC=1;
else if ageatdeath_HABC=. then dieafter90_HABC= .;
run;

proc freq data=die_before_90; table dieafter90_HABC;run;
proc contents data=die_before_90;run;
/*n=253 died after 90*/

/*save variable*/
data outHABC.Dieafter90var_HABC;
set die_before_90;
keep habcid ageatdeath_HABC dieafter90_HABC;
run;

proc print data= outHABC.Dieafter90var_HABC;var ageatdeath_HABC id ageint6_HABC-ageint18_HABC; where DOD ne . AND DOB = .; run;



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


/*for some reason, we only have vital statuses up to y11 in our dataset, so will need to go in and pull y12 and 13 data. so after i get those, re-run this code*/
/*first change Z's to missing*/

/*Create vital status variables*/
proc contents data=healthabc;run;

data harmonized_simple;
    set healthabc;

    /* --- Step 1: Copy study VITALs to harmonized VITALs --- */
    
VITAL1=1;
VITAL2=VITAL12M;
VITAL3=VITAL24M;
VITAL4=VITAL36M;
VITAL5=VITAL48M;
VITAL6=VITAL60M;
VITAL7=VITAL72M;
VITAL8=VITAL84M;
VITAL9=VITAL96M;
VITAL10=VITAL108M;
VITAL11=VITAL120M;
VITAL12=VITAL132M;
VITAL13=VITAL144M;


	/*replace death=2 with death=0 and replace Z with .*/
    array vital[1:13] Vital1 - Vital13;

    do i = 1 to 13;
        if vital[i] = 2 then vital[i] = 0;
		if vital[i] = .Z then vital[i]= .;
    end;

    drop i;

    /* --- Step 2: Create STATUS based on VITAL10 --- */
      if vital13=1 then STATUS_HABC=1; /*ALIVE through end*/
	  else if vital13=0 then STATUS_HABC=0; /*DIED before end*/
	  else STATUS_HABC=2; /*ltfu*/
 
run;

data outHABC.vitalstatus_HABC; 
set harmonized_simple (rename=(
VITAL1=VITAL6_HABC
VITAL2=VITAL7_HABC
VITAL3=VITAL8_HABC
VITAL4=VITAL9_HABC
VITAL5=VITAL10_HABC
VITAL6=VITAL11_HABC
VITAL7=VITAL12_HABC
VITAL8=VITAL13_HABC
VITAL9=VITAL14_HABC
VITAL10=VITAL15_HABC
VITAL11=VITAL16_HABC
VITAL12=VITAL17_HABC
VITAL13=VITAL18_HABC));
keep habcid VITAL6_HABC
VITAL7_HABC
VITAL8_HABC
VITAL9_HABC
VITAL10_HABC
VITAL11_HABC
VITAL12_HABC
VITAL13_HABC
VITAL14_HABC
VITAL15_HABC
VITAL16_HABC
VITAL17_HABC
VITAL18_HABC;
run;

proc contents data=outHABC.vitalstatus_HABC;run;
