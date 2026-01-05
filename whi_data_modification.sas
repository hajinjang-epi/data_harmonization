/******************************************************************************/
/**************************** CREATING WHI DATASET ****************************/
/**************************** HAJIN JANG, 5/17/24 *****************************/
/******************************************************************************/

/******************************** Last updated: *******************************/
* 5/20/24, Hajin Jang;
* 5/21/24, Hajin Jang;
* 5/23/24, Hajin Jang;
* 5/24/24, Hajin Jang;
* 6/11/24, Hajin Jang;
* 6/23/24, Hajin Jang;
/******************************************************************************/
/******************************************************************************/


/* LIBRARY */
*Set folders to import/save datasets;
libname a "Z:\Jang"; *where I save modified datasets;
libname b "Z:\Jang\SAS Data copies"; *where copy datasets are stored;


/* MACRO */
*readin(data): Read in dataset;
%macro readin(data);
data &data.;
	set b.&data.;
run;
%mend readin;

*keep(data,var): Only keep specified variables;
%macro keep(data,var);
data &data.;
	set &data.;
	keep &var.;
run;
%mend keep;

*rename(data,old,new): Rename;
%macro rename(data,old,new);
data &data.;
	set &data.;
	rename &old.=&new.;
run;
%mend rename;

*merge(data1,data2): Merge;
%macro merge(data1,data2);
proc sort data=&data1.; by id; run;
proc sort data=&data2.; by id; run;
data whi_merge;
	merge &data1. &data2.;
	by id;
run;
%mend merge;

*merge2(data1,data2): Merge for transposed data;
%macro merge2(data1,data2);
data whi_merge;
	merge &data1. &data2.;
	by id;
run;
%mend merge2;

*nodup(data,identifier): Removes rows that are exact duplicates across all variables after 'keep' procedure;
%macro nodup(data,identifier);
proc sort data=&data. nodup;
	by &identifier.;
run;
%mend nodup;

*order(data,identifier): Give order;
%macro order(data,identifier);
proc sort data=&data.;
	by &identifier.;
run;
data &data.; set &data.;
	by id;
	if first.id then event_number=1;
	else event_number+1;
run;
%mend order;

*transpose(data,var,out): Transpose long to wide;
%macro transpose(data,var,out);
proc transpose data=&data. out=&out. (drop=_name_) prefix=&var._;
	by id;
	id event_number;
	var &var.;
run;
%mend transpose;



/******************************************************************************/
/* ONE ROW PER PARTICIPANT */
/* The datasets have one row per ID, so transposing is unnecessary. The detailed steps for merging are specified below */

/* 1. Read in the dataset using the "%readin(data)" macro. */
/* 2. Keep only the necessary variables using the "%keep(data,var)" macro. */
/* 3. Rename variables if needed to avoid overwriting using the "%rename(data,old,new)" macro. */
/* 4. Merge the variables with the previously merged "whi_merge" dataset using the "%merge(data1,data2)" macro. */

*dem_ctos_inv;
%readin(dem_ctos_inv);
%keep(dem_ctos_inv, ID AGE EXT2DAYS INCOME EDUC REGION);

*f2_ctos_inv;
%readin(f2_ctos_inv);
%keep(f2_ctos_inv, ID F2DAYS DIAB DIABAGE TIA CHF_F2 STROKE BRCA_F2 COLON_F2 MELAN_F2 SKIN_F2 ENDO_F2 OTHCA10Y INSULINW); 
%rename(f2_ctos_inv, TIA, TIA_f2);
%rename(f2_ctos_inv, STROKE, STROKE_f2);
%rename(f2_ctos_inv, INSULINW, INSULINW_f2);
%merge(dem_ctos_inv,f2_ctos_inv);

*f41_imputed_ctos_inv;
%readin(f41_imputed_ctos_inv);
%keep(f41_imputed_ctos_inv, ID RACENIH ETHNICNIH);
%merge(whi_merge,f41_imputed_ctos_inv);

*f20_ctos_inv;
%readin(f20_ctos_inv);
%keep(f20_ctos_inv, ID MARITAL CAREPROV);
%merge(whi_merge,f20_ctos_inv);

*outc_death_all_discovered_inv;
%readin(outc_death_all_discovered_inv);
%keep(outc_death_all_discovered_inv, ID DEATHALLDY DEATHALLSRC DEATHALLCAUSE DEATHALLCAUSESRC DEATHALL);
%merge(whi_merge,outc_death_all_discovered_inv);

*outc_ct_os_inv;
%readin(outc_ct_os_inv);
%keep(outc_ct_os_inv, ID DEATHSRC DEATHCAUSE DEATHCAUSESRC DEATH DEATHDY CHDDY CHD MI MIDY ANGINA ANGINADY TIA TIADY CHF PE STROKE ANYCANCER);
%rename(outc_ct_os_inv, DEATHSRC, DEATHSRC_outc);
%rename(outc_ct_os_inv, DEATHCAUSE, DEATHCAUSE_outc);
%rename(outc_ct_os_inv, DEATHCAUSESRC, DEATHCAUSESRC_outc);
%rename(outc_ct_os_inv, DEATH, DEATH_outc);
%rename(outc_ct_os_inv, DEATHDY, DEATHDY_outc);
%rename(outc_ct_os_inv, TIA, TIA_outc);
%rename(outc_ct_os_inv, TIADY, TIADY_outc);
%rename(outc_ct_os_inv, STROKE, STROKE_outc);
%merge(whi_merge,outc_ct_os_inv);

*f155_ctos_inv;
%readin(f155_ctos_inv);
%keep(f155_ctos_inv, ID LIVALN LIVPRT LIVCHLD LIVREL LIVFRNDS MEETFAMILY SOCSUPP HAPPY HEARING VISION HLTHC1Y SMOKNOW ALCOFTEN GROCSHOP FEEDSELF TAKEMEDS LIFTGROC BENDING RESTSLP TRBSLEEP 
BACKSLP UPEARLY NAP MEDSLEEP FALLSLP CALCIUM HRPST1YR);
%rename(f155_ctos_inv, trbsleep, trbsleep_f155);
%rename(f155_ctos_inv, backslp, backslp_f155);
%rename(f155_ctos_inv, upearly, upearly_f155);
%merge(whi_merge,f155_ctos_inv);

*f157_ctos_inv;
%readin(f157_ctos_inv);
%keep(f157_ctos_inv, ID CURLIV);
%rename(f157_ctos_inv, CURLIV, CURLIV_f157);
%merge(whi_merge, f157_ctos_inv);

*f31_ctos_inv;
%readin(f31_ctos_inv);
%keep(f31_ctos_inv, ID ANYMENSA MENO);
%merge(whi_merge, f31_ctos_inv);

*f301_whills_inv;
%readin(f301_whills_inv);
%keep(f301_whills_inv, ID EXAMDY HEIGHT WEIGHT WAIST SYSTBP1 SYSTBP2 DIASBP1 DIASBP2 GRIPDOM LGRIPSTR1 LGRIPSTR2 RGRIPSTR1 RGRIPSTR2 SBSTIME RPTCHRTIME RPTCHRSTNDCOMP SNGCHRSTNDCOMP 
SEMITDMCOMP SEMITDMTIME ONELEGCOMP ONELEGTIME1 ONELEGTIME2 TDMTIME1 TDMTIME2 ASSTDVC TMDWALKTIME1 TMDWALKTIME2 EPESESPPB LASPPB DIABMEDS TMDWALKTIME1 TMDWALKTIME2 COURSLEN);
%rename(f301_whills_inv, DIABMEDS, DIABMEDS_f301);
%merge(whi_merge, f301_whills_inv);

*f34_ctos_inv;
%readin(f34_ctos_inv);
%keep(f34_ctos_inv, ID SMOKING ALCOHOL SMOKEVR PACKYRS CIGSDAY QSMOKAGE ALCSWK);
%merge(whi_merge, f34_ctos_inv);

*f159_ctos_inv;
%readin(f159_ctos_inv);
%keep(f159_ctos_inv, ID GAINED10LB2YRS LOST10LB2YRS TRYLOSEWEIGHT TRBSLEEP BACKSLP UPEARLY WAKENGHT);
%rename(f159_ctos_inv, TRBSLEEP, TRBSLEEP_f159);
%rename(f159_ctos_inv, BACKSLP, BACKSLP_f159);
%rename(f159_ctos_inv, UPEARLY, UPEARLY_f159);
%rename(f159_ctos_inv, WAKENGHT, WAKENGHT_f159);
%merge(whi_merge, f159_ctos_inv);

*f158_ctos_inv;
%readin(f158_ctos_inv);
%keep(f158_ctos_inv, ID COGCHANGE);
%merge(whi_merge, f158_ctos_inv);

*outc_self_ctos_inv;
%readin(outc_self_ctos_inv);
%keep(outc_self_ctos_inv, ID F33DEPRESSION F33DEPRESSIONDY F33OSTEOAR F33OSTEOARDY F33CATARACT);
%merge(whi_merge, outc_self_ctos_inv);

*f30_ctos_inv;
%readin(f30_ctos_inv);
%keep(f30_ctos_inv, ID CVD HYPT HYPTPILL EMPHYSEM CHF_F30 ASTHMA PAD PADSURG CATARACT NUMFALLS);
%rename(f30_ctos_inv, CVD, CVD_f30);
%rename(f30_ctos_inv, PAD, PAD_f30);
%rename(f30_ctos_inv, CATARACT, CATARACT_f30);
%merge(whi_merge, f30_ctos_inv);

*outc_self_x2_mrc_inv;
%readin(outc_self_x2_mrc_inv);
%keep(outc_self_x2_mrc_inv, ID F33COPD F33OSTEOAR F33OSTEOARDY F33HOSPTIMESX2);
%rename(outc_self_x2_mrc_inv, F33OSTEOAR, F33OSTEOAR_self_x2_mrc);
%rename(outc_self_x2_mrc_inv, F33OSTEOARDY, F33OSTEOARDY_self_x2_mrc);
%merge(whi_merge, outc_self_x2_mrc_inv);

*f156_ctos_inv;
%readin(f156_ctos_inv);
%keep(f156_ctos_inv, ID PNEUMONIA CATARACT);
%rename(f156_ctos_inv, CATARACT, CATARACT_f156);
%merge(whi_merge, f156_ctos_inv);

*f190_covid1_inv;
%readin(f190_covid1_inv);
%keep(f190_covid1_inv, ID LOOPDIUR THIAZDIUR CALCCHANBLOCK ASPIRIN IBUPROFEN NAPROXEN ALPHABLOCK BETABLOCK ACEINHIB HIBPMED ANGIOTENSIN SULFONYLUREA GLUCOPHAGE SGLT2INHIB INSULIN);
%rename(f190_covid1_inv, INSULIN, INSULIN_f190);
%merge(whi_merge, f190_covid1_inv);

*f143_av3_os_inv;
%readin(f143_av3_os_inv);
%keep(f143_av3_os_inv, ID MARITAL_3 INCOME_3 EMPHYSEM_3 ASTHMA_3 CATARACT_3 HLTHINSR_3 INSPREPD_3 PPMEDPD_3 INSMDCAD_3 MEDADDCV_3 INSMLTRY_3 INSOTHPV_3 CAREPROV_3 NEWDR3Y_3);
%merge(whi_merge, f143_av3_os_inv);

*f144_av4_os_inv;
%readin(f144_av4_os_inv);
%keep(f144_av4_os_inv, ID MARITAL_4 EMPHYSEM_4 ASTHMA_4 CATARACT_4 HRPST1YR_4);
%merge(whi_merge, f144_av4_os_inv);

*f145_av5_os_inv;
%readin(f145_av5_os_inv);
%keep(f145_av5_os_inv, ID MARITAL_5 EMPHYSEM_5 ASTHMA_5 CATARACT_5 HRPST1YR_5);
%merge(whi_merge, f145_av5_os_inv);

*f146_av6_os_inv;
%readin(f146_av6_os_inv);
%keep(f146_av6_os_inv, ID MARITAL_6 INCOME_6 HSHLDALL_6 HSHLDLS18_6 HSHLD1864 HSHLDGR65 EMPHYSEM_6 ASTHMA_6 CATARACT_6 HLTHINSR_6 INSPREPD_6 PPMEDPD_6 INSMDCAD_6 MEDADDCV_6 INSMLTRY_6 INSOTHPV_6 CAREPROV_6 NEWDR3Y_6 HRPST1YR_6);
%merge(whi_merge, f146_av6_os_inv);

*f147_av7_os_inv;
%readin(f147_av7_os_inv);
%keep(f147_av7_os_inv, ID MARITAL_7 EMPHYSEM_7 ASTHMA_7 CATARACT_7 HRPST1YR_7);
%merge(whi_merge, f147_av7_os_inv);

*f148_av8_os_inv;
%readin(f148_av8_os_inv);
%keep(f148_av8_os_inv, ID MARITAL_8 EMPHYSEM_8 ASTHMA_8 CATARACT_8 HRPST1YR_8);
%merge(whi_merge, f148_av8_os_inv);

*outc_death_inv;
%readin(outc_death_inv);
%keep(outc_death_inv, ID DEATHSRC DEATHCAUSE DEATHCAUSESRC DEATH);
%rename(outc_death_inv, DEATHSRC, DEATHSRC_outc_death);
%rename(outc_death_inv, DEATHCAUSE, DEATHCAUSE_outc_death);
%rename(outc_death_inv, DEATHCAUSESRC, DEATHCAUSESRC_outc_death);
%rename(outc_death_inv, DEATH, DEATH_outc_death);
%merge(whi_merge, outc_death_inv);

*opach_calendar_summary;
%readin(opach_calendar_summary);
%rename(opach_calendar_summary, calcount, calcount_opach_calendar);
%rename(opach_calendar_summary, fallcount, fallcount_opach_calendar);
%rename(opach_calendar_summary, fallrate, fallrate_opach_calendar);
%merge(whi_merge, opach_calendar_summary);



*Save as permanent data;
data a.whi_merge; set whi_merge; run;
data whi_merge; set a.whi_merge; run; 


/******************************************************************************/
/* ONE ROW PER FORM/BMD SCAN/SPECIMEN DRAW/SPECIMEN RESULT/FALLS */
/* The datasets have one row per form/bmd scan/specimen draw/specimen result/falls, so transposing is necessary. The detailed steps for merging are specified below */

/* 1. Read in the dataset using the "%readin(data)" macro. */
/* 2. Keep only the necessary variables using the "%keep(data,var)" macro. */
/* 3. Rename variables if needed to avoid overwriting using the "%rename(data,old,new)" macro. */
/* 4. Remove exact duplicates to facilitate ordering and transposing using the "%nodup(data, identifier)" macro. */
/* 5. Sort the data by the unique identifiers (e.g., ID + F151DAYS) to list rows and assign numbers in order of the identifiers using the "%order(data,identifier)" macro. */
/* 6. Transpose variables from long to wide format and create "data_wide#" dataset for each variable using the "%transpose(data,var,out)" macro. */
/* 7. Merge transposed variables with the previously merged "whi_merge" dataset using the "%merge2(data1,data2)" macro. */

*f37_ctos_inv;
%readin(f37_ctos_inv);
%keep(f37_ctos_inv, ID f37days HEARLOSS TRBSEE GENHEL HLTHYANY HLTHEXCL LIFTGROC BENDING RESTSLP TRBSLEEP BACKSLP UPEARLY WAKENGHT NAP MEDSLEEP FALLSLP INCONT FRQINCON CGHINCON TOINCON SLPINCON OTHINCON DIZZY);
%rename(f37_ctos_inv, HLTHEXCL, f37_HLTHEXCL);
%rename(f37_ctos_inv, LIFTGROC, f37_LIFTGROC);
%rename(f37_ctos_inv, BENDING, f37_BENDING);
%rename(f37_ctos_inv, RESTSLP, f37_RESTSLP);
%rename(f37_ctos_inv, TRBSLEEP, f37_TRBSLEEP);
%rename(f37_ctos_inv, BACKSLP, f37_BACKSLP);
%rename(f37_ctos_inv, UPEARLY, f37_UPEARLY);
%rename(f37_ctos_inv, WAKENGHT, f37_WAKENGHT);
%rename(f37_ctos_inv, NAP, f37_NAP);
%rename(f37_ctos_inv, MEDSLEEP, f37_MEDSLEEP);
%rename(f37_ctos_inv, FALLSLP, f37_FALLSLP);
%rename(f37_ctos_inv, INCONT, f37_INCONT);
%rename(f37_ctos_inv, FRQINCON, f37_FRQINCON);
%rename(f37_ctos_inv, CGHINCON, f37_CGHINCON);
%rename(f37_ctos_inv, TOINCON, f37_TOINCON);
%rename(f37_ctos_inv, SLPINCON, f37_SLPINCON);
%rename(f37_ctos_inv, OTHINCON, f37_OTHINCON);
%rename(f37_ctos_inv, DIZZY, f37_DIZZY);

%nodup(f37_ctos_inv, ID f37days);
%order(f37_ctos_inv, ID f37days);
%transpose(f37_ctos_inv, f37days, f37_wide0);
%transpose(f37_ctos_inv, HEARLOSS, f37_wide1);
%transpose(f37_ctos_inv, TRBSEE, f37_wide2);
%transpose(f37_ctos_inv, GENHEL, f37_wide3);
%transpose(f37_ctos_inv, HLTHYANY, f37_wide4);
%transpose(f37_ctos_inv, f37_HLTHEXCL, f37_wide5);
%transpose(f37_ctos_inv, f37_LIFTGROC, f37_wide6);
%transpose(f37_ctos_inv, f37_BENDING, f37_wide7);
%transpose(f37_ctos_inv, f37_RESTSLP, f37_wide8);
%transpose(f37_ctos_inv, f37_TRBSLEEP, f37_wide9);
%transpose(f37_ctos_inv, f37_BACKSLP, f37_wide10);
%transpose(f37_ctos_inv, f37_UPEARLY, f37_wide11);
%transpose(f37_ctos_inv, f37_WAKENGHT, f37_wide12);
%transpose(f37_ctos_inv, f37_NAP, f37_wide13);
%transpose(f37_ctos_inv, f37_MEDSLEEP, f37_wide14);
%transpose(f37_ctos_inv, f37_FALLSLP, f37_wide15);
%transpose(f37_ctos_inv, f37_INCONT, f37_wide16);
%transpose(f37_ctos_inv, f37_FRQINCON, f37_wide17);
%transpose(f37_ctos_inv, f37_CGHINCON, f37_wide18);
%transpose(f37_ctos_inv, f37_TOINCON, f37_wide19);
%transpose(f37_ctos_inv, f37_SLPINCON, f37_wide20);
%transpose(f37_ctos_inv, f37_OTHINCON, f37_wide21);
%transpose(f37_ctos_inv, f37_DIZZY, f37_wide22);

%merge2(whi_merge, f37_wide0 f37_wide1 f37_wide2 f37_wide3 f37_wide4 f37_wide5 f37_wide6 f37_wide7 f37_wide8 f37_wide9 f37_wide10
f37_wide11 f37_wide12 f37_wide13 f37_wide14 f37_wide15 f37_wide16 f37_wide17 f37_wide18 f37_wide19 f37_wide20 f37_wide21 f37_wide22);

*f38_ctos_inv;
%readin(f38_ctos_fu_inv);
%keep(f38_ctos_fu_inv, ID f38days HLTHEXCL LIFTGROC BENDING RESTSLP TRBSLEEP BACKSLP UPEARLY WAKENGHT NAP MEDSLEEP FALLSLP INCONT FRQINCON CGHINCON TOINCON SLPINCON OTHINCON DIZZY);
%rename(f38_ctos_fu_inv, HLTHEXCL, f38_HLTHEXCL);
%rename(f38_ctos_fu_inv, LIFTGROC, f38_LIFTGROC);
%rename(f38_ctos_fu_inv, BENDING, f38_BENDING);
%rename(f38_ctos_fu_inv, RESTSLP, f38_RESTSLP);
%rename(f38_ctos_fu_inv, TRBSLEEP, f38_TRBSLEEP);
%rename(f38_ctos_fu_inv, BACKSLP, f38_BACKSLP);
%rename(f38_ctos_fu_inv, UPEARLY, f38_UPEARLY);
%rename(f38_ctos_fu_inv, WAKENGHT, f38_WAKENGHT);
%rename(f38_ctos_fu_inv, NAP, f38_NAP);
%rename(f38_ctos_fu_inv, MEDSLEEP, f38_MEDSLEEP);
%rename(f38_ctos_fu_inv, FALLSLP, f38_FALLSLP);
%rename(f38_ctos_fu_inv, INCONT, f38_INCONT);
%rename(f38_ctos_fu_inv, FRQINCON, f38_FRQINCON);
%rename(f38_ctos_fu_inv, CGHINCON, f38_CGHINCON);
%rename(f38_ctos_fu_inv, TOINCON, f38_TOINCON);
%rename(f38_ctos_fu_inv, SLPINCON, f38_SLPINCON);
%rename(f38_ctos_fu_inv, OTHINCON, f38_OTHINCON);
%rename(f38_ctos_fu_inv, DIZZY, f38_DIZZY);

%nodup(f38_ctos_fu_inv, ID f38days);
%order(f38_ctos_fu_inv, ID f38days);
%transpose(f38_ctos_fu_inv, f38days, f38_wide0);
%transpose(f38_ctos_fu_inv, f38_HLTHEXCL, f38_wide1);
%transpose(f38_ctos_fu_inv, f38_LIFTGROC, f38_wide2);
%transpose(f38_ctos_fu_inv, f38_BENDING, f38_wide3);
%transpose(f38_ctos_fu_inv, f38_RESTSLP, f38_wide4);
%transpose(f38_ctos_fu_inv, f38_TRBSLEEP, f38_wide5);
%transpose(f38_ctos_fu_inv, f38_BACKSLP, f38_wide6);
%transpose(f38_ctos_fu_inv, f38_UPEARLY, f38_wide7);
%transpose(f38_ctos_fu_inv, f38_WAKENGHT, f38_wide8);
%transpose(f38_ctos_fu_inv, f38_NAP, f38_wide9);
%transpose(f38_ctos_fu_inv, f38_MEDSLEEP, f38_wide10);
%transpose(f38_ctos_fu_inv, f38_FALLSLP, f38_wide11);
%transpose(f38_ctos_fu_inv, f38_INCONT, f38_wide12);
%transpose(f38_ctos_fu_inv, f38_FRQINCON, f38_wide13);
%transpose(f38_ctos_fu_inv, f38_CGHINCON, f38_wide14);
%transpose(f38_ctos_fu_inv, f38_TOINCON, f38_wide15);
%transpose(f38_ctos_fu_inv, f38_SLPINCON, f38_wide16);
%transpose(f38_ctos_fu_inv, f38_OTHINCON, f38_wide17);
%transpose(f38_ctos_fu_inv, f38_DIZZY, f38_wide18);

%merge2(whi_merge, f38_wide0 f38_wide1 f38_wide2 f38_wide3 f38_wide4 f38_wide5 f38_wide6 f38_wide7 f38_wide8 f38_wide9 f38_wide10 
 f38_wide11 f38_wide12 f38_wide13 f38_wide14 f38_wide15 f38_wide16 f38_wide17 f38_wide18);

*bmd_hip_ctos_inv;
%readin(bmd_hip_ctos_inv);
%keep(bmd_hip_ctos_inv, ID hipdays HIPBMD HIPNKBMD HIPBMC HIPNKBMC HIPAREA HIPNKAREA);

%nodup(bmd_hip_ctos_inv, ID hipdays);
%order(bmd_hip_ctos_inv, ID hipdays);
%transpose(bmd_hip_ctos_inv, hipdays, bmdhip_wide0);
%transpose(bmd_hip_ctos_inv, HIPBMD, bmdhip_wide1);
%transpose(bmd_hip_ctos_inv, HIPNKBMD, bmdhip_wide2);
%transpose(bmd_hip_ctos_inv, HIPBMC, bmdhip_wide3);
%transpose(bmd_hip_ctos_inv, HIPNKBMC, bmdhip_wide4);
%transpose(bmd_hip_ctos_inv, HIPAREA, bmdhip_wide5);
%transpose(bmd_hip_ctos_inv, HIPNKAREA, bmdhip_wide6);

%merge2(whi_merge, bmdhip_wide0 bmdhip_wide1 bmdhip_wide2 bmdhip_wide3 bmdhip_wide4 bmdhip_wide5 bmdhip_wide6);

*bmd_wbody_ctos_inv;
%readin(bmd_wbody_ctos_inv);
%keep(bmd_wbody_ctos_inv, ID wbdays WBLFLEGBMC WBRTLEGBMC WBBMC WBLFLEGAREA WBRTLEGAREA WBAREA WBLFLEGLEN WBRTLEGLEN WBLEAN WBLFLEGFAT WBRTLEGFAT WBFAT WBLFLEGFFM WBRTLEGFFM WBFFM);

%nodup(bmd_wbody_ctos_inv, ID wbdays);
%order(bmd_wbody_ctos_inv, ID wbdays);
%transpose(bmd_wbody_ctos_inv, wbdays, bmdwbody_wide0);
%transpose(bmd_wbody_ctos_inv, WBLFLEGBMC, bmdwbody_wide1);
%transpose(bmd_wbody_ctos_inv, WBRTLEGBMC, bmdwbody_wide2);
%transpose(bmd_wbody_ctos_inv, WBBMC, bmdwbody_wide3);
%transpose(bmd_wbody_ctos_inv, WBLFLEGAREA, bmdwbody_wide4);
%transpose(bmd_wbody_ctos_inv, WBRTLEGAREA, bmdwbody_wide5);
%transpose(bmd_wbody_ctos_inv, WBAREA, bmdwbody_wide6);
%transpose(bmd_wbody_ctos_inv, WBLFLEGLEN, bmdwbody_wide7);
%transpose(bmd_wbody_ctos_inv, WBRTLEGLEN, bmdwbody_wide8);
%transpose(bmd_wbody_ctos_inv, WBLEAN, bmdwbody_wide9);
%transpose(bmd_wbody_ctos_inv, WBLFLEGFAT, bmdwbody_wide10);
%transpose(bmd_wbody_ctos_inv, WBRTLEGFAT, bmdwbody_wide11);
%transpose(bmd_wbody_ctos_inv, WBFAT, bmdwbody_wide12);
%transpose(bmd_wbody_ctos_inv, WBLFLEGFFM, bmdwbody_wide13);
%transpose(bmd_wbody_ctos_inv, WBRTLEGFFM, bmdwbody_wide14);
%transpose(bmd_wbody_ctos_inv, WBFFM, bmdwbody_wide15);

%merge2(whi_merge, bmdwbody_wide0 bmdwbody_wide1 bmdwbody_wide2 bmdwbody_wide3 bmdwbody_wide4 bmdwbody_wide5 bmdwbody_wide6 
bmdwbody_wide7 bmdwbody_wide8 bmdwbody_wide9 bmdwbody_wide10 bmdwbody_wide11 bmdwbody_wide12 bmdwbody_wide13 bmdwbody_wide14 bmdwbody_wide15);

*f321_paq_inv;
%readin(f321_paq_inv);
%keep(f321_paq_inv, ID WALKNRML30 WALKSLOW30);

%merge(whi_merge, f321_paq_inv);

*f90_ct_inv;
%readin(f90_ct_inv);
%keep(f90_ct_inv, ID f90days CHRSTAND);

%nodup(f90_ct_inv, ID f90days);
%order(f90_ct_inv, ID f90days);
%transpose(f90_ct_inv, f90days, f90_wide0);
%transpose(f90_ct_inv, CHRSTAND, f90_wide1);

%merge2(whi_merge, f90_wide0 f90_wide1);


*spec_draws_ctos_inv;
%readin(spec_draws_ctos_inv);
%keep(spec_draws_ctos_inv, ID PPTDRW DIABMEDS);
%rename(spec_draws_ctos_inv, DIABMEDS, specdraw_DIABMEDS);

%nodup(spec_draws_ctos_inv, ID PPTDRW);
%order(spec_draws_ctos_inv, ID PPTDRW);
%transpose(spec_draws_ctos_inv, PPTDRW, specdraw_wide0);
%transpose(spec_draws_ctos_inv, specdraw_DIABMEDS, specdraw_wide1);

%merge2(whi_merge, specdraw_wide0 specdraw_wide1);

*f45_ctos_inv;
%readin(f45_ctos_inv);
%keep(f45_ctos_inv, ID f45days F45VTB12 F45VITD);

%nodup(f45_ctos_inv, ID f45days);
%order(f45_ctos_inv, ID f45days);
%transpose(f45_ctos_inv, f45days, f45_wide0);
%transpose(f45_ctos_inv, F45VTB12, f45_wide1);
%transpose(f45_ctos_inv, F45VITD, f45_wide2);

%merge2(whi_merge, f45_wide0 f45_wide1 f45_wide2);

*f44_ctos_inv;
%readin(f44_ctos_inv);
%keep(f44_ctos_inv, ID f44days CORT);

%nodup(f44_ctos_inv, ID f44days);
%order(f44_ctos_inv, ID f44days);
%transpose(f44_ctos_inv, f44days, f44_wide0);
%transpose(f44_ctos_inv, CORT, f44_wide1);

%merge2(whi_merge, f44_wide0 f44_wide1);

*f150_ht_inv;
%readin(f150_ht_inv);
%keep(f150_ht_inv, ID f150days HRPST1YR_X);

%nodup(f150_ht_inv, ID f150days);
%order(f150_ht_inv, ID f150days);
%transpose(f150_ht_inv, f150days, f150_wide0);
%transpose(f150_ht_inv, HRPST1YR_X, f150_wide1);

%merge2(whi_merge, f150_wide0 f150_wide1);

*f60_ctos_inv;
%readin(f60_ctos_inv);
%keep(f60_ctos_inv, ID f60days F60VB12);

%nodup(f60_ctos_inv, ID f60days);
%order(f60_ctos_inv, ID f60days);
%transpose(f60_ctos_inv, f60days, f60_wide0);
%transpose(f60_ctos_inv, F60VB12, f60_wide1);

%merge2(whi_merge, f60_wide0 f60_wide1);

*f153_medications_ctos_inv;
%readin(f153_medications_ctos_inv);
%keep(f153_medications_ctos_inv, ID f153seq F153MEDTYPE F153TAKEMED F153FREQ F153DUR);

%nodup(f153_medications_ctos_inv, ID f153seq);
%order(f153_medications_ctos_inv, ID f153seq);
%transpose(f153_medications_ctos_inv, f153seq, f153_wide0);
%transpose(f153_medications_ctos_inv, F153MEDTYPE, f153_wide1);
%transpose(f153_medications_ctos_inv, F153TAKEMED, f153_wide2);
%transpose(f153_medications_ctos_inv, F153FREQ, f153_wide3);
%transpose(f153_medications_ctos_inv, F153DUR, f153_wide4);

%merge2(whi_merge, f153_wide0 f153_wide1 f153_wide2 f153_wide3 f153_wide4);

*opach_all_falls;
%readin(opach_all_falls);
%rename(opach_all_falls, falldays, opachfall_falldays);
%rename(opach_all_falls, interviewed, opachfall_interviewed);

%nodup(opach_all_falls, ID opachfall_falldays);
%order(opach_all_falls, ID opachfall_falldays);
%transpose(opach_all_falls, opachfall_falldays, opachfall_wide0);
%transpose(opach_all_falls, opachfall_interviewed, opachfall_wide1);

%merge2(whi_merge, opachfall_wide0 opachfall_wide1);


*opach_falls_interviews_inv;
%readin(opach_falls_interviews_inv);
%keep(opach_falls_interviews_inv, ID falldays FRACTURE TOEFRACTURE ANKLEFRACTURE KNEEFRACTURE LEGFRACTURE THIGHFRACTURE HIPFRACTURE BACKFRACTURE 
ABSFRACTURE CHESTFRACTURE FINGERFRACTURE ARMFRACTURE WRISTFRACTURE ELBOWFRACTURE SHOULDERFRACTURE NECKFRACTURE FACEFRACTURE HEADFRACTURE OTHERFRACTURE DKFRACTURE 
INJFALLFLG MEDICALTX SPRAINED 
TOEJOINT ANKLEJOINT KNEEJOINT LEGJOINT THIGHJOINT HIPJOINT BACKJOINT ABSJOINT CHESTJOINT FINGERJOINT 
ARMJOINT WRISTJOINT ELBOWJOINT SHOULDERJOINT NECKJOINT FACEJOINT HEADJOINT OTHERJOINT DKJOINT 
BRUISING TOEBRUISE ANKLEBRUISE KNEEBRUISE LEGBRUISE THIGHBRUISE HIPBRUISE BACKBRUISE ABSBRUISE CHESTBRUISE 
FINGERBRUISE ARMBRUISE WRISTBRUISE ELBOWBRUISE SHOULDERBRUISE NECKBRUISE FACEBRUISE HEADBRUISE OTHERBRUISE DKBRUISE 
TOECUT ANKLECUT KNEECUT LEGCUT THIGHCUT HIPCUT BACKCUT ABSCUT CHESTCUT FINGERCUT 
ARMCUT WRISTCUT ELBOWCUT SHOULDERCUT NECKCUT FACECUT HEADCUT OTHERCUT DKCUT 
TOEOTHER ANKLEOTHER KNEEOTHER LEGOTHER THIGHOTHER HIPOTHER BACKOTHER ABSOTHER CHESTOTHER 
FINGEROTHER ARMOTHER WRISTOTHER ELBOWOTHER SHOULDEROTHER NECKOTHER FACEOTHER HEADOTHER 
OTHERFRACTURE OTHERJOINT OTHERBRUISE OTHERCUT OTHERSCRAPE OTHERSORE OTHEROTHER DKOTHER 

SORE TOESORE ANKLESORE KNEESORE LEGSORE THIGHSORE HIPSORE BACKSORE ABSSORE CHESTSORE FINGERSORE 
ARMSORE WRISTSORE ELBOWSORE SHOULDERSORE NECKSORE FACESORE HEADSORE OTHERSORE DKSORE 

SCRAPE TOESCRAPE ANKLESCRAPE KNEESCRAPE LEGSCRAPE THIGHSCRAPE HIPSCRAPE BACKSCRAPE ABSSCRAPE CHESTSCRAPE 
FINGERSCRAPE ARMSCRAPE WRISTSCRAPE ELBOWSCRAPE SHOULDERSCRAPE NECKSCRAPE FACESCRAPE HEADSCRAPE OTHERSCRAPE DKSCRAPE );

%rename(opach_falls_interviews_inv, falldays, opachint_falldays);

%nodup(opach_falls_interviews_inv, ID opachint_falldays);
%order(opach_falls_interviews_inv, ID opachint_falldays);

%transpose(opach_falls_interviews_inv, opachint_falldays, opachint_wide0);
%transpose(opach_falls_interviews_inv, FRACTURE , opachint_wide1);
%transpose(opach_falls_interviews_inv, TOEFRACTURE , opachint_wide2);
%transpose(opach_falls_interviews_inv, ANKLEFRACTURE , opachint_wide3);
%transpose(opach_falls_interviews_inv, KNEEFRACTURE , opachint_wide4);
%transpose(opach_falls_interviews_inv, LEGFRACTURE , opachint_wide5);
%transpose(opach_falls_interviews_inv, THIGHFRACTURE , opachint_wide6);
%transpose(opach_falls_interviews_inv, HIPFRACTURE , opachint_wide7);
%transpose(opach_falls_interviews_inv, BACKFRACTURE , opachint_wide8);
%transpose(opach_falls_interviews_inv, ABSFRACTURE , opachint_wide9);
%transpose(opach_falls_interviews_inv, CHESTFRACTURE , opachint_wide10);
%transpose(opach_falls_interviews_inv, FINGERFRACTURE , opachint_wide11);
%transpose(opach_falls_interviews_inv, ARMFRACTURE , opachint_wide12);
%transpose(opach_falls_interviews_inv, WRISTFRACTURE , opachint_wide13);
%transpose(opach_falls_interviews_inv, ELBOWFRACTURE , opachint_wide14);
%transpose(opach_falls_interviews_inv, SHOULDERFRACTURE , opachint_wide15);
%transpose(opach_falls_interviews_inv, NECKFRACTURE , opachint_wide16);
%transpose(opach_falls_interviews_inv, FACEFRACTURE , opachint_wide17);
%transpose(opach_falls_interviews_inv, HEADFRACTURE , opachint_wide18);
%transpose(opach_falls_interviews_inv, OTHERFRACTURE , opachint_wide19);
%transpose(opach_falls_interviews_inv, DKFRACTURE , opachint_wide20);
%transpose(opach_falls_interviews_inv, INJFALLFLG , opachint_wide21);
%transpose(opach_falls_interviews_inv, MEDICALTX , opachint_wide22);
%transpose(opach_falls_interviews_inv, SPRAINED , opachint_wide23);
%transpose(opach_falls_interviews_inv, TOEJOINT , opachint_wide24);
%transpose(opach_falls_interviews_inv, ANKLEJOINT , opachint_wide25);
%transpose(opach_falls_interviews_inv, KNEEJOINT , opachint_wide26);
%transpose(opach_falls_interviews_inv, LEGJOINT , opachint_wide27);
%transpose(opach_falls_interviews_inv, THIGHJOINT , opachint_wide28);
%transpose(opach_falls_interviews_inv, HIPJOINT , opachint_wide29);
%transpose(opach_falls_interviews_inv, BACKJOINT , opachint_wide30);
%transpose(opach_falls_interviews_inv, ABSJOINT , opachint_wide31);
%transpose(opach_falls_interviews_inv, CHESTJOINT , opachint_wide32);
%transpose(opach_falls_interviews_inv, FINGERJOINT , opachint_wide33);
%transpose(opach_falls_interviews_inv, ARMJOINT , opachint_wide34);
%transpose(opach_falls_interviews_inv, WRISTJOINT , opachint_wide35);
%transpose(opach_falls_interviews_inv, ELBOWJOINT , opachint_wide36);
%transpose(opach_falls_interviews_inv, SHOULDERJOINT , opachint_wide37);
%transpose(opach_falls_interviews_inv, NECKJOINT , opachint_wide38);
%transpose(opach_falls_interviews_inv, FACEJOINT , opachint_wide39);
%transpose(opach_falls_interviews_inv, HEADJOINT , opachint_wide40);
%transpose(opach_falls_interviews_inv, OTHERJOINT , opachint_wide41);
%transpose(opach_falls_interviews_inv, DKJOINT , opachint_wide42);
%transpose(opach_falls_interviews_inv, BRUISING , opachint_wide43);
%transpose(opach_falls_interviews_inv, TOEBRUISE , opachint_wide44);
%transpose(opach_falls_interviews_inv, ANKLEBRUISE , opachint_wide45);
%transpose(opach_falls_interviews_inv, KNEEBRUISE , opachint_wide46);
%transpose(opach_falls_interviews_inv, LEGBRUISE , opachint_wide47);
%transpose(opach_falls_interviews_inv, THIGHBRUISE , opachint_wide48);
%transpose(opach_falls_interviews_inv, HIPBRUISE , opachint_wide49);
%transpose(opach_falls_interviews_inv, BACKBRUISE , opachint_wide50);
%transpose(opach_falls_interviews_inv, ABSBRUISE , opachint_wide51);
%transpose(opach_falls_interviews_inv, CHESTBRUISE , opachint_wide52);
%transpose(opach_falls_interviews_inv, FINGERBRUISE , opachint_wide53);
%transpose(opach_falls_interviews_inv, ARMBRUISE , opachint_wide54);
%transpose(opach_falls_interviews_inv, WRISTBRUISE , opachint_wide55);
%transpose(opach_falls_interviews_inv, ELBOWBRUISE , opachint_wide56);
%transpose(opach_falls_interviews_inv, SHOULDERBRUISE , opachint_wide57);
%transpose(opach_falls_interviews_inv, NECKBRUISE , opachint_wide58);
%transpose(opach_falls_interviews_inv, FACEBRUISE , opachint_wide59);
%transpose(opach_falls_interviews_inv, HEADBRUISE , opachint_wide60);
%transpose(opach_falls_interviews_inv, OTHERBRUISE , opachint_wide61);
%transpose(opach_falls_interviews_inv, DKBRUISE , opachint_wide62);
%transpose(opach_falls_interviews_inv, TOECUT , opachint_wide63);
%transpose(opach_falls_interviews_inv, ANKLECUT , opachint_wide64);
%transpose(opach_falls_interviews_inv, KNEECUT , opachint_wide65);
%transpose(opach_falls_interviews_inv, LEGCUT , opachint_wide66);
%transpose(opach_falls_interviews_inv, THIGHCUT , opachint_wide67);
%transpose(opach_falls_interviews_inv, HIPCUT , opachint_wide68);
%transpose(opach_falls_interviews_inv, BACKCUT , opachint_wide69);
%transpose(opach_falls_interviews_inv, ABSCUT , opachint_wide70);
%transpose(opach_falls_interviews_inv, CHESTCUT , opachint_wide71);
%transpose(opach_falls_interviews_inv, FINGERCUT , opachint_wide72);
%transpose(opach_falls_interviews_inv, ARMCUT , opachint_wide73);
%transpose(opach_falls_interviews_inv, WRISTCUT , opachint_wide74);
%transpose(opach_falls_interviews_inv, ELBOWCUT , opachint_wide75);
%transpose(opach_falls_interviews_inv, SHOULDERCUT , opachint_wide76);
%transpose(opach_falls_interviews_inv, NECKCUT , opachint_wide77);
%transpose(opach_falls_interviews_inv, FACECUT , opachint_wide78);
%transpose(opach_falls_interviews_inv, HEADCUT , opachint_wide79);
%transpose(opach_falls_interviews_inv, OTHERCUT , opachint_wide80);
%transpose(opach_falls_interviews_inv, DKCUT , opachint_wide81);
%transpose(opach_falls_interviews_inv, TOEOTHER , opachint_wide82);
%transpose(opach_falls_interviews_inv, ANKLEOTHER , opachint_wide83);
%transpose(opach_falls_interviews_inv, KNEEOTHER , opachint_wide84);
%transpose(opach_falls_interviews_inv, LEGOTHER , opachint_wide85);
%transpose(opach_falls_interviews_inv, THIGHOTHER , opachint_wide86);
%transpose(opach_falls_interviews_inv, HIPOTHER , opachint_wide87);
%transpose(opach_falls_interviews_inv, BACKOTHER , opachint_wide88);
%transpose(opach_falls_interviews_inv, ABSOTHER , opachint_wide89);
%transpose(opach_falls_interviews_inv, CHESTOTHER , opachint_wide90);
%transpose(opach_falls_interviews_inv, FINGEROTHER , opachint_wide91);
%transpose(opach_falls_interviews_inv, ARMOTHER , opachint_wide92);
%transpose(opach_falls_interviews_inv, WRISTOTHER , opachint_wide93);
%transpose(opach_falls_interviews_inv, ELBOWOTHER , opachint_wide94);
%transpose(opach_falls_interviews_inv, SHOULDEROTHER , opachint_wide95);
%transpose(opach_falls_interviews_inv, NECKOTHER , opachint_wide96);
%transpose(opach_falls_interviews_inv, FACEOTHER , opachint_wide97);
%transpose(opach_falls_interviews_inv, HEADOTHER , opachint_wide98);
%transpose(opach_falls_interviews_inv, OTHERFRACTURE , opachint_wide99);
%transpose(opach_falls_interviews_inv, OTHERJOINT , opachint_wide100);
%transpose(opach_falls_interviews_inv, OTHERBRUISE , opachint_wide101);
%transpose(opach_falls_interviews_inv, OTHERCUT , opachint_wide102);
%transpose(opach_falls_interviews_inv, OTHERSCRAPE , opachint_wide103);
%transpose(opach_falls_interviews_inv, OTHERSORE , opachint_wide104);
%transpose(opach_falls_interviews_inv, OTHEROTHER , opachint_wide105);
%transpose(opach_falls_interviews_inv, DKOTHER , opachint_wide106);
%transpose(opach_falls_interviews_inv, SORE , opachint_wide107);
%transpose(opach_falls_interviews_inv, TOESORE , opachint_wide108);
%transpose(opach_falls_interviews_inv, ANKLESORE , opachint_wide109);
%transpose(opach_falls_interviews_inv, KNEESORE , opachint_wide110);
%transpose(opach_falls_interviews_inv, LEGSORE , opachint_wide111);
%transpose(opach_falls_interviews_inv, THIGHSORE , opachint_wide112);
%transpose(opach_falls_interviews_inv, HIPSORE , opachint_wide113);
%transpose(opach_falls_interviews_inv, BACKSORE , opachint_wide114);
%transpose(opach_falls_interviews_inv, ABSSORE , opachint_wide115);
%transpose(opach_falls_interviews_inv, CHESTSORE , opachint_wide116);
%transpose(opach_falls_interviews_inv, FINGERSORE , opachint_wide117);
%transpose(opach_falls_interviews_inv, ARMSORE , opachint_wide118);
%transpose(opach_falls_interviews_inv, WRISTSORE , opachint_wide119);
%transpose(opach_falls_interviews_inv, ELBOWSORE , opachint_wide120);
%transpose(opach_falls_interviews_inv, SHOULDERSORE , opachint_wide121);
%transpose(opach_falls_interviews_inv, NECKSORE , opachint_wide122);
%transpose(opach_falls_interviews_inv, FACESORE , opachint_wide123);
%transpose(opach_falls_interviews_inv, HEADSORE , opachint_wide124);
%transpose(opach_falls_interviews_inv, OTHERSORE , opachint_wide125);
%transpose(opach_falls_interviews_inv, DKSORE , opachint_wide126);
%transpose(opach_falls_interviews_inv, SCRAPE , opachint_wide127);
%transpose(opach_falls_interviews_inv, TOESCRAPE , opachint_wide128);
%transpose(opach_falls_interviews_inv, ANKLESCRAPE , opachint_wide129);
%transpose(opach_falls_interviews_inv, KNEESCRAPE , opachint_wide130);
%transpose(opach_falls_interviews_inv, LEGSCRAPE , opachint_wide131);
%transpose(opach_falls_interviews_inv, THIGHSCRAPE , opachint_wide132);
%transpose(opach_falls_interviews_inv, HIPSCRAPE , opachint_wide133);
%transpose(opach_falls_interviews_inv, BACKSCRAPE , opachint_wide134);
%transpose(opach_falls_interviews_inv, ABSSCRAPE , opachint_wide135);
%transpose(opach_falls_interviews_inv, CHESTSCRAPE , opachint_wide136);
%transpose(opach_falls_interviews_inv, FINGERSCRAPE , opachint_wide137);
%transpose(opach_falls_interviews_inv, ARMSCRAPE , opachint_wide138);
%transpose(opach_falls_interviews_inv, WRISTSCRAPE , opachint_wide139);
%transpose(opach_falls_interviews_inv, ELBOWSCRAPE , opachint_wide140);
%transpose(opach_falls_interviews_inv, SHOULDERSCRAPE , opachint_wide141);
%transpose(opach_falls_interviews_inv, NECKSCRAPE , opachint_wide142);
%transpose(opach_falls_interviews_inv, FACESCRAPE , opachint_wide143);
%transpose(opach_falls_interviews_inv, HEADSCRAPE , opachint_wide144);
%transpose(opach_falls_interviews_inv, OTHERSCRAPE , opachint_wide145);
%transpose(opach_falls_interviews_inv, DKSCRAPE, opachint_wide146);

%merge2(whi_merge, 
    opachint_wide0 opachint_wide1 opachint_wide2 opachint_wide3 
    opachint_wide4 opachint_wide5 opachint_wide6 opachint_wide7 
    opachint_wide8 opachint_wide9 opachint_wide10 opachint_wide11 
    opachint_wide12 opachint_wide13 opachint_wide14 opachint_wide15 
    opachint_wide16 opachint_wide17 opachint_wide18 opachint_wide19 
    opachint_wide20 opachint_wide21 opachint_wide22 opachint_wide23 
    opachint_wide24 opachint_wide25 opachint_wide26 opachint_wide27 
    opachint_wide28 opachint_wide29 opachint_wide30 opachint_wide31 
    opachint_wide32 opachint_wide33 opachint_wide34 opachint_wide35 
    opachint_wide36 opachint_wide37 opachint_wide38 opachint_wide39 
    opachint_wide40 opachint_wide41 opachint_wide42 opachint_wide43 
    opachint_wide44 opachint_wide45 opachint_wide46 opachint_wide47 
    opachint_wide48 opachint_wide49 opachint_wide50 opachint_wide51 
    opachint_wide52 opachint_wide53 opachint_wide54 opachint_wide55 
    opachint_wide56 opachint_wide57 opachint_wide58 opachint_wide59 
    opachint_wide60 opachint_wide61 opachint_wide62 opachint_wide63 
    opachint_wide64 opachint_wide65 opachint_wide66 opachint_wide67 
    opachint_wide68 opachint_wide69 opachint_wide70 opachint_wide71 
    opachint_wide72 opachint_wide73 opachint_wide74 opachint_wide75 
    opachint_wide76 opachint_wide77 opachint_wide78 opachint_wide79 
    opachint_wide80 opachint_wide81 opachint_wide82 opachint_wide83 
    opachint_wide84 opachint_wide85 opachint_wide86 opachint_wide87 
    opachint_wide88 opachint_wide89 opachint_wide90 opachint_wide91 
    opachint_wide92 opachint_wide93 opachint_wide94 opachint_wide95 
    opachint_wide96 opachint_wide97 opachint_wide98 opachint_wide99 
    opachint_wide100 opachint_wide101 opachint_wide102 opachint_wide103 
    opachint_wide104 opachint_wide105 opachint_wide106 opachint_wide107 
    opachint_wide108 opachint_wide109 opachint_wide110 opachint_wide111 
    opachint_wide112 opachint_wide113 opachint_wide114 opachint_wide115 
    opachint_wide116 opachint_wide117 opachint_wide118 opachint_wide119 
    opachint_wide120 opachint_wide121 opachint_wide122 opachint_wide123 
    opachint_wide124 opachint_wide125 opachint_wide126 opachint_wide127 
    opachint_wide128 opachint_wide129 opachint_wide130 opachint_wide131 
    opachint_wide132 opachint_wide133 opachint_wide134 opachint_wide135 
    opachint_wide136 opachint_wide137 opachint_wide138 opachint_wide139 
    opachint_wide140 opachint_wide141 opachint_wide142 opachint_wide143 
    opachint_wide144 opachint_wide145 opachint_wide146
);


/* whi_merge2 */
data a.whi_merge2; set whi_merge; run;



/* Specimen results */
* 1. Read in Specimen datasets (spec_draws_ctos_inv, spec_results_ctos_inv, spec_tests_ctos_inv). ;
data draws; set b.spec_draws_ctos_inv; run;
data results; set b.spec_results_ctos_inv; run;
data tests; set b.spec_tests_ctos_inv; run;

proc sort data=draws; by id pptdrw; run;
proc sort data=results; by TESTVERID; run;
proc sort data=tests; by TESTVERID; run;

* 2. Combined1: Merge Results & Tests datasets (identifier: testverid). ;
data combined1; merge results tests; by testverid; run;
proc sort data=combined1; by id pptdrw; run;

* 3. Combined2: Merge 2 & Draws (identifier: id pptdrw). ;
data combined2; merge combined1 draws; by id pptdrw; run;

* 4. Spec_all_combined: Keep only necessary variables. ;
data spec_all_combined; set combined2;
if TESTABBR in ("TCHO","HDLC","LDLC","TRI","GLUC","INSU","CRP","HGB","CREA"); run;

data b.spec_all_combined; set spec_all_combined; run;

* 5. Spec_all_whills: Keep only the WHI LLS results (W64=WHI LLS study ID). ;
data spec_all_whills; set spec_all_combined; if studyid="W64"; run;

* 6. Generate independent columns by specimen variables. ;
data spec_all_whills; set spec_all_whills;
if testabbr="TCHO" then testval_tcho=testval; else testval_tcho=.;
if testabbr="HDLC" then testval_hdlc=testval; else testval_hdlc=.;
if testabbr="LDLC" then testval_ldlc=testval; else testval_ldlc=.;
if testabbr="TRI" then testval_tri=testval; else testval_tri=.;
if testabbr="GLUC" then testval_gluc=testval; else testval_gluc=.;
if testabbr="INSU" then testval_insu=testval; else testval_insu=.;
if testabbr="CRP" then testval_crp=testval; else testval_crp=.;
if testabbr="HGB" then testval_hgb=testval; else testval_hgb=.;
if testabbr="CREA" then testval_crea=testval; else testval_crea=.;
run;

* 7. Generate datasets for each of the specified specimen variables. ;
%macro spec(var);
data &var.; set spec_all_whills; 
if testval_&var. ne .; 
keep id testval_&var.; run;
proc sort data=&var.; by id; run;
%mend spec;

%spec(tcho);
%spec(hdlc);
%spec(ldlc);
%spec(tri);
%spec(gluc);
%spec(insu);
%spec(crp);
%spec(hgb);
%spec(crea);

* 8. Merge specimen datasets. ;
data whi_merge; set a.whi_merge2; run;
proc sort data=whi_merge; by id; run;
data whi_merge; 
merge whi_merge tcho hdlc ldlc tri gluc insu crp hgb crea; by id; run;



/* Cardiovascular & stroke outcome variables */
*outc_cardio_inv;
%readin(outc_cardio_inv);
%keep(outc_cardio_inv, ID ANGINA CHF PADDX PADPLAQ PADABDOP PADEXER PADSURG PADAMP PADLEG PADULANR PADSRANR);

* One row per outcome form - Generate independent datasets by each variable and keep only those who have no missing value. ;
%macro cardio(var);
data outc_cardio_&var.; set outc_cardio_inv; keep id &var.; if &var. ne .; run;
proc sort data=outc_cardio_&var. nodup; by id; run;
%mend cardio;

%cardio(angina);
%cardio(chf);
%cardio(paddx);
%cardio(padplaq);
%cardio(padabdop);
%cardio(padexer);
%cardio(padsurg);
%cardio(padamp);
%cardio(padleg);
%cardio(padulanr);
%cardio(padsranr);

* Merge the outcomes by id. ;
data outc_cardio_combined;
merge outc_cardio_angina outc_cardio_chf outc_cardio_paddx outc_cardio_padplaq outc_cardio_padabdop 
outc_cardio_padexer outc_cardio_padsurg outc_cardio_padamp outc_cardio_padleg outc_cardio_padulanr outc_cardio_padsranr; by id; run;

data whi_merge;
merge whi_merge outc_cardio_combined; by id; run;


*outc_strk_carotid_inv;
%readin(outc_strk_carotid_inv);
%keep(outc_strk_carotid_inv, ID Stroke);

data outc_strk_carotid_inv; set outc_strk_carotid_inv; keep id stroke; if stroke ne .; run;
proc sort data=outc_strk_carotid_inv nodup; by id; run;

data whi_merge; 
merge whi_merge outc_strk_carotid_inv; by id; run;


data f33_cleaned; set b.f33_cleaned; run;
data f151_cleaned; set b.f151_cleaned; run;
data f151b_cleaned; set b.f151b_cleaned; run;

proc sort data=f33_cleaned; by id; run;
proc sort data=f151_cleaned; by id; run;
proc sort data=f151b_cleaned; by id; run;

data whi_vde;
merge f33_cleaned f151_cleaned f151b_cleaned; by id; run;

data whi_merge_102224; merge whi_merge whi_vde; by id; run;

data whi_merge_110824; set whi_merge_102224;
if id in (100650, 108069, 124743, 216205, 236871, 246083, 290500) then f33_membership=0; else f33_membership=1;
run;


/* whi_merge_complete: Permanently save the dataset */
/*data a.whi_merge_complete; set whi_merge; run;*/
data a.whi_merge_110824; set whi_merge_110824; run;

data whi_merge_110824_lls; set whi_merge_110824; 
if examdy ne .; lls_membership=1; run;

data a.whi_merge_110824_lls; set whi_merge_110824_lls ; run;

proc univariate data=whi_merge_110824_lls; var testval_ldlc testval_hdlc; run;
