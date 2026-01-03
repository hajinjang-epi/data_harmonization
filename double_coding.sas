
/*******************************/
/* Double coding by Hajin Jang */
/***** Writer: Hajin Jang ******/
/******* Date: 10/6/2025 *******/
/*******************************/


	dm "odsresults; clear";
	dm "log; clear";

	ods results on;  /* Ensure the Results Viewer is enabled */

	options nofmterr;


libname nffi "Z:\Fall Injuries Combined Studies\habc, mros, chs, whi Combined\Final Datasets for NFFI";
libname double "Z:\Fall Injuries Combined Studies\Harmonization Coding\Hajin code\Double-coding";
libname kerri "Z:\Fall Injuries Combined Studies\Harmonization Coding\Hajin code\Double-coding\Kerri's\Kerri datasets";


/******************/
/* coded datasets */
/******************/

%macro chsid(data);
data &data.; set kerri.&data.; id=idno; run;
proc sort data=&data.; by id; run;
%mend chsid;
%macro habcid(data);
data &data.; set kerri.&data.; id=habcid; run;
proc sort data=&data.; by id; run;
%mend habcid;

%chsid(agevar_chs);
%chsid(heightvar_chs);
%chsid(physactivityvar_chs);
%chsid(racesexedvar_chs);
%chsid(dieafter90var_chs);

%habcid(agevar_habc);
%habcid(heightvar_habc);
%habcid(physactivityvar_habc);
%habcid(racesexedvar_habc);
%habcid(dieafter90var_habc);

%macro sort(data);
data &data.; set kerri.&data.; run;
proc sort data=&data.; by id; run;
%mend sort;

%sort(agevar_mros);
%sort(heightvar_mros);
%sort(physactivityvar_mros);
%sort(racesexedvar_mros);

data dieafter90var_mros; set kerri.dieafter90var_mros;
rename id_mros=id; 
run;
proc sort data=dieafter90var_mros; by id; run;

%sort(agevar_whi);
%sort(heightvar_whi);
%sort(physactivityvar_whi);
%sort(racesexedvar_whi);
%sort(dieafter90var_whi);

%macro merge(final,data1,data2,data3,data4,data5);
data &final.; merge &data1. &data2. &data3. &data4. &data5.; by id; run;
%mend merge;

%merge(chs_k,agevar_chs,heightvar_chs,physactivityvar_chs,racesexedvar_chs,dieafter90var_chs);
%merge(habc_k,agevar_habc,heightvar_habc,physactivityvar_habc,racesexedvar_habc,dieafter90var_habc);
%merge(mros_k,agevar_mros,heightvar_mros,physactivityvar_mros,racesexedvar_mros,dieafter90var_mros);
%merge(whi_k,agevar_whi,heightvar_whi,physactivityvar_whi,racesexedvar_whi,dieafter90var_whi);


/********/
/* Data */
/********/

*  chs: chs_master_032124  ;
*  habc: habc_master_kf_091525  ;
*  mros: mros_master_nh_012425  ;
*  whi: whi_master_hj_032425  ;

data chs; set nffi.chs_master_032124; id=idno; run;
data habc; set nffi.habc_master_kf_091525; id=habcid; run;
data mros; set nffi.mros_master_nh_012425; run;
data whi; set nffi.whi_master_hj_032425; run;

data chs_k; set kerri.chs_k; run;
data habc_k; set kerri.habc_k; run;
data mros_k; set kerri.mros_k; run;
data whi_k; set kerri.whi_k; run;


/*****************/
/* Compare macro */
/*****************/

%macro compare(data,var);
proc compare base=&data. compare=&data._k; id id;
var &var.;
run;
%mend compare;


/********/
/* Race */
/********/

*'Race Harmonized Variable: white (1), Black(2), Asian, American Indian or Alaskan Native(3), Native Hawaiian or other Pacific Islander(4), Other (5)';

proc freq data=chs; table race01; run;
data chs; set chs; race_chs=race01; run;
%compare(chs,race_chs);

proc freq data=habc; table race; run;
data habc; set habc; race_habc=race; run;
%compare(habc,race_habc);

proc freq data=mros; table girace; run;
data mros; set mros; 
if girace=. then race_mros=.;
else if girace=1 then race_mros=1;
else if girace=2 then race_mros=2;
else if girace in (3,5) then race_mros=3;
else if girace=4 then race_mros=4;
else if girace=6 then race_mros=5;
else if girace=7 then race_mros=.;
run;
%compare(mros,race_mros); /* asian, native indian, hawaiian */
proc freq data=mros_k; table race_mros; run;
proc freq data=mros; table race_mros; run;

proc freq data=whi; table racenih; run;
data whi; set whi;
if racenih=. then race_whi=.;
else if racenih=5 then race_whi=1;
else if racenih=4 then race_whi=2;
else if racenih in (1,2) then race_whi=3;
else if racenih=3 then race_whi=4;
else if racenih=6 then race_whi=5;
else if racenih=9 then race_whi=.;
run;
%compare(whi,race_whi); /* missing 2 */
proc freq data=whi; table race_whi; run;
proc freq data=whi_k; table race_whi; run;


/**********/
/* Gender */
/**********/

* 'Gender Harmonized Variable: Male(1), Female(0)';
proc freq data=chs; table gend01; run;
data chs; set chs;
if gend01=. then gender_chs=.;
else if gend01=1 then gender_chs=1;
else if gend01=0 then gender_chs=0;
run;
%compare(chs,gender_chs);

proc freq data=habc; table gender; run;
data habc; set habc;
if gender=. then gender_habc=.;
else if gender=1 then gender_habc=1;
else if gender=2 then gender_habc=0;
run;
%compare(habc,gender_habc);

data mros; set mros;
gender_mros=1;
run;
%compare(mros,gender_mros);

data whi; set whi;
gender_whi=0;
run;
%compare(whi,gender_whi);


/*************/
/* Education */
/*************/

*EDUCA_H (Definition: 1=less than HS, 2=HS grad, 3=postsecondary);

data chs; set chs;
if grade01=. then educa_chs=.;
else if grade01 in (0,1,2,3,4,5,6,7,8,9,10,11) then educa_chs=1;
else if grade01 in (12,13,14,15,16) then educa_chs=2;
else if grade01 in (17,18,19,20,21) then educa_chs=3;
run;
/* chs vocational school included as highschool graduate? In whi, vocational school is included in postsecondary */
%compare(chs,educa_chs);

data habc; set habc;
educa_habc=educ;
run;
%compare(habc,educa_habc);

data mros; set mros;
if gieduc=. then educa_mros=.;
else if gieduc in (1,2,3) then educa_mros=1;
else if gieduc=4 then educa_mros=2;
else if gieduc in (5,6,7,8) then educa_mros=3;
run;
%compare(mros,educa_mros);

data whi; set whi;
if educ=. then educa_whi=.;
else if educ in (1,2,3,4) then educa_whi=1;
else if educ=5 then educa_whi=2;
else if educ in (6,7,8,9,10,11) then educa_whi=3;
run;
%compare(whi,educa_whi);


/*******/
/* Age */
/*******/

data chs; set chs;
age1_chs=agea_y5;
age2_chs=agea_y6;
age3_chs=agea_y7;
age4_chs=agea_y8;
age5_chs=agea_y9;
age6_chs=agea_y10;
age7_chs=agea_y11;
age14_chs=agea_y18;
run;
%compare(chs,age1_chs);
%compare(chs,age2_chs);
%compare(chs,age3_chs);
%compare(chs,age4_chs);
%compare(chs,age5_chs);
%compare(chs,age6_chs);
%compare(chs,age7_chs);
%compare(chs,age14_chs);

data habc; set habc;
age6_habc=cv1age;
age7_habc=cv2age;
age8_habc=cv3age;
age9_habc=cv4age;
age10_habc=cv5age;
age11_habc=cv6age;
age12_habc=cv7age;
age13_habc=cv8age;
age14_habc=cv9age;
age15_habc=cv10age;
age16_habc=cv11age;
age17_habc=cv12age;
age18_habc=cv13age;
run;
%compare(habc,age6_habc);
%compare(habc,age7_habc);
%compare(habc,age8_habc);
%compare(habc,age9_habc);
%compare(habc,age10_habc);
%compare(habc,age11_habc);
%compare(habc,age12_habc);
%compare(habc,age13_habc);
%compare(habc,age14_habc);
%compare(habc,age15_habc);
%compare(habc,age16_habc);
%compare(habc,age17_habc); /* id=1215 mine: 76, Kerri's: 81 */
%compare(habc,age18_habc);

data mros; set mros;
age9_mros=giage1;
age11_mros=viage1;
age14_mros=v2age1;
age16_mros=v3age1;
age18_mros=vi2age1;
age23_mros=v4age1;
run;
%compare(mros,age9_mros);
%compare(mros,age11_mros);
%compare(mros,age14_mros);
%compare(mros,age16_mros);
%compare(mros,age18_mros);
%compare(mros,age23_mros);

data whi; set whi;
f2day_diff = examdy-f2days;
age_whi = age+ (f2day_diff/365.25);
age21_whi= round(age_whi);
run;
%compare(whi,age21_whi);


/**********/
/* Height */
/**********/

data chs; set chs;
height1_chs=stht_y5*10;
height5_chs=stht_y9*10;
height14_chs=stht_y18*10;
run;
%compare(chs,height1_chs);
%compare(chs,height5_chs);
%compare(chs,height14_chs);

data habc; set habc;
height6_habc=p2sh;
height9_habc=d2sh;
height11_habc=f3sh;
height13_habc=s4sh_y8;
height15_habc=s4sh_y10;
height16_habc=y11sh;
run;
%compare(habc,height6_habc);
%compare(habc,height9_habc);
%compare(habc,height11_habc);
%compare(habc,height13_habc);
%compare(habc,height15_habc);
%compare(habc,height16_habc);

data mros; set mros;
height9_mros=hwhgt_V1*10;
height14_mros=hwhgt_V2*10;
height16_mros=hwhgt_V3*10;
height23_mros=hwhgt_V4*10;
run;
%compare(mros,height9_mros);
%compare(mros,height14_mros);
%compare(mros,height16_mros);
%compare(mros,height23_mros);

data whi; set whi;
height21_whi=height*25.4;
run;
%compare(whi,height21_whi);


/*********************/
/* Physical activity */
/*********************/

data chs; set chs;
physactivity1_chs = log(kcal_y5);
physactivity5_chs = log(kcal_y9);
run;
proc standard data=chs mean=0 std=1 out=chs;
var physactivity1_chs physactivity5_chs;
run;
%compare(chs,physactivity1_chs);
%compare(chs,physactivity5_chs);

data habc; set habc;
physactivity6_habc=log(totkkwk);
run;
proc standard data=habc mean=0 std=1 out=habc;
var physactivity6_habc;
run;
%compare(habc,physactivity6_habc);

data mros; set mros;
physactivity9_mros=log(pascore_v1);
physactivity14_mros=log(pascore_v2);
physactivity16_mros=log(pascore_v3);
physactivity18_mros=log(pascore_vi2);
physactivity23_mros=log(pascore_v4);
run;
proc standard data=mros mean=0 std=1 out=mros;
var physactivity9_mros physactivity14_mros physactivity16_mros physactivity18_mros physactivity23_mros;
run;
%compare(mros,physactivity9_mros);
%compare(mros,physactivity14_mros);
%compare(mros,physactivity16_mros);
%compare(mros,physactivity18_mros);
%compare(mros,physactivity23_mros); /* not matching */


/**********/
/* Weight */
/**********/

data chs; set chs;



/*********/
/* Waist */
/*********/





/****************/
/* Age at death */
/****************/

data chs; set nffi.chs_master_032124; id=idno; run;
data habc; set nffi.habc_master_kf_091525; id=habcid; run;
data mros; set nffi.mros_master_nh_012425; run;
data whi; set nffi.whi_master_hj_032425; run;

data chs_k; set kerri.dieafter90var_chs; rename idno=id; run;
data habc_k; set kerri.dieafter90var_habc; rename habcid=id; run;
data mros_k; set kerri.dieafter90var_mros; rename id_mros=id; run;
data whi_k; set kerri.dieafter90var_whi; run;


%macro compare(data,var);
proc compare base=&data. compare=&data._k; id id;
var &var.;
run;
%mend compare;


*habc;
data habc; set habc;
ageatdeath_habc = yrdif(dob, dod, 'AGE');
run;
%compare(habc,ageatdeath_habc);


*chs;
proc contents data=chs; run;
/* age at death = age at Year5 + time to death from Year5 */

/*
data chs; set chs;
if ttodth_y5 ne . then do;
ageatdeath_chs = agea_y5 + (ttodth_y5 / 365.25);
end;
run;
*/

proc means data=chs; var ageatdeath_chs; run;
proc means data=chs_k; var ageatdeath_chs; run;

libname har 'Z:\Fall Injuries Combined Studies\Harmonization Coding\Harmonized dataset';
data final_full; set har.final_full_081525; run;
proc contents data=final_full; run;
data agechs; set final_full; keep id_chs ageint1_chs; if ageint1_chs ne .; run;
data agechs; set agechs; rename id_chs = id; run;

proc sort data=chs; by id; run;
proc sort data=agechs; by id; run;

data chs2; merge chs agechs; by id; run;

data chs2; set chs2;
if ttodth_y5 ne . then do;
ageatdeath_chs = ageint1_chs + (ttodth_y5 / 365.25);
end;
run;

data chs; set chs2; run;
%compare(chs,ageatdeath_chs);


*whi;
data whi; set whi;
f2day_diff=deathdy_outc-f2days; 
age_re= age + (f2day_diff/365.25); 
ageatdeath_whi= round(age_re);
run;
%compare(whi,ageatdeath_whi);


*mros;
data mros; set mros; run;
data agemros; set final_full; keep id_mros ageint9_mros; if ageint9_mros ne .; run;
data agemros; set agemros; rename id_mros = id; run;

proc sort data=mros; by id; run;
proc sort data=agemros; by id; run;

data mros2; merge mros agemros; by id; run;

data mros2; set mros2;
if FUCYTIME ne . then do;
ageatdeath_mros = ageint9_mros + (FUCYTIME / 365.25);
end;
run;

proc means data=mros2; var ageatdeath_mros; run;
proc means data=mros_k; var ageatdeath_mros; run;




/****************/
/* vital status */
/****************/

* Yearly vital status: 1=alive / 0=died / 2=lost to follow-up or censored or missing ;

data chs; set nffi.chs_master_032124; id=idno; run;
data habc; set nffi.habc_master_kf_091525; id=habcid; run;
data mros; set nffi.mros_master_nh_012425; run;
data whi; set nffi.whi_master_hj_032425; run;

data chs_k; set kerri.vitalstatus_chs; rename idno=id; run;
data habc_k; set kerri.vitalstatus_habc; rename habcid=id; run;
data mros_k; set kerri.vitalstatus_mros; rename id_mros=id; run;
data whi_k; set kerri.vitalstatus_whi; run;

%macro compare(data,var);
proc compare base=&data. compare=&data._k; id id;
var &var.;
run;
%mend compare;


*chs;
data chs2;set chs(rename=(stdytimea_y5imp = stdytimea_y5));

array st[5:18] stdytimea_y5 - stdytimea_y18;
if not missing(st[18]) then death14 = 0;                              /* Alive at Y18 */
else if not missing(ttodth_y5) and ttodth_y5 < 5117 then death14 = 1; /* Died = 14 years */
else death14 = 0;

last_study_time = .;
do k = 5 to 18;
if not missing(st[k]) then last_study_time = st[k];
end;

death_day = ifn(death14=1, stdytimea_y5 + ttodth_y5, .);

ltfu = 0;
if death14=0 and missing(st[18]) then ltfu = 1;                       /* Alive but no Y18 */
else if death14=1 and death_day > last_study_time + 365 then ltfu = 1;/* Died long after */

if death14=1 and ltfu=0 then status_chs = 0;
else if death14=0 and ltfu=0 then status_chs = 1;
else status_chs = .; 

drop k;

/* annual vital status */
array vital[1:14] vital1 - vital14;
  do kvis = 5 to 18;
    idx = kvis - 4;

    if not missing(st[kvis]) then vital[idx] = 1; /* Seen alive */
    else do;
     
      later_nonmissing = 0;
      do kk = kvis+1 to 18;
        if not missing(st[kk]) then later_nonmissing = 1;
      end;

      if later_nonmissing = 1 then vital[idx] = 1; /* Temporary missing */
      else if ltfu = 1 then vital[idx] = .;        /* LTFU at end */
      else vital[idx] = 0;                         /* Died before next visit */
    end;
  end;

  drop kvis kk later_nonmissing;
run;


data chs2; set chs2;
rename
vital1  = vital1_chs
vital2  = vital2_chs
vital3  = vital3_chs
vital4  = vital4_chs
vital5  = vital5_chs
vital6  = vital6_chs
vital7  = vital7_chs
vital8  = vital8_chs
vital9  = vital9_chs
vital10 = vital10_chs
vital11 = vital11_chs
vital12 = vital12_chs
vital13 = vital13_chs
vital14 = vital14_chs;
run;

data chs; set chs2; run;

%compare(chs,vital1_chs);
%compare(chs,vital2_chs);
%compare(chs,vital3_chs);
%compare(chs,vital4_chs);
%compare(chs,vital5_chs);
%compare(chs,vital6_chs);
%compare(chs,vital7_chs);
%compare(chs,vital8_chs);
%compare(chs,vital9_chs);
%compare(chs,vital10_chs);
%compare(chs,vital11_chs);
%compare(chs,vital12_chs);
%compare(chs,vital13_chs);
%compare(chs,vital14_chs);

proc contents data=chs; run;


*mros;
data mros2;
  set mros;
  if efstatus in (0,3) then status=1;
  else if efstatus=1 then status=0;
  else status=.;
run;

data mros3;
  set mros2;
  vital0=1;
  array vital[14] vital1 - vital14;

  do k = 1 to 14;
    if status=0 and fucytime<k then vital[k]=0;
    else if fucytime>=k then vital[k]=1;
    else vital[k]=.;
  end;

  if vital[14]=1 then status=1;
  drop k;
run;

data mros4; 
  set mros3;
rename
    vital0=vital9_mros
    vital1=vital10_mros
    vital2=vital11_mros
    vital3=vital12_mros
    vital4=vital13_mros
    vital5=vital14_mros
    vital6=vital15_mros
    vital7=vital16_mros
    vital8=vital17_mros
    vital9=vital18_mros
    vital10=vital19_mros
    vital11=vital20_mros
    vital12=vital21_mros
    vital13=vital22_mros
    vital14=vital23_mros
 ;
run;

data mros; set mros4; run;

%compare(mros,vital10_mros);
%compare(mros,vital11_mros);
%compare(mros,vital12_mros);
%compare(mros,vital13_mros);
%compare(mros,vital14_mros);
%compare(mros,vital15_mros);
%compare(mros,vital16_mros);
%compare(mros,vital17_mros);
%compare(mros,vital18_mros);
%compare(mros,vital19_mros);
%compare(mros,vital20_mros);
%compare(mros,vital21_mros);
%compare(mros,vital22_mros);
%compare(mros,vital23_mros);


*habc;
data habc2; set habc;
array src[12] vital12M vital24M vital36M vital48M vital60M vital72M vital84M vital96M vital108M vital120M vital132M vital144M;
array vital[12] vital1 - vital12;

vital[1] = 1;
do i = 2 to 12;
vital[i] = src[i-1];
end;

do i = 1 to 12;
if vital[i] = 2 then vital[i] = 0;
if vital[i] = .Z then vital[i] = .;
end;

drop i;

select (vital[12]);
when (1) STATUS_habc = 1;   /* alive through final follow-up */
when (0) STATUS_habc = 0;   /* died before study end */
otherwise STATUS_habc = .;  /* lost to follow-up or missing */
end;
run;

data habc2; set habc2;
rename 
vital1  = vital6_habc
vital2  = vital7_habc
vital3  = vital8_habc
vital4  = vital9_habc
vital5  = vital10_habc
vital6  = vital11_habc
vital7  = vital12_habc
vital8  = vital13_habc
vital9  = vital14_habc
vital10 = vital15_habc
vital11 = vital16_habc
vital12 = vital17_habc
;
run;

data habc; set habc2; run;

%compare(habc,vital6_habc);
%compare(habc,vital7_habc);
%compare(habc,vital8_habc);
%compare(habc,vital9_habc);
%compare(habc,vital10_habc);
%compare(habc,vital11_habc);
%compare(habc,vital12_habc);
%compare(habc,vital13_habc);
%compare(habc,vital14_habc);
%compare(habc,vital15_habc);
%compare(habc,vital16_habc);
%compare(habc,vital17_habc);


*whi;
proc contents data=whi; run;

data whi2; set whi;
  array visits[5] f33days_1-f33days_5;
  array vital[5] vital1-vital5;

  last_visit_index=0;
  last_visit_day=.;
  do i=5 to 1 by -1;
    if not missing(visits[i]) then do;
      last_visit_index=i;
      last_visit_day=visits[i];
      leave;
    end;
  end;

  ltfu=0;
  if deathall=0 then ltfu=0;
  else if deathall=1 then do;
    if last_visit_index<5 and not missing(deathalldy) and not missing(last_visit_day)
       and (deathalldy-last_visit_day)>=365 then ltfu=1;
  end;

  died_flag=0;
  do k=1 to 5;
    if died_flag=1 then vital[k]=0;
    else do;
      if not missing(visits[k]) and (deathall=0 or visits[k]<=deathalldy) then vital[k]=1;
      else if deathall=1 and not missing(deathalldy) and (visits[k]=. or deathalldy<=visits[k]) then do;
        vital[k]=0;
        died_flag=1;
      end;
      else if ltfu=1 then vital[k]=.;
      else vital[k]=.;
    end;
  end;

  cnt_nonmiss=n(of vital1-vital5);
  sum_v=sum(of vital1-vital5);

  if ltfu=1 then status_whi=2;
  else if cnt_nonmiss=5 and sum_v=5 then status_whi=1;
  else if min(of vital1-vital5)=0 then status_whi=0;
  else status_whi=.;

  drop i k died_flag cnt_nonmiss sum_v last_visit_index last_visit_day;
run;

data whi3; set whi2;
rename
vital1 = vital21_whi
vital2 = vital22_whi
vital3 = vital23_whi
vital4 = vital24_whi
vital5 = vital25_whi
;
run;

data whi; set whi3; run;

%compare(whi,vital21_whi);
%compare(whi,vital22_whi);
%compare(whi,vital23_whi);
%compare(whi,vital24_whi);
%compare(whi,vital25_whi);


