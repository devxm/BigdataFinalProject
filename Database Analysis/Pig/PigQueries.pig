 /*
#####################################################################################################
# 							CSP554 - BigData Technologies Final Project								#
#####################################################################################################
# Team					: 	BigData Developers														#
# Team Members			:	Akhil Padgilwar (A20427219), Ganga Gudi (A20428842)						#
#							Ridhima Bhalerao (A20422550), Sagar Ippili (A20417999)					#
# Project				: 	Predictive & Descriptive Analytics on NYC Parking Violations			#
#							(2016, 2017, 2018, 2019)												#
#####################################################################################################
# Pig Queries	:	For performing data analysis using Pig											#
#																									#
#####################################################################################################
*/


/*################Pre setting(Loading Data into PIG)#############################*/

parking_data= Load '/user/ggudi/FullParkingNYCData.csv' USING PigStorage(',') as (id:int, f1:chararray, f2:chararray, f3:chararray, f4:chararray, f5:int, f6:chararray, f7:chararray, f8:chararray, f9:int, f10:int, f11:int, f12:int, f13:int, f14:int, f15:int, f16:int, f17:int, f18:int, f19:int, f20:chararray, f21:chararray, f22:chararray, f23:int, f24:chararray, f25:chararray, f26:int, f27:int, f28:chararray, f29:chararray, f30:chararray, f31:chararray, f32:chararray, f33:chararray, f34:int, f35:int, f36:chararray, f37:int);


/*################Creating Subset#############################*/
parking_data_subset= foreach parking_data generate id,f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f21,f22,f23,f24,f26,f27,f28,f30,f31,f32,f33,f34,f35,f36,f37;

/*################Number of Parking Violation By NY state and Others#############################*/

vrs = GROUP parking_data_subset BY f2;
count_vrs = FOREACH vrs GENERATE ($0 MATCHES 'NY'? 'NY':'Others'), COUNT(parking_data_subset.f5) AS c;
order_vrs = ORDER count_vrs BY c DESC;
top_vrs_10 = LIMIT order_vrs 10;
dump top_vrs_10;

/*################Number of Parking Violation By vehicle Make#############################*/

vm = FOREACH parking_data_subset GENERATE f5,f7; 
group_vm = GROUP parking_data_subset BY f7;
count_vm = FOREACH group_vm GENERATE $0, COUNT(parking_data_subset.f5) AS d;
order_vm = ORDER count_vm BY d DESC;
pick_vm = FOREACH order_vm GENERATE $0;
top_10_vm = LIMIT pick_vm 10;
dump top_10_vm;

/*################Number of Parking Violation By Registration State #############################*/
vs = FOREACH parking_data_subset GENERATE f5,f2; 
group_vs = GROUP parking_data_subset BY f2;
count_vs = FOREACH group_vs GENERATE $0, COUNT(parking_data_subset.f5) AS e;
order_vs = ORDER count_vs BY e DESC;
top_10_vs = LIMIT order_vs 10;
dump top_10_vs;


/*########################## Number of Parking Violation By Plate ID and Number of Parking Violation count ######################*/
vp = FOREACH parking_data_subset GENERATE f1,f5; 
group_vp = GROUP parking_data_subset BY f1;
count_vp = FOREACH group_vp GENERATE $0, COUNT(parking_data_subset.f5) AS f;
order_vp = ORDER count_vp BY f DESC;
top_5_vp = LIMIT order_vp 5;
dump top_5_vp;

/*##################### Number of Parking Violation By Street and Number of Parking Violation count ##############################*/
vst = FOREACH parking_data_subset GENERATE f24,f5; 
group_vst = GROUP parking_data_subset BY f24;
count_vst = FOREACH group_vst GENERATE $0, COUNT(parking_data_subset.f5) AS g;
order_vst = ORDER count_vst BY g DESC;
top_10_vst = LIMIT order_vst 10;
dump top_10_vst;
