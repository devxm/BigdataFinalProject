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
# Hive Queries	:	For performing data analysis using Hive											#
#																									#
#####################################################################################################
*/
 
 /*############# Creating Database 'parkingtickets' #################*/
 create database parkingtickets;
 
 use parkingtickets;
 
 /*############# Creating Table 'parkingtickets' #################*/
 create table parkingtickets(summons_number int,Plateid string,state string,Plate_Type string,Issue_Date date,Violation_Code int, Vehicle_Body_Type string,Vehicle_Make string, Issuing_Agency string,Street_Code1 int,Street_Code2 int, Street_Code3 int,Vehicle_Expiration_Date string,Violation_Location int, Violation_Precinct int,Issuer_Precinct int,Issuer_Code int,Issuer_Command int,Issuer_Squad int,Violation_Time string,Time_First_Observed string, Violation_County string,Violation_In_Front_Of_Or_Opposite string,House string,Street_Name string,Intersecting_Street string,Date_First_Observed string, Law_Section int,Sub_Division string,Violation_Legal_Code string,Days_Parking_In_Effect string,From_Hours_In_Effect string,To_Hours_In_Effect string,Color string,Unregistered_Vehicle int,Vehicle_Year int,Meter_Number string,Feet_From_Curb int,Violation_Post_Code string,Violation_Description string,No_Standing_or_Stopping_Violation string,Hydrant_Violation string,Double_Parking_Violation string) row format delimited fields TERMINATED BY ',';
 
 /* ############# Loading data into the table from HDFS #############*/
  LOAD DATA LOCAL INPATH '/home/maria_dev/FullParkingNYCData.csv' overwrite into table parkingtickets;
  
  /* ############# Number of Parking Violation By NY state and Others #############*/
   select state,case when state='NY' then 'NY Tickets' else 'Other State Tickets' end,sum(violation_code) as sum from parkingtickets GROUP BY state order by sum desc limit 10;
  
  /*############# Number of Parking Violation By vehicle Make #############*/
   select sum(violation_code) AS sum,vehicle_make from parkingtickets group by vehicle_make order by sum desc limit 10;
  
  
  /*############# Number of Parking Violation By Registration State #############*/
  select sum(violation_code) AS sum,state from parkingtickets group by state order by sum desc limit 10;
  
  
  /*############# Number of Parking Violation By Plate ID and Number of Parking Violation count #############*/
   select count(violation_code) AS count,plateid from parkingtickets group by plateid order by count desc limit 5;
  
  
  /*############# Number of Parking Violation By Street and Number of Parking Violation count #############*/
  select count(violation_code) AS count,street_name from parkingtickets group by street_name order by count desc limit 15;
  