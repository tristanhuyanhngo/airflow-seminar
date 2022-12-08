create database stage

create database nds

create database dds

create database metadata

create database southern

create table if not exists data_flow  (
  "ID" integer not null generated always as identity (increment by 1),
  "TableName" varchar(50) NULL,
  "CET" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  "LSET" TIMESTAMP WITH TIME ZONE DEFAULT NULL
)

create table if not exists katinat_customer_rainbow_drink (
  "ID" integer not null generated always as identity (increment by 1),
  "Name" varchar(50) NULL,
  "Gender" varchar(10) NULL,
  "TimeArrived"TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  "TimeAway" TIMESTAMP WITH TIME ZONE DEFAULT NULL,
)

