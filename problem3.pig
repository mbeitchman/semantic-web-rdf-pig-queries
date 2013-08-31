{\rtf1\ansi\ansicpg1252\cocoartf1138\cocoasubrtf230
{\fonttbl\f0\fswiss\fcharset0 Helvetica;\f1\fmodern\fcharset0 Courier;}
{\colortbl;\red255\green255\blue255;}
\margl1440\margr1440\vieww16160\viewh9980\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural

\f0\fs24 \cf0 -- CLUSTER OF 15 medium INSTANCES with 1 medium master\
-- TOTAL RUNTIME WAS 16min 40 sec\
\
register s3n://uw-cse344-code/myudfs.jar\
\
raw = LOAD 's3n://uw-cse344/btc-2010-chunk-000' USING TextLoader as (line:chararray); \
\
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);\
\
-- filter on rdfabout.com\
filtered_triples = FILTER ntriples BY subject MATCHES '.*rdfabout\\\\.com.*';
\f1 \
\
-- make copy of tuples\

\f0 filtered_triples_copy  = 
\f1 FOREACH filtered_triples GENERATE subject as subject2, predicate as predicate2, object as object2;
\f0 \
\
-- join\
joined_triples = JOIN filtered_triples BY object, filtered_triples_copy BY subject2;\
\
-- return distinct triples\
joined_triples_distinct = DISTINCT joined_triples;\
\
-- order by predicate\
results = ORDER joined_triples_distinct BY predicate;\
\
-- store results\
store results into '/user/hadoop/results' using PigStorage();}