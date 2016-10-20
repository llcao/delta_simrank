#!/bin/sh
#
# Author: mtsai2@illinois.edu (Min-Hsuan Tsai)
# Beckman Institute, University of Illinois, 2010-2011
#
data?=facebook_graph
data_fdr?=../../simrank/data/facebook
result_fdr?=../results/$data
jobname?=deltaonly-${data}
adj_file?=${data}_adj.txt
inLen_file?=${data}_inLen.txt
input_local?=${data}_init_delta.txt
input_local_value?=${data}_init_delta_value.txt
iter?=10
output_local?=output-${jobname}-iter$iter.txt
result_local?=result-${jobname}-iter$iter.txt

hadoop_home?=/usr/local/hadoop
hadoop_fdr?=/hdfs/mtsai2/delta_simrank

numMap=32
numRed=16

skip_step1=0
skip_step2=0
skip_step3=0

[ -d $result_fdr ] || mkdir -p $result_fdr

if [ $skip_step1 = 0 ]; then

hadoop dfs -rm $hadoop_fdr/$input_local
hadoop dfs -put $data_fdr/$input_local $hadoop_fdr/$input_local
cp $data_fdr/$adj_file adj.txt
cp $data_fdr/$inLen_file inLen.txt
#hdfs dfs -put $cache_file $hadoop_fdr/$cache_file

hadoop dfs -rmr $hadoop_fdr/$output_local
hadoop dfs -rmr $hadoop_fdr/${output_local}_pre*

dumbo start delta_simRank_twostep.py \
	-hadoop $hadoop_home \
	-input $hadoop_fdr/$input_local \
	-output $hadoop_fdr/$output_local \
	-file adj.txt \
	-file inLen.txt \
	-file simRank_twostep.py \
	-nummaptasks $numMap \
	-numreducetasks $numRed \
	-name $jobname \
	-memlimit 1000000000 \
	-param iter=$iter

fi

if [ $skip_step2 = 0 ]; then

#### very stupid, is there any smart way to merge files in HDFS?????
rm $result_fdr/$output_local
dumbo cat $hadoop_fdr/$output_local -hadoop $hadoop_home > $result_fdr/$output_local
hadoop dfs -rmr $hadoop_fdr/$data
hadoop dfs -put $result_fdr/$output_local $hadoop_fdr/$data/$output_local

rm $result_fdr/${output_local}_pre*
for i in `seq 2 2 $iter` 
do
    dumbo cat $hadoop_fdr/${output_local}_pre$i -hadoop $hadoop_home > $result_fdr/${output_local}_pre$i
    hadoop dfs -put $result_fdr/${output_local}_pre$i $hadoop_fdr/$data/${output_local}_pre$i
done

hadoop dfs -put $data_fdr/$input_local_value $hadoop_fdr/$data/$input_local_value

fi

if [ $skip_step3 = 0 ]; then

numRed=32
jobname=combine_score-$data

hadoop dfs -rmr $hadoop_fdr/$result_local

dumbo start combine_score_dumbo.py \
	-hadoop $hadoop_home \
	-input $hadoop_fdr/$data/* \
	-output $hadoop_fdr/$result_local \
	-nummaptasks $numMap \
	-numreducetasks $numRed \
	-name $jobname

rm $result_fdr/$result_local
dumbo cat $hadoop_fdr/$result_local -hadoop $hadoop_home > $result_fdr/$result_local

fi
