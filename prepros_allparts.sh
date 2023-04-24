#!/bin/bash

# removes previous output
hadoop fs -rm -r /chess_2016_dataset/output

# run MapReduce job on each input file separately
for i in $(seq 1 10); do
    python3 test/test_mapreduce.py \
        --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar \
        --jobconf mapred.output.textoutputformat.separator=" " \
        -r hadoop hdfs://namenode:9000/chess_2016_dataset/input_files/chess_part_$i.txt \
        --output-dir hdfs://namenode:9000/chess_2016_dataset/output_$i --no-output
done

