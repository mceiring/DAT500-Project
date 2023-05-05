# removes previouce output
hadoop fs -rm -r /chess_2016_dataset/output

# runs current files, with specified input and output
python3 mapreduce.py \
     --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar \
     --jobconf mapred.output.textoutputformat.separator=" " \
     -r hadoop hdfs://namenode:9000/chess_2016_dataset/input_files/input_files/*.txt \
     --output-dir hdfs://namenode:9000/chess_2016_dataset/output --no-output

