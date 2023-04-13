### RUN THESE COMMANDS
# TESTING
# hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
#     -mapper chess_structure_mapper.py \ 
#     -reducer chess_structure_reducer.py \
#     -input /chess_2016_dataset/chees_part_1.txt \
#     -output /chess_2016_dataset/chess_2016_structureddataset \
#     -file chess_structure_mapper.py \
#     -file chess_structure_reducer.py

# hadoop fs -ls /chess_2016_dataset/output

# removes previouce output
hadoop fs -rm -r /chess_2016_dataset/output

### RUN ALL SHIT
# runs current files, with specified input and output
python3 test/test_mapreduce.py \
     --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar \
     --jobconf mapred.textoutputformat.separator=" " \
     -r hadoop hdfs://namenode:9000/chess_2016_dataset/mini_chess.txt \
     --output-dir hdfs://namenode:9000/chess_2016_dataset/output --no-output
     

### TEST RUN WITH LIMITED DATA
# # runs current files, with specified input and output
# python3 test/test_mapreduce.py \
#      --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar \
#      --jobconf mapred.textoutputformat.separator=" " \
#      -r hadoop hdfs://namenode:9000/chess_2016_dataset/chess_part_1.txt \
#      --output-dir hdfs://namenode:9000/chess_2016_dataset/output --no-output

# transfere output to csv file
# hadoop fs -text /chess_2016_dataset/output/part* > chess_2016_dataset/chess_data_1.csv

# py chess_structure_mapper.py inline -r chess_2016_dataset/chess_part_1.txt
# py chess_structure_mapper.py -r inline chess_2016_dataset/chess_part_1.txt

# SPARK WEB UI ssh namenode -L 4040:localhost:4040

# python3 chess_csv_mapper.py \
#      --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar \
#      -r hadoop hdfs://namenode:9000/chess_2016_dataset/output1/part* \
#      --output-dir hdfs://namenode:9000/chess_2016_dataset/clean --no-output
