#!/usr/bin/env python


# Pipeline steps:
# •Hadoop reads unstructured data from a text dataset
    # TODO: make mapper
    # TODO: make reducer
    # TODO: run hadoop_preproc.sh

# RUN THESE COMMANDS#
# hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
#     -mapper chess_structure_mapper.py \ 
#     -reducer chess_structure_reducer.py \
#     -input /chess_2016_dataset/*.txt \
#     -output /chess_2016_dataset/chess_2016_structureddataset \
#     -file chess_structure_mapper.py \
#     -file chess_structure_reducer.py

# hadoop fs -text /dis_materials/output1/part*

# •Hadoop converts the unstructured data into a structured dataset.
# •Hadoop saves the structured dataset into csv format in the HDFS.
# •Spark create data schema and enforce schema. 
# •Spark read the csv files into Spark dataframe.
# •Spark transform text variables to proper types (timestamp, double, integer, etc)
# •Spark implement algorithm to enhance the dataset
# •Spark update the results in the Delta table and make sure that there are no duplicates.

#INPUT LINE: Event "Rated Bullet tournament https://lichess.org/tournament/IaRkDsvp"Site "https://lichess.org/r0cYFhsy"White "GreatGig"Black "hackattack"Result "0-1"UTCDate "2016.04.30"UTCTime "22:00:03"WhiteElo "1777"BlackElo "1809"WhiteRatingDiff "-11"BlackRatingDiff "+11"ECO "B01"Opening "Scandinavian Defense: Mieses-Kotroc Variation"TimeControl "60+0"Termination "Time forfeit"1. e4 d5 2. exd5 Qxd5 3. Nc3 Qd8 4. d4 Nf6 5. Nf3 Bg4 6. h3 Bxf3 7. gxf3 c6 8. Bg2 Nbd7 9. Be3 e6 10. Qd2 Nd5 11. Nxd5 cxd5 12. O-O-O Be7 13. c3 Qc7 14. Kb1 O-O-O 15. f4 Kb8 16. Rhg1 Ka8 17. Bh1 g6 18. h4 Bxh4 19. f3 Be7 20. Qc2 Nf6 21. Bg2 Nh5 22. Bh3 Nxf4 23. Bxf4 Qxf4 24. Rdf1 Qd6 25. Rg4 Rdf8 26. Rfg1 f5 27. R4g2 Bf6 28. Rg3 Rfg8 29. Bf1 Rg7 30. Bd3 Rhg8 31. Qh2 Qb8 32. Qg2 Qc8 33. f4 Qc6 34. Qf2 Bh4 35. Rxg6 Bxf2 36. Rxg7 Rxg7 37. Rxg7 a6 38. Rg8+ Ka7 39. Rh8 Qd7 40. Rxh7 Qxh7 0-1
#OUTPUT: header, så data rows


import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split()
    # output each word to STDOUT (standard output)
    for word in words:
        print('%s\t%s' % (word, 1))


# stoopid code for testing

# headers, values = [], []
# for index, value in enumerate(line):
#     if index% 2 == 0:
#         headers.append(value)
#     else:
#         values.append(value)

# res = dict(zip(headers, values))

