from pyspark.sql.functions import split, col, translate, udf
from pyspark.sql.types import ArrayType, FloatType, IntegerType
from matplotlib import pyplot as plt
import numpy as np
import pyspark
#
def convert_types(df):
    df = df.withColumn("Moves", split(col("Moves"),","))
    df = df.withColumn("Eval", translate(col("Eval"), "'", ""))
    df = df.withColumn("Eval", split(col("Eval"),",").cast(ArrayType(FloatType())))
    return df

@udf(returnType=IntegerType())
def find_white_blunders(arr, difference):
    count = 0
    i = 2
    while i < len(arr)-1:
        if abs(arr[i-1]) >= 10 and abs(arr[i-1]) <= 20:
            difference = 8.0
        if abs(arr[i-1]) > 20:
            difference = 20.0
        if (arr[i-1] - arr[i]) >= difference: 
            if abs(arr[i]) < 1000 and abs(arr[i-1]) < 1000:
                count += 1
        i += 2
    return count

@udf(returnType=IntegerType())
def find_black_blunders(arr, difference):
    count = 0
    i = 1
    while i < len(arr)-1:
        if abs(arr[i-1]) >= 10 and abs(arr[i-1]) <= 20:
            difference = 8.0
        if abs(arr[i-1]) > 20:
            difference = 20.0
        if (arr[i] - arr[i-1]) >= difference:
            if abs(arr[i]) < 1000 and abs(arr[i-1]) < 1000:
                count += 1
        i += 2
    return count

def plot_eval_game(eval_games: pyspark.sql.dataframe.DataFrame) -> None:
    ## Get Necessary Data for Plotting...
    eval = eval_games.select("Eval").orderBy(col("WhiteBlunders").desc()).collect()[0][0]
    white_blunders = eval_games.select("WhiteBlunders").orderBy(col("WhiteBlunders").desc()).collect()[0][0]
    black_blunders = eval_games.select("BlackBlunders").orderBy(col("WhiteBlunders").desc()).collect()[0][0]
    white_name = eval_games.select("White").orderBy(col("WhiteBlunders").desc()).collect()[0][0]
    black_name = eval_games.select("Black").orderBy(col("WhiteBlunders").desc()).collect()[0][0]
    white_elo = eval_games.select("WhiteElo").orderBy(col("WhiteBlunders").desc()).collect()[0][0]
    black_elo = eval_games.select("BlackElo").orderBy(col("WhiteBlunders").desc()).collect()[0][0]

    eval = [e for e in eval if abs(e) < 1000]
    black_and_white = [f"W{idx+1}" if idx%2==0 or idx==0 else f"B{idx+1}" for idx in range(len(eval))]
    y2 = np.array([0 for _ in range(len(black_and_white))])
    fig, ax = plt.subplots(figsize=(30,7))
    
    ax.fill_between(black_and_white, eval, y2, where=eval>=y2, color="skyblue")
    ax.fill_between(black_and_white, eval, y2, where=eval<=y2, color="black")
    ax.set_title(f"""White Player: {white_name} ({white_elo} ELO) ({white_blunders} blunders) vs \
Black Player: {black_name} ({black_elo} ELO) ({black_blunders} blunders)""")
    ax.set_xlabel("Moves")
    ax.set_ylabel("Evaluation")
    ax.margins(x=0)
    
    fig.tight_layout()
    plt.savefig("chess_2016_dataset/images/blunder.png")
