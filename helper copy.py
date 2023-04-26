from pyspark.sql.functions import split, col, translate, udf, size
from pyspark.sql.types import ArrayType, FloatType, IntegerType
from matplotlib import pyplot as plt
import numpy as np
import pyspark
import seaborn as sns
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
def find_black_blunders(evals, difference):
    count = 0
    i = 1
    while i < len(evals)-1:
        if abs(evals[i-1]) >= 10 and abs(evals[i-1]) <= 20:
            difference = 8.0
        if abs(evals[i-1]) > 20:
            difference = 20.0
        if (evals[i] - evals[i-1]) >= difference:
            if abs(evals[i]) < 1000 and abs(evals[i-1]) < 1000:
                count += 1
        i += 2
    return count

def find_black_blunders(color, evals, difference):
    blunder_count = 0
    prev_eval = 0
    evals = evals[::2] if color=='white' else evals[1::2]   
    for eval in evals: # every other eval, 
        # set new difference margin
        if 10 <= eval <= 20:
            difference = 8.0
        elif eval > 20:
            difference = 20.0

        if eval - prev_eval >= difference and eval < 1000 and prev_eval < 1000:
            blunder_count += 1
        
        prev_eval = eval
    
    return blunder_count



def plot_eval_game(eval_games: any) -> None:
    ## Get Necessary Data for Plotting...
    eval = eval_games.select("Eval").orderBy(col("WhiteBlunders").desc()).where(size("Moves") < 25).collect()[0][0]
    white_blunders = eval_games.select("WhiteBlunders").orderBy(col("WhiteBlunders").desc()).where(size("Moves") < 25).collect()[0][0]
    black_blunders = eval_games.select("BlackBlunders").orderBy(col("WhiteBlunders").desc()).where(size("Moves") < 25).collect()[0][0]
    white_name = eval_games.select("White").orderBy(col("WhiteBlunders").desc()).where(size("Moves") < 25).collect()[0][0]
    black_name = eval_games.select("Black").orderBy(col("WhiteBlunders").desc()).where(size("Moves") < 25).collect()[0][0]
    white_elo = eval_games.select("WhiteElo").orderBy(col("WhiteBlunders").desc()).where(size("Moves") < 25).collect()[0][0]
    black_elo = eval_games.select("BlackElo").orderBy(col("WhiteBlunders").desc()).where(size("Moves") < 25).collect()[0][0]

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

def plot_eval_game_optimized(eval_games: any) -> None:
    ## Get Necessary Data for Plotting...
    game = eval_games.orderBy(col("WhiteBlunders").desc()).where(size("Moves") < 25).collect()[0]
    eval = game[10]
    print(eval)
    white_blunders = game[12]
    black_blunders = game[13]
    white_name = game[1]
    black_name = game[2]
    white_elo = game[4]
    black_elo = game[5]
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

    
def plot_elo_distribution(elo: list) -> None:
    # set the background style of the plot
    sns.set_style('whitegrid')
    sns.distplot(elo, kde = True, color ='red', bins = 30)
