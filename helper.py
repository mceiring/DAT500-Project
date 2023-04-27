from pyspark.sql.functions import split, col, translate, udf, expr, arrays_zip, size
from pyspark.sql.types import ArrayType, FloatType, IntegerType
from matplotlib import pyplot as plt
import numpy as np
import seaborn as sns
#
def convert_types(df):
    df = df.withColumn("Moves", split(col("Moves"),","))
    df = df.withColumn("Eval", translate(col("Eval"), "'", ""))
    df = df.withColumn("Eval", split(col("Eval"),",").cast(ArrayType(FloatType())))
    return df

@udf(returnType=IntegerType())
def find_white_blunders(arr, difference):
    # Blunder counter
    count = 0
    # Start at white player second move.
    i = 2
    # Loop through eval list
    while i < len(arr)-1:
        # Change blunder treshold based of previous eval
        if abs(arr[i-1]) >= 10 and abs(arr[i-1]) <= 20:
            difference = 8.0
        if abs(arr[i-1]) > 20:
            difference = 20.0
        # Check if current eval is worse by "difference" compared to previous move.
        if (arr[i-1] - arr[i]) >= difference:
            # Removing Mating outliers.
            if abs(arr[i]) < 1000 and abs(arr[i-1]) < 1000:
                count += 1
        i += 2
    return count

@udf(returnType=IntegerType())
def find_black_blunders(arr, difference):
    # Blunder counter
    count = 0
    # Start at Black player first move.
    i = 1
    # Loop through eval list
    while i < len(arr)-1:
        # Change blunder treshold based of previous eval
        if abs(arr[i-1]) >= 10 and abs(arr[i-1]) <= 20:
            difference = 8.0
        if abs(arr[i-1]) > 20:
            difference = 20.0
        # Check if current eval is worse by "difference" compared to previous move.
        if (arr[i] - arr[i-1]) >= difference:
            # Removing Mating outliers.
            if abs(arr[i]) < 1000 and abs(arr[i-1]) < 1000:
                count += 1
        i += 2
    return count

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

def replace_UDF(df: any) -> any:
    eval_difference = 3.0

    eval_games = df.where(col("Eval")[0].isNotNull())

    # Create an array column of tuples containing the previous and current elements of the "Eval" column
    pairs = arrays_zip(expr("slice(Eval, 2, size(Eval)-1)"), expr("slice(Eval, 1, size(Eval)-2)"))

    # Create a new array column with the absolute difference between each pair of elements
    diffs = expr("transform(pairs, x -> abs(x[0] - x[1]))")

    # Create a new array column with the adjusted threshold values for each element
    thresholds = expr("transform(slice(Eval, 2, size(Eval)-1), x -> if(abs(x) >= 10 and abs(x) <= 20, 8.0, if(abs(x) > 20, 20.0, null)))")

    # Combine the diffs and thresholds arrays into a single array column
    combined = arrays_zip(diffs, thresholds)

    # Create a new array column with the number of "white blunders" for each pair of elements
    counts = expr("transform(filter(combined, x -> x[1] is not null), x -> if(x[0] >= x[1], 1, 0))")

    # Sum the counts array to get the total number of "white blunders" for each row
    total_count = expr("aggregate(counts, 0, (acc, x) -> acc + x)")

    # Add the total_count as a new column to the eval_games DataFrame
    eval_games = eval_games.withColumn("WhiteBlunders", total_count)
    return eval_games