from datetime import datetime
from mrjob.job import MRJob
from mrjob.protocol import BytesValueProtocol
#Event "Rated Bullet tournament https://lichess.org/tournament/IaRkDsvp"Site "https://lichess.org/r0cYFhsy"White "GreatGig"Black "hackattack"Result "0-1"UTCDate "2016.04.30"UTCTime "22:00:03"WhiteElo "1777"BlackElo "1809"WhiteRatingDiff "-11"BlackRatingDiff "+11"ECO "B01"Opening "Scandinavian Defense: Mieses-Kotroc Variation"TimeControl "60+0"Termination "Time forfeit"1. e4 d5 2. exd5 Qxd5 3. Nc3 Qd8 4. d4 Nf6 5. Nf3 Bg4 6. h3 Bxf3 7. gxf3 c6 8. Bg2 Nbd7 9. Be3 e6 10. Qd2 Nd5 11. Nxd5 cxd5 12. O-O-O Be7 13. c3 Qc7 14. Kb1 O-O-O 15. f4 Kb8 16. Rhg1 Ka8 17. Bh1 g6 18. h4 Bxh4 19. f3 Be7 20. Qc2 Nf6 21. Bg2 Nh5 22. Bh3 Nxf4 23. Bxf4 Qxf4 24. Rdf1 Qd6 25. Rg4 Rdf8 26. Rfg1 f5 27. R4g2 Bf6 28. Rg3 Rfg8 29. Bf1 Rg7 30. Bd3 Rhg8 31. Qh2 Qb8 32. Qg2 Qc8 33. f4 Qc6 34. Qf2 Bh4 35. Rxg6 Bxf2 36. Rxg7 Rxg7 37. Rxg7 a6 38. Rg8+ Ka7 39. Rh8 Qd7 40. Rxh7 Qxh7 0-1
TEST_HEADERS = {"Black": 1 , "White": 2}
HEADERS = ["Event", "White", "Black", "Result", "WhiteElo", "BlackElo", "Opening", "TimeControl", "Termination", "Moves","Eval", "UTCTimestamp"]

def remove_url_if_exist(value:list) -> str:
        ## Url is always last word in first index of list
        url = value[-1]
        if "https" in url:
            return " ".join(value[0:-1])
        else:
            return " ".join(value)


def combine_datetime(headers:list, values:list) -> list:
    date = ""
    time = ""
    for idx in range(len(headers)-1):
        if headers[idx] == "UTCDate":
            date = values[idx]
            values.remove(values[idx])
            headers.remove(headers[idx])
        if headers[idx] == "UTCTime":
            time = values[idx]
            values.remove(values[idx])
            headers.remove(headers[idx])
            break

    if date == "" or time == "":
        values.append("NaT")
        headers.append("UTCTimestamp")
    else:
        timestamp = date+" "+time
        values.append(datetime.strptime(timestamp, "%Y.%m.%d %H:%M:%S"))
        headers.append("UTCTimestamp")
    return values, headers

def handle_moves(headers: list, values: list):
    idx = headers.index("Moves") # finn kolonna me moves
    moves = values[idx].split(" ") # henta en sting me moves, aka den radens moves, til list#
    moves = moves[:-1]
    if "{" in moves:
        return moves_eval(moves)
    else:
        # return convert_moves(moves)
        return take_moves(moves)

def convert_moves(moves: list):
    result_moves = ""
    move_counter = 1
    k = 0
    while k < (len(moves)-3):
        result_moves += moves[k+1]+","+moves[k+2]+","
        k += 3
        move_counter += 1
    if len(moves)%2 == 1:
        result_moves += moves[-1]
    return result_moves, "null"

# INPUT: 1. mv mv 2. mv mv 3. mv mv 4. mv
# 25. mv25 1-0
# 25. mv25 mv25 0-1
# To cases, siste moves e to moves, eller ett move
def take_moves(values: list):
    moves = []
    for i, v in enumerate(values):
        if "." in v and i == len(values)-2: # om d bare e et move igjen, vil num. mv, num. vær nest siste element
            moves.append(values[i+1].strip("''"))
        elif "." in v and i <= len(values)-3: # ellers ta to moves
            moves.append(values[i+1].strip("''"))
            moves.append(values[i+2].strip("''"))

    return str(moves).strip("[]"), "null" # returns moves, eval

# ta inn liste av moves OG evals, "moves.split()"
def moves_eval(values: list):
    moves = []
    evals = []
    for i, v in enumerate(values):
        # finn markør for eval
        if v == '{':
            moves.append(values[i-1].strip('!?')) # fjern tegn som antyda rare moves
        elif v == '}':
            if '#' in values[i-1]:
                # noen moves har en 'Mate in # moves*:  18. dxc5?! { [%eval #-5] } 18... Bxc5+?! { [%eval #-7] }
                # mate_moves.append([len(moves),str(int(list[i-1].strip('#-]'))*1000)]) # return index of move, and num moves to mate
                evals.append(values[i-1].strip("#]")+"000")
            else:
                evals.append(values[i-1].strip(']')) # fjern char fra tallet, bare float ut

    return str(moves).strip("[]"), str(evals).strip("[]") # listan til strings, fjern 'liste'-chars

### USE
# moves, evals = moves_eval(moves_string.split())


class MRCountSum(MRJob): # kanskje endre navn?

    OUTPUT_PROTOCOL = BytesValueProtocol
    def mapper(self, _, line):
        result = ["null"]*len(HEADERS)
        line = line.strip()
        line = line.split('"')
        if len(line) > 5:
            ## Find headers
            headers = line[0::2]
            headers[-1] = "Moves"
            headers = [x.strip() for x in headers]

            ## Find values
            values = line[1::2]
            values.append(line[-1])

            # Convert Moves
            moves, evals = handle_moves(headers, values)
            values[-1] = moves
            values.append(evals)
            headers.append("Eval")


            # Clean Event Tag
            values[0] = remove_url_if_exist(values[0].split())

            # Combine UTCDate and UTCTime
            values, headers = combine_datetime(headers, values)

            # Escape comma values
            values = [f"\"{value}\"" for value in values]

            ## Find values corresponding to headers
            for idx, header in enumerate(headers):
                if header in HEADERS:
                    index = HEADERS.index(header)
                    result[index] = values[idx]
            result = ",".join(result)
            yield None, result

    def reducer(self, _, values):
        final_headers = [f"\"{head}\"" for head in HEADERS]
        yield None, bytes("\n".join(values), "utf-8")

    """ def combiner(self, key, values):
        yield key, list(values) # EVENT, storstinr"""


if __name__ == '__main__':
    MRCountSum.run()