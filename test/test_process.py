
test = """"'Rated Blitz game', 'https://lichess.org/EmIZZDYA', 'Pearthief5', 'neoaulug', '1-0', '2016.05.01', '17:56:03', '1843', '1841', '+11', '-10', 'B32', 'Sicilian Defense: Open #3', '180+0', 'Normal'"""
# headers = {}
# words = test.split(" ")
# print("")
# for idx, word in enumerate(words):
#     print(word)#


print(test.replace('"',"").replace("'", ""))