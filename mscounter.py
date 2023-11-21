import csv
import src.utils.menota_parser as mp
from src.utils.constants import *
from collections import Counter

on_pamph = mp.get_parallelized_text(PSDG47)
mss = []
mss_indi = []
for verse in on_pamph.verses:
    verse_mss = verse.aligned
    mss.append(verse_mss)
    individual_mss = verse_mss.replace(' ', '').split(',')
    mss_indi.extend(individual_mss)

count_mss = Counter(mss)
count_mss_indi = Counter(mss_indi)

with open('count_mss_combs.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['manuscript', 'count'])
    for manuscript, count in count_mss.items():
        writer.writerow([manuscript, count])

with open('count_mss_indi.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['manuscript', 'count'])
    for manuscript, count in count_mss_indi.items():
        writer.writerow([manuscript, count])