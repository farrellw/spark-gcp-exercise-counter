import json
from csv import reader


city_aos = []

with open("/Users/wgfarrell/code/workout-counter/src/main/resources/city-ao.txt", "r") as read_obj:
    csv_reader = reader(read_obj)

    for row in csv_reader:
        city_aos.append(row[0])

# print(city_aos)
with open('/Users/wgfarrell/beatdowns_202208021340.json', 'r') as f:
    data = json.load(f)
    with open('src/main/resources/second_city_filtered_single_line_beatdown.json', "a") as f_2:

        beatdowns = data["beatdowns"]
        count = 0
        for bd in beatdowns:
            # if bd["ao_id"] in city_aos:
            f_2.write(json.dumps(bd))
            f_2.write("\n")

        f_2.close()
    f.close()

# with open('/Users/wgfarrell/code/workout-counter/src/main/resources/workout-data.json', 'r') as f:
#     data = json.load(f)
#     with open('src/main/resources/all_single_line_excercises.txt', "a") as f_2:
#
#         beatdowns = data["data"]
#         count = 0
#         for bd in beatdowns:
#             f_2.write(json.dumps(bd[0]))
#             f_2.write("\n")
#             # count +=1
#             # if(count > 100):
#             #     break
#
#         f_2.close
#     f.close