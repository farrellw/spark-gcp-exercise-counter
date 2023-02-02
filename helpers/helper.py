import json
# from csv import reader


# city_aos = []
#
# with open("/Users/wgfarrell/code/workout-counter/src/main/resources/city-ao.txt", "r") as read_obj:
#     csv_reader = reader(read_obj)
#
#     for row in csv_reader:
#         city_aos.append(row[0])

# print(city_aos)
with open('new_city_beatdowns.json', 'r') as f:
    data = json.load(f)
    with open('src/main/resources/new_city_backfilled_single_line.txt', "a") as f_2:

        beatdowns = data["beatdowns"]
        count = 0
        for bd in beatdowns:
            f_2.write(json.dumps(bd))
            f_2.write("\n")

        f_2.close()
    f.close()

# with open('/Users/wgfarrell/code/workout-counter/beatdowns_city_aos_sept_28.json', 'r') as f:
#     data = json.load(f)
#     with open('src/main/resources/bd_city_aos_single_line.txt', "a") as f_2:
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
