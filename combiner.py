import json
from csv import reader
from csv import writer


ao_mappings = {}

with open("/Users/wgfarrell/code/workout-counter/aos_mapping.csv", "r") as read_obj:
    csv_reader = reader(read_obj)

    for row in csv_reader:
        ao_mappings[row[2]] = row

# print(json.dumps(ao_mappings))

user_mappings = {}

with open("/Users/wgfarrell/code/workout-counter/users_mapping.csv", "r") as read_obj:
    csv_reader = reader(read_obj)

    for row in csv_reader:
        if(len(row) > 8 ):
            user_mappings[row[7]] = row

# print(json.dumps(user_mappings))

bd_attendance = []

# with open("/Users/wgfarrell/code/workout-counter/bd_attendance_sept_27_county.csv", "r") as read_obj:
#     csv_reader = reader(read_obj)

#     for row in csv_reader:
#         bd_attendance.append(row)

beatdowns = []

not_found_users_count = 0
found_users_count = 0

new_bd= []

with open("/Users/wgfarrell/code/workout-counter/old_city_beatdowns.json", "r") as read_obj:
    json_found = json.load(read_obj)

    for bd in json_found["beatdowns"] :
        q_id = bd["q_user_id"]
        found_user = user_mappings.get(q_id)
        if( not found_user):
            new_bd.append(bd)
            bd["q_user_id"] = "U046EL2GJ5Q"
            not_found_users_count += 1
        else:
            bd["q_user_id"] = found_user[0]
            new_bd.append(bd)

print(not_found_users_count)

with open("/Users/wgfarrell/code/workout-counter/backfilled_city_beatdowns.json", "w") as write_obj:
    write_obj.write(json.dumps(new_bd))      
    

# not_found_users_count = 0
# found_users_count = 0
# new_bd_attendance = []
# for attendance in bd_attendance:
#     user = attendance[0]
#     ao = attendance[1]
#     found_ao = ao_mappings[ao]
#     found_user = user_mappings.get(user)
#     if( not found_user):
#         not_found_users_count += 1
#     else:
#         found_users_count +=1
#         new_ao_id = found_ao[0]
#         new_user_id = found_user[0]
#         potential_q = user_mappings.get(attendance[3])
#         if not potential_q:
#             new_q_id = ""
#         else:
#             new_q_id = potential_q[0]
#         new_bd_attendance.append([new_user_id, new_ao_id, attendance[2], new_q_id])

# with open("city_backfill_bd_attendance.csv", 'w') as csvfile:
#     bd_attendanceWriter = writer(csvfile, delimiter=",")
#     for newer in new_bd_attendance:
#         bd_attendanceWriter.writerow(newer)
