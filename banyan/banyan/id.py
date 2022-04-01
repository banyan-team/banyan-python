ResourceId = str

generated_message_ids = set()
num_message_ids_issued = 0

def generate_message_id():
    global generated_message_ids
    global num_message_ids_issued
    num_message_ids_issued += 1
    v = str(num_message_ids_issued)
    generated_message_ids.add(v)
    return v
