ResourceId = str

generated_value_ids = set()
num_value_ids_issued = 0

generated_message_ids = set()
num_message_ids_issued = 0

num_bang_values_issued = 0

def generate_value_id():
    """generates value.
    
    Parameter
    ---------

    Returns
    -------
   
    """
    global generated_value_ids
    global num_value_ids_issued
    num_value_ids_issued += 1
    v = str(num_value_ids_issued)
    generated_value_ids.add(v)
    return v

def generate_message_id():
    """generates message.
    
    Parameter
    ---------

    Returns
    -------
      
    """
    global generated_message_ids
    global num_message_ids_issued
    num_message_ids_issued += 1
    v = str(num_message_ids_issued)
    generated_message_ids.add(v)
    return v

def generate_bang_value():
    """generates message.
    
    Parameter
    ---------

    Returns
    -------
      
    """
    global num_bang_values_issued
    num_bang_values_issued += 1
    v = str(num_bang_values_issued)
    return v

def get_num_bang_values_issued():
    global num_bang_values_issued
    num_bang_values_issued

def set_num_bang_values_issued(new_num_bang_values_issued):
    global num_bang_values_issued
    num_bang_values_issued = new_num_bang_values_issued