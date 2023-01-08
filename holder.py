from settings import config



def set_status(index_name,item): 
    if index_name == config['KAVOSH_INDEX']:
        global kavosh_item
        kavosh_item = item
    elif index_name == config['MAP_INDEX']:
        global map_item
        map_item = item  
    elif index_name == config['RTP_INDEX']:
        global rtp_item
        rtp_item = item 
       

def get_status(index_name):    
    if index_name == config['KAVOSH_INDEX']:
        if "kavosh_item" in globals():
            return kavosh_item
        else:
            return False
    elif index_name == config['MAP_INDEX']:
        if "map_item" in globals():
            return map_item
        else:
            return False
    elif index_name == config['RTP_INDEX']:
        if "rtp_item" in globals():
            return rtp_item
        else:
            return False
def set_check_index(item):
    global index_time
    index_time = item
    return index_time

def get_check_index():
    if 'index_time' in globals():
        return index_time
    else:
        return '0'
