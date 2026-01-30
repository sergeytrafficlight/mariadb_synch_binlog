
#for pytest

class statistic_class:

    def __init__(self):
        self.initiate_full_regeneration = 0
        self.finished_full_regeneration = 0
        self.initiate_synch_mode = 0
        self.tear_down = 0
        self.process_event_insert = 0
        self.process_event_update = 0
        self.process_event_delete = 0

statistic = statistic_class()

def initiate_full_regeneration():
    #print(f'initiate_full_regeneration')
    global statistic
    statistic.initiate_full_regeneration +=1

def finished_full_regeneration():
    #print('initiate_full_regeneration')
    global statistic
    statistic.finished_full_regeneration +=1

def initiate_synch_mode():
    #print('initiate_synch_mode')
    global statistic
    statistic.initiate_synch_mode +=1

def tear_down():
    #print('tear_down')
    global statistic
    statistic.tear_down +=1


def process_event(event_type, schema, table, event):
    #in full regeneration mode - it may be called in multithread mode
    #in synch mode - only one thread mode
    #print(f"Even type: {event_type} schema: {schema} table: {table} event: {event}")

    global statistic
    if event_type == 'insert':
        statistic.process_event_insert +=1
    elif event_type == 'update':
        statistic.process_event_update += 1
    elif event_type == 'delete':
        statistic.process_event_delete += 1
    else:
        raise RuntimeError(f"Unknown event type '{event_type}'")