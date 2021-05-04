import json
import time
import schedule
import threading
from kafka import KafkaConsumer
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(bootstrap_servers=['52.146.2.26:9092'],value_serializer=json_serializer)

def get_attr(data):
    start_time, end_time, interval, day, repeat, job_id = 'NOW', '', '', '', 'NO', 0

    if('start_time' in data):
        start_time = data['start_time']

    if('end_time' in data):
        end_time = data['end_time']
    
    if('interval' in data):
        interval = data['interval']

    if('day' in data):
        day = data['day']
    
    if('repeat' in data):
        repeat = data['repeat']
    
    if('job_id' in data):
        job_id = data['job_id']
    
    return start_time, end_time, interval, day, repeat, job_id

def schedule_on_a_day(day, time, data, is_onetime, instance_id):
    if(day.lower() == 'monday'):
        schedule.every().monday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    elif(day.lower() == 'tuesday'):
        schedule.every().tuesday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    elif(day.lower() == 'wednesday'):
        schedule.every().wednesday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    elif(day.lower() == 'thursday'):
        schedule.every().thursday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    elif(day.lower() == 'friday'):
        schedule.every().friday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    elif(day.lower() == 'saturday'):
        schedule.every().saturday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    elif(day.lower() == 'sunday'):
        schedule.every().sunday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    
def start_schedule(start_time, end_time, interval, day, repeat, job_id, instance_id, data):   
    if(repeat.lower() == 'yes'):
        if(day == ''):
            #run everyday at the given start time
            schedule.every().day.at(start_time).do(send_to_deployer, data, False).tag(str(instance_id))
            #send stop command to deployer everyday at the given end time
            if(end_time != ''):
                data['scheduling_info']['request_type'] = 'stop'
                schedule.every().day.at(end_time).do(send_to_deployer, data, False).tag(str(instance_id))
        else:
            #run on that day every week at the given start time 
            schedule_on_a_day(day, start_time, data, False, instance_id)
            if(end_time != ''):
                data['scheduling_info']['request_type'] = 'stop'
                schedule_on_a_day(day, end_time, data, False, instance_id)
    else:
        # Run only once or every interval
        if(day == ''):
            # start at start_time
            if(interval == ''):
                schedule.every().day.at(start_time).do(send_to_deployer, data, True).tag(str(instance_id))
            else:
                if(start_time == ''):
                    ## TODO -> change to hours
                    schedule.every(int(interval)).seconds.do(send_to_deployer, data, False).tag(str(instance_id))
                else:
                    schedule.every(int(interval)).hours.at(start_time).do(send_to_deployer, data, False).tag(str(instance_id))
            if(end_time != ''):
                data['scheduling_info']['request_type'] = 'stop'
                schedule.every().day.at(end_time).do(send_to_deployer, data, True).tag(str(instance_id))
        else:
            #start on the given day at given start time
            schedule_on_a_day(day, start_time, data, True, instance_id)
            if(end_time != ''):
                data['scheduling_info']['request_type'] = 'stop'
                schedule_on_a_day(day, end_time, data, True, instance_id)

def handle_schedule_info(sched_info, instance_id, data):

    if 'request_type' not in sched_info:
        print('[ERROR] : Scheduling request type not found')
        return -1

    request_type = sched_info['request_type']
    start_time, end_time, interval, day, repeat, job_id = get_attr(sched_info) 

    if(request_type.lower() == 'start'):
        start_schedule(start_time, end_time, interval, day, repeat, job_id, instance_id, data)
    elif(request_type.lower() == 'stop'):
        schedule.clear(str(instance_id))
    else:
        print('[ERROR] : Invalid scheduling request type found')
        return -1
    
def run_pending_jobs():
    while True:
        schedule.run_pending()
        time.sleep(1)

def send_to_deployer(data, is_onetime):
    print('[Scheduler] : Sent to Deployer')

    producer.send('scheduler_to_deployer', data)

    if(is_onetime):
        return schedule.CancelJob

def consume_from_sensor_binder():

    consumer_for_sensor_binder = KafkaConsumer(
        "sensor_binder_to_scheduler",
        bootstrap_servers='52.146.2.26:9092',
        auto_offset_reset='earliest',
        group_id='consumer-group-a')
        
    for msg in consumer_for_sensor_binder:
        data = json.loads(msg.value)
        threading.Thread(target=handle_schedule_info, args=(data['scheduling_info'], data['instance_id'], data)).start()

if __name__ == "__main__":

    threading.Thread(target=run_pending_jobs, args=()).start()
    
    threading.Thread(target=consume_from_sensor_binder, args = ()).start()
