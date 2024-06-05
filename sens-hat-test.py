# from sense_emu import SenseHat
# sense = SenseHat()
# sense.show_message("Hello World!", text_colour=(255,0,0))


from sense_emu import SenseHat
import json
import time
import threading
import queue

# Initialize the Sense HAT emulator
sense = SenseHat()


# Generate weather data for Berlin city
def generate_weather_data_berlin(data_queue):
    while True:
        data = {
            'city': 'Berlin',
            'temperature': sense.get_temperature(),
            'humidity': sense.get_humidity(),
            'pressure': sense.get_pressure(),
            'timestamp': time.time()
        }

        # producer.send('sensehat-data', data)
        data_queue.put(data)

        print(f'Sent Sense HAT Data from Berlin city: {data}')
        # print(f'Sent Sense HAT Data from Berlin city.')
        time.sleep(1)


# Generate weather data for Rostock city
def generate_weather_data_rostock(data_queue):
    while True:
        data = {
            'city': 'Rostock',
            'temperature': sense.get_temperature(),
            'humidity': sense.get_humidity(),
            'pressure': sense.get_pressure(),
            'timestamp': time.time()
        }

        # producer.send('sensehat-data', data)
        data_queue.put(data)

        print(f'Sent Sense HAT Data from Rostock city: {data}')
        # print(f'Sent Sense HAT Data from Rostock city.')
        time.sleep(1)


def filterTemperatureData(stream1, stream2, data_queue, temperature_data_queue, lock):
    while True:
        with lock:
            temp_list = list(data_queue.queue)  # Create a copy of the queue's elements while holding the lock
            for item in temp_list:
                
                # Check that temperature value is available in data 
                if "temperature" in item:

                    # Put temperature data in a seperate queue for further processing
                    temperature_data_queue.put({'city': item['city'], 'temperature': item['temperature']})

                    # Clearing data_queue after filtering temperature data
                    with data_queue.mutex:
                        data_queue.queue.clear()

            print(f'\nfiltered data. {list(temperature_data_queue.queue)}')
            print(f'\nFilter: Temperature data is filtered.\n')
        
        # Sleep for a short while to simulate processing time
        time.sleep(1.5)



def main():
    
    # Define a lock
    lock = threading.Lock()
    
    data_queue = queue.Queue()

    temperature_data_queue = queue.Queue();

    # assign the weather data stream thread
    weatherDataFromBerlin = threading.Thread(target=generate_weather_data_berlin, args=(data_queue,))
    weatherDataFromRostock = threading.Thread(target=generate_weather_data_rostock, args=(data_queue,))

    # Filter only temperature data from both city's weather data
    filtered_data = threading.Thread(target=filterTemperatureData, args=(weatherDataFromBerlin, weatherDataFromRostock, data_queue, temperature_data_queue, lock))


    # Set the threads as daemon so they will exit when the main program exits
    weatherDataFromBerlin.daemon = True
    weatherDataFromRostock.daemon = True
    filtered_data.daemon = True



    # start weather data streaming
    weatherDataFromBerlin.start()
    weatherDataFromRostock.start()


    filtered_data.start()

    # Need to join data


    # weatherDataFromBerlin.join()
    # weatherDataFromRostock.join()
    
    # filtered_data.join()

    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("Program terminated by user")


if __name__ == "__main__":
    main()