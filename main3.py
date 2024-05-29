import os
import json
import xml.etree.ElementTree as ET
import math
import gi
import socket
import struct
import time
import sched
import datetime

gi.require_version('Gst', '1.0')
from gi.repository import Gst

Gst.init(None)
frame_sample_buffer = []  # Buffer to keep samples until the entire frame is available
object_tracking_buffer = []
object_info_tracking_stack = {}
UDP_IP = '127.0.0.1'
UDP_PORT = 3157

scheduler = sched.scheduler(time.time, time.sleep)
data_to_send = {}  # Data to be sent every 100ms

def on_new_sample(appsink):
    try:
        sample = appsink.emit("pull-sample")
        if sample:
            buffer = sample.get_buffer()
            payload_size = buffer.get_size()
            payload_data = buffer.extract_dup(0, payload_size)

            rtp_header = payload_data[:12]
            timestamp = int.from_bytes(rtp_header[4:8], byteorder='big')
            sequence_number = int.from_bytes(rtp_header[2:4], byteorder='big')
            payload_body = payload_data[12:]
            decoded_data = payload_body.decode('UTF-8')
            
            if _is_complete_metadata_frame(decoded_data):
                frame_sample_buffer.append(decoded_data)
                combined_metadata = "".join(frame_sample_buffer)
                frame_sample_buffer.clear()
                _process_metadata(combined_metadata)
            else:
                frame_sample_buffer.append(decoded_data)
    except Exception as e:
        print(f"An error occurred in on_new_sample: {e}", flush=True)
    return Gst.FlowReturn.OK

def _is_complete_metadata_frame(data):
    return data.endswith("</tt:MetadataStream>")

def _process_metadata(data):
    try:
        # Tracking Notification Topics 
        entering_topic = "tns1:IVA/EnteringField/Entering_field"
        leaving_topic = "tns1:IVA/LeavingField/Leaving_field"
        infield_topc = "tns1:IVA/ObjectInField/Object_in_Field_1"

        data_by_object_id = {}
        
        root = ET.fromstring(data)
        # print(root,flush=True)
        for notification_message in root.findall('.//wsnt:NotificationMessage', namespaces={'wsnt': 'http://docs.oasis-open.org/wsn/b-2'}):
            topic = notification_message.find('./wsnt:Topic', namespaces={'wsnt': 'http://docs.oasis-open.org/wsn/b-2'}).text
            print(topic,flush=True)
            if topic == entering_topic:
                _process_entering_object(notification_message)

            elif topic == leaving_topic:
                _process_leaving_object(notification_message)
        # print("len(object_info_tracking_stack)",len(object_info_tracking_stack),flush=True)
        if len(object_info_tracking_stack) > 0:
            for target_object_id in object_info_tracking_stack:
                object_data = _extract_object_data(root, target_object_id)

                if object_data:
                    data_by_object_id[target_object_id] = object_data
        # print(data_by_object_id,flush=True)
        data_to_send.update(data_by_object_id)
    
    except ET.ParseError as parse_error:
        print(f"Error parsing XML data: {parse_error}", flush=True)
    except KeyError as key_error:
        print(f"KeyError: {key_error}", flush=True)
    except Exception as e:
        print(f"An unexpected error occurred in _process_metadata: {e}", flush=True)

def _process_entering_object(notification_message):
    # print("Entering",flush=True)
    try:
        entering_object_keys = notification_message.find(".//tt:Message/tt:Key", namespaces={"tt": "http://www.onvif.org/ver10/schema"})
        for key_element in entering_object_keys:
            value = key_element.get("Value")
            object_tracking_buffer.append(value)
            if value not in object_info_tracking_stack:
                object_info_tracking_stack[value] = {"initial_heading_x": None, "initial_heading_y": None,"initial_heading_x1": None, "initial_heading_y1": None}
    except Exception as e:
        print(f"An error occurred in _process_entering_object: {e}", flush=True)

def _process_leaving_object(notification_message):
    # print("leaving",flush=True)
    try:
        exiting_object_keys = notification_message.find(".//tt:Message/tt:Key", namespaces={"tt": "http://www.onvif.org/ver10/schema"})
        if exiting_object_keys:
            for key_element in exiting_object_keys:
                value = key_element.get("Value")
                object_tracking_buffer.remove(value)
                object_info_tracking_stack.pop(value)
    except Exception as e:
        print(f"An error occurred in _process_leaving_object: {e}", flush=True)

def _extract_object_data(root, target_object_id):
    # print("Inside extract data ",flush=True)
    try:
        object_data = {}
        
        for object_elem in root.findall(".//tt:Object", namespaces={"tt": "http://www.onvif.org/ver10/schema"}):
            if object_elem.get("ObjectId") == target_object_id:
                utc_time = root.find(".//tt:Frame", namespaces={"tt": "http://www.onvif.org/ver10/schema"}).get('UtcTime')
                if utc_time:
                    object_data["utc_time"] = utc_time[:-1]
                
                center_of_gravity_elem = object_elem.find(".//tt:CenterOfGravity", namespaces={"tt": "http://www.onvif.org/ver10/schema"})
                if center_of_gravity_elem is not None:
                    object_data["x"] = center_of_gravity_elem.get("x")
                    object_data["y"] = center_of_gravity_elem.get("y")
                    # object_data["Heading"] = _calculate_heading_position(object_data["x"],object_data["y"],object_info_tracking_stack[target_object_id]["initial_heading_x"],object_info_tracking_stack[target_object_id]["initial_heading_y"])
                    
                    # if object_info_tracking_stack[target_object_id]["initial_heading_x"] is None:
                    #     object_info_tracking_stack[target_object_id]["initial_heading_x"] = center_of_gravity_elem.get("x")
                    # if object_info_tracking_stack[target_object_id]["initial_heading_y"] is None:
                    #     object_info_tracking_stack[target_object_id]["initial_heading_y"] = center_of_gravity_elem.get("y")
                    
                    # object_data["Heading"] = math.degrees(math.atan2(
                    #     float(center_of_gravity_elem.get("y")) - float(object_info_tracking_stack[target_object_id]["initial_heading_y"]),
                    #     float(center_of_gravity_elem.get("x")) - float(object_info_tracking_stack[target_object_id]["initial_heading_x"])))
                    #Update initial_heading values to current value to calculate the next heading
                    # object_info_tracking_stack[target_object_id]["initial_heading_x"] = center_of_gravity_elem.get("x")
                    # object_info_tracking_stack[target_object_id]["initial_heading_y"] = center_of_gravity_elem.get("y")
                    # print(object_data["Heading"],flush=True)
                class_candidate_elem = object_elem.find(".//tt:ClassCandidate", namespaces={"tt": "http://www.onvif.org/ver10/schema"})
                if class_candidate_elem is not None:
                    object_data["class_candidate_type"] = class_candidate_elem.find(".//tt:Type", namespaces={"tt": "http://www.onvif.org/ver10/schema"}).text
                    object_data["likelihood"] = class_candidate_elem.find(".//tt:Likelihood", namespaces={"tt": "http://www.onvif.org/ver10/schema"}).text
                
                
                geolocation_elem = object_elem.find(".//tt:GeoLocation", namespaces={"tt": "http://www.onvif.org/ver10/schema"})
                if geolocation_elem is not None:
                    object_data["lat"] = geolocation_elem.get("lat")
                    object_data["lon"] = geolocation_elem.get("lon")
                    object_data["elevation"] = geolocation_elem.get("elevation")
                    if object_info_tracking_stack[target_object_id]["initial_heading_x1"] is None:
                        object_info_tracking_stack[target_object_id]["initial_heading_x1"] = object_data["lat"]
                    if object_info_tracking_stack[target_object_id]["initial_heading_y1"] is None:
                        object_info_tracking_stack[target_object_id]["initial_heading_y1"] = object_data["lon"]
                    
                    # object_data["Heading"]=calculate_bearing(object_info_tracking_stack[target_object_id]["initial_heading_x1"],object_info_tracking_stack[target_object_id]["initial_heading_y1"],object_data["lat"],object_data["lon"])
                    # object_info_tracking_stack[target_object_id]["initial_heading_x1"] = geolocation_elem.get("lat")
                    # object_info_tracking_stack[target_object_id]["initial_heading_y1"] = geolocation_elem.get("lon")
                    # print(object_data["Heading"],flush=True)
                speed_elem = object_elem.find(".//tt:Speed", namespaces={"tt": "http://www.onvif.org/ver10/schema"})
                if speed_elem is not None:
                    object_data["Speed"] = speed_elem.text

                break
    except Exception as e:
        print(f"An error occurred in _extract_object_data: {e}", flush=True)

    return object_data

def _calculate_heading(heading_data):
    print(int(float(heading_data) / 0.0125),flush=True)
    print(int(float(heading_data) *80),flush=True)
    return int(float(heading_data) / 0.0125)

def _calculate_heading_position(current_x,current_y,previous_x,previous_y):
    if previous_x is None or previous_y is None:
        return 28800
    else:
        return abs(int(math.degrees(math.atan2(float(current_y) - float(previous_y),float(current_x) - float(previous_x)))/0.0125))


import math

def calculate_bearing(lat1, lon1, lat2, lon2):
    try:
        
        # Convert degrees to radians
        lat1 = math.radians(float(lat1))
        lon1 = math.radians(float(lon1))
        lat2 = math.radians(float(lat2))
        lon2 = math.radians(float(lon2))
        
        # Calculate differences
        delta_lon = lon2 - lon1
        
        # Calculate bearing
        x = math.sin(delta_lon) * math.cos(lat2)
        y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1) * math.cos(lat2) * math.cos(delta_lon))
        initial_bearing = math.atan2(x, y)
        
        # Convert radians to degrees
        initial_bearing = math.degrees(initial_bearing)
        
        # Normalize bearing to 0-360/0.0125
        compass_bearing = ((initial_bearing + 360) % 360) / 0.0125
        
        return abs(int(compass_bearing))

    except Exception as e:
        print("An error occurred:", str(e), flush=True)
        return None


def _send_data_to_client(data_by_object_id):
    # current_time = time.time()
    # if not hasattr(_send_data_to_client,'last_send_time'):
    #     _send_data_to_client.last_send_time = current_time

    # if current_time - _send_data_to_client.last_send_time >=0.1:
    try:
        # Constants for packing data
        DATA_FORMAT = "IIQiiiiii"
        HEADER_FORMAT = "Ii"
        # Define the number of objects
        NUM_OBJECTS = len(data_by_object_id)
        # Create a header for the object data list
        ObjectDataList = struct.pack(HEADER_FORMAT, 0xdeadbeef, NUM_OBJECTS)

        # Pack and send data for each object
        packed_data = b""
        for object_id, value in data_by_object_id.items():
            if value.get("utc_time") and value.get("class_candidate_type") == "Human":
                # Extract data for packing
                object_type = 2  
                time_str = value.get("utc_time")
                time_dt = time.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%f")  # Assuming time is in ISO format
                time_ms = int(time.mktime(time_dt) * 1000) #Convert to ms
                current_latitude_micro_deg = int(float(value.get("lat")) * 1e7)  # Convert latitude to micro-degrees
                current_longitude_micro_deg = int(float(value.get("lon")) * 1e7)  # Convert longitude to micro-degrees
                elevation =  int(float(value.get("elevation")) / 10)  # Convert elevation to units of 10cm steps
                speed = int(float(value.get("Speed")) * 50)  # Convert speed to units of 0.02 m/s
                # heading = (value.get("Heading"))  # Convert heading to units of 0.0125 degrees
                # Calculate heading here
                initial_lat = object_info_tracking_stack[object_id]["initial_heading_x1"]
                initial_lon = object_info_tracking_stack[object_id]["initial_heading_y1"]
                current_lat = value.get("lat")
                current_lon = value.get("lon")
                heading = calculate_bearing(initial_lat, initial_lon, current_lat, current_lon)
                print(heading,flush=True)
                # Update initial positions
                object_info_tracking_stack[object_id]["initial_heading_x1"] = current_lat
                object_info_tracking_stack[object_id]["initial_heading_y1"] = current_lon

                pad_value = 0    # pad value
                # Pack the data for the current object
                packed_data += struct.pack(DATA_FORMAT, int(object_id), object_type, time_ms,
                                        current_latitude_micro_deg, current_longitude_micro_deg,
                                        elevation, speed, heading, pad_value)
        # Combine header and packed data
        Msg = ObjectDataList + packed_data
        # Send the data over UDP
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(Msg, (UDP_IP, UDP_PORT))
        # print(f"Message : {Msg} {time.time()}", flush=True)
    except Exception as e:
        print(f"An error occurred in _send_data_to_client: {e}", flush=True)

def schedule_send_data():
    scheduler.enter(0, 1, send_data_periodically, (scheduler,))
    scheduler.run()

def send_data_periodically(sc):
    global data_to_send
    if data_to_send:
        _send_data_to_client(data_to_send)
        data_to_send = {}  # Clear the data after sending
    scheduler.enter(0.2, 1, send_data_periodically, (sc,))  # Schedule next send after 100ms

if __name__ == "__main__":
    try:
        rtsp_url = os.getenv("RTSP_URL")
        pipeline_str = f"rtspsrc location={rtsp_url} ! application/x-rtp, media=application, payload=107, encoding-name=VND.ONVIF.METADATA! rtpjitterbuffer ! appsink name=appsink"
        pipeline = Gst.parse_launch(pipeline_str)

        appsink = pipeline.get_by_name("appsink")
        appsink.set_property("emit-signals", True)
        appsink.connect("new-sample", on_new_sample)

        pipeline.set_state(Gst.State.PLAYING)

        schedule_send_data()

    except KeyboardInterrupt:
        print("Application stopped by user", flush=True)
    except Exception as e:
        print(f"An error occurred: {e}", flush=True)
