import socket
import time

UDP_IP = '127.0.0.1'  # Update this with your target IP address
UDP_PORT = 3157  # Update this with your target UDP port

def send_message(message):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message.encode(), (UDP_IP, UDP_PORT))
        print(f"Message sent: {message}",flush=True)  # Add this line for debugging
    except Exception as e:
        print(f"An error occurred while sending message: {e}")

if __name__ == "__main__":
    try:
        for i in range(1, 101):  # Count from 1 to 100
            send_message(str(i))
            time.sleep(0.1)  # Sleep for 100ms before sending the next message
    except Exception as e:
        print(f"An error occurred: {e}")
