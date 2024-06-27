import signal
import socket

HEARTBEAT_MSG = b'H'
ALIVE_MSG = b'V'
WAKER_PORT = 5000
HEALTHCHECK_TIMEOUT = 10
BUFFER_BYTES = 1

class HealthcheckReceiver():
    def __init__(self, worker_id, main_thread):
        self.worker_id = worker_id
        self.finished = False
        self.main_thread = main_thread

    def handle_heartbeat_SIGTERM(self, _signum, _frame):
        print(f"[Worker {self.worker_id}] HealthcheckReceiver - SIGTERM detected\n\n")
        self.close()

    def create_worker_socket(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('', WAKER_PORT))
        self.socket.settimeout(HEALTHCHECK_TIMEOUT)
        print(f"[Worker {self.worker_id}] Listening healthchecks from port {WAKER_PORT}")    

    def start(self):
        print(f"[Worker {self.worker_id}] Starting Healthcheck Receiver")
        signal.signal(signal.SIGTERM, self.handle_heartbeat_SIGTERM)

        self.create_worker_socket()
        while not self.finished:    
            if not self.main_thread.is_alive():
                print(f"[Worker {self.worker_id}] Worker thread is not alive. Closing Healthcheck Receiver")
                self.close()
                break        
            try:
                #print(f"[Worker {self.worker_id}] Waiting for healthcheck")
                msg, addr = self.socket.recvfrom(BUFFER_BYTES)
                if msg == HEARTBEAT_MSG:
                    #print(f"[Worker {self.worker_id}] Received {msg.decode()} from {addr}")
                    self.send_alive(addr)
            except socket.timeout:
                #print(f"[Worker {self.worker_id}] Timeout waiting for healthcheck. Retrying...")
                continue
            except OSError as e:
                print(f"[Worker {self.worker_id}] Socket error: ", e)
                break

    def send_alive(self, addr):
        #print(f"[Worker {self.worker_id}] Sending {ALIVE_MSG.decode()} to {addr}")
        try:
            self.socket.sendto(ALIVE_MSG, addr)
        except OSError as e:
            #print(f"[Worker {self.worker_id}] Error sending alive to {addr}: {e}")
            self.main_thread.terminate()
    
    def close(self):
        self.finished = True
        self.socket.close()
        self.main_thread.terminate()
        self.main_thread.join()
        print(f"[Worker {self.worker_id}] Healthcheck Receiver closed")