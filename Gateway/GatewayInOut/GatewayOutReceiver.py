from multiprocessing import Process, Queue
import signal
from CommunicationMiddleware.middleware import Communicator
from utils.Batch import Batch

RECV = 1
ACK = 2
AMOUNT = 3
END = 4
GATEWAY_QUEUE_NAME = 'Gateway'

class GatewayOutReceiverHandler():
    def __init__(self) -> None:
        self.send_queue = Queue()
        self.recv_queue = Queue()
        receiver = GatewayOutReceiver(self.send_queue, self.recv_queue)
        self.handle = Process(target=receiver.start_receiver)
        self.handle.daemon = True
        self.handle.start()
        self.receiving = False
        self.batch = None
        
    def start(self):
        return self.recv_queue.get()
    
    #batch needs to be received before executing other operations
    def recv_batch(self, timeout=None):
        if self.batch != None:
            batch = self.batch
            self.batch = None
            return batch
        if not self.receiving:
            self.receiving = True
            self.send_queue.put(RECV)
        batch = self.recv_queue.get(timeout=timeout)
        self.receiving = False
        return batch

    def ack_batch(self):
        self.send_queue.put(ACK)
        return self.recv_queue.get()
    
    def ammount_of_messages_in_queue(self):
        self.send_queue.put(AMOUNT)
        return self.recv_queue.get()
    
    def keep_batch(self, batch):
        self.batch = batch
    
    def close(self):
        self.handle.terminate()
        self.send_queue.put(None)
        self.handle.join()

class GatewayOutReceiver():
    def __init__(self, recv_queue: Queue, send_queue: Queue):
        self.com = None
        self.sigterm_queue = Queue()
        self.recv_queue = recv_queue
        self.send_queue = send_queue
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n [GatewayOutReceiver] SIGTERM detected\n\n")
        self.sigterm_queue.put(True)
        self.close()

    def connect(self):
        self.com = Communicator.new(self.sigterm_queue, auto_ack=False)
        if not self.com:
            print("Failed to connect")
            self.send_queue.put(None)
            return False
        self.send_queue.put(ACK)
        return True

    def start_receiver(self):
        switch = {
            RECV: self.recv_batch,
            ACK: self.ack_last_batch,
            AMOUNT: self.get_amount_of_batches_in_queue,
            None: lambda: False
        }

        connected = self.connect()
        while connected:
            operation = self.recv_queue.get()
            connected = switch[operation]()
        self.close()
        self.recv_queue.cancel_join_thread()

    def recv_batch(self):
        batch = None
        while batch == None:
            batch_bytes = self.com.consume_message(GATEWAY_QUEUE_NAME)
            if not batch_bytes:
                print(f"[GatewayOut] Disconnected from MOM")
                return False
            batch = Batch.from_bytes(batch_bytes)

            if not batch:
                if not self.com.acknowledge_last_message():
                    print(f"[GatewayOut] Disconnected from MOM, while acking_message")
                    return False

        self.send_queue.put(batch)
        return True

    def ack_last_batch(self):        
        if not self.com.acknowledge_last_message():
            print(f"[GatewayOut] Disconnected from MOM, while acking_message")
            return False
        self.send_queue.put(ACK)
        return True

    def get_amount_of_batches_in_queue(self):
        pending_messages = self.com.pending_messages(GATEWAY_QUEUE_NAME)
        if pending_messages == None:
            return False
        self.send_queue.put(pending_messages)
        return True

    def close(self):
        self.send_queue.put(None)
        if self.com:
            self.com.close_connection()
        