from utils.Batch import SeqNumGenerator
from utils.auxiliar_functions import smalles_scale_for_str
from .Worker import Worker
from utils.QueryMessage import QueryMessage, CATEGORIES_FIELD, YEAR_FIELD, TITLE_FIELD, REVIEW_MSG_TYPE

class Filter(Worker):
    def __init__(self, id, next_pools, eof_to_receive, field, valid_values, droping_fields):
        super().__init__(id, next_pools, eof_to_receive)
        self.field = field
        self.valid_values = valid_values
        self.droping_fields = droping_fields

    @classmethod
    def new(cls, field, valid_values, droping_fields=[]):
        id, next_pools, eof_to_receive = Filter.get_env()
        if id == None or eof_to_receive == None or not next_pools:
            return None
        filter = Filter(id, next_pools, eof_to_receive, field, valid_values, droping_fields)
        if not filter.connect():
            return None
        return filter

    def get_context_storage_types(self, scale_of_update_file):
        return [], []
    
    def add_to_context(self, client_id, title):
        self.client_contexts[client_id].add(title)
        scale = smalles_scale_for_str(title)
        if scale not in self.client_context_storage_updates:
            self.client_context_storage_updates[scale] = {}
        self.client_context_storage_updates[scale][title] = (None, [])

    #self.client_context_storage_updates[scale][author] = (old_value, [new_value])
    def process_message(self, client_id, msg: QueryMessage):
        #print(f"client: {client_id}, type: {msg.msg_type}, title: {msg.title}, under batch {SeqNumGenerator.seq_num + 1}")
        self.client_contexts[client_id] = self.client_contexts.get(client_id, set())
        if msg.msg_type == REVIEW_MSG_TYPE and msg.title in self.client_contexts[client_id]:
            return self.transform_to_result(msg)
        if self.filter_book(msg):
            self.add_to_context(client_id, msg.title)
            msg = msg.copy_droping_fields(self.droping_fields)
            return self.transform_to_result(msg)
        return None
    
    def remove_client_context(self, client_id):
        if client_id in self.client_contexts:
            self.client_contexts.pop(client_id)

    def filter_book(self, msg:QueryMessage):
        switch = {
            CATEGORIES_FIELD: msg.contains_category,
            YEAR_FIELD: msg.between_years,
            TITLE_FIELD: msg.contains_in_title,
        }
        method = switch.get(self.field, None)
        if not method:
            return False
        return method(self.valid_values)

    def add_previous_context(self, previous_context, client_id):
        self.client_contexts[client_id] = self.client_contexts.get(client_id,set())
        self.client_contexts[client_id].update(previous_context.keys())

    def get_final_results(self, _client_id):
        return []