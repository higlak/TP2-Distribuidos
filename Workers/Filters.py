from .Worker import Worker
from utils.QueryMessage import QueryMessage, CATEGORIES_FIELD, YEAR_FIELD, TITLE_FIELD, REVIEW_MSG_TYPE
from Persistance.log import ChangeContextNoArgs

class Filter(Worker):
    def __init__(self, id, next_pools, eof_to_receive, field, valid_values, droping_fields):
        super().__init__(id, next_pools, eof_to_receive)
        self.field = field
        self.valid_values = valid_values
        self.droping_fields = droping_fields
        self.filtered_client_books = {}

    @classmethod
    def new(cls, field, valid_values, droping_fields=[]):
        id, next_pools, eof_to_receive = Filter.get_env()
        if id == None or eof_to_receive == None or not next_pools:
            return None
        filter = Filter(id, next_pools, eof_to_receive, field, valid_values, droping_fields)
        if not filter.connect():
            return None
        return filter

    def dump_context_to_disc(self):
        #dump context
        pass
    
    def process_message(self, client_id, msg: QueryMessage):
        self.filtered_client_books[client_id] = self.filtered_client_books.get(client_id, set())
        if msg.msg_type == REVIEW_MSG_TYPE and msg.title in self.filtered_client_books[client_id]:
            return self.transform_to_result(msg)
        if self.filter_book(msg):
            self.filtered_client_books[client_id].add(msg.title)
            msg = msg.copy_droping_fields(self.droping_fields)
            return self.transform_to_result(msg)
        return None
    
    def remove_client_context(self, client_id):
        if client_id in self.filtered_client_books:
            self.filtered_client_books.pop(client_id)

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

    def get_final_results(self, _client_id):
        return []