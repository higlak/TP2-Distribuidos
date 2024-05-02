from .Worker import Worker
from utils.QueryMessage import QueryMessage, CATEGORIES_FIELD, YEAR_FIELD, TITLE_FIELD, REVIEW_MSG_TYPE

class Filter(Worker):
    def __init__(self, field, valid_values, droping_fields=[]):
        super().__init__()
        self.field = field
        self.valid_values = valid_values
        self.droping_fields = droping_fields
        self.filtered_books_titles = set()

    def process_message(self, msg: QueryMessage):
        if msg.msg_type == REVIEW_MSG_TYPE and msg.title in self.filtered_books_titles:
            return self.transform_to_result(msg)
        if self.filter_book(msg):
            self.filtered_books_titles.add(msg.title)
            msg = msg.copy_droping_fields(self.droping_fields)
            return self.transform_to_result(msg)
        return None
    
    def reset_context(self):
        self.filtered_books_titles = set()        

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

    def get_final_results(self):
        return []