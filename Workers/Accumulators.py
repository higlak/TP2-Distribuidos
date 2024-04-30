from .Worker import Worker
from utils.QueryMessage import QueryMessage, CATEGORIES_FIELD, YEAR_FIELD, TITLE_FIELD, AUTHOR_FIELD
import unittest
from unittest import TestCase

class Accumulator(Worker):
    def __init__(self, field, values, accumulate_by):
        super().__init__()
        self.field = field
        self.values = values
        self.accumulate_by = accumulate_by
        self.context = {}

    def process_message(self, msg: QueryMessage):
        switch = {
            (YEAR_FIELD, AUTHOR_FIELD): self.accumulate_decade_by_authors,
        }
        method = switch.get((self.field, self.accumulate_by), None)
        if not method:
            return None
        
        return [self.transform_to_result(m) for m in method(msg)]
    
    def accumulate_decade_by_authors(self, msg):
        if msg.authors == None:
            return None
        results = []
        for author in msg.authors:
            if self.accumulate_decade_by_author(author, msg.decade()):
                msg_aux = msg.copy_droping_fields([YEAR_FIELD])
                msg_aux.authors = [author]
                results.append(msg_aux)
        return results
                
            
    def accumulate_decade_by_author(self, author, msg_decade):
        author_decades = self.context.get(author, [])
        if len(author_decades) == self.values:
            return False
        if msg_decade == None:
            return False
        if not (msg_decade in self.context.get(author, [])):
            print(f"Accumulating Author {author} decade {msg_decade}")
            self.context[author] = self.context.get(author, []) + [msg_decade]
            if len(self.context.get(author, [])) == self.values:
                return True