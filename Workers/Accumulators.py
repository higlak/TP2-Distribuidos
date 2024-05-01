from .Worker import Worker
from utils.QueryMessage import QueryMessage, CATEGORIES_FIELD, YEAR_FIELD, TITLE_FIELD, AUTHOR_FIELD, BOOK_MSG_TYPE, REVIEW_MSG_TYPE, RATING_FIELD
import unittest
from unittest import TestCase

REVIEW_COUNT = "review_count"

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
            (REVIEW_COUNT, TITLE_FIELD): self.accumulate_amount_of_reviews,
        }
        method = switch.get((self.field, self.accumulate_by), None)
        if not method:
            return None
        
        results = method(msg)
        if not results:
            return None
        return [self.transform_to_result(m) for m in results]
    
    def accumulate_amount_of_reviews(self, msg):
        if msg.msg_type == BOOK_MSG_TYPE and not msg.title in self.context.keys():
            self.context[msg.title] = [msg, 0]
        if msg.msg_type == REVIEW_MSG_TYPE:
            self.context[msg.title][1] += 1
            if msg.title == 'Paradise':
                print("Paradise reviews: ", self.context[msg.title][1])
            if self.context[msg.title][1] == int(self.values):
                print("Reached 500 ", msg.title)
                return [self.context[msg.title][0].copy_droping_fields([RATING_FIELD])]
        return None

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
            self.context[author] = self.context.get(author, []) + [msg_decade]
            if len(self.context.get(author, [])) == self.values:
                return True