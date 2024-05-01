from .Worker import Worker
from utils.QueryMessage import QueryMessage, CATEGORIES_FIELD, YEAR_FIELD, TITLE_FIELD, AUTHOR_FIELD, BOOK_MSG_TYPE, REVIEW_MSG_TYPE, RATING_FIELD
import unittest
from unittest import TestCase
import heapq

REVIEW_COUNT = "review_count"

class Accumulator(Worker):
    def __init__(self, field, values, accumulate_by):
        super().__init__()
        self.field = field
        self.values = values
        self.accumulate_by = accumulate_by
        switch = {
            (YEAR_FIELD, AUTHOR_FIELD): {},
            (REVIEW_COUNT, TITLE_FIELD): {},
            (RATING_FIELD, TITLE_FIELD): [],
        }
        self.context = switch[(field, accumulate_by)]

    def process_message(self, msg: QueryMessage):
        switch = {
            (YEAR_FIELD, AUTHOR_FIELD): self.accumulate_decade_by_authors,
            (REVIEW_COUNT, TITLE_FIELD): self.accumulate_amount_of_reviews,
            (RATING_FIELD, TITLE_FIELD): self.accumulate_rating_by_title
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
            self.context[msg.title] = [msg, 0, 0]
        if msg.msg_type == REVIEW_MSG_TYPE:
            self.context[msg.title][1] += 1
            self.context[msg.title][2] += msg.rating 
            #if self.context[msg.title][1] == int(self.values):
            #    print("Reached 500 ", msg.title)
            #    return [self.context[msg.title][0].copy_droping_fields([RATING_FIELD])]
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
    
    def accumulate_rating_by_title(self, msg):
        if len(self.context) < int(self.values):
            heapq.heappush(self.context, RatingOfBook(msg.title, msg.rating))
        elif self.context[0].rating < msg.rating:
            heapq.heappop(self.context)
            heapq.heappush(self.context, RatingOfBook(msg.title, msg.rating))
        
    def get_final_results(self):
        switch = {
            (YEAR_FIELD, AUTHOR_FIELD): None,
            (REVIEW_COUNT, TITLE_FIELD): self.amount_of_reviews_final_results,
            (RATING_FIELD, TITLE_FIELD): self.rating_by_title_final_results
        }
        method = switch.get((self.field, self.accumulate_by), None)
        if not method:
            return []
        results = method()
        return [self.transform_to_result(msg) for msg in results]
    
    def amount_of_reviews_final_results(self):
        results = []
        for potential_result in self.context.values():
            if potential_result[1] >= int(self.values):
                potential_result[0].rating = potential_result[2] / potential_result[1]
                results.append(potential_result[0])
        return results
                
    def rating_by_title_final_results(self):
        results = []
        while len(self.context) > 0:
            results.append(heapq.heappop(self.context).get_query_message())
        results.reverse()
        return results
        
class RatingOfBook():
    def __init__(self, title, rating):
        self.title = title
        self.rating = rating

    def __lt__(self, other):
        return self.rating < other.rating
    
    def __le__(self, other):
        return self.rating <= other.rating
    
    def get_query_message(self):
        return QueryMessage(BOOK_MSG_TYPE, title=self.title, rating=self.rating)