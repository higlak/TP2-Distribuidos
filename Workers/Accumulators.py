from abc import ABC, abstractmethod
import time
from .Worker import Worker
from utils.QueryMessage import QueryMessage, YEAR_FIELD, TITLE_FIELD, AUTHOR_FIELD, BOOK_MSG_TYPE, REVIEW_MSG_TYPE, RATING_FIELD, MSP_FIELD, REVIEW_TEXT_FIELD
import unittest
from unittest import TestCase
import heapq
import bisect
try:
    from textblob import TextBlob
except:
    pass

REVIEW_COUNT = "review_count"

def sentiment_analysis(texto):
    if isinstance(texto, str):
        blob = TextBlob(texto)
        sentimiento = blob.sentiment.polarity
        return sentimiento
    else:
        return None

class Accumulator(Worker, ABC):
    def __init__(self,  id, next_pools, eof_to_receive, field, values, accumulate_by):
        super().__init__(id, next_pools, eof_to_receive)
        self.field = field
        self.values = values
        self.accumulate_by = accumulate_by
        self.client_contexts = {}

    @classmethod
    def new(cls, field, values, accumulate_by):
        accumulator_class = cls.accumulator_type(field, accumulate_by)
        if not accumulator_class:
            return None
        
        id, next_pools, eof_to_receive = accumulator_class.get_env()
        if id == None or eof_to_receive == None or not next_pools:
            return None
        accumulator = accumulator_class(id, next_pools, eof_to_receive, field, values, accumulate_by)
        accumulator.handle_waker_leader()
        if not accumulator.connect():
            return None
        return accumulator
        

    @classmethod
    def accumulator_type(cls, field, accumulate_by):
        switch = {
            (YEAR_FIELD, AUTHOR_FIELD): DecadeByAuthorAccumulator,
            (REVIEW_COUNT, TITLE_FIELD): AmountOfReviewByTitleAccumulator,
            (RATING_FIELD, TITLE_FIELD): RatingByTitleAccumulator,
            (REVIEW_TEXT_FIELD, TITLE_FIELD): ReviewTextByTitleAccumulator,
            (MSP_FIELD, TITLE_FIELD): MeanSentimentPolarityByTitleAccumulator,
        }
        return switch.get((field, accumulate_by), None)
    
    @abstractmethod
    def get_new_context(cls):
        pass

    def remove_client_context(self, client_id):
        if client_id in self.client_contexts:
            self.client_contexts.pop(client_id)
    
    @abstractmethod
    def accumulate(cls, client_id, msg):
        pass

    def process_message(self, client_id, msg: QueryMessage):
        self.client_contexts[client_id] = self.client_contexts.get(client_id, self.get_new_context())
        results = self.accumulate(client_id, msg)
        if not results:
            return None
        return [self.transform_to_result(m) for m in results]
    
    @abstractmethod
    def final_results(cls, client_id):
        pass

    def get_final_results(self, client_id):
        if client_id in self.client_contexts:
            return [self.transform_to_result(msg) for msg in self.final_results(client_id)]
        

class DecadeByAuthorAccumulator(Accumulator):
    def get_new_context(cls):
        return {}
    
    def accumulate(self, client_id, msg):
        if msg.authors == None:
            return None
        results = []
        for author in msg.authors:
            if self.accumulate_decade_by_author(client_id, author, msg.decade()):
                msg_aux = msg.copy_droping_fields([YEAR_FIELD])
                msg_aux.authors = [author]
                results.append(msg_aux)
        return results
    
    def accumulate_decade_by_author(self, client_id, author, msg_decade):
        author_decades = self.client_contexts[client_id].get(author, [])
        if len(author_decades) == self.values:
            return False
        if msg_decade == None:
            return False
        if not (msg_decade in self.client_contexts[client_id].get(author, [])):
            self.client_contexts[client_id][author] = self.client_contexts[client_id].get(author, []) + [msg_decade]
            if len(self.client_contexts[client_id].get(author, [])) == self.values:
                return True
            
    def final_results(self, client_id):
        return []
    
class AmountOfReviewByTitleAccumulator(Accumulator):
    def get_new_context(cls):
        return {}
    
    def accumulate(self, client_id, msg):
        if msg.msg_type == BOOK_MSG_TYPE and not msg.title in self.client_contexts[client_id].keys():
            self.client_contexts[client_id][msg.title] = [msg, 0, 0]
        if msg.msg_type == REVIEW_MSG_TYPE:
            self.client_contexts[client_id][msg.title][1] += 1
            self.client_contexts[client_id][msg.title][2] += msg.rating 
            if self.client_contexts[client_id][msg.title][1] == int(self.values):
                print(f"[Worker {self.id}] Accumulated {self.values} of {msg.title} for client {client_id}")
        return None
    
    def final_results(self, client_id):
        results = []
        for potential_result in self.client_contexts[client_id].values():
            if potential_result[1] >= int(self.values):
                potential_result[0].rating = potential_result[2] / potential_result[1]
                results.append(potential_result[0])
        return results
    
class RatingByTitleAccumulator(Accumulator):
    def get_new_context(cls):
        return []
    
    def accumulate(self, client_id, msg):
        if msg.rating == None:
            return
        if len(self.client_contexts[client_id]) < int(self.values):
            heapq.heappush(self.client_contexts[client_id], BookAtribute(msg.title, msg.rating))
        elif self.client_contexts[client_id][0].attribute < msg.rating:
            heapq.heappop(self.client_contexts[client_id])
            heapq.heappush(self.client_contexts[client_id], BookAtribute(msg.title, msg.rating))

    def final_results(self, client_id):
        results = []
        while len(self.client_contexts[client_id]) > 0:
            result = heapq.heappop(self.client_contexts[client_id])
            results.append(QueryMessage(BOOK_MSG_TYPE, title=result.title, rating= result.attribute))
        results.reverse()
        return results
    
class ReviewTextByTitleAccumulator(Accumulator):
    def get_new_context(cls):
        return {}
    
    def accumulate(self, client_id, msg):
        if msg.msg_type == REVIEW_MSG_TYPE:
            if not msg.title in self.client_contexts[client_id].keys():
                self.client_contexts[client_id][msg.title] = [0, 0]
            sent_analysis = sentiment_analysis(msg.review_text)
            if sent_analysis != None:
                self.client_contexts[client_id][msg.title][0] += 1
                self.client_contexts[client_id][msg.title][1] += sent_analysis
        return None
    
    def final_results(self, client_id):
        results = []
        for title, result in self.client_contexts[client_id].items():
            msp = result[1] / result[0]
            results.append(QueryMessage(msg_type=BOOK_MSG_TYPE, title=title, mean_sentiment_polarity=msp))
        return results
    
class MeanSentimentPolarityByTitleAccumulator(Accumulator):
    def get_new_context(cls):
        return []
    
    def accumulate(self, client_id, msg):
        if msg.title and msg.mean_sentiment_polarity != None:
            bisect.insort(self.client_contexts[client_id], BookAtribute(msg.title, msg.mean_sentiment_polarity))

    def final_results(self, client_id):
        percentil_90_index = int(len(self.client_contexts[client_id]) * (int(self.values) / 100))
        results = []
        for a in self.client_contexts[client_id]:
            if a >= self.client_contexts[client_id][percentil_90_index]:
                results.append(a)

        return [QueryMessage(BOOK_MSG_TYPE, title=result.title) for result in results]

class BookAtribute():
    def __init__(self, title, attribute):
        self.title = title
        self.attribute = attribute

    def __lt__(self, other):
        return self.attribute < other.attribute
    
    def __le__(self, other):
        return self.attribute <= other.attribute