from abc import ABC, abstractmethod
from .Worker import Worker
from utils.QueryMessage import QueryMessage, YEAR_FIELD, TITLE_FIELD, AUTHOR_FIELD, BOOK_MSG_TYPE, REVIEW_MSG_TYPE, RATING_FIELD, MSP_FIELD, REVIEW_TEXT_FIELD
import unittest
from unittest import TestCase
import heapq
import bisect
from textblob import TextBlob

REVIEW_COUNT = "review_count"


def sentiment_analysis(texto):
    if isinstance(texto, str):
        blob = TextBlob(texto)
        sentimiento = blob.sentiment.polarity
        return sentimiento
    else:
        return None

class Accumulator(Worker, ABC):
    def __init__(self, field, values, accumulate_by):
        super().__init__()
        self.field = field
        self.values = values
        self.accumulate_by = accumulate_by
        self.context = self.get_new_context()

    @staticmethod
    def new(field, values, accumulate_by):
        switch = {
            (YEAR_FIELD, AUTHOR_FIELD): DecadeByAuthorAccumulator,
            (REVIEW_COUNT, TITLE_FIELD): AmountOfReviewByTitleAccumulator,
            (RATING_FIELD, TITLE_FIELD): RatingByTitleAccumulator,
            (REVIEW_TEXT_FIELD, TITLE_FIELD): ReviewTextByTitleAccumulator,
            (MSP_FIELD, TITLE_FIELD): MeanSentimentPolarityByTitleAccumulator,
        }
        return switch[(field, accumulate_by)](field, values, accumulate_by)

    @abstractmethod
    def get_new_context(cls):
        pass

    def reset_context(self):
        self.context = self.get_new_context()
    
    @abstractmethod
    def accumulate(cls):
        pass

    def process_message(self, msg: QueryMessage):        
        results = self.accumulate(msg)
        if not results:
            return None
        return [self.transform_to_result(m) for m in results]
    
    @abstractmethod
    def final_results(cls):
        pass

    def get_final_results(self):
        return [self.transform_to_result(msg) for msg in self.final_results()]
        

class DecadeByAuthorAccumulator(Accumulator):
    def get_new_context(cls):
        return {}
    
    def accumulate(self, msg):
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
            
    def final_results(self):
        return []
    
class AmountOfReviewByTitleAccumulator(Accumulator):
    def get_new_context(cls):
        return {}
    
    def accumulate(self, msg):
        if msg.msg_type == BOOK_MSG_TYPE and not msg.title in self.context.keys():
            self.context[msg.title] = [msg, 0, 0]
        if msg.msg_type == REVIEW_MSG_TYPE:
            self.context[msg.title][1] += 1
            self.context[msg.title][2] += msg.rating 
            if self.context[msg.title][1] == int(self.values):
                print(f"[Worker {self.id}] Accumulated {self.values} of {msg.title}")
        return None
    
    def final_results(self):
        results = []
        for potential_result in self.context.values():
            if potential_result[1] >= int(self.values):
                potential_result[0].rating = potential_result[2] / potential_result[1]
                results.append(potential_result[0])
        return results
    
class RatingByTitleAccumulator(Accumulator):
    def get_new_context(cls):
        return []
    
    def accumulate(self, msg):
        if msg.rating == None:
            return
        if len(self.context) < int(self.values):
            heapq.heappush(self.context, BookAtribute(msg.title, msg.rating))
        elif self.context[0].attribute < msg.rating:
            heapq.heappop(self.context)
            heapq.heappush(self.context, BookAtribute(msg.title, msg.rating))

    def final_results(self):
        results = []
        while len(self.context) > 0:
            result = heapq.heappop(self.context)
            results.append(QueryMessage(BOOK_MSG_TYPE, title=result.title, rating= result.attribute))
        results.reverse()
        return results
    
class ReviewTextByTitleAccumulator(Accumulator):
    def get_new_context(cls):
        return {}
    
    def accumulate(self, msg):
        if msg.msg_type == REVIEW_MSG_TYPE:
            if not msg.title in self.context.keys():
                self.context[msg.title] = [0, 0]
            sent_analysis = sentiment_analysis(msg.review_text)
            if sent_analysis != None:
                self.context[msg.title][0] += 1
                self.context[msg.title][1] += sent_analysis
        return None
    
    def final_results(self):
        results = []
        for title, result in self.context.items():
            msp = result[1] / result[0]
            results.append(QueryMessage(msg_type=BOOK_MSG_TYPE, title=title, mean_sentiment_polarity=msp))
        return results
    
class MeanSentimentPolarityByTitleAccumulator(Accumulator):
    def get_new_context(cls):
        return []
    
    def accumulate(self, msg):
        if msg.title and msg.mean_sentiment_polarity != None:
            bisect.insort(self.context, BookAtribute(msg.title, msg.mean_sentiment_polarity))

    def final_results(self):
        percentil_90_index = int(len(self.context) * (int(self.values) / 100))
        results = []
        for a in self.context:
            if a >= self.context[percentil_90_index]:
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