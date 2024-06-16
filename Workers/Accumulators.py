from abc import ABC, abstractmethod
import time

from Persistance.KeyValueStorage import KeyValueStorage
from Persistance.log import ChangingFile
from utils.auxiliar_functions import next_power_of_2_exponent
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
CONTEXT_FLOAT_BYTES = 4
CONTEXT_INT_BYTES = 4

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

    @classmethod
    def new(cls, field, values, accumulate_by):
        accumulator_class = cls.accumulator_type(field, accumulate_by)
        if not accumulator_class:
            return None
        
        id, next_pools, eof_to_receive = accumulator_class.get_env()
        if id == None or eof_to_receive == None or not next_pools:
            return None
        return accumulator_class(id, next_pools, eof_to_receive, field, values, accumulate_by)
        

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
    def get_context_storage_types(self, scale_of_update_file):
        pass 
    
    @abstractmethod
    def process_previous_context(self, previous_context):
        pass
    
    def load_context(self, path, filename,  client_id, scale_of_update_file):
        storage_types, storage_types_size = self.get_context_storage_types(scale_of_update_file)
        self.client_contexts_storage[client_id][filename], previous_context = KeyValueStorage.new(path, str, 2**scale_of_update_file, storage_types, storage_types_size)
        if not self.client_contexts_storage[client_id][filename] or previous_context == None:
            return False
        context = self.process_previous_context(previous_context)
        self.client_contexts[client_id] = context
        return True

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
    
    def change_context_log(self, client_id, keys, entries):
        return ChangingFile(client_id, keys, entries)
    
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
            self.add_to_context(client_id, author, msg_decade)
            if len(self.client_contexts[client_id].get(author, [])) == self.values:
                return True
            
    def add_to_context(self, client_id, author, msg_decade):
        old_value = self.client_contexts[client_id].get(author, None)
        if old_value != None:
            old_value = [old_value]
        scale = smalles_scale_for_str(author)
        if scale not in self.client_context_storage_updates:
            self.client_context_storage_updates[scale] = {}
        if author in self.client_context_storage_updates[scale]:
            old_value = self.client_context_storage_updates[scale][author][0]
        new_value = self.client_contexts[client_id].get(author, []) + [msg_decade]
        self.client_context_storage_updates[scale][author] = (old_value, [new_value])
        self.client_contexts[client_id][author] = new_value

    def get_context_storage_types(self, scale_of_update_file):
        return [(list, int)], [self.values]
    
    def process_previous_context(self, previous_context):
        return previous_context

    def final_results(self, client_id):
        return []
    
class AmountOfReviewByTitleAccumulator(Accumulator):
    def get_new_context(cls):
        return {}
    
    def accumulate(self, client_id, msg):
        if msg.msg_type == BOOK_MSG_TYPE:
            authors = ';'.join(msg.authors)
            self.add_to_context(client_id, msg.msg_type, msg.title, msg.rating, authors)
            if "Best Gay Erotica 2000".lower() in msg.title.lower():
                print(f"\n\n {list(authors.encode())} \n\n")
        elif msg.msg_type == REVIEW_MSG_TYPE:
            if msg.title not in self.client_contexts[client_id]:
                return None
            self.add_to_context(client_id, msg.msg_type, msg.title, msg.rating)
            if self.client_contexts[client_id][msg.title][0] == int(self.values):
                print(f"[Worker {self.id}] Accumulated {self.values} of {msg.title} for client {client_id}")
        return None
    
    def add_to_context(self, client_id, msg_type, title, rating, authors=None):
        old_value = self.client_contexts[client_id].get(title, None)
        if authors == None:
            authors = old_value[2]
        
        scale = max(smalles_scale_for_str(title),smalles_scale_for_str(authors))
        if scale not in self.client_context_storage_updates:
            self.client_context_storage_updates[scale] = {}
        if title in self.client_context_storage_updates[scale]:
            old_value = self.client_context_storage_updates[scale][title][0]
        new_value = self.client_contexts[client_id].get(title, [0, 0.0, authors])
        if msg_type == REVIEW_MSG_TYPE:
            new_value[0] +=1
            new_value[1] += rating
        self.client_context_storage_updates[scale][title] = (old_value, new_value)
        self.client_contexts[client_id][title] = new_value

    def final_results(self, client_id):
        results = []
        for title, accum in self.client_contexts[client_id].items():
            if accum[0] >= int(self.values):
                authors = accum[2].split(';')
                result = QueryMessage(msg_type=BOOK_MSG_TYPE, title=title, authors=authors, rating=accum[1] / accum[0])
                results.append(result)
        return results
    
    def get_context_storage_types(self, scale_of_update_file):
        return [int, float, str], [CONTEXT_INT_BYTES, CONTEXT_FLOAT_BYTES, 2**scale_of_update_file]
    
    def process_previous_context(self, previous_context):
        return previous_context
    
class RatingByTitleAccumulator(Accumulator):
    def get_new_context(cls):
        return []
    
    def accumulate(self, client_id, msg):
        if msg.rating == None or msg.title == None:
            return
        if len(self.client_contexts[client_id]) < int(self.values):
            self.add_to_context(client_id, msg.title, msg.rating)
        elif self.client_contexts[client_id][0].attribute < msg.rating:
            self.remove_smallest_from_context(client_id)
            self.add_to_context(client_id, msg.title, msg.rating)

    def add_to_context(self, client_id, title, rating):
        old_value = None
        new_value = rating
        scale = smalles_scale_for_str(title)
        if scale not in self.client_context_storage_updates:
            self.client_context_storage_updates[scale] = {}
        self.client_context_storage_updates[scale][title] = (old_value, [new_value])
        heapq.heappush(self.client_contexts[client_id], BookAtribute(title, rating))
    
    def remove_smallest_from_context(self, client_id):
        old_book_attribute = heapq.heappop(self.client_contexts[client_id])
        new_value = None
        scale = smalles_scale_for_str(old_book_attribute.title)
        if scale not in self.client_context_storage_updates:
            self.client_context_storage_updates[scale] = {}
        self.client_context_storage_updates[scale][old_book_attribute.title] = ([old_book_attribute.attribute], new_value)

    def final_results(self, client_id):
        results = []
        while len(self.client_contexts[client_id]) > 0:
            result = heapq.heappop(self.client_contexts[client_id])
            results.append(QueryMessage(BOOK_MSG_TYPE, title=result.title, rating= result.attribute))
        results.reverse()
        return results
    
    def get_context_storage_types(self, scale_of_update_file):
        return [float], [CONTEXT_FLOAT_BYTES]
    
    def process_previous_context(self, previous_context):
        context = []
        for title, rating in previous_context.items():
            heapq.heappush(context, BookAtribute(title, rating))
        return context
    
class ReviewTextByTitleAccumulator(Accumulator):
    def get_new_context(cls):
        return {}
    
    def accumulate(self, client_id, msg):
        if msg.msg_type == REVIEW_MSG_TYPE:
            if not msg.title in self.client_contexts[client_id].keys():
                self.client_contexts[client_id][msg.title] = [0, 0.0]
            sent_analysis = sentiment_analysis(msg.review_text)
            if sent_analysis != None:
                self.add_to_context(client_id, msg.title, sent_analysis)
        return None
    
    def add_to_context(self, client_id, title, sentiment_analisys):
        old_value = self.client_contexts[client_id].get(title, None)
        scale = smalles_scale_for_str(title)
        if scale not in self.client_context_storage_updates:
            self.client_context_storage_updates[scale] = {}
        if title in self.client_context_storage_updates[scale]:
            old_value = self.client_context_storage_updates[scale][title][0]
        new_value = self.client_contexts[client_id].get(title, [0, 0.0])
        new_value[0] += 1
        new_value[1] += sentiment_analisys
        self.client_context_storage_updates[scale][title] = (old_value, new_value)
        self.client_contexts[client_id][title] = new_value

    def final_results(self, client_id):
        results = []
        for title, result in self.client_contexts[client_id].items():
            msp = result[1] / result[0]
            results.append(QueryMessage(msg_type=BOOK_MSG_TYPE, title=title, mean_sentiment_polarity=msp))
        return results
    
    def get_context_storage_types(self, scale_of_update_file):
        return [int, float], [CONTEXT_INT_BYTES, CONTEXT_FLOAT_BYTES]
    
    def process_previous_context(self, previous_context):
        return previous_context
    
class MeanSentimentPolarityByTitleAccumulator(Accumulator):
    def get_new_context(cls):
        return []
    
    def accumulate(self, client_id, msg):
        if msg.title and msg.mean_sentiment_polarity != None:
            self.add_to_context(client_id, msg.title, msg.mean_sentiment_polarity)
    
    def add_to_context(self, client_id, title, msp):
        old_value = None
        new_value = msp
        scale = smalles_scale_for_str(title)
        if scale not in self.client_context_storage_updates:
            self.client_context_storage_updates[scale] = {}
        self.client_context_storage_updates[scale][title] = (old_value, [new_value])
        bisect.insort(self.client_contexts[client_id], BookAtribute(title, msp))

    def final_results(self, client_id):
        percentil_90_index = int(len(self.client_contexts[client_id]) * (int(self.values) / 100))
        results = []
        for a in self.client_contexts[client_id]:
            if a >= self.client_contexts[client_id][percentil_90_index]:
                results.append(a)

        return [QueryMessage(BOOK_MSG_TYPE, title=result.title) for result in results]
    
    def get_context_storage_types(self, scale_of_update_file):
        return [float], [CONTEXT_FLOAT_BYTES]
    
    def process_previous_context(self, previous_context):
        context = []
        for title, msp in previous_context.items():
            bisect.insort(context, BookAtribute(title, msp))
        return context
    
    def change_context_log(self, client_id, keys, values):
        return ChangingFile(client_id, keys, values)

class BookAtribute():
    def __init__(self, title, attribute):
        self.title = title
        self.attribute = attribute

    def __lt__(self, other):
        return self.attribute < other.attribute
    
    def __le__(self, other):
        return self.attribute <= other.attribute
    
def smalles_scale_for_str(string):
    return next_power_of_2_exponent(len(string.encode()))