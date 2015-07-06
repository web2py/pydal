from .base import BaseAdapter

class NoneAdapter(BaseAdapter):

    def insert(self, table, fields):
        return None

    def bulk_insert(self, table, items):
        return []

    def update(self, tablename, query, fields):
        return 0

    def delete(self, tablename, query):
        return 0

    def select(self, query, fields, attributes):
        return []

    def count(self, query, distinct=None):
        return 0

    def log_execute(self, *a, **b):
        return None

    def execute(self, *a, **b):
        return None

