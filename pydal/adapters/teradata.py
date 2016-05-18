from .base import SQLAdapter
from . import adapters


@adapters.register_for('teradata')
class Teradata(SQLAdapter):
    dbengine = ''
    drivers = ('pyodbc',)

    def _initialize_(self, do_connect):
        super(Teradata, self)._initialize_(do_connect)
        self.ruri = self.uri.split('://', 1)[1]

    def connector(self):
        return self.driver.connect(self.ruri, **self.driver_args)

    def close(self):
        # Teradata does not implicitly close off the cursor
        # leading to SQL_ACTIVE_STATEMENTS limit errors
        self.cursor.close()
        super(Teradata, self).close()

    def lastrowid(self, table):
        # Teradata cannot retrieve the lastrowid for an IDENTITY Column
        # and they are not sequential anyway.  
        # Similar to the NullCursor class, return 1
        return 1
        