import os
import re
from .._compat import pjoin
from .._globals import THREAD_LOCAL
from .._gae import ndb, rdbms
from ..helpers.classes import UseDatabaseStoredFile
from .mysql import MySQL
from . import adapters, with_connection_or_raise


@adapters.register_for('google:sql')
class GoogleSQL(UseDatabaseStoredFile, MySQL):
    uploads_in_blob = True
    REGEX_URI = re.compile('^(?P<instance>.*)/(?P<db>.*)$')

    def _initialize_(self, do_connect):
        super(MySQL, self)._initialize_(do_connect)
        self.folder = self.folder or pjoin(
            '$HOME', THREAD_LOCAL._pydal_folder_.split(
                os.sep+'applications'+os.sep, 1)[1])
        ruri = self.uri.split('://', 1)[1]
        m = self.REGEX_URI.match(ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        self.driver_args['instance'] = self.credential_decoder(
            m.group('instance'))
        self.dbstring = self.credential_decoder(m.group('db'))
        self.createdb = self.adapter_args.get('createdb', True)
        if not self.createdb:
            self.driver_args['database'] = self.dbstring

    def find_driver(self):
        self.driver = "google"

    def connector(self):
        return rdbms.connect(**self.driver_args)

    def after_connection(self):
        if self.createdb:
            self.execute('CREATE DATABASE IF NOT EXISTS %s' % self.dbstring)
            self.execute('USE %s' % self.dbstring)
        self.execute("SET FOREIGN_KEY_CHECKS=1;")
        self.execute("SET sql_mode='NO_BACKSLASH_ESCAPES';")

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        command = self.filter_sql_command(args[0]).decode('utf8')
        handlers = self._build_handlers_for_execution()
        for handler in handlers:
            handler.before_execute(command)
        #self.db._lastsql = command
        rv = self.cursor.execute(command, *args[1:], **kwargs)
        for handler in handlers:
            handler.after_execute(command)
        return rv

    def clear_cache(self):
        ndb.get_context().clear_cache()

    def ignore_cache_for(self, entities=None):
        entities = entities or []
        ndb.get_context().set_cache_policy(
            lambda key: key.kind() not in entities)
