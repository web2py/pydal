import re
from .._compat import PY2, basestring
from ..adapters.mongo import Mongo
from ..exceptions import NotOnNOSQLError
from ..objects import Field
from .base import NoSQLDialect
from . import dialects

_aggregate_map = {
    'SUM': '$sum',
    'MAX': '$max',
    'MIN': '$min',
    'AVG': '$avg',
}

_extract_map = {
    'dayofyear':    '$dayOfYear',
    'day':          '$dayOfMonth',
    'dayofweek':    '$dayOfWeek',
    'year':         '$year',
    'month':        '$month',
    'week':         '$week',
    'hour':         '$hour',
    'minute':       '$minute',
    'second':       '$second',
    'millisecond':  '$millisecond',
    'string':       '$dateToString',
}


def needs_aggregation_pipeline(f):
    def wrap(self, first, *args, **kwargs):
        self.adapter._parse_data(first, 'pipeline', True)
        if len(args) > 0:
            self.adapter._parse_data(args[0], 'pipeline', True)
        return f(self, first, *args, **kwargs)
    return wrap


def validate_second(f):
    def wrap(*args, **kwargs):
        if len(args) < 3 or args[2] is None:
            raise RuntimeError("Cannot compare %s with None" % args[1])
        return f(*args, **kwargs)
    return wrap


def check_fields_for_cmp(f):
    def wrap(self, first, second=None, *args, **kwargs):
        if (self.adapter._parse_data((first, second), 'pipeline')):
            pipeline = True
        elif not isinstance(first, Field) or self._has_field(second):
            pipeline = True
            self.adapter._parse_data((first, second), 'pipeline', True)
        else:
            pipeline = False
        return f(self, first, second, *args, pipeline=pipeline, **kwargs)
    return wrap


@dialects.register_for(Mongo)
class MongoDialect(NoSQLDialect):
    GROUP_MARK = "__#GROUP#__"
    AS_MARK = "__#AS#__"
    REGEXP_MARK1 = "__#REGEXP_1#__"
    REGEXP_MARK2 = "__#REGEXP_2#__"
    REGEX_SELECT_AS_PARSER = re.compile("\\'" + AS_MARK + "\\': \\'(\\S+)\\'")

    @staticmethod
    def _has_field(expression):
        try:
            return expression.has_field
        except AttributeError:
            return False

    def invert(self, first):
        return '-%s' % self.expand(first)

    def _not(self, val):
        op = self.expand(val)
        op_k = list(op)[0]
        op_body = op[op_k]
        rv = None
        if type(op_body) is list:
            # apply De Morgan law for and/or
            # not(A and B) -> not(A) or not(B)
            # not(A or B)  -> not(A) and not(B)
            not_op = '$and' if op_k == '$or' else '$or'
            rv = {not_op: [self._not(val.first), self._not(val.second)]}
        else:
            try:
                sub_ops = list(op_body.keys())
                if len(sub_ops) == 1 and sub_ops[0] == '$ne':
                    rv = {op_k: op_body['$ne']}
            except AttributeError:
                rv = {op_k: {'$ne': op_body}}
            if rv is None:
                rv = {op_k: {'$not': op_body}}
        return rv

    def _and(self, first, second):
        # pymongo expects: .find({'$and': [{'x':'1'}, {'y':'2'}]})
        if isinstance(second, bool):
            if second:
                return self.expand(first)
            return self.ne(first, first)
        return {'$and': [self.expand(first), self.expand(second)]}

    def _or(self, first, second):
        # pymongo expects: .find({'$or': [{'name':'1'}, {'name':'2'}]})
        if isinstance(second, bool):
            if not second:
                return self.expand(first)
            return True
        return {'$or': [self.expand(first), self.expand(second)]}

    def belongs(self, first, second):
        if isinstance(second, str):
            # this is broken, the only way second is a string is if it has
            # been converted to SQL. This no worky. This might be made to
            # work if adapter._select did not return SQL.
            raise RuntimeError("nested queries not supported")
        items = [self.expand(item, first.type) for item in second]
        return {self.expand(first): {"$in": items}}

    def _cmp_ops_aggregation_pipeline(self, op, first, second):
        try:
            type = first.type
        except:
            type = None
        return {op: [self.expand(first), self.expand(second, type)]}

    @check_fields_for_cmp
    def eq(self, first, second=None, pipeline=False):
        if pipeline:
            return self._cmp_ops_aggregation_pipeline('$eq', first, second)
        return {self.expand(first): self.expand(second, first.type)}

    @check_fields_for_cmp
    def ne(self, first, second=None, pipeline=False):
        if pipeline:
            return self._cmp_ops_aggregation_pipeline('$ne', first, second)
        return {self.expand(first): {'$ne': self.expand(second, first.type)}}

    @validate_second
    @check_fields_for_cmp
    def lt(self, first, second=None, pipeline=False):
        if pipeline:
            return self._cmp_ops_aggregation_pipeline('$lt', first, second)
        return {self.expand(first): {'$lt': self.expand(second, first.type)}}

    @validate_second
    @check_fields_for_cmp
    def lte(self, first, second=None, pipeline=False):
        if pipeline:
            return self._cmp_ops_aggregation_pipeline('$lte', first, second)
        return {self.expand(first): {'$lte': self.expand(second, first.type)}}

    @validate_second
    @check_fields_for_cmp
    def gt(self, first, second=None, pipeline=False):
        if pipeline:
            return self._cmp_ops_aggregation_pipeline('$gt', first, second)
        return {self.expand(first): {'$gt': self.expand(second, first.type)}}

    @validate_second
    @check_fields_for_cmp
    def gte(self, first, second=None, pipeline=False):
        if pipeline:
            return self._cmp_ops_aggregation_pipeline('$gte', first, second)
        return {self.expand(first): {'$gte': self.expand(second, first.type)}}

    @needs_aggregation_pipeline
    def add(self, first, second):
        op_code = '$add'
        for field in [first, second]:
            try:
                if field.type in ['string', 'text', 'password']:
                    op_code = '$concat'
                    break
            except:
                pass
        return {op_code: [self.expand(first), self.expand(second, first.type)]}

    @needs_aggregation_pipeline
    def sub(self, first, second):
        return {'$subtract': [
            self.expand(first), self.expand(second, first.type)]}

    @needs_aggregation_pipeline
    def mul(self, first, second):
        return {'$multiply': [
            self.expand(first), self.expand(second, first.type)]}

    @needs_aggregation_pipeline
    def div(self, first, second):
        return {'$divide': [
            self.expand(first), self.expand(second, first.type)]}

    @needs_aggregation_pipeline
    def mod(self, first, second):
        return {'$mod': [
            self.expand(first), self.expand(second, first.type)]}

    @needs_aggregation_pipeline
    def aggregate(self, first, what):
        if what == 'ABS':
            return {
                "$cond": [
                    {"$lt": [self.expand(first), 0]},
                    {"$subtract": [0, self.expand(first)]},
                    self.expand(first)]}
        try:
            expanded = {_aggregate_map[what]: self.expand(first)}
        except KeyError:
            raise NotImplementedError("'%s' not implemented" % what)

        self.adapter._parse_data(first, 'need_group', True)
        return {self.GROUP_MARK: expanded}

    @needs_aggregation_pipeline
    def count(self, first, distinct=None):
        self.adapter._parse_data(first, 'need_group', True)
        if distinct:
            ret = {self.GROUP_MARK: {"$addToSet": self.expand(first)}}
            if self.adapter.server_version_major >= 2.6:
                # '$size' not present in server versions < 2.6
                ret = {'$size': ret}
            return ret
        return {self.GROUP_MARK: {"$sum": 1}}

    @needs_aggregation_pipeline
    def extract(self, first, what):
        try:
            return {_extract_map[what]: self.expand(first)}
        except KeyError:
            raise NotImplementedError("EXTRACT(%s) not implemented" % what)

    @needs_aggregation_pipeline
    def epoch(self, first):
        return {"$divide": [
            {"$subtract": [self.expand(first), self.adapter.epoch]}, 1000]}

    @needs_aggregation_pipeline
    def case(self, query, true_false):
        return {"$cond": [
            self.expand(query), self.expand(true_false[0]),
            self.expand(true_false[1])]}

    @needs_aggregation_pipeline
    def _as(self, first, second):
        # put the AS_MARK into the structure.  The 'AS' name will be parsed
        # later from the string of the field name.
        if isinstance(first, Field):
            return [{self.AS_MARK: second}, self.expand(first)]
        else:
            result = self.expand(first)
            result[self.AS_MARK] = second
        return result

    # We could implement an option that simulates a full featured SQL
    # database. But I think the option should be set explicit or
    # implemented as another library.
    def on(self, first, second):
        raise NotOnNOSQLError()

    def comma(self, first, second):
        # returns field name lists, to be separated via split(',')
        return '%s,%s' % (self.expand(first), self.expand(second))

    # TODO verify full compatibilty with official SQL Like operator
    def _build_like_regex(self, first, second, case_sensitive=True,
                          escape=None, ends_with=False, starts_with=False,
                          whole_string=True, like_wildcards=False):
        base = self.expand(second, 'string')
        need_regex = (whole_string or not case_sensitive or starts_with or
                      ends_with or like_wildcards and
                      ('_' in base or '%' in base))
        if not need_regex:
            return base
        expr = re.escape(base)
        if like_wildcards:
            if escape:
                # protect % and _ which are escaped
                expr = expr.replace(escape+'\\%', '%')
                if PY2:
                    expr = expr.replace(escape+'\\_', '_')
                elif escape+'_' in expr:
                    set_aside = str(self.adapter.object_id('<random>'))
                    while set_aside in expr:
                        set_aside = str(self.adapter.object_id('<random>'))
                    expr = expr.replace(escape+'_', set_aside)
                else:
                    set_aside = None
            expr = expr.replace('\\%', '.*')
            if PY2:
                expr = expr.replace('\\_', '.')
            else:
                expr = expr.replace('_', '.')
            if escape:
                # convert to protected % and _
                expr = expr.replace('%', '\\%')
                if PY2:
                    expr = expr.replace('_', '\\_')
                elif set_aside:
                    expr = expr.replace(set_aside, '_')
        if starts_with:
            pattern = '^%s'
        elif ends_with:
            pattern = '%s$'
        elif whole_string:
            pattern = '^%s$'
        else:
            pattern = '%s'
        return self.regexp(first, pattern % expr, case_sensitive)

    def like(self, first, second, case_sensitive=True, escape=None):
        return self._build_like_regex(
            first, second, case_sensitive=case_sensitive, escape=escape,
            like_wildcards=True)

    def ilike(self, first, second, escape=None):
        return self.like(first, second, case_sensitive=False, escape=escape)

    def startswith(self, first, second):
        return self._build_like_regex(first, second, starts_with=True)

    def endswith(self, first, second):
        return self._build_like_regex(first, second, ends_with=True)

    # TODO verify full compatibilty with official oracle contains operator
    def contains(self, first, second, case_sensitive=True):
        if isinstance(second, self.adapter.ObjectId):
            ret = {self.expand(first): second}
        elif isinstance(second, Field):
            if second.type in ['string', 'text']:
                if isinstance(first, Field):
                    if first.type in ['list:string', 'string', 'text']:
                        ret = {
                            '$where': "this.%s.indexOf(this.%s) > -1" % (
                                first.name, second.name)}
                    else:
                        raise NotImplementedError(
                            "field.CONTAINS() not implemented for field " +
                            "type of '%s'" % first.type)
                else:
                    raise NotImplementedError(
                        "x.CONTAINS() not implemented for x type of '%s'" %
                        type(first))
            elif second.type in ['integer', 'bigint']:
                ret = {
                    '$where': "this.%s.indexOf(this.%s + '') > -1" % (
                        first.name, second.name)}
            else:
                raise NotImplementedError(
                    "CONTAINS(field) not implemented for field type '%s'" %
                    second.type)
        elif isinstance(second, (basestring, int)):
            whole_string = isinstance(first, Field) and \
                first.type == 'list:string'
            ret = self._build_like_regex(
                first, second, case_sensitive=case_sensitive,
                whole_string=whole_string)
            # first.type in ('string', 'text', 'json', 'upload')
            # or first.type.startswith('list:'):
        else:
            raise NotImplementedError(
                "CONTAINS() not implemented for type '%s'" % type(second))
        return ret

    @needs_aggregation_pipeline
    def substring(self, field, parameters):
        def parse_parameters(pos0, length):
            """
            The expression object can return these as string based expressions.
            We can't use that so we have to tease it apart.

            These are the possibilities:

              pos0 = '(%s - %d)' % (self.len(), abs(start) - 1)
              pos0 = start + 1

              length = self.len()
              length = '(%s - %d - %s)' % (self.len(), abs(stop) - 1, pos0)
              length = '(%s - %s)' % (stop + 1, pos0)

            Two of these five require the length of the string which is not
            supported by Mongo, so for now these cause an Exception and
            won't reach here.

            If this were to ever be supported it may require a change to
            Expression.__getitem__ so that it either returned the base
            expression to be expanded here, or converted length to a string
            to be parsed back to a call to STRLEN()
            """
            if isinstance(length, basestring):
                return (pos0 - 1, eval(length))
            # take the rest of the string
            return (pos0 - 1, -1)

        parameters = parse_parameters(*parameters)
        return {'$substr': [self.expand(field), parameters[0], parameters[1]]}

    @needs_aggregation_pipeline
    def lower(self, first):
        return {'$toLower': self.expand(first)}

    @needs_aggregation_pipeline
    def upper(self, first):
        return {'$toUpper': self.expand(first)}

    def regexp(self, first, second, case_sensitive=True):
        """ MongoDB provides regular expression capabilities for pattern
            matching strings in queries. MongoDB uses Perl compatible
            regular expressions (i.e. 'PCRE') version 8.36 with UTF-8 support.
        """
        if (isinstance(first, Field) and
                first.type in ['integer', 'bigint', 'float', 'double']):
            return {
                '$where': "RegExp('%s').test(this.%s + '')" % (
                    self.expand(second, 'string'), first.name)}
        expanded_first = self.expand(first)
        regex_second = {'$regex': self.expand(second, 'string')}
        if not case_sensitive:
            regex_second['$options'] = 'i'
        if (self.adapter._parse_data((first, second), 'pipeline')):
            name = str(expanded_first)
            return {
                self.REGEXP_MARK1: {name: expanded_first},
                self.REGEXP_MARK2: {name: regex_second}}
        try:
            return {expanded_first: regex_second}
        except TypeError:
            # if first is not hashable, then will need the pipeline
            self.adapter._parse_data((first, second), 'pipeline', True)
            return {}

    def length(self, first):
        """
        Mongo has committed $strLenBytes, $strLenCP, and $substrCP to $project
        aggregation stage in dev branch V3.3.4

        https://jira.mongodb.org/browse/SERVER-14670
        https://jira.mongodb.org/browse/SERVER-22580
        db.coll.aggregate([{
            $project: {
                byteLength: {$strLenBytes: "$string"},
                cpLength: {$strLenCP: "$string"}
                byteSubstr: {$substrBytes: ["$string", 0, 4]},
                cpSubstr: {$substrCP: ["$string", 0, 4]}
            }
        }])

        https://jira.mongodb.org/browse/SERVER-5319
        https://github.com/afchin/mongo/commit/f52105977e4d0ccb53bdddfb9c4528a3f3c40bdf
        """
        if self.adapter.server_version_major <= 3.2:
            # $strLenBytes not supported by mongo before version 3.4
            raise NotImplementedError()

        # implement here  :-)
        raise NotImplementedError()

    @needs_aggregation_pipeline
    def coalesce(self, first, second):
        if len(second) > 1:
            second = [self.coalesce(second[0], second[1:])]
        return {"$ifNull": [self.expand(first), self.expand(second[0])]}

    @property
    def random(self):
        """ ORDER BY RANDOM()

        Mongo has released the '$sample' pipeline stage in V3.2
        https://docs.mongodb.org/manual/reference/operator/aggregation/sample/

        https://github.com/mongodb/cookbook/blob/master/content/patterns/random-attribute.txt
        http://stackoverflow.com/questions/19412/how-to-request-a-random-row-in-sql
        https://jira.mongodb.org/browse/SERVER-533
        """

        if self.adapter.server_version_major <= 3.0:
            # '$sample' not present until server version 3.2
            raise NotImplementedError()

        # implement here  :-)
        raise NotImplementedError()
