from .regex import REGEX_SEARCH_PATTERN, REGEX_SQUARE_BRACKETS
from .._compat import long

def to_num(num):
    result = 0
    try:
        result = long(num)
    except NameError as e:
        result = int(num)
    return result

class RestParser(object):
    def __init__(self, db):
        self.db = db

    def auto_table(self, table, base='', depth=0):
        patterns = []
        for field in self.db[table].fields:
            if base:
                tag = '%s/%s' % (base, field.replace('_', '-'))
            else:
                tag = '/%s/%s' % (
                    table.replace('_', '-'), field.replace('_', '-'))
            f = self.db[table][field]
            if not f.readable:
                continue
            if f.type == 'id' or 'slug' in field or \
               f.type.startswith('reference'):
                tag += '/{%s.%s}' % (table, field)
                patterns.append(tag)
                patterns.append(tag+'/:field')
            elif f.type.startswith('boolean'):
                tag += '/{%s.%s}' % (table, field)
                patterns.append(tag)
                patterns.append(tag+'/:field')
            elif f.type in ('float', 'double', 'integer', 'bigint'):
                tag += '/{%s.%s.ge}/{%s.%s.lt}' % (table, field, table, field)
                patterns.append(tag)
                patterns.append(tag+'/:field')
            elif f.type.startswith('list:'):
                tag += '/{%s.%s.contains}' % (table, field)
                patterns.append(tag)
                patterns.append(tag+'/:field')
            elif f.type in ('date', 'datetime'):
                tag += '/{%s.%s.year}' % (table, field)
                patterns.append(tag)
                patterns.append(tag+'/:field')
                tag += '/{%s.%s.month}' % (table, field)
                patterns.append(tag)
                patterns.append(tag+'/:field')
                tag += '/{%s.%s.day}' % (table, field)
                patterns.append(tag)
                patterns.append(tag+'/:field')
            if f.type in ('datetime', 'time'):
                tag += '/{%s.%s.hour}' % (table, field)
                patterns.append(tag)
                patterns.append(tag+'/:field')
                tag += '/{%s.%s.minute}' % (table, field)
                patterns.append(tag)
                patterns.append(tag+'/:field')
                tag += '/{%s.%s.second}' % (table, field)
                patterns.append(tag)
                patterns.append(tag+'/:field')
            if depth > 0:
                for f in self.db[table]._referenced_by:
                    tag += '/%s[%s.%s]' % (table, f.tablename, f.name)
                    patterns.append(tag)
                    patterns += self.auto_table(table, base=tag, depth=depth-1)
        return patterns

    def parse(self, patterns, args, vars, queries=None, nested_select=True):
        """
        Example:
            Use as::

                db.define_table('person',Field('name'),Field('info'))
                db.define_table('pet',
                    Field('ownedby',db.person),
                    Field('name'),Field('info')
                )

                @request.restful()
                def index():
                    def GET(*args,**vars):
                        patterns = [
                            "/friends[person]",
                            "/{person.name}/:field",
                            "/{person.name}/pets[pet.ownedby]",
                            "/{person.name}/pets[pet.ownedby]/{pet.name}",
                            "/{person.name}/pets[pet.ownedby]/{pet.name}/:field",
                            ("/dogs[pet]", db.pet.info=='dog'),
                            ("/dogs[pet]/{pet.name.startswith}", db.pet.info=='dog'),
                            ]
                        parser = db.parse_as_rest(patterns,args,vars)
                        if parser.status == 200:
                            return dict(content=parser.response)
                        else:
                            raise HTTP(parser.status,parser.error)

                    def POST(table_name,**vars):
                        if table_name == 'person':
                            return db.person.validate_and_insert(**vars)
                        elif table_name == 'pet':
                            return db.pet.validate_and_insert(**vars)
                        else:
                            raise HTTP(400)
                    return locals()
        """

        re1 = REGEX_SEARCH_PATTERN
        re2 = REGEX_SQUARE_BRACKETS

        if patterns == 'auto':
            patterns = []
            for table in self.db.tables:
                if not table.startswith('auth_'):
                    patterns.append('/%s[%s]' % (table, table))
                    patterns += self.auto_table(table, base='', depth=1)
        else:
            i = 0
            while i < len(patterns):
                pattern = patterns[i]
                if not isinstance(pattern, str):
                    pattern = pattern[0]
                tokens = pattern.split('/')
                if tokens[-1].startswith(':auto') and re2.match(tokens[-1]):
                    new_patterns = self.auto_table(
                        tokens[-1][tokens[-1].find('[')+1:-1],
                        '/'.join(tokens[:-1]))
                    patterns = patterns[:i]+new_patterns+patterns[i+1:]
                    i += len(new_patterns)
                else:
                    i += 1
        if '/'.join(args) == 'patterns':
            return self.db.Row({
                'status': 200, 'pattern': 'list', 'error': None,
                'response': patterns})
        for pattern in patterns:
            basequery, exposedfields = None, []
            if isinstance(pattern, tuple):
                if len(pattern) == 2:
                    pattern, basequery = pattern
                elif len(pattern) > 2:
                    pattern, basequery, exposedfields = pattern[0:3]
            otable = table = None
            if not isinstance(queries, dict):
                dbset = self.db(queries)
                if basequery is not None:
                    dbset = dbset(basequery)
            i = 0
            tags = pattern[1:].split('/')
            if len(tags) != len(args):
                continue
            for tag in tags:
                if re1.match(tag):
                    tokens = tag[1:-1].split('.')
                    table, field = tokens[0], tokens[1]
                    if not otable or table == otable:
                        if len(tokens) == 2 or tokens[2] == 'eq':
                            query = self.db[table][field] == args[i]
                        elif tokens[2] == 'ne':
                            query = self.db[table][field] != args[i]
                        elif tokens[2] == 'lt':
                            query = self.db[table][field] < args[i]
                        elif tokens[2] == 'gt':
                            query = self.db[table][field] > args[i]
                        elif tokens[2] == 'ge':
                            query = self.db[table][field] >= args[i]
                        elif tokens[2] == 'le':
                            query = self.db[table][field] <= args[i]
                        elif tokens[2] == 'year':
                            query = self.db[table][field].year() == args[i]
                        elif tokens[2] == 'month':
                            query = self.db[table][field].month() == args[i]
                        elif tokens[2] == 'day':
                            query = self.db[table][field].day() == args[i]
                        elif tokens[2] == 'hour':
                            query = self.db[table][field].hour() == args[i]
                        elif tokens[2] == 'minute':
                            query = self.db[table][field].minutes() == args[i]
                        elif tokens[2] == 'second':
                            query = self.db[table][field].seconds() == args[i]
                        elif tokens[2] == 'startswith':
                            query = self.db[table][field].startswith(args[i])
                        elif tokens[2] == 'contains':
                            query = self.db[table][field].contains(args[i])
                        else:
                            raise RuntimeError("invalid pattern: %s" % pattern)
                        if len(tokens) == 4 and tokens[3] == 'not':
                            query = ~query
                        elif len(tokens) >= 4:
                            raise RuntimeError("invalid pattern: %s" % pattern)
                        if not otable and isinstance(queries, dict):
                            dbset = self.db(queries[table])
                            if basequery is not None:
                                dbset = dbset(basequery)
                        dbset = dbset(query)
                    else:
                        raise RuntimeError(
                            "missing relation in pattern: %s" % pattern)
                elif re2.match(tag) and args[i] == tag[:tag.find('[')]:
                    ref = tag[tag.find('[')+1:-1]
                    if '.' in ref and otable:
                        table, field = ref.split('.')
                        selfld = '_id'
                        if self.db[table][field].type.startswith('reference '):
                            refs = [
                                x.name for x in self.db[otable]
                                if x.type == self.db[table][field].type]
                        else:
                            refs = [
                                x.name for x in self.db[table]._referenced_by
                                if x.tablename == otable]
                        if refs:
                            selfld = refs[0]
                        if nested_select:
                            try:
                                dbset = self.db(self.db[table][field].belongs(
                                    dbset._select(self.db[otable][selfld])))
                            except ValueError:
                                return self.db.Row({
                                    'status': 400, 'pattern': pattern,
                                    'error': 'invalid path', 'response': None})
                        else:
                            items = [
                                item.id for item in dbset.select(
                                    self.db[otable][selfld])
                            ]
                            dbset = self.db(
                                self.db[table][field].belongs(items))
                    else:
                        table = ref
                        if not otable and isinstance(queries, dict):
                            dbset = self.db(queries[table])
                        dbset = dbset(self.db[table])
                elif tag == ':field' and table:
                    # print 're3:'+tag
                    field = args[i]
                    if field not in self.db[table]:
                        break
                    # hand-built patterns should respect .readable=False as well
                    if not self.db[table][field].readable:
                        return self.db.Row({
                            'status': 418, 'pattern': pattern,
                            'error': 'I\'m a teapot', 'response': None})
                    try:
                        distinct = vars.get('distinct', False) == 'True'
                        offset = to_num(vars.get('offset', None) or 0)
                        limits = (
                            offset,
                            to_num(vars.get('limit', None) or 1000) + offset)
                    except ValueError:
                        return self.db.Row({
                            'status': 400, 'error': 'invalid limits',
                            'response': None})
                    items = dbset.select(
                        self.db[table][field], distinct=distinct,
                        limitby=limits)
                    if items:
                        return self.db.Row({
                            'status': 200, 'response': items,
                            'pattern': pattern})
                    else:
                        return self.db.Row({
                            'status': 404, 'pattern': pattern,
                            'error': 'no record found', ' response': None})
                elif tag != args[i]:
                    break
                otable = table
                i += 1
                if i == len(tags) and table:
                    if hasattr(self.db[table], '_id'):
                        ofields = vars.get(
                            'order', self.db[table]._id.name).split('|')
                    else:
                        ofields = vars.get(
                            'order', self.db[table]._primarykey[0]).split('|')
                    try:
                        orderby = [
                            self.db[table][f] if not f.startswith('~')
                            else ~self.db[table][f[1:]] for f in ofields]
                    except (KeyError, AttributeError):
                        return self.db.Row({
                            'status': 400, 'error': 'invalid orderby',
                            'response': None})
                    if exposedfields:
                        fields = [
                            field for field in self.db[table]
                            if str(field).split('.')[-1] in exposedfields and
                            field.readable]
                    else:
                        fields = [
                            field for field in self.db[table]
                            if field.readable]
                    count = dbset.count()
                    try:
                        offset = to_num(vars.get('offset', None) or 0)
                        limits = (
                            offset,
                            to_num(vars.get('limit', None) or 1000) + offset)
                    except ValueError:
                        return self.db.Row({
                            'status': 400, 'error': ' invalid limits',
                            'response': None})
                    try:
                        response = dbset.select(
                            limitby=limits, orderby=orderby, *fields)
                    except ValueError:
                        return self.db.Row({
                            'status': 400, 'pattern': pattern,
                            'error': 'invalid path', 'response': None})
                    return self.db.Row({
                        'status': 200, 'response': response,
                        'pattern': pattern, 'count': count})
        return self.db.Row({
            'status': 400, 'error': 'no matching pattern', 'response': None})
