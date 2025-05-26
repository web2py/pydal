from pydal import Field, DAL, QueryBuilder


db = DAL("sqlite://storage.sql", folder="/tmp")
db.define_table(
    "thing",
    Field("name"),
    Field("weight", "double"),
    Field("quantity", "integer"),
    Field("tags", "list:string"),
)

builder = QueryBuilder(debug=False)
print(
    builder.parse(
        db.thing,
        'not weight is greater than 10.5 and quantity is not null or  name  lower startswith "do\\"g" or name belongs "one", "two", "three"',
    )
)

print(
    builder.parse(
        db.thing,
        'not( not weight is greater than 10.5 and(quantity is not null or((name lower startswith "dog") or name belongs "one", "two", "three")))',
    )
)
