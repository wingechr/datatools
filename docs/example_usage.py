from datatools import StorageTemp as Storage

st = Storage()


def make_data():
    return {"a": 1}


res = st.resource(make_data)

data = res.load()
print(data)
