from datatools import StorageTemp as Storage


def make_data():
    return {"a": 1}


with Storage() as st:

    res = st.resource(make_data)

    data = res.load()

    assert data["a"] == 1
