# in a real example, you probably don't want StorageTemp
from datatools import StorageTemp as Storage


def make_data():
    return {"a": 1}


with Storage() as st:
    # new resource descriptor
    res = st.resource(make_data, name="MyData.pickle")

    # has not been saved or loaded
    assert not res.exists()

    # load (and save implicitly)
    data = res.load()
    assert data["a"] == 1

    # now data is stored persitently
    assert res.exists()
    assert res.uri == "data:///mydata.pickle", res.uri

    # load later
    res2 = st.resource("data:///mydata.pickle")
    assert res2.exists()
    data = res2.load()
    assert data["a"] == 1
