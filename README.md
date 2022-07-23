# Under construction

- protocols

  - LOCAL
    - file (file or nothing)
    - datapackage-resource (resource)
  - REMOTE
    - sqlalchmy (dbtype+dialect)
    - web (http[s])
    - hash (sha256, md5)?

- cli actions:
  - LOAD URI_TGT URI_SRC --replace=False > JSON_REPORT (up-/download, copy)
    - if remote-remote: we need temporary local
  - TRANSFORM URI_TGT METHOD --replace=False --name1=URI_1 --name2=URI_1 ... > JSON_REPORT
  - VALIDATE URI_TGT [URI_SCHEMA] > JSON_REPORT
- api actions:
  - load(uri_tgt: uri, uri_src: uri, replace:bool = False) -> json
    - special case of transform
  - transform(uri_tgt: uri, method: callable, uri_src: replace:bool = False, \*\*uri_kwargs) -> json
  - validate(uri_tgt: uri, uri_schema=None) -> json
