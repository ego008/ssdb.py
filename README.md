ssdb.py
=======

Ssdb Python Client Library (threading local), SSDB is a fast nosql database, an alternative to redis (https://github.com/ideawu/ssdb).

Latest version: v0.1.6 ![](https://travis-ci.org/hit9/ssdb.py.svg)

- Support **Python 2.6+ or Python 3.3+**
- Support ssdb version: **SSDB 1.6.8.8+**
- UTF-8 encoding for all conversions between bytes and unicodes.

Changes for 0.1.6:

* Faster protocol parser via [spp_py](https://github.com/hit9/spp_py) built on C extension.
* No more fix types for zset scores, now is str. (Non-Backward Compact)
* Add api `client.close`

Ports
-----

- Node.js ssdb client: https://github.com/eleme/node-ssdb
- Lua ssdb client (on nginx): https://github.com/eleme/lua-resty-ssdb

Installation
------------

```bash
$ pip install ssdb.py
```

API Reference (Redis-py like)
-----------------------------

### Sample Usage

```python
>>> import ssdb
>>> c = ssdb.Client()
>>> c.set('key', 'val')
1
>>> c.get('key')
'val'
```

### Pipeline

```python
>>> with c.pipeline() as pipe:
...   pipe.set('k1', 'v1')
...   pipe.set('k2', 'v2')
...   pipe.multi_get('k1', 'k2')
...   pipe.execute()
...
[1, 1, ['k1', 'v1', 'k2', 'v2']]
```

### Returns

1. If response status is `"ok"`, return the data.
2. If response status is `"not_found"`, return `None`.
3. If response status is `"client_error"` or other(errors), raise `SSDBException`.


### Built-in Exceptions

```python
class SSDBException(Exception):
    pass
```

Types
-----

The data type each command returns can be found in [ssdb.py](ssdb.py) `commands`.

Documentation
--------------

Detail docs for this module can be found at https://github.com/hit9/ssdb.api.docs

License
-------

[LICENSE-BSD2](LICENSE-BSD2)


Update 0.1.7.1
------

remove Pipeline

all commands [ssdb API]http://ssdb.io/docs/zh_cn/php/index.html
