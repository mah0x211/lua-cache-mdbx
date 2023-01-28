# lua-cache-mdbx

[![test](https://github.com/mah0x211/lua-cache-mdbx/actions/workflows/test.yml/badge.svg)](https://github.com/mah0x211/lua-cache-mdbx/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/mah0x211/lua-cache-mdbx/branch/master/graph/badge.svg)](https://codecov.io/gh/mah0x211/lua-cache-mdbx)

cache storage module with mdbx backend.


## Installation

```sh
luarocks install cache-mdbx
```

---

## Usage

```lua
local sleep = require('nanosleep.sleep')
local cache = require('cache.mdbx')
-- set default ttl 2 seconds
local c = cache.new(2)
local key = 'test'
local val = 'test val'

print(c:set(key, val)) -- true
print(c:get(key)) -- 'test val'
print(c:delete(key)) -- true

print(c:set(key, val)) -- true
-- after 2 seconds
sleep(2)
print(c:get(key)) -- nil
```


## c = cache.new( ttl [, pathname]] )

create an instance of cache.  

**Parameters**

- `ttl:integer|nil`: default expiration seconds.
- `pathname:string`: pathname of database working directory. (default `'.'`)

**Returns**

- `c:cache`: instance of `cache`.


## ok, err, timeout = cache:set( key, val [, ttl] )

set a key-value pair.  

**Parameters**

- `key:string`: a string that matched to the pattern `^[a-zA-Z0-9_%-]+$'`.
- `val:string`: any value except `nil`.
- `ttl:integer`: expiration seconds greater or equal to `0`. (optional)

**Returns**

- `ok:boolean`: `true` on success, or `false` on failure.
- `err:any`: error message.
- `timeout:boolean`: `true` if operation has timed out.


## val, err, timeout = cache:get( key [, ttl] )

get a value associated with a `key` and update an expiration seconds if `ttl` is specified.

**Parameters**

- `key:string`: a string that matched to the pattern `^[a-zA-Z0-9_%-]+$'`.
- `ttl:integer`: update an expiration seconds. (optional)  
    **NOTE:** If the `ttl` argument is specified, the `GETEX` command is invoked.

**Returns**

- `val:any`: a value.
- `err:any`: error message.
- `timeout:boolean`: `true` if operation has timed out.


## ok, err, timeout = cache:delete( key )

delete a value associated with a `key`.  

**Parameters**

- `key:string`: a string that matched to the pattern `^[a-zA-Z0-9_%-]+$'`.

**Returns**

- `ok:boolean`: `true` on success, or `false` on failure.
- `err:any`: error message.
- `timeout:boolean`: `true` if operation has timed out.


## ok, err, timeout = cache:rename( oldkey, newkey )

rename the `oldkey` name to `newkey`.  

**Parameters**

- `oldkey:string`: a string that matched to the pattern `^[a-zA-Z0-9_%-]+$'`.
- `new key:string`: a string that matched to the pattern `^[a-zA-Z0-9_%-]+$'`.

**Returns**

- `ok:boolean`: `true` on success, or `false` on failure.
- `err:any`: error message.
- `timeout:boolean`: `true` if operation has timed out.


## ok, err, timeout = cache:keys( callback )

execute a provided function once for each key. it is aborted if it returns `false` or an error.

**Parameters**

- `callback:function`: a function that called with each key.
    ```
    ok, err = callback(key)
    - ok:boolean: true on continue.
    - err:any: an error message.
    - key:string: cached key string.
    ```

**Returns**

- `ok:boolean`: `true` on success, or `false` on failure.
- `err:any`: error message.
- `timeout:boolean`: `true` if operation has timed out.


## n = cache:evict( callback [, n] )

this method does nothing and always returns 0.

**Parameters**

- `callback:function`: a function that called with each expired key.
    ```
    ok, err = callback(key)
    - ok:boolean: true on continue.
    - err:any: an error message.
    - key:string: cached key string.
    ```
- `n:integer`: maximum number of keys to be evicted.

**Returns**

- `n:integer`: number of keys evicted.

