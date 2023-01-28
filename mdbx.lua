--
-- Copyright (C) 2023 Masatoshi Fukunaga
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in
-- all copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
-- THE SOFTWARE.
--
local pcall = pcall
local tostring = tostring
local tonumber = tonumber
local gettime = require('clock').gettime
local format = require('print').format
local libmdbx = require('libmdbx')
local new_cache = require('cache').new
-- constants
local KVPAIRS = 'key_value_pairs'
local EKPAIRS = 'expiry_key_pairs'
local TXN_RDONLY = libmdbx.TXN_RDONLY
local TXN_TRY = libmdbx.TXN_TRY
local EKEYEXIST = libmdbx.errno.KEYEXIST

--- @class cache.mdbx
local Cache = {}

--- init
--- @param ttl integer
--- @param pathname string
--- @return cache.mdbx? c
--- @return any err
function Cache:init(ttl, pathname)
    if pathname == nil then
        pathname = '.'
    elseif type(pathname) ~= 'string' then
        error('pathname must be string')
    end

    local err
    self.env, err = libmdbx.new()
    if err then
        return nil, err
    end

    -- set number of db
    local ok
    ok, err = self.env:set_maxdbs(2)
    if not ok then
        return nil, err
    end

    ok, err = self.env:set_flags(true, libmdbx.NOMETASYNC, libmdbx.NOTLS,
                                 libmdbx.LIFORECLAIM)
    if not ok then
        return nil, err
    end

    ok, err = self.env:open(pathname)
    if not ok then
        return nil, err
    end

    -- create or open tables
    local txn
    txn, err = self.env:begin()
    if err then
        return nil, err
    end
    self.dbi_kvp, err = txn:dbi_open(KVPAIRS, libmdbx.CREATE)
    if err then
        return nil, err
    end
    self.dbi_ekp, err = txn:dbi_open(EKPAIRS, libmdbx.CREATE, libmdbx.DUPSORT)
    if err then
        return nil, err
    end
    ok, err = txn:commit()
    if not ok then
        return nil, err
    end

    return new_cache(self, ttl)
end

--- close
--- @return boolean ok
--- @return any err
function Cache:close()
    return self.env:close()
end

--- begin
--- @param f function
--- @param failval any
--- @return any res
--- @return any err
function Cache:begin(f, failval, ...)
    local txn, err = self.env:begin(...)
    if not txn then
        return failval, format('failed to begin transaction: %s', err)
    end

    local kvp
    kvp, err = self.dbi_kvp:dbh_open(txn)
    if not kvp then
        txn:abort()
        return failval, format('failed to open dbi %q: %s', KVPAIRS, err)
    end

    local ekp
    ekp, err = self.dbi_ekp:dbh_open(txn)
    if not ekp then
        txn:abort()
        return failval, format('failed to open dbi %q: %s', EKPAIRS, err)
    end

    local res
    res, err = f(kvp, ekp, txn)
    if err then
        txn:abort()
        return failval, err
    end

    local ok
    ok, err = txn:commit()
    if not ok then
        return failval, format('failed to commit: %s', err)
    end

    return res
end

--- set
--- @param key string
--- @param val string
--- @param ttl integer
--- @return boolean ok
--- @return any err
function Cache:set(key, val, ttl)
    local newexp = tostring(gettime() + ttl)

    return self:begin(function(kvp, ekp)
        -- insert key-value pair
        local ok, err = kvp:op_upsert(key, val)
        if not ok then
            return false, err
        end

        -- update index
        local oldexp
        oldexp, err = ekp:get(key)
        if err then
            return false, err
        elseif oldexp then
            -- delete old expiry-key pair index
            ok, err = ekp:del(oldexp, key)
            if not ok and err then
                return false, err
            end
        end

        -- insert new expiry-key pair index
        ok, err = ekp:op_upsert(key, newexp)
        if not ok then
            return false, err
        end
        return ekp:put(newexp, key)
    end, false, TXN_TRY)
end

--- delete_key
--- @param key string
--- @param kvp libmdbx.dbh
--- @param ekp libmdbx.dbh
--- @return boolean ok
--- @return any err
local function delete_key(key, kvp, ekp)
    local ok, err = kvp:del(key)
    if not ok then
        return false, err
    end

    -- delete expiry-key pair index
    local exp
    exp, err = ekp:get(key)
    if err then
        return false, err
    elseif exp then
        ok, err = ekp:del(key)
        if not ok and err then
            return false, err
        end
        return ekp:del(exp, key)
    end
    return true
end

--- get
--- @param key string
--- @param ttl? integer
--- @return string? val
--- @return any err
function Cache:get(key, ttl)
    return self:begin(function(kvp, ekp)
        local val, err = kvp:get(key)
        if err then
            return nil, err
        elseif not val then
            -- not found
            return nil
        end

        local exp
        exp, err = ekp:get(key)
        if err then
            return nil, err
        end

        local t = gettime()
        if tonumber(exp) > t then
            if ttl then
                -- delete old expiry-key pair index
                local ok
                ok, err = ekp:del(exp, key)
                if not ok and err then
                    return nil, err
                end

                -- insert new expiry-key pair index
                exp = tostring(t + ttl)
                ok, err = ekp:put(exp, key)
                if not ok then
                    return nil, err
                end
                ok, err = ekp:op_upsert(key, exp)
                if not ok then
                    return nil, err
                end
            end

            return val
        end

        -- delete key-value pair
        local ok
        ok, err = delete_key(key, kvp, ekp)
        if not ok then
            return nil, err
        end
        return nil
    end, nil, TXN_TRY)
end

--- delete
--- @param key string
--- @return boolean ok
--- @return any err
function Cache:delete(key)
    return self:begin(function(kvp, ekp)
        return delete_key(key, kvp, ekp)
    end, true, TXN_TRY)
end

--- rename
--- @param oldkey string
--- @param newkey string
--- @return boolean ok
--- @return any err
function Cache:rename(oldkey, newkey)
    return self:begin(function(kvp, ekp)
        local val, err = kvp:get(oldkey)
        if err then
            return false, err
        elseif not val then
            -- not found
            return false
        end

        local exp
        exp, err = ekp:get(oldkey)
        if err then
            return false, err
        elseif not exp then
            return false, format(
                       'index %q is corrupt: expiration of %q not found',
                       EKPAIRS, oldkey)
        elseif tonumber(exp) <= gettime() then
            -- expired
            local _
            _, err = delete_key(oldkey, kvp, ekp)
            return false, err
        end

        -- insert new key-value pair
        local ok
        ok, err = kvp:op_insert(newkey, val)
        if not ok then
            if err == EKEYEXIST then
                return false
            end
            return false, err
        end
        -- insert new expiry-key pair index
        ok, err = ekp:put(exp, newkey)
        if not ok then
            return false, err
        end
        ok, err = ekp:op_upsert(newkey, exp)
        if not ok then
            return false, err
        end

        -- delete old key-value pair
        return delete_key(oldkey, kvp, ekp)
    end, false, TXN_TRY)
end

--- keys
--- @param callback fun(key:string):(ok:boolean, err:any)
--- @return boolean ok
--- @return any err
function Cache:keys(callback)
    return self:begin(function(kvp, ekp)
        local cur, cerr = kvp:cursor_open()
        if not cur then
            return false, cerr
        end

        local t = gettime()
        local key, _, err = cur:get_first()
        while key do
            local exp
            exp, err = ekp:get(key)
            if err then
                break
            elseif tonumber(exp) > t then
                local ok, res
                ok, res, err = pcall(callback, key)
                if not ok then
                    err = res
                    break
                elseif res ~= true then
                    break
                end
            end
            key, _, err = cur:get_next()
        end

        cur:close()
        if err then
            return false, err
        end
        return true
    end, false, TXN_RDONLY)
end

--- evict
--- @param callback fun(key:string):(ok:boolean, err:any)
--- @param n integer
--- @return integer nevict
--- @return any err
function Cache:evict(callback, n)
    local nevict = 0
    local _, err = self:begin(function(kvp, ekp)
        local cur, err = ekp:cursor_open()
        if not cur then
            return false, err
        end

        local t = gettime()
        local exp, key
        exp, key, err = cur:get_first()
        while exp do
            if tonumber(exp) > t or n >= 0 and nevict > n then
                break
            end

            local ok, res
            ok, res, err = pcall(callback, key)
            if not ok then
                -- abort
                err = res
                break
            elseif res ~= true then
                break
            end

            -- remove expired key
            ok, err = kvp:del(key)
            if not ok and err then
                break
            end
            -- delete expiry-key pair index
            ok, err = cur:del()
            if not ok and err then
                break
            end
            ok, err = ekp:del(key)
            if not ok and err then
                break
            end
            nevict = nevict + 1

            exp, key, err = cur:get_next()
        end

        cur:close()
        if err then
            return false, err
        end

        return true
    end, false, TXN_TRY)
    return nevict, err
end

return {
    new = require('metamodule').new(Cache),
}

