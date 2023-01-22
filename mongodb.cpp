#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/exception/exception.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <lua.hpp>

mongocxx::client conn;

static int l_mongodb_connect(lua_State* L) {
    const char* url = luaL_checkstring(L, 1);
    mongocxx::instance inst{};
    conn = mongocxx::client{mongocxx::uri{url}};
    return 0;
}

static int l_mongodb_find(lua_State* L) {
    const char* db = luaL_checkstring(L, 1);
    const char* coll = luaL_checkstring(L, 2);
    const char* json_query = luaL_checkstring(L, 3);
    auto collection = conn[db][coll];
    bsoncxx::stdx::optional<bsoncxx::document::value> maybe_result =
        collection.find_one(bsoncxx::from_json(json_query));
    if(maybe_result) {
        auto result = *maybe_result;
        std::string json_result = bsoncxx::to_json(result);
        lua_pushstring(L, json_result.c_str());
        return 1;
    }
    return 0;
}

static int l_mongodb_execute_query(lua_State* L) {
    const char* db = luaL_checkstring(L, 1);
    const char* coll = luaL_checkstring(L, 2);
    const char* json_query = luaL_checkstring(L, 3);
    auto collection = conn[db][coll];
    try {
        auto cursor = collection.find(bsoncxx::from_json(json_query));
        lua_newtable(L);
        int index = 1;
        for(auto&& doc : cursor) {
            std::string json_result = bsoncxx::to_json(doc);
            lua_pushinteger(L, index++);
            lua_pushstring(L, json_result.c_str());
            lua_settable(L, -3);
        }
        return 1;
    } catch (const mongocxx::exception& e) {
        lua_pushstring(L, e.what());
        return lua_error(L);
    }
}

static int l_mongodb_allocate_memory(lua_State* L) {
    size_t size = luaL_checkinteger(L, 1);
    void* pointer = malloc(size);
    lua_pushlightuserdata(L, pointer);
    return 1;
}

static int l_mongodb_free_memory(lua_State* L) {
    void* pointer = lua_touserdata(L, 1);
    free(pointer);
    return 0;
}

static const luaL_Reg mongodb[] = {
    {"connect", l_mongodb_connect},
    {"find", l_mongodb_find},
    {"executeQuery", l_mongodb_execute_query},
    {"allocateMemory", l_mongodb_allocate_memory},
    {"freeMemory", l_mongodb_free_memory},
    {NULL, NULL}
};

extern "C" int luaopen_mongodb(lua_State* L) {
    luaL_newlib(L, mongodb);
    return 1;
}
