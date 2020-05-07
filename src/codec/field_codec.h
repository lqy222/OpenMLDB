//
// field_codec.h
// Copyright (C) 2020 4paradigm.com
// Author wangbao
// Date 2020-04-07
//

#pragma once

#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <string>
#include "base/endianconv.h"
#include "boost/lexical_cast.hpp"
#include "logging.h"  //NOLINT
#include "proto/type.pb.h"

namespace rtidb {
namespace codec {

using ::baidu::common::WARNING;
using ::rtidb::type::DataType;

/**
 *  encode part
 */
static inline void Convert(bool data, char* buffer) {
    uint8_t bool_true = 1;
    uint8_t bool_false = 0;
    if (data) {
        memcpy(buffer, static_cast<const void*>(&bool_true), 1);
    } else {
        memcpy(buffer, static_cast<const void*>(&bool_false), 1);
    }
}

static inline void Convert(int16_t data, char* buffer) {
    memrev16ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 2);
}

static inline void Convert(int32_t data, char* buffer) {
    memrev32ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 4);
}

static inline void Convert(int64_t data, char* buffer) {
    memrev64ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 8);
}

static inline void Convert(float data, char* buffer) {
    memrev32ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 4);
}

static inline void Convert(double data, char* buffer) {
    memrev64ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 8);
}

static inline bool Convert(const std::string& str, DataType data_type,
                    std::string* out) {
    try {
        switch (data_type) {
            case ::rtidb::type::kBool: {
                out->resize(1);
                char* buffer = const_cast<char*>(out->data());
                std::string tmp = str;
                std::transform(tmp.begin(), tmp.end(), tmp.begin(), ::tolower);
                if (tmp == "true") {
                    Convert(true, buffer);
                } else if (tmp == "false") {
                    Convert(false, buffer);
                } else {
                    PDLOG(WARNING, "input format error, %s.", str.c_str());
                    return false;
                }
                break;
            }
            case ::rtidb::type::kSmallInt: {
                out->resize(2);
                char* buffer = const_cast<char*>(out->data());
                int16_t val = boost::lexical_cast<int16_t>(str);
                Convert(val, buffer);
                break;
            }
            case ::rtidb::type::kInt: {
                out->resize(4);
                char* buffer = const_cast<char*>(out->data());
                int32_t val = boost::lexical_cast<int32_t>(str);
                Convert(val, buffer);
                break;
            }
            case ::rtidb::type::kBigInt: {
                out->resize(8);
                char* buffer = const_cast<char*>(out->data());
                int64_t val = boost::lexical_cast<int64_t>(str);
                Convert(val, buffer);
                break;
            }
            case ::rtidb::type::kFloat: {
                out->resize(4);
                char* buffer = const_cast<char*>(out->data());
                float val = boost::lexical_cast<float>(str);
                Convert(val, buffer);
                break;
            }
            case ::rtidb::type::kDouble: {
                out->resize(8);
                char* buffer = const_cast<char*>(out->data());
                double val = boost::lexical_cast<double>(str);
                Convert(val, buffer);
                break;
            }
            case ::rtidb::type::kVarchar:
            case ::rtidb::type::kString: {
                *out = str;
                break;
            }
            default: {
                PDLOG(WARNING, "unsupported data type %s.",
                      rtidb::type::DataType_Name(data_type).c_str());
                return false;
            }
        }
    } catch (std::exception const& e) {
        PDLOG(WARNING, "input format error, %s.", str.c_str());
        return false;
    }
    return true;
}

/**
 *  decode part
 */
static inline void GetBool(const char* ch, void* res) { memcpy(res, ch, 1); }

static inline void GetInt16(const char* ch, void* res) {
    memcpy(res, ch, 2);
    memrev32ifbe(static_cast<void*>(res));
}

static inline void GetInt32(const char* ch, void* res) {
    memcpy(res, ch, 4);
    memrev32ifbe(static_cast<void*>(res));
}

static inline void GetInt64(const char* ch, void* res) {
    memcpy(res, ch, 8);
    memrev64ifbe(static_cast<void*>(res));
}

static inline void GetFloat(const char* ch, void* res) {
    memcpy(res, ch, 4);
    memrev32ifbe(static_cast<void*>(res));
}

static inline void GetDouble(const char* ch, void* res) {
    memcpy(res, ch, 8);
    memrev64ifbe(static_cast<void*>(res));
}

}  // namespace codec
}  // namespace rtidb