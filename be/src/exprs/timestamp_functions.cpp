// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exprs/timestamp_functions.h"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/date_time/local_time/local_time.hpp>

#include "exprs/expr.h"
#include "exprs/anyval_util.h"
#include "runtime/tuple_row.h"
#include "runtime/datetime_value.h"
#include "runtime/timestamp_value.h"
#include "runtime/runtime_state.h"
#include "util/path_builder.h"
#include "runtime/string_value.hpp"
#include "util/debug_util.h"

namespace doris {

void TimestampFunctions::init() {
}

// TODO: accept Java data/time format strings:
// http://docs.oracle.com/javase/1.4.2/docs/api/java/text/SimpleDateFormat.html
// Convert them to boost format strings.
bool TimestampFunctions::check_format(const StringVal& format, DateTimeValue& t) {
    // For now the format  must be of the form: yyyy-MM-dd HH:mm:ss
    // where the time part is optional.
    switch (format.len) {
    case 10:
        if (strncmp((const char*)format.ptr, "yyyy-MM-dd", 10) == 0) {
            t.set_type(TIME_DATE);
            return true;
        }

        break;

    case 19:
        if (strncmp((const char*)format.ptr, "yyyy-MM-dd HH:mm:ss", 19) == 0) {
            t.set_type(TIME_DATETIME);
            return true;
        }
        break;

    default:
        break;
    }

    report_bad_format(&format);
    return false;
}

void TimestampFunctions::report_bad_format(const StringVal* format) {
    std::string format_str((char *)format->ptr, format->len);
    // LOG(WARNING) << "Bad date/time conversion format: " << format_str
    //             << " Format must be: 'yyyy-MM-dd[ HH:mm:ss]'";
}

IntVal TimestampFunctions::year(
        FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.year());
}

IntVal TimestampFunctions::quarter(
        FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal((ts_value.month() - 1) / 3 + 1);
}

IntVal TimestampFunctions::month(
        FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.month());
}
IntVal TimestampFunctions::day_of_week(
        FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal((ts_value.weekday() + 1 ) % 7 + 1);
}

IntVal TimestampFunctions::day_of_month(
        FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.day());
}

IntVal TimestampFunctions::day_of_year(
        FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.day_of_year());
}

IntVal TimestampFunctions::week_of_year(
        FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.week(mysql_week_mode(3)));
}

IntVal TimestampFunctions::hour(
        FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.hour());
}

IntVal TimestampFunctions::minute(
        FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.minute());
}

IntVal TimestampFunctions::second(
        FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.second());
}

DateTimeVal TimestampFunctions::to_date(
        FunctionContext* ctx, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return DateTimeVal::null();
    }
    DateTimeValue ts_value = DateTimeValue::from_datetime_val(ts_val);
    ts_value.cast_to_date();
    DateTimeVal result;
    ts_value.to_datetime_val(&result);
    return result;
}

DateTimeVal TimestampFunctions::str_to_date(
        FunctionContext* ctx, const StringVal& str, const StringVal& format) {
    if (str.is_null || format.is_null) {
        return DateTimeVal::null();
    }
    DateTimeValue ts_value;
    if (!ts_value.from_date_format_str((const char*)format.ptr, format.len,
                                       (const char*)str.ptr, str.len)) {
        return DateTimeVal::null();
    }
    DateTimeVal ts_val;
    ts_value.to_datetime_val(&ts_val);
    return ts_val;
}

StringVal TimestampFunctions::month_name(
        FunctionContext* ctx, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return StringVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    const char* name = ts_value.month_name();
    if (name == NULL) {
        return StringVal::null();
    }
    return AnyValUtil::from_string_temp(ctx, name);
}

StringVal TimestampFunctions::day_name(
        FunctionContext* ctx, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return StringVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    const char* name = ts_value.day_name();
    if (name == NULL) {
        return StringVal::null();
    }
    return AnyValUtil::from_string_temp(ctx, name);
}

DateTimeVal TimestampFunctions::years_add(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<YEAR>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::years_sub(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<YEAR>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::months_add(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<MONTH>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::months_sub(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<MONTH>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::weeks_add(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<WEEK>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::weeks_sub(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<WEEK>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::days_add(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<DAY>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::days_sub(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<DAY>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::hours_add(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<HOUR>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::hours_sub(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<HOUR>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::minutes_add(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<MINUTE>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::minutes_sub(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<MINUTE>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::seconds_add(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<SECOND>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::seconds_sub(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<SECOND>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::micros_add(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<MICROSECOND>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::micros_sub(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count) {
    return timestamp_time_op<MICROSECOND>(ctx, ts_val, count, false);
}

template <TimeUnit unit>
DateTimeVal TimestampFunctions::timestamp_time_op(
        FunctionContext* ctx, const DateTimeVal& ts_val,
        const IntVal& count, bool is_add) {
    if (ts_val.is_null || count.is_null) {
        return DateTimeVal::null();
    }
    TimeInterval interval(unit, count.val, !is_add);

    DateTimeValue ts_value = DateTimeValue::from_datetime_val(ts_val);
    if (!ts_value.date_add_interval(interval, unit)) {
        return DateTimeVal::null();
    }
    DateTimeVal new_ts_val;
    ts_value.to_datetime_val(&new_ts_val);

    return new_ts_val;
}

StringVal TimestampFunctions::date_format(
        FunctionContext* ctx, const DateTimeVal& ts_val, const StringVal& format) {
    if (ts_val.is_null || format.is_null) {
        return StringVal::null();
    }
    DateTimeValue ts_value = DateTimeValue::from_datetime_val(ts_val);
    if (ts_value.compute_format_len((const char*)format.ptr, format.len) >= 128) {
        return StringVal::null();
    }
    char buf[128];
    if (!ts_value.to_format_string((const char*)format.ptr, format.len, buf)) {
        return StringVal::null();
    }
    return AnyValUtil::from_string_temp(ctx, buf);
}

DateTimeVal TimestampFunctions::from_days(
        FunctionContext* ctx, const IntVal& days) {
    if (days.is_null) {
        return DateTimeVal::null();
    }
    DateTimeValue ts_value;
    if (!ts_value.from_date_daynr(days.val)) {
        return DateTimeVal::null();
    }
    DateTimeVal ts_val;
    ts_value.to_datetime_val(&ts_val);
    return ts_val;
}

IntVal TimestampFunctions::to_days(
        FunctionContext* ctx, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.daynr());
}

DoubleVal TimestampFunctions::time_diff(
        FunctionContext* ctx, const DateTimeVal& ts_val1, const DateTimeVal& ts_val2) {
    if (ts_val1.is_null || ts_val2.is_null) {
        return DoubleVal::null();
    }
    const DateTimeValue& ts_value1 = DateTimeValue::from_datetime_val(ts_val1);
    const DateTimeValue& ts_value2 = DateTimeValue::from_datetime_val(ts_val2);
    int64_t timediff = ts_value1.unix_timestamp() - ts_value2.unix_timestamp();

    return DoubleVal(timediff);
}

IntVal TimestampFunctions::date_diff(
        FunctionContext* ctx, const DateTimeVal& ts_val1, const DateTimeVal& ts_val2) {
    if (ts_val1.is_null || ts_val2.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value1 = DateTimeValue::from_datetime_val(ts_val1);
    const DateTimeValue& ts_value2 = DateTimeValue::from_datetime_val(ts_val2);
    return IntVal(ts_value1.daynr() - ts_value2.daynr());
}


// TimeZone relative function
DateTimeVal TimestampFunctions::timestamp(
        FunctionContext* ctx, const DateTimeVal& val) {
    return val;
}

void* TimestampFunctions::from_utc(Expr* e, TupleRow* row) {
    return NULL;
    // DCHECK_EQ(e->get_num_children(), 2);
    // Expr* op1 = e->children()[0];
    // Expr* op2 = e->children()[1];
    // DateTimeValue* tv = reinterpret_cast<DateTimeValue*>(op1->get_value(row));
    // StringValue* tz = reinterpret_cast<StringValue*>(op2->get_value(row));

    // if (tv == NULL || tz == NULL) {
    //     return NULL;
    // }

    // if (tv->not_a_date_time()) {
    //     return NULL;
    // }

     //boost::local_time::time_zone_ptr timezone = TimezoneDatabase::find_timezone(tz->debug_string());

    // This should raise some sort of error or at least null. Hive just ignores it.
    // if (timezone == NULL) {
    //     LOG(ERROR) << "Unknown timezone '" << *tz << "'" << std::endl;
    //     e->_result.timestamp_val = *tv;
    //     return &e->_result.timestamp_val;
    // }

    // boost::posix_time::ptime temp;
    // tv->to_ptime(&temp);
    // boost::local_time::local_date_time lt(temp, timezone);
    // e->_result.timestamp_val = lt.local_time();
    // return &e->_result.timestamp_val;
}

void* TimestampFunctions::to_utc(Expr* e, TupleRow* row) {
    return NULL;
    // DCHECK_EQ(e->get_num_children(), 2);
    // Expr* op1 = e->children()[0];
    // Expr* op2 = e->children()[1];
    // DateTimeValue* tv = reinterpret_cast<DateTimeValue*>(op1->get_value(row));
    // StringValue* tz = reinterpret_cast<StringValue*>(op2->get_value(row));

    // if (tv == NULL || tz == NULL) {
    //     return NULL;
    // }

    // if (tv->not_a_date_time()) {
    //     return NULL;
    // }

    // boost::local_time::time_zone_ptr timezone = TimezoneDatabase::find_timezone(tz->debug_string());

    // This should raise some sort of error or at least null. Hive just ignores it.
    // if (timezone == NULL) {
    //     LOG(ERROR) << "Unknown timezone '" << *tz << "'" << std::endl;
    //     e->_result.timestamp_val = *tv;
    //     return &e->_result.timestamp_val;
    // }

    // boost::local_time::local_date_time lt(tv->date(), tv->time_of_day(),
    //                    timezone, boost::local_time::local_date_time::NOT_DATE_TIME_ON_ERROR);
    // e->_result.timestamp_val = DateTimeValue(lt.utc_time());
    // return &e->_result.timestamp_val;
}

StringVal TimestampFunctions::from_unix(
        FunctionContext* context, const IntVal& unix_time) {
    if (unix_time.is_null) {
        return StringVal::null();
    }
    TimestampValue timestamp(unix_time.val);
    return AnyValUtil::from_string_temp(context,
            timestamp.to_datetime_string(context->impl()->state()->timezone()));
}

StringVal TimestampFunctions::from_unix(
            FunctionContext* context, const IntVal& unix_time, const StringVal& fmt) {
    if (unix_time.is_null || fmt.is_null) {
        return StringVal::null();
    }

    TimestampValue timestamp(unix_time.val);
    DateTimeValue t;
    timestamp.to_datetime_value(t, context->impl()->state()->timezone());
    if (!check_format(fmt, t)) {
        return StringVal::null();
    }

    char buf[64];
    t.to_string(buf);
    return AnyValUtil::from_string_temp(context, buf);
}

IntVal TimestampFunctions::to_unix(FunctionContext* context) {
    return IntVal(context->impl()->state()->timestamp() / 1000);
}

IntVal TimestampFunctions::to_unix(
            FunctionContext* context, const StringVal& string_val, const StringVal& fmt) {
    if (string_val.is_null || fmt.is_null) {
        return IntVal::null();
    }
    DateTimeValue tv;
    if (!tv.from_date_format_str(
            (const char *) fmt.ptr, fmt.len, (const char *) string_val.ptr, string_val.len)) {
        return IntVal::null();
    }

    TimestampValue timestamp(tv, context->impl()->state()->timezone());
    return timestamp.val;
}

IntVal TimestampFunctions::to_unix(
            FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue &tv = DateTimeValue::from_datetime_val(ts_val);

    TimestampValue ts(tv, context->impl()->state()->timezone());
    return ts.val;
}

DateTimeVal TimestampFunctions::utc_timestamp(FunctionContext* context) {
    TimestampValue timestamp(context->impl()->state()->timestamp());
    DateTimeVal return_val;
    timestamp.to_datetime_val(&return_val);
    return return_val;
}

DateTimeVal TimestampFunctions::now(FunctionContext* context) {
    TimestampValue ts(context->impl()->state()->timestamp());
    DateTimeVal return_val;
    ts.to_datetime_val(&return_val, context->impl()->state()->timezone());
    return return_val;
}

DoubleVal TimestampFunctions::curtime(FunctionContext* context) {
    TimestampValue ts(context->impl()->state()->timestamp());
    DoubleVal return_val;
    ts.to_time_val(&return_val, context->impl()->state()->timezone());
    return return_val;
}

DateTimeVal TimestampFunctions::convert_tz(FunctionContext* ctx, const DateTimeVal& ts_val,
                                               const StringVal& from_tz, const StringVal& to_tz) {
    const DateTimeValue &ts_value = DateTimeValue::from_datetime_val(ts_val);
    TimestampValue ts(ts_value, std::string((char *) from_tz.ptr, from_tz.len));
    DateTimeVal return_val;
    ts.to_datetime_val(&return_val, std::string((char *) to_tz.ptr, to_tz.len));
    return return_val;
}

}
