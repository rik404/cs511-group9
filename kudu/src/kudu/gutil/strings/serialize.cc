// Copyright 2003, Google Inc.  All rights reserved.

#include "kudu/gutil/strings/serialize.h"

#include <cinttypes>
#include <cstdlib>
#include <string>
#include <utility>
#include <unordered_map>
#include <vector>

#include "kudu/gutil/casts.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strtoint.h"

using std::unordered_map;
using std::make_pair;
using std::pair;
using std::string;
using std::vector;

// Convert a uint32 to a 4-byte string.
string Uint32ToKey(uint32 u32) {
  string key;
  KeyFromUint32(u32, &key);
  return key;
}

string Uint64ToKey(uint64 fp) {
  string key;
  KeyFromUint64(fp, &key);
  return key;
}

// Convert a uint128 to a 16-byte string.
string Uint128ToKey(uint128 u128) {
  string key;
  KeyFromUint128(u128, &key);
  return key;
}

// Converts int32 to a 4-byte string key
// NOTE: Lexicographic ordering of the resulting strings does not in
// general correspond to any natural ordering of the corresponding
// integers. For non-negative inputs, lexicographic ordering of the
// resulting strings corresponds to increasing ordering of the
// integers. However, negative inputs are sorted *after* the non-negative
// inputs. To obtain keys such that lexicographic ordering corresponds
// to the natural total order on the integers, use OrderedStringFromInt32()
// or ReverseOrderedStringFromInt32() instead.
void KeyFromInt32(int32 i32, string* key) {
  // TODO(user): Redefine using bit_cast<> and KeyFromUint32()?
  key->resize(sizeof(i32));
  for (int i = sizeof(i32) - 1; i >= 0; --i) {
    (*key)[i] = i32 & 0xff;
    i32  = (i32 >> 8);
  }
}

// Converts a 4-byte string key (typically generated by KeyFromInt32)
// into an int32 value
int32 KeyToInt32(const StringPiece& key) {
  int32 i32 = 0;
  CHECK_EQ(key.size(), sizeof(i32));
  for (size_t i = 0; i < sizeof(i32); ++i) {
    i32 = (i32 << 8) | static_cast<unsigned char>(key[i]);
  }
  return i32;
}

// Converts a double value to an 8-byte string key, so that
// the string keys sort in the same order as the original double values.
void KeyFromDouble(double x, string* key) {
  uint64 n = bit_cast<uint64>(x);
  // IEEE standard 754 floating point representation
  //   [sign-bit] [exponent] [mantissa]
  //
  // Let "a", "b" be two double values, and F(.) be following
  // transformation.  We have:
  //   If 0 < a < b:
  //     0x80000000ULL < uint64(F(a)) < uint64(F(b))
  //   If a == -0.0, b == +0.0:
  //     uint64(F(-0.0)) == uint64(F(+0.0)) = 0x80000000ULL
  //   If a < b < 0:
  //     uint64(F(a)) < uint64(F(b)) < 0x80000000ULL
  const uint64 sign_bit = GG_ULONGLONG(1) << 63;
  if ((n & sign_bit) == 0) {
    n += sign_bit;
  } else {
    n = -n;
  }
  KeyFromUint64(n, key);
}

// Version of KeyFromDouble that returns the key.
string DoubleToKey(double x) {
  string key;
  KeyFromDouble(x, &key);
  return key;
}

// Converts key generated by KeyFromDouble() back to double.
double KeyToDouble(const StringPiece& key) {
  int64 n = KeyToUint64(key);
  if (n & (GG_ULONGLONG(1) << 63))
    n -= (GG_ULONGLONG(1) << 63);
  else
    n = -n;
  return bit_cast<double>(n);
}

// Converts int32 to a 4-byte string key such that lexicographic
// ordering of strings is equivalent to sorting in increasing order by
// integer values. This can be useful when constructing secondary
void OrderedStringFromInt32(int32 i32, string* key) {
  uint32 ui32 = static_cast<uint32>(i32) ^ 0x80000000;
  key->resize(sizeof ui32);
  for ( int i = (sizeof ui32) - 1; i >= 0; --i ) {
    (*key)[i] = ui32 & 0xff;
    ui32  = (ui32 >> 8);
  }
}

string Int32ToOrderedString(int32 i32) {
  string key;
  OrderedStringFromInt32(i32, &key);
  return key;
}

// The inverse of the above function.
int32 OrderedStringToInt32(const StringPiece& key) {
  uint32 ui32 = 0;
  CHECK(key.size() == sizeof ui32);
  for ( int i = 0; i < sizeof ui32; ++i ) {
    ui32 = (ui32 << 8);
    ui32 = ui32 | static_cast<unsigned char>(key[i]);
  }
  return static_cast<int32>(ui32 ^ 0x80000000);
}

// Converts int64 to a 8-byte string key such that lexicographic
// ordering of strings is equivalent to sorting in increasing order by
// integer values.
void OrderedStringFromInt64(int64 i64, string* key) {
  uint64 ui64 = static_cast<uint64>(i64) ^ (GG_ULONGLONG(1) << 63);
  key->resize(sizeof ui64);
  for ( int i = (sizeof ui64) - 1; i >= 0; --i ) {
    (*key)[i] = ui64 & 0xff;
    ui64  = (ui64 >> 8);
  }
}

string Int64ToOrderedString(int64 i64) {
  string key;
  OrderedStringFromInt64(i64, &key);
  return key;
}

// The inverse of the above function.
int64 OrderedStringToInt64(const StringPiece& key) {
  uint64 ui64 = 0;
  CHECK(key.size() == sizeof ui64);
  for ( int i = 0; i < sizeof ui64; ++i ) {
    ui64 = (ui64 << 8);
    ui64 = ui64 | static_cast<unsigned char>(key[i]);
  }
  return static_cast<int64>(ui64 ^ (GG_ULONGLONG(1) << 63));
}

// Converts int32 to a 4-byte string key such that lexicographic
// ordering of strings is equivalent to sorting in decreasing order
// by integer values. This can be useful when constructing secondary
void ReverseOrderedStringFromInt32(int32 i32, string* key) {
  // ~ is like -, but works even for INT_MIN. (-INT_MIN == INT_MIN,
  // but ~x = -x - 1, so ~INT_MIN = -INT_MIN - 1 = INT_MIN - 1 = INT_MAX).
  OrderedStringFromInt32(~i32, key);
}

string Int32ToReverseOrderedString(int32 i32) {
  string key;
  ReverseOrderedStringFromInt32(i32, &key);
  return key;
}

// The inverse of the above function.
int32 ReverseOrderedStringToInt32(const StringPiece& key) {
  return ~OrderedStringToInt32(key);
}

// Converts int64 to an 8-byte string key such that lexicographic
// ordering of strings is equivalent to sorting in decreasing order
// by integer values. This can be useful when constructing secondary
void ReverseOrderedStringFromInt64(int64 i64, string* key) {
  return OrderedStringFromInt64(~i64, key);
}

string Int64ToReverseOrderedString(int64 i64) {
  string key;
  ReverseOrderedStringFromInt64(i64, &key);
  return key;
}

// The inverse of the above function.
int64 ReverseOrderedStringToInt64(const StringPiece& key) {
  return ~OrderedStringToInt64(key);
}

// --------------------------------------------------------------------------
// DictionaryInt32Encode
// DictionaryInt64Encode
// DictionaryDoubleEncode
// DictionaryInt32Decode
// DictionaryInt64Decode
// DictionaryDoubleDecode
//   Routines to serialize/unserialize simple dictionaries
//   (string->T hashmaps). We use ':' to separate keys and values,
//   and commas to separate entries.
// --------------------------------------------------------------------------

string DictionaryInt32Encode(const unordered_map<string, int32>* dictionary) {
  vector<string> entries;
  for (const auto& entry : *dictionary) {
    entries.push_back(StringPrintf("%s:%d", entry.first.c_str(), entry.second));
  }

  string result;
  JoinStrings(entries, ",", &result);
  return result;
}

string DictionaryInt64Encode(const unordered_map<string, int64>* dictionary) {
  vector<string> entries;
  for (const auto& entry : *dictionary) {
    entries.push_back(StringPrintf("%s:%" PRId64,
                                   entry.first.c_str(), entry.second));
  }

  string result;
  JoinStrings(entries, ",", &result);
  return result;
}

string DictionaryDoubleEncode(const unordered_map<string, double>* dictionary) {
  vector<string> entries;
  for (const auto& entry : *dictionary) {
    entries.push_back(StringPrintf("%s:%g", entry.first.c_str(), entry.second));
  }

  string result;
  JoinStrings(entries, ",", &result);
  return result;
}

bool DictionaryParse(const string& encoded_str,
                     vector<pair<string, string> >* items) {
  vector<string> entries;
  SplitStringUsing(encoded_str, ",", &entries);
  for (const auto& entry : entries) {
    vector<string> fields;
    SplitStringAllowEmpty(entry, ":", &fields);
    if (fields.size() != 2)  // parsing error
      return false;
    items->push_back(make_pair(fields[0], fields[1]));
  }
  return true;
}

bool DictionaryInt32Decode(unordered_map<string, int32>* dictionary,
                           const string& encoded_str) {
  vector<pair<string, string> > items;
  if (!DictionaryParse(encoded_str, &items))
    return false;

  dictionary->clear();
  for (const auto& item : items) {
    char *error = nullptr;
    const int32 value = strto32(item.second.c_str(), &error, 0);
    if (error == item.second.c_str() || *error != '\0') {
      // parsing error
      return false;
    }
    (*dictionary)[item.first] = value;
  }
  return true;
}

bool DictionaryInt64Decode(unordered_map<string, int64>* dictionary,
                           const string& encoded_str) {
  vector<pair<string, string> > items;
  if (!DictionaryParse(encoded_str, &items))
    return false;

  dictionary->clear();
  for (const auto& item : items) {
    char *error = nullptr;
    const int64 value = strto64(item.second.c_str(), &error, 0);
    if (error == item.second.c_str() || *error != '\0')  {
      // parsing error
      return false;
    }
    (*dictionary)[item.first] = value;
  }
  return true;
}


bool DictionaryDoubleDecode(unordered_map<string, double>* dictionary,
                            const string& encoded_str) {
  vector<pair<string, string> > items;
  if (!DictionaryParse(encoded_str, &items))
    return false;

  dictionary->clear();
  for (const auto& item : items) {
    char *error = nullptr;
    const double value = strtod(item.second.c_str(), &error);
    if (error == item.second.c_str() || *error != '\0') {
      // parsing error
      return false;
    }
    (*dictionary)[item.first] = value;
  }
  return true;
}
