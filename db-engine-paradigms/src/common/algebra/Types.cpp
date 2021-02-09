#include "common/algebra/Types.hpp"
#include "common/runtime/Types.hpp"

namespace algebra {

Integer::operator std::string() const { return "Integer"; }
size_t Integer::rt_size() const { return sizeof(types::Integer); };
const std::string& Integer::cppname() const {
   static std::string cppname = "Integer";
   return cppname;
}

const std::string& Integer::to_voila() const {
   static std::string cppname = "i32";
   return cppname;
}


BigInt::operator std::string() const { return "BigInt"; }
size_t BigInt::rt_size() const { return sizeof(int64_t); };
const std::string& BigInt::cppname() const {
   static std::string cppname = "BigInt";
   return cppname;
}

const std::string& BigInt::to_voila() const {
   static std::string cppname = "i64";
   return cppname;
}


Date::operator std::string() const { return "Integer"; }
size_t Date::rt_size() const { return sizeof(types::Date); };
const std::string& Date::cppname() const {
   static std::string cppname = "Date";
   return cppname;
}

const std::string& Date::to_voila() const {
   static std::string cppname = "i32";
   return cppname;
}


Numeric::Numeric(uint32_t s, uint32_t p) : size(s), precision(p) {}
Numeric::operator std::string() const { return "Integer"; }
size_t Numeric::rt_size() const {
   /* currently, all numerics are of same size */
   return sizeof(types::Numeric<40, 2>);
};

const std::string& Numeric::cppname() const {
   static std::string cppname;
   cppname = "Numeric<" + std::to_string(size) + "," +
             std::to_string(precision) + ">";
   return cppname;
}

const std::string& Numeric::to_voila() const {
   static std::string cppname = "i64";
   return cppname;
}

Char::Char(uint32_t s) : size(s) {}
Char::operator std::string() const { return "Char"; }
size_t Char::rt_size() const { return size == 1 ? 1 : size + 1; };
const std::string& Char::cppname() const {
   static std::string cppname;
   cppname = "Char<" + std::to_string(size) + ">";
   return cppname;
}

const std::string& Char::to_voila() const {
   static std::string cppname1 = "u8";
   static std::string cppnamen = "varchar";
   return size == 1 ? cppname1 : cppnamen;
}


Varchar::Varchar(uint32_t s) : size(s) {}
Varchar::operator std::string() const { return "Varchar"; }
size_t Varchar::rt_size() const { return size + 1; };
const std::string& Varchar::cppname() const {
   static std::string cppname;
   cppname = cppname = "Varchar<" + std::to_string(size) + ">";
   return cppname;
}

const std::string& Varchar::to_voila() const {
   static std::string cppname = "varchar";
   return cppname;
}

}
