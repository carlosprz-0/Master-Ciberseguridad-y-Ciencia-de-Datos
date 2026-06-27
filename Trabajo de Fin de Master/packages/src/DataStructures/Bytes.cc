/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date July 18 2024
 * @version v0.1.0
 * @brief This file contains the implementation of the methods of the class Bytes
 */

#include "Bytes.h"
#include <algorithm>
#include <sstream>
#include <iomanip>

/**
 * @brief Construct a new Bytes:: Bytes object
 * 
 * @param kBytes : The bytes to be stored
 */
Bytes::Bytes(const std::vector<int>& kBytes) {
  bytes_.reserve(kBytes.size());
  for (int byte : kBytes) {
    if (byte < 0 || byte > 255) {
      throw std::invalid_argument("Byte value out of range.");
    }
    bytes_.push_back(static_cast<unsigned char>(byte));
  }
}

/**
 * @brief Construct a new Bytes:: Bytes object
 * 
 * @param kBytes : The bytes to be stored
 */
Bytes::Bytes(const std::vector<unsigned char>& kBytes) {
  for (size_t i = 0; i < kBytes.size(); ++i) {
    if (kBytes[i] < 0 || kBytes[i] > 255) {
      throw std::invalid_argument("Byte value out of range.");
    }
  }
  bytes_ = kBytes;
}

/**
 * @brief Apply a bitwise operation to two Bytes objects
 * 
 * @param kBytes Target Bytes object
 * @param kOperation The operation to be applied
 * @return Bytes 
 */
Bytes Bytes::ApplyBitwiseOperation(const Bytes& kBytes, const std::function<unsigned char(unsigned char, unsigned char)>& kOperation) const {
  Bytes result;
  result.bytes_.resize(std::max(bytes_.size(), kBytes.bytes_.size()));
  std::transform(bytes_.begin(), bytes_.end(), kBytes.bytes_.begin(), result.bytes_.begin(), kOperation);
  return result;
}

/**
 * @brief Apply a bitwise operation to a Bytes object
 * 
 * @param kOperation The operation to be applied
 * @return Bytes 
 */
Bytes Bytes::ApplyBitwiseOperation(const std::function<unsigned char(unsigned char)>& kOperation) const {
  Bytes result;
  result.bytes_.resize(bytes_.size());
  std::transform(bytes_.begin(), bytes_.end(), result.bytes_.begin(), kOperation);
  return result;
}

/**
 * @brief Overload the + operator to concatenate two Bytes objects
 * 
 * @param kBytes : The Bytes object to be concatenated
 * @return Bytes : The concatenated Bytes object
 */
Bytes Bytes::operator+(const Bytes& kBytes) const {
  Bytes result = *this;
  result.bytes_.insert(result.bytes_.end(), kBytes.bytes_.begin(), kBytes.bytes_.end());
  return result;
}

/**
 * @brief Overload the << operator to shift the Bytes object to the left
 * 
 * @param kShift : The number of bits to shift
 * @return Bytes : The shifted Bytes object
 */
Bytes Bytes::operator<<(const int& kShift) const {
  std::vector<unsigned char> result = {};
  for (int i = 0; i < bytes_.size(); ++i) {
    result.push_back(bytes_[i] << kShift);
  }
  return Bytes(result);
}

/**
 * @brief Overload the >> operator to shift the Bytes object to the right
 * 
 * @param kShift : The number of bits to shift
 * @return Bytes : The shifted Bytes object
 */
Bytes Bytes::operator>>(const int& kShift) const {
  std::vector<unsigned char> result = {};
  for (int i = 0; i < bytes_.size(); ++i) {
    result.push_back(bytes_[i] >> kShift);
  }
  return Bytes(result);
}

/**
 * @brief Get the Bytes object in Big Endian format
 * 
 * @return std::string : The Bytes object in Big Endian format
 */
Bytes Bytes::toBigEndian() const {
  std::vector<unsigned char> result = {};
  for (int i = 0; i < int(bytes_.size()); ++i) {
    //reverse the string order
    std::string byte = std::bitset<8>(bytes_[i]).to_string();
    std::reverse(byte.begin(), byte.end());
    result.push_back(std::bitset<8>(byte).to_ulong());
  }
  return Bytes(result);
}

/**
 * @brief Convert the Bytes object to a string of bits
 * 
 * @return std::string : The string of bits
 */
std::string Bytes::FromBytesToBits() const {
  std::string result = "";
  for (int i = 0; i < int(bytes_.size()); ++i) {
    result += std::bitset<8>(bytes_[i]).to_string();
  }
  return result;
}

/**
 * @brief Convert a string of bits to a Bytes object
 * 
 * @param kBits : The string of bits
 * @return std::string : The Bytes object
 */
Bytes Bytes::FromBitsToBytes(const std::string& kBits) {
  std::vector<unsigned char> result = {};
  for (int i = 0; i < int(kBits.size()); i += 8) {
    std::string byte = kBits.substr(i, 8);
    // reverse the string order
    std::reverse(byte.begin(), byte.end());
    result.push_back(std::bitset<8>(byte).to_ulong());
  }
  return result;
}

/**
 * @brief Convert the Bytes object to a string of hexadecimal numbers
 * 
 * @return std::string : The string of hexadecimal numbers
 */
std::string Bytes::FromBytesToHex() const {
  std::string result;
  const char* hex_chars = "0123456789abcdef";
  // Have in mind that in the first push we are adding the first 4 bits of the byte
  // and in the second push we are adding the last 4 bits of the byte.
  for (const auto& byte : bytes_) {
    result.push_back(hex_chars[byte >> 4]);
    result.push_back(hex_chars[byte & 0x0F]);
  }
  return result;
}


/**
 * @brief Convert the Bytes object to a string of numbers
 * 
 * @return long : The number
 */
long Bytes::FromBytesToNumbers() const {
  long result = 0;
  for (size_t i = 0; i < bytes_.size(); ++i) {
    result |= static_cast<long>(bytes_[i]) << (i * 8);
  }
  return result;
}

/**
 * @brief Get the Bytes object as a vector of numbers
 * 
 * @return std::vector<int> : The vector of numbers
 */
std::vector<int> Bytes::GetBytesAsNumbersVector() const {
  std::vector<int> result;
  result.reserve(bytes_.size());
  std::transform(bytes_.begin(), bytes_.end(), std::back_inserter(result), [](unsigned char byte) { return static_cast<int>(byte); });
  return result;
}

/**
 * @brief Get the first n bytes of the Bytes object
 * 
 * @param start_index : The index to start the extraction
 * @param kN : The number of bytes to extract
 * @return Bytes : The extracted Bytes object
 */
Bytes Bytes::GetNBytes(const int& start_index, const int& kN) const {
  if (start_index + kN > static_cast<int>(bytes_.size())) {
    throw std::invalid_argument("The number of bytes to extract is bigger than the number of bytes in the Bytes object.");
  }
  auto begin = bytes_.begin() + start_index;
  auto end = begin + kN;
  std::vector<unsigned char> result(begin, end);
  return Bytes(result);
}

/**
 * @brief Set the Bytes object
 * 
 * @param kBytes : The Bytes object to be set
 */
void Bytes::SetBytes(const std::string& kBytes) {
  bytes_ = {};
  for (const auto& element : kBytes) {
    bytes_.push_back(static_cast<int>(static_cast<unsigned char>(element)));
  }
}

/**
 * @brief Change the direction of the Bytes object
 * 
 * @return Bytes : The Bytes object with the direction changed
 */
Bytes Bytes::ChangeByteDirection() const {
  std::vector<unsigned char> result = {};
  for (size_t i = 0; i < bytes_.size(); ++i) {
    std::bitset<8> bits(bytes_[i]);
    std::string bits_string = bits.to_string();
    std::reverse(bits_string.begin(), bits_string.end());
    result.push_back(std::bitset<8>(bits_string).to_ulong());
  }
  return Bytes(result);
}

/**
 * @brief Get the bit at a given index
 * @param kIndex : The index of the bit
 * @return int : The bit at the given index
 */
int Bytes::GetBit(const int& kIndex) const {
  int byte_index = kIndex / 8;
  int bit_position = kIndex % 8;
  return (bytes_[byte_index] >> bit_position) & 1;
}

/**
 * @brief Print the Bytes object
 */
void Bytes::PrintBytes() const {
  std::cout << "[";
  for (const unsigned char& byte : bytes_) {
    std::cout << int(byte) << ", ";
  }
  std::cout << "]" << std::endl;
}
