/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date July 18 2024
 * @version v0.1.0
 * @brief This file contains the header declaration of the class Bytes
 */
#include <iostream>
#include <functional>
#include <cstdint>
#include <bitset>
#include <cmath>
#include <string>
#include <vector>

#pragma once

class Bytes {
 public:
  Bytes() = default;
  Bytes(const std::string& kBytes) : bytes_(kBytes.begin(), kBytes.end()) {};
  Bytes(const Bytes& kBytes) = default;
  Bytes(const uint8_t& kByte) : bytes_({kByte}) {};
  Bytes(const std::vector<int>& kBytes);
  Bytes(const std::vector<unsigned char>& kBytes);
  ~Bytes() = default;
  void Reserve(const int& kSize) { bytes_.reserve(kSize); }

  // Getters
  int GetBytesSize() const { return bytes_.size(); }
  std::vector<unsigned char> GetBytes() const { return bytes_; }
  int GetBit(const int& kIndex) const;

  // Setters
  void SetBytes(const std::string& kBytes);

  // Operator overload

  // Selector
  unsigned char& operator[](int index) { return bytes_[index]; }
  const unsigned char& operator[](int index) const { return bytes_[index]; }
  // Unary
  Bytes operator~() const { return ApplyBitwiseOperation(std::bit_not<unsigned char>()); }
  Bytes operator<<(const int& kShift) const;
  Bytes operator>>(const int& kShift) const;
  Bytes operator<<=(const int& kShift) { return *this = *this << kShift; }
  Bytes operator>>=(const int& kShift) { return *this = *this >> kShift; }

  // Binary
  Bytes operator+(const Bytes& kBytes) const;
  Bytes operator^(const Bytes& kBytes) const { return ApplyBitwiseOperation(kBytes, std::bit_xor<unsigned char>()); }
  Bytes operator|(const Bytes& kBytes) const { return ApplyBitwiseOperation(kBytes, std::bit_or<unsigned char>()); }
  Bytes operator&(const Bytes& kBytes) const { return ApplyBitwiseOperation(kBytes, std::bit_and<unsigned char>()); }
  Bytes operator+=(const Bytes& kBytes) { return *this = *this + kBytes; };
  Bytes operator^=(const Bytes& kBytes) { return *this = *this ^ kBytes; }
  Bytes operator|=(const Bytes& kBytes) { return *this = *this | kBytes; };
  Bytes operator&=(const Bytes& kBytes) { return *this = *this & kBytes; };
  bool operator==(const Bytes& kBytes) const { return bytes_ == kBytes.bytes_; }
  bool operator!=(const Bytes& kBytes) const { return bytes_ != kBytes.bytes_; }

  // Methods
  Bytes toBigEndian() const;
  std::string FromBytesToBits() const;
  std::string FromBytesToHex() const;
  long FromBytesToNumbers() const;
  Bytes GetNBytes(const int& start_index, const int& kN) const;
  std::vector<int> GetBytesAsNumbersVector() const;
  Bytes ChangeByteDirection() const;
  std::string FromBytesToAscii() const { return std::string(bytes_.begin(), bytes_.end()); }

  // Print method
  void PrintBytes() const;
  
  static Bytes FromBitsToBytes(const std::string& kBits);
  
 private:
  std::vector<unsigned char> bytes_ = {};
  Bytes ApplyBitwiseOperation(const Bytes& kBytes, const std::function<unsigned char(unsigned char, unsigned char)>& kOperation) const;
  Bytes ApplyBitwiseOperation(const std::function<unsigned char(unsigned char)>& kOperation) const;
};