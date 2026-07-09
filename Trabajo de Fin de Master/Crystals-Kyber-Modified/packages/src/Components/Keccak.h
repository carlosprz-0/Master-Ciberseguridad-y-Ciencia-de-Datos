/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date July 18 2024
 * @version v0.1.0
 * @brief This file contains the header declaration of the class Keccak, which give us 
 * some cryptographic functionalities.
 */

#pragma once

#include <string>
#include <tuple>
#include <cryptopp/hex.h>
#include <cryptopp/sha3.h>
#include <cryptopp/shake.h>
#include <cryptopp/filters.h>

#include "../DataStructures/Bytes.h"

class Keccak {
 public:
  static Bytes H(const Bytes& input_bytes, int length);
  static Bytes XOF(const Bytes& bytes32, const Bytes& a, const Bytes& b, int length);
  static Bytes PRF(const Bytes& sigma, int byte_to_hash, int length);
  static Bytes KDF(const Bytes& input_bytes, int length);
  static std::pair<Bytes, Bytes> G(const Bytes& input_bytes);

 private:
  static Bytes _shake128(const Bytes& input_data, int output_length);
  static Bytes _shake256(const Bytes& input_data, int output_length);
  static Bytes _sha3_512(const Bytes& input_data);

};