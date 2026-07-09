/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date July 18 2024
 * @version v0.1.0
 * @brief This file contains the header implementation of the class Keccak
 */

#include "Keccak.h"

/**
 * @brief This function is used to hash the input data using the SHAKE128 algorithm
 * 
 * @param bytes32 : The first input data
 * @param a : The second input data
 * @param b : The third input data
 * @param length : The length of the output data
 * @return Bytes Hashed data
 */
Bytes Keccak::XOF(const Bytes& bytes32, const Bytes& a, const Bytes& b, int length) {
  Bytes input_bytes = bytes32 + a + b;
  return _shake128(input_bytes, length);
}

/**
 * @brief This function is used to hash the input data using the SHAKE256 algorithm
 * 
 * @param input_bytes : The input data
 * @return Bytes Hashed data
 */
Bytes Keccak::H(const Bytes& input_bytes, int length) {
  return _shake256(input_bytes, length);
}

/**
 * @brief This function is used to hash the input data using the SHAKE256 algorithm
 * 
 * @param sigma : The input data
 * @param byte_to_hash : The length of the output data
 * @param length : The length of the output data
 * @return Bytes 
 */
Bytes Keccak::PRF(const Bytes& sigma, int byte_to_hash, int length) {
  Bytes input_bytes = sigma + Bytes(byte_to_hash);
  return _shake256(input_bytes, length);
}

/**
 * @brief This function is used to hash the input data using the SHAKE256 algorithm
 * 
 * @param input_bytes : The input data
 * @return Bytes 
 */
Bytes Keccak::KDF(const Bytes& input_bytes, int length) {
  return _shake256(input_bytes, length);
}

/**
 * @brief This function is used to hash the input data using the SHAKE256 algorithm
 * 
 * @param input_bytes : The input data
 * @return std::tuple<Bytes, Bytes> 
 */
std::pair<Bytes, Bytes> Keccak::G(const Bytes& input_bytes) {
  Bytes result_hash = _sha3_512(input_bytes);
  Bytes output1 = result_hash.GetNBytes(0, 32);
  Bytes output2 = result_hash.GetNBytes(32, 32);
  return std::pair<Bytes, Bytes>{output1, output2};
}

/**
 * @brief This function is used to hash the input data using the SHAKE128 algorithm
 * 
 * @param input_data : The input data
 * @param output_length : The length of the output data
 * @return Bytes 
 */
Bytes Keccak::_shake128(const Bytes& input_data, int output_length) {
  CryptoPP::SHAKE128 shake;
  // Get the bytes from the input data
  const std::vector<unsigned char>& input = input_data.GetBytes();
  // Create the buffer to store the output & update the hash with the input data
  std::vector<CryptoPP::byte> output(output_length);
  shake.Update(reinterpret_cast<const CryptoPP::byte*>(input.data()), input_data.GetBytesSize());  
  // Get the output from the hash
  shake.TruncatedFinal(output.data(), output_length);

  std::string result(output.begin(), output.end());
  return Bytes(result);
}

/**
 * @brief This function is used to hash the input data using the SHAKE256 algorithm
 * 
 * @param input_data : The input data
 * @param output_length : The length of the output data
 * @return Bytes 
 */
Bytes Keccak::_shake256(const Bytes& input_data, int output_length) {
  CryptoPP::SHAKE256 shake;
  // Get the bytes from the input data
  const std::vector<unsigned char>& input = input_data.GetBytes();
  // Create the buffer to store the output & update the hash with the input data
  std::vector<CryptoPP::byte> output(output_length);
  shake.Update(reinterpret_cast<const CryptoPP::byte*>(input.data()), input_data.GetBytesSize());  
  // Get the output from the hash
  shake.TruncatedFinal(output.data(), output_length);

  std::string result(output.begin(), output.end());
  return Bytes(result);
}

/**
 * @brief This function is used to hash the input data using the SHA3-512 algorithm
 * 
 * @param input_data : The input data
 * @return Bytes 
 */
Bytes Keccak::_sha3_512(const Bytes& input_data) {
  CryptoPP::SHA3_512 sha3;
  // Get the bytes from the input data
  const std::vector<unsigned char>& input = input_data.GetBytes();
  // Create the buffer to store the output & update the hash with the input data
  std::vector<CryptoPP::byte> output(CryptoPP::SHA3_512::DIGESTSIZE);
  sha3.Update(reinterpret_cast<const CryptoPP::byte*>(input.data()), input_data.GetBytesSize());
  // Get the output from the hash
  sha3.Final(output.data());
  // Convert the output to a string and return it as a Bytes object
  std::string result(output.begin(), output.end());
  return Bytes(result);
}