/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date August 14 2024
 * @version v0.1.0
 * @brief This file contains the definition of methods of the class EncDecUnit, that
 * will encode and decode the messages in the lattice-based cryptosystem.
 */

#include "EncDecUnit.h"

/**
 * @brief This method encodes a matrix of polynomials into a bytes sequence. This method is used in serialization process.
 * 
 * @param input_matrix : The matrix that is going to be encoded
 * @param bits_per_coefficient : The bits per coefficient that is going to be used for representation
 * @return Bytes 
 */
Bytes EncDecUnit::EncodeMatrixToBytes(const Matrix<Polynomial<int>>& input_matrix, const int bits_per_coefficient) const {
  Bytes result;
  const int krows = input_matrix.GetRowsSize();
  const int kcols = input_matrix.GetColumnsSize();
  result.Reserve(krows * kcols * n_ * bits_per_coefficient / BYTE_SIZE);
  for (int i = 0; i < krows; i++) {
    for (int j = 0; j < kcols; j++) {
      result += Encode_(input_matrix(i, j), bits_per_coefficient);
    }
  }
  return result;
}


/**
 * @brief This method encodes a polynomial into a bytes sequence. This method is used in serialization process.
 * 
 * @param polynomial : The polynomial that is going to be encoded
 * @param bits_per_coefficient : The bits per coefficient that is going to be used for representation
 * @return Bytes 
 */
Bytes EncDecUnit::Encode_(const Polynomial<int>& polynomial, int bits_per_coefficient) const {
  if (bits_per_coefficient < 1) {
    // Iterate through the vector and see the number of bits needed to represent the biggest coefficient
    bits_per_coefficient = std::ceil(std::log2(*std::max_element(polynomial.GetCoefficients().begin(), polynomial.GetCoefficients().end()) + 1));
  }
  std::string result_string = "";
  for (const auto& element : polynomial.GetCoefficients()) {
    std::string binary_string = std::bitset<32>(element).to_string();
    // removing ceros until the number of bits is the same as bits_per_coefficient
    binary_string = binary_string.substr(32 - bits_per_coefficient);
    // reverse the string order
    std::reverse(binary_string.begin(), binary_string.end());
    result_string += binary_string;
  }
  while (result_string.length() % BYTE_SIZE != 0) { result_string += "0"; }
  Bytes result = Bytes::FromBitsToBytes(result_string);
  return result;
}


/**
 * @brief This method encodes a matrix of polynomials into a bytes sequence. This method is used in serialization process.
 * 
 * @param input_bytes The bytes sequence that is going to be decoded
 * @param rows The number of rows of the matrix
 * @param cols The number of columns of the matrix
 * @param length The length of the bytes sequence
 * @return Matrix<Polynomial<int>> 
 */
Matrix<Polynomial<int>> EncDecUnit::DecodeBytesToMatrix(const Bytes& input_bytes, const int rows, const int cols, int length) const {
  if (length < 1) {
    const int denominator = n_ * rows * cols;
    const int total_bits = BYTE_SIZE * input_bytes.GetBytesSize();
    length = total_bits / denominator;
    // if (total_bits % denominator != 0 || n_ * length * rows * cols > total_bits) {
    //   throw std::invalid_argument("The input byte list must have a valid length.");
    // }
  }

  const int chunk_length = CHUNK_LENGTH * length;
  const int total_chunks = input_bytes.GetBytesSize() / chunk_length;

  bool is_last_chunk = input_bytes.GetBytesSize() % chunk_length != 0;
  std::vector<Bytes> coefficients(total_chunks + (is_last_chunk ? 1 : 0));

  for (int i = 0; i < total_chunks; ++i) {
    coefficients[i] = input_bytes.GetNBytes(i * chunk_length, chunk_length);
  }
  
  if (is_last_chunk) {
    coefficients.back() = input_bytes.GetNBytes(total_chunks * chunk_length, input_bytes.GetBytesSize() % chunk_length);
  }

  Matrix<Polynomial<int>> result(rows, cols, n_);
  for (int i = 0; i < rows; ++i) {
    for (int j = 0; j < cols; ++j) {
      result(i, j) = Decode_(coefficients[i * cols + j], length);
    }
  }

  return result;
}


/**
 * @brief This method decodes a bytes sequence into a polynomial. This method is used in deserialization process.
 * 
 * @param input_bytes : The bytes sequence that is going to be decoded
 * @param bits_per_coefficient : The bits per coefficient that is going to be used for representation
 * @return Polynomial<int> 
 */
Polynomial<int> EncDecUnit::Decode_(const Bytes& input_bytes, int bits_per_coefficient) const {
  if (bits_per_coefficient < 1) {
    bits_per_coefficient = (BYTE_SIZE * input_bytes.GetBytesSize()) / n_;
    // if ((BYTE_SIZE * input_bytes.GetBytesSize()) % n_ != 0) {
    //   throw std::invalid_argument("The input byte list must have a length multiple of n_ * bits_per_coefficient.");
    // }
  }
  const int expected_size = n_ * bits_per_coefficient;
  // if (expected_size != BYTE_SIZE * input_bytes.GetBytesSize()) {
  //   throw std::invalid_argument("The input byte list size does not match the expected size.");
  // }
  Polynomial<int> coefficients(n_);
  const std::string binary_string = input_bytes.toBigEndian().FromBytesToBits();

  for (int i = 0; i < n_; ++i) {
    int value = 0;
    for (int j = 0; j < bits_per_coefficient; ++j) {
      value |= (binary_string[i * bits_per_coefficient + j] - '0') << j;
    }
    coefficients[i] = value;
  }

  return coefficients;
}

