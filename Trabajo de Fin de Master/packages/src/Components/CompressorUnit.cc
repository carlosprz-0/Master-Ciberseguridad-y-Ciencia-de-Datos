/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date August 20 2024
 * @version v0.1.0
 * @brief This file contains the definition of methods of the class CompressorUnit, that
 * will be used to compress and decompress the messages in the lattice-based cryptosystem.
 *
 *  > The main reason for defining the Compress and Decompress functions is to be able to discard some low-order
 *  bits in the ciphertext which do not have much effect on the correctness probability of decryption – thus 
 *  reducing the size of ciphertexts.
 *  
 *  > The Compressq and Decompressq are also used for a purpose other than compression – namely to perform 
 *  the usual LWE error correction during encryption and decryption
 * 
 */

#include "CompressorUnit.h"

/**
 * @brief This method compresses a matrix of polynomials by compressing each polynomial
 * 
 * @param matrix : Matrix of polynomials to compress
 * @param bits_per_coefficient : Number of bits per coefficient representation
 * @return Matrix<Polynomial<int>> 
 */
Matrix<Polynomial<int>> CompressorUnit::CompressMatrix(const Matrix<Polynomial<int>>& matrix, int bits_per_coefficient) const {
  const int rows = matrix.GetRowsSize();
  const int columns = matrix.GetColumnsSize();
  Matrix<Polynomial<int>> compressed_matrix(rows, columns);
  for (int i = 0; i < rows; ++i) {
    for (int j = 0; j < columns; ++j) {
      compressed_matrix(i, j) = Compress_(matrix(i, j), bits_per_coefficient);
    }
  }
  return compressed_matrix;
}

/**
 * @brief This method decompresses a matrix of polynomials by decompressing each polynomial
 * 
 * @param matrix : Matrix of polynomials to decompress
 * @param bits_per_coefficient : Number of bits per coefficient representation
 * @return Matrix<Polynomial<int>> 
 */
Matrix<Polynomial<int>> CompressorUnit::DecompressMatrix(const Matrix<Polynomial<int>>& matrix, int bits_per_coefficient) const {
  const int rows = matrix.GetRowsSize();
  const int columns = matrix.GetColumnsSize();
  Matrix<Polynomial<int>> decompressed_matrix(rows, columns, matrix.GetSizeElements());
  for (int i = 0; i < rows; ++i) {
    for (int j = 0; j < columns; ++j) {
      decompressed_matrix(i, j) = Decompress_(matrix(i, j), bits_per_coefficient);
    }
  }
  return decompressed_matrix;
}


/**
 * @brief This method compresses a polynomial by multiplying each coefficient by a number
 *   > Takes an element x ∈ Zq and outputs an integer in {0, . . . , 2^bits_per_coefficient − 1}, where bits_per_coefficient < [log2(q)].
 * @param polynomial : Polynomial to compress
 * @param bits_per_coefficient : Number of bits per coefficient representation
 * @return Polynomial<int> 
 */
Polynomial<int> CompressorUnit::Compress_(const Polynomial<int>& polynomial, int bits_per_coefficient) const {
  const double compressed_coefficient = static_cast<double>(1 << bits_per_coefficient);
  Polynomial<int> compressed_polynomial(polynomial.GetSize());
  const double factor = compressed_coefficient / q_;
  for (int i = 0; i < polynomial.GetSize(); ++i) {
    compressed_polynomial[i] = static_cast<int>(_round_up(polynomial[i] * factor)) % static_cast<int>(compressed_coefficient);
  }
  return compressed_polynomial;
}

/**
 * @brief This method decompresses a polynomial by dividing each coefficient by a number
 * 
 * @param polynomial : Polynomial to decompress
 * @param bits_per_coefficient : Number of bits per coefficient representation
 * @return Polynomial<int> 
 */
Polynomial<int> CompressorUnit::Decompress_(const Polynomial<int>& polynomial, int bits_per_coefficient) const {
  const double factor = q_ / static_cast<double>(1 << bits_per_coefficient);
  Polynomial<int> decompressed_polynomial(polynomial.GetSize());
  for (int i = 0; i < polynomial.GetSize(); ++i) {
    decompressed_polynomial[i] = static_cast<int>(_round_up(polynomial[i] * factor));
  }
  return decompressed_polynomial;
}



