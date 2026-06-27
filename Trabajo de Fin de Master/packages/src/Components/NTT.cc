/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date July 18 2024
 * @version v0.1.0
 * @brief This file contains the header declaration of the class NTT, which give us 
 * The Number Theorical Transformation.
 */

#include "NTT.h"
#include "Keccak.h"

/**
 * @brief This method applies the Number Theorical Transformation to a polynomial.
 * 
 * @param kPolynomial : The polynomial to apply the NTT.
 * @return Polynomial<int> 
 */
Polynomial<int> NTT::NTT_Kyber(const Polynomial<int>& kPolynomial, bool is_ntt) const{
  Polynomial<int> polynomial_copy = kPolynomial;
  if (kPolynomial.GetSize() != n_) {
    Polynomial<int> auxiliar(n_ - kPolynomial.GetSize());
    for (const auto& element : auxiliar.GetCoefficients()) {
      polynomial_copy.append(element);
    }
  }
  // Initialize polynomials
  Polynomial<int> even_coefficients = Polynomial<int>(0);
  Polynomial<int> odd_coefficients = Polynomial<int>(0);
  for (int i = 0; i < polynomial_copy.GetSize(); ++i) {
    (i % 2 == 0) ? even_coefficients.append(polynomial_copy[i]) : odd_coefficients.append(polynomial_copy[i]);
  }
  Polynomial<int> even_coefficients_ntt = is_ntt ? NTT_(even_coefficients) : INTT_(even_coefficients);
  Polynomial<int> odd_coefficients_ntt = is_ntt ? NTT_(odd_coefficients) : INTT_(odd_coefficients);
  // Merging results
  Polynomial<int> result = Polynomial<int>(0);
  for (int i = 0; i < n_; ++i) {
    (i % 2 == 0) ? result.append(even_coefficients_ntt[i / 2]) : result.append(odd_coefficients_ntt[(i - 1) / 2]);
  }
  return result;
}

/**
 * @brief This method applies the Number Theorical Transformation to a polynomial.
 * 
 * @param kPolynomial : The polynomial to apply the NTT.
 * @return Polynomial<int> 
 */
Polynomial<int> NTT::NTT_(const Polynomial<int>& kPolynomial) const {
  int n = kPolynomial.GetSize();
  int mid_index = n / 2;
  // Calculate the first primitive root
  int phi = (n == MODULUS_3329) ? FIRST_PRIMITIVE_ROOT_3329 : FirstPrimitiveRoot_(2 * n);
  Polynomial<int> result = kPolynomial;
  // Iterating over the polynomial - First we chunk the polynomial in sizes of 2 * i
  for (int i = 1; i < n; i *= 2) {
    // Iterating over the chunk that we've created
    for (int j = 0; j < i; ++j) {
      int left_interval = 2 * j * mid_index; 
      int right_interval = left_interval + mid_index - 1;
      int S = PowerWithMod_(phi, BitReverse_(i + j, n), q_);
      for (int k = left_interval; k <= right_interval; ++k) {
        int temp_element = result[k];
        int temp_mirror_element = result[k + mid_index];
        result[k] = (temp_element + temp_mirror_element * S) % q_;
        result[k + mid_index] = (temp_element - temp_mirror_element * S) % q_;
        while (result[k] < 0) { result[k] += q_; }
        while (result[k + mid_index] < 0) { result[k + mid_index] += q_; }
      }
    }
    mid_index /= 2;
  }
  return result;
}

/**
 * @brief This method applies the Inverse Number Theorical Transformation to a polynomial.
 * 
 * @param kPolynomial : The polynomial to apply the INTT.
 * @return Polynomial<int> 
 */
Polynomial<int> NTT::INTT_(const Polynomial<int>& kPolynomial) const {
  int n = kPolynomial.GetSize();
  int phi = (n == MODULUS_3329) ? FIRST_PRIMITIVE_ROOT_3329 : FirstPrimitiveRoot_(2 * n);
  int phi_inverse = PowerWithMod_(phi, 2 * n - 1, q_);
  Polynomial<int> result = kPolynomial;
  int mid_index = n / 2;
  int k = 1;
  while (mid_index > 0) {
    for (int i = 0; i < mid_index; ++i) {
      int left_interval = 2 * i * k;
      int right_interval = left_interval + k - 1;
      int S = PowerWithMod_(phi_inverse, BitReverse_(mid_index + i, n), q_);
      for (int j = left_interval; j <= right_interval; ++j) {
        int temp_element = result[j];
        int temp_mirror_element = result[j + k];
        result[j] = (temp_element + temp_mirror_element) % q_;
        result[j + k] = ((temp_element - temp_mirror_element) * S) % q_;
        while (result[j] < 0) { result[j] += q_; }
        while (result[j + k] < 0) { result[j + k] += q_; }
      }      
    }
    mid_index /= 2;
    k *= 2;
  }
  // Every element in Zq pow q - 2 = inverse of n.
  int n_inverse = PowerWithMod_(n, q_ - 2 , q_);
  for (int i = 0; i < n; ++i) {
    result[i] = (result[i] * n_inverse) % q_; 
  }
  return result;
}

/**
 * @brief This method applies the BitReverse algorithm to a number.
 * 
 * @param element : The element to apply the BitReverse.
 * @param length_of_sequence : The length of the sequence.
 * @return int 
 */
int NTT::BitReverse_(int element, int length_of_sequence) const {
  int reversed = 0;
  while (length_of_sequence > 1) {
    reversed = (reversed << 1) | (element & 1);
    element >>= 1;
    length_of_sequence >>= 1;
  }
  return reversed;
}


  /**
   * This method finds the first primitive root of n. 
   * That means, the first number which every power of it is different from 1, with prime q.
   * 
   * @param n The number to find the primitive root.
   * @return The first primitive root of n.
   */
int NTT::FirstPrimitiveRoot_(int n) const {
  // Checking conditions
  if (!_IsPrime(q_)) {
    throw std::invalid_argument("Q must be a prime number.");
  }
  if ((q_ - 1) % n != 0) {
    throw std::invalid_argument("n must divide q - 1.");
  }
  // We are going to check if base^n = 1 and base to another lower power is not 1.
  for (int base = 2; base < q_; base++) {
    bool is_primitive_root = true;
    // Check if base is a primitive root
    for (int i = 1; i < n; ++i) {
      if (PowerWithMod_(base, i, q_) == 1) {
        is_primitive_root = false;
        break;
      }
    }
    if (is_primitive_root && PowerWithMod_(base, n, q_) == 1) {
      return base;
    }
  }
  throw std::invalid_argument("Primitive root not found.");
}


/**
 * This method calculates the power of a number with a module.
 * 
 * @param base The base number.
 * @param exp The exponent.
 * @param mod The module.
 * @return The result of the power with module.
 */
int NTT::PowerWithMod_(int base, int exp, int mod) const {
  int result = 1;
  base = base % mod;
  while (exp > 0) {
    if (exp % 2 == 1) {
      result = (result * base) % mod;
    }
    exp = exp >> 1;
    base = (base * base) % mod;
    while (base < 0) {
      base += mod;
    }
  }
  return result;
}


/**
 * This method checks if a number is prime.
 * 
 * @param n The number to check.
 * @return True if the number is prime, false otherwise.
 */
bool NTT::_IsPrime(int n) const {
  if (n <= 1) {
    return false;
  }
  if (n <= 3) {
    return true;
  }
  if (n % 2 == 0 || n % 3 == 0) {
    return false;
  }
  for (int i = 5; i * i <= n; i += 6) {
    if (n % i == 0 || n % (i + 2) == 0) {
      return false;
    }
  }
  return true;
}

/**
 * This method generates the matrix for the NTT.
 * 
 * @param k_size The size of the matrix.
 * @param rho The rho value.
 * @param traspose
 * @return The generated matrix.
 */
Matrix<Polynomial<int>> NTT::GenerateMatrix_(int k_size, Bytes rho, bool traspose) const {
  Matrix<Polynomial<int>> matrix(k_size, k_size, n_);
  for (int i = 0; i < k_size; ++i) {
    for (int j = 0; j < k_size; ++j) {
      Bytes generated_bytes = Keccak::XOF(rho, Bytes(traspose ? j : i), Bytes(traspose ? i : j), 3 * n_);
      matrix(i, j) = ParsePolynomial_(generated_bytes);
    }
  }
  return matrix;
}

/**
 * This method parses a byte array into a polynomial.
 * 
 * @param kBytes The byte array.
 * @return The parsed polynomial.
 */
Polynomial<int> NTT::ParsePolynomial_(const Bytes& kBytes) const {
  Polynomial<int> polynomial(n_);
  int byte_index = 0;
  int list_index = 0;
  int size_bytes = kBytes.GetBytesSize();
  while ((list_index < n_) && (byte_index + 3 < size_bytes)) {
    int byte1 = kBytes[byte_index] + n_ * (kBytes[byte_index + 1] % 16);
    int byte2 = int(kBytes[byte_index + 1] / 16) + 16 * kBytes[byte_index + 2];
    if (byte1 < q_) {
      polynomial[list_index] = byte1;
      list_index++;
    }
    if (byte2 < q_ && list_index < n_) {
      polynomial[list_index] = byte2;
      list_index++;
    }
    byte_index += 3;
  }
  return polynomial;
}


/**
 * @brief This method multiplies two matrices using pointwise multiplication in NTT
 * 
 * @param A The first matrix
 * @param B The second matrix
 * @return Matrix<Polynomial<int>> 
 */
Matrix<Polynomial<int>> NTT::multMatrixViaNTT(const Matrix<Polynomial<int>>& A, const Matrix<Polynomial<int>>& B) {
  int rowsA = A.GetRowsSize();
  int columnsA = A.GetColumnsSize();
  int columnsB = B.GetColumnsSize();
  Matrix<Polynomial<int>> result(rowsA, columnsB);
  for (int i = 0; i < rowsA; ++i) {
    for (int j = 0; j < columnsB; ++j) {
      Polynomial<int> sum_polynomial = Polynomial<int>(n_);
        for (int k = 0; k < columnsA; ++k) {
          sum_polynomial = sum_polynomial + pointwise_(A(i, k), B(k, j));
        }
      result(i, j) = sum_polynomial;
    }
  }
  return result;
}

/**
 * @brief This function performs the pointwise multiplication of two polynomials
 * 
 * @param p : polynomial p
 * @param g : polynomial g
 * @return Polynomial<int> 
 */
Polynomial<int> NTT::pointwise_(const Polynomial<int>& p, const Polynomial<int>& g) {
  int phi = FirstPrimitiveRoot_(n_);
  // If the degree of any polynomial is less than 256 we apply NTT 
  // we fill with zeros since it does not change the polynomial.
  Polynomial<int> p_filled = p;
  Polynomial<int> g_filled = g;
  if (p.GetSize() < n_) {
    p_filled = p_filled.ReturnAppend(Polynomial<int>(n_ - p.GetSize()));
  } 
  if (g.GetSize() < n_) {
    g_filled = g_filled.ReturnAppend(Polynomial<int>(n_ - g.GetSize()));
  }
  Polynomial<int> p_odd(0);
  Polynomial<int> p_even(0);
  Polynomial<int> g_odd(0);
  Polynomial<int> g_even(0);
  for (int i = 0; i < n_; i++) {
    if (i % 2 == 0) {
      p_even.append(p_filled[i]);
      g_even.append(g_filled[i]);
    } else {
      p_odd.append(p_filled[i]);
      g_odd.append(g_filled[i]);
    }
  }
  Polynomial<int> p_wise_mult_even = Polynomial<int>(0);
  Polynomial<int> p_wise_mult_odd = Polynomial<int>(0);
  for (int i = 0; i < n_; ++i) {
    int left = 0;
    int right = 0;
    if (i % 2 == 0) {
      int mid = i / 2;
      int left = (p_even[mid] * g_even[mid]) % q_;
      int phi_power = PowerWithMod_(phi, 2 * BitReverse_(mid, n_ / 2) + 1, q_);
      int right = (p_odd[mid] * g_odd[mid]) % q_;
      right = (right * phi_power) % q_;
      p_wise_mult_even.append((left + right) % q_);
    } else {
      int mid = (i - 1) / 2;
      int left = (p_even[mid] * g_odd[mid]) % q_;
      int right = (p_odd[mid] * g_even[mid]) % q_;
      p_wise_mult_odd.append((left + right) % q_);
    }
  }
  Polynomial<int> result = Polynomial<int>(n_);
  for (int i = 0; i < n_; i++) {
    result[i] = (i % 2 == 0) ? p_wise_mult_even[i / 2] : p_wise_mult_odd[(i - 1) / 2];
  }
  return result;
}


