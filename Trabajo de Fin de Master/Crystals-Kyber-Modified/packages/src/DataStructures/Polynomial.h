/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date July 18 2024
 * @version v0.1.0
 * @brief This file contains the header declaration and implementation of the class Polynomial
 */

#include <vector>
#include <assert.h>
#include <iostream>


#pragma once

template <typename T>
class Polynomial {
 public:
  Polynomial(const unsigned int& kSize, const int& module = 3329) : vector_(kSize, 0), module_(module) {}
  void append(const T& kElement) { vector_.push_back(kElement); }
  Polynomial<T> ReturnAppend(const Polynomial<T>& kElements); 

  // Getters
  int GetSize() const { return vector_.size(); }
  std::vector<T> GetCoefficients() const { return vector_; }
  Polynomial<T> GetSubPolynomial(const int& kStart, const int& kEnd) const;

  // Setters
  void SetCoefficients(const std::vector<T>& kCoefficients) { vector_ = kCoefficients; }
  // Operator overload
  const T& operator[](int index) const;
  T& operator[](int index);
  Polynomial<T> operator+(const Polynomial<T>& kPolynomial2) const;
  Polynomial<T> operator-(const Polynomial<T>& kPolynomial2) const;
  bool operator==(const Polynomial<T>& kPolynomial2) const { return vector_ == kPolynomial2.vector_; }
  bool operator!=(const Polynomial<T>& kPolynomial2) const { return !(*this == kPolynomial2); }
  
  template <typename B> friend std::ostream& operator<<(std::ostream& os, const Polynomial<B> kPolynomial);
 private:
  std::vector<T> vector_;
  int module_;
};

/**
 * @brief Operator overload for the << operator
 * 
 * @tparam T : Template type parameter
 * @param os : The output stream
 * @param kPolynomial : The polynomial to be printed
 * @return std::ostream& 
 */
template <typename T>
std::ostream& operator<<(std::ostream& os, const Polynomial<T> kPolynomial) {
  os << "[";
  for (size_t i = 0; i < kPolynomial.vector_.size(); ++i) {
    os << kPolynomial.vector_[i];
    if (i != kPolynomial.vector_.size() - 1) os << ", ";
  }
  os << "]";
  return os;
}

/**
 * @brief This method returns the element at the given index - A const version
 * 
 * @tparam T : Template type parameter
 * @param index : The index of the element to be returned
 * @return const T& 
 */
template <typename T> 
const T& Polynomial<T>::operator[](int index) const {
  assert(index < vector_.size());
  return vector_[index];
}

/**
 * @brief This method returns the element at the given index
 * 
 * @tparam T : Template type parameter
 * @param index : The index of the element to be returned
 * @return T& 
 */
template <typename T> 
T& Polynomial<T>::operator[](int index) {
  assert(index < GetSize());
  return vector_[index];
}

/**
 * @brief This method adds two polynomials
 * 
 * @tparam T : Template type parameter
 * @param kPolynomial2 : The polynomial to be added
 * @return Polynomial<T> 
 */
template <typename T>
Polynomial<T> Polynomial<T>::operator+(const Polynomial<T>& kPolynomial2) const {
  assert(vector_.size() == kPolynomial2.vector_.size());
  Polynomial<T> result(vector_.size(), module_);
  for (size_t i = 0; i < vector_.size(); ++i) {
    result[i] = (vector_[i] + kPolynomial2.vector_[i]) % module_;
    if (result[i] < 0) result[i] += module_;
  }
  return result;
}

/**
 * @brief This method subtracts two polynomials
 * 
 * @tparam T : Template type parameter
 * @param kPolynomial2 : The polynomial to be subtracted
 * @return Polynomial<T> 
 */
template <typename T>
Polynomial<T> Polynomial<T>::operator-(const Polynomial<T>& kPolynomial2) const {
  assert(vector_.size() == kPolynomial2.vector_.size());
  Polynomial<T> result(vector_.size(), module_);
  for (size_t i = 0; i < vector_.size(); ++i) {
    result[i] = vector_[i] - kPolynomial2.vector_[i];
    while (result[i] < 0) result[i] += module_;
  }
  return result;
}

/**
 * @brief This method appends a polynomial to another
 * 
 * @tparam T : Template type parameter
 * @param kElements : The polynomial to be appended
 * @return Polynomial<T> 
 */
template <typename T>
Polynomial<T> Polynomial<T>::ReturnAppend(const Polynomial<T>& kElements) {
  Polynomial<T> result(vector_.size() + kElements.vector_.size(), module_);
  std::copy(vector_.begin(), vector_.end(), result.vector_.begin());
  std::copy(kElements.vector_.begin(), kElements.vector_.end(), result.vector_.begin() + vector_.size());
  return result;
}

/**
 * @brief This method returns a subpolynomial
 * @param kStart : The start index
 * @param kEnd : The end index
 * @return Polynomial<T>
 */
template <typename T>
Polynomial<T> Polynomial<T>::GetSubPolynomial(const int& kStart, const int& kEnd) const {
  Polynomial<T> result(0, module_);
  for (int i = kStart; i < kEnd; ++i) {
    result.append(vector_[i]);
  }
  return result;
}